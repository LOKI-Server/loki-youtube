//! The Media Downloader worker: a small loopback service that runs the server's own
//! yt-dlp (and ffmpeg, when installed) for the Media Downloader LOKI app.
mod jobs;
mod settings;
mod tools;
mod web;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use clap::{Parser, Subcommand};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// The address the LOKI app is allowed to reach. Keep it on loopback.
pub const ADDRESS: &str = "127.0.0.1:44317";
const KEY_FILE: &str = "worker-key.txt";

#[derive(Parser)]
#[command(version, about = "Runs yt-dlp for the Media Downloader LOKI app.")]
struct Cli {
    /// Where the worker keeps its key, settings and in-progress downloads.
    #[arg(long, env = "MEDIA_DOWNLOADER_DATA")]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the worker (the default).
    Serve,
    /// Print the key to paste into the app's panel in LOKI.
    Key,
    /// Replace the key. The app needs the new one before it can connect again.
    NewKey,
    /// Show which yt-dlp, ffmpeg and Deno the worker would use.
    Check,
}

#[derive(Clone)]
pub struct State(Arc<Inner>);

pub struct Inner {
    pub data: PathBuf,
    pub key: String,
    pub settings: settings::Store,
    pub jobs: jobs::Jobs,
    pub tools: Arc<tools::Tools>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let data = data_dir(cli.data_dir)?;
    match cli.command.unwrap_or(Command::Serve) {
        Command::Key => println!("{}", key(&data, false)?),
        Command::NewKey => {
            let key = key(&data, true)?;
            println!("{key}\n\nPaste this key into the Media Downloader panel in LOKI, then restart the worker.");
        }
        Command::Check => {
            let settings = settings::Store::open(&data)?;
            let status = tools::Tools::default().status(&settings.get()).await;
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        Command::Serve => serve(data).await?,
    }
    Ok(())
}

async fn serve(data: PathBuf) -> anyhow::Result<()> {
    let _lock = lock(&data)?;
    let key = key(&data, false)?;
    let settings = settings::Store::open(&data)?;
    let state = State(Arc::new(Inner {
        jobs: jobs::Jobs::open(&data)?,
        tools: Arc::default(),
        settings,
        key: key.clone(),
        data,
    }));
    let status = state.0.tools.status(&state.0.settings.get()).await;
    let listener = tokio::net::TcpListener::bind(ADDRESS)
        .await
        .map_err(|error| anyhow::anyhow!("Could not listen on {ADDRESS}: {error}. Is another worker already running?"))?;
    println!("Media Downloader worker {} is listening on {ADDRESS}.", env!("CARGO_PKG_VERSION"));
    match &status.yt_dlp {
        Some(tool) => println!("yt-dlp {} at {}", tool.version, tool.path),
        None => println!("yt-dlp was not found. Install it, or set its path in {}.", settings::FILE),
    }
    match &status.ffmpeg {
        Some(tool) => println!("ffmpeg {}", tool.version),
        None => println!("ffmpeg was not found, so only files that already have sound and picture together are offered."),
    }
    println!("\nWorker key (paste it into the Media Downloader panel in LOKI):\n{key}\n");
    let cleaner = tokio::spawn(jobs::maintain(state.clone()));
    let stopping = state.clone();
    axum::serve(listener, web::router(state))
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            stopping.0.jobs.cancel_all();
        })
        .await?;
    cleaner.abort();
    Ok(())
}

fn data_dir(path: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    let path = path
        .or_else(|| {
            directories::ProjectDirs::from("org", "LOKI", "MediaDownloader")
                .map(|dirs| dirs.data_local_dir().to_owned())
        })
        .ok_or_else(|| anyhow::anyhow!("Choose a data directory with --data-dir"))?;
    std::fs::create_dir_all(&path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(path)
}

/// One worker per data directory, so two never share a spool.
fn lock(data: &Path) -> anyhow::Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(data.join("worker.lock"))?;
    file.try_lock()
        .map_err(|_| anyhow::anyhow!("Another worker is already using {}", data.display()))?;
    Ok(file)
}

/// The shared key, made on first use. `replace` makes a new one.
fn key(data: &Path, replace: bool) -> anyhow::Result<String> {
    let path = data.join(KEY_FILE);
    if !replace && let Ok(text) = std::fs::read_to_string(&path) {
        let key = text.trim().to_owned();
        if valid_key(&key) {
            return Ok(key);
        }
    }
    let key = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
    write_private(&path, format!("{key}\n").as_bytes())?;
    Ok(key)
}

pub fn valid_key(key: &str) -> bool {
    key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

pub type Result<T> = std::result::Result<T, Error>;

/// An answer the app shows people as it is, so messages are written for them.
#[derive(Debug)]
pub struct Error {
    pub status: StatusCode,
    pub message: String,
}

impl Error {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self { status, message: message.into() }
    }
    pub fn bad(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }
    pub fn busy(message: impl Into<String>) -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, message)
    }
    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "That download is no longer on the worker.")
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for Error {
    fn from(error: E) -> Self {
        tracing::warn!(%error, "request failed");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "The worker could not finish this request.")
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.status, Json(serde_json::json!({ "error": self.message }))).into_response()
    }
}
