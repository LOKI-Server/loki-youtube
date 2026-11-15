mod youtube;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use clap::{Parser, Subcommand};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

const ADDRESS: &str = "127.0.0.1:44317";
const DEFAULT_LIMITS: &str = "limits.json";

#[derive(Parser)]
struct Cli {
    #[arg(long, env = "LOKI_YOUTUBE_DATA")]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve the LOKI app on a loopback-only port.
    Serve {
        #[arg(long, env = "LOKI_YOUTUBE_KEY_FILE")]
        key_file: PathBuf,
        /// Read download limits from a JSON file.
        #[arg(long)]
        limits: Option<PathBuf>,
    },
    /// Check the sandbox and optionally inspect a video without saving it.
    Check {
        #[arg(long)]
        video_url: Option<String>,
    },
    /// Generate a random key file for this worker and its LOKI app.
    CreateKey { path: PathBuf },
    #[cfg(windows)]
    WorkerSid,
}

#[derive(Clone)]
struct State(Arc<Inner>);

struct Inner {
    data: PathBuf,
    db: sqlx::SqlitePool,
    settings: youtube::Settings,
    youtube: youtube::Jobs,
    youtube_workers: Arc<Semaphore>,
    key: String,
    stopping: AtomicBool,
    rates: Mutex<HashMap<(&'static str, String), (Instant, u32)>>,
}

impl State {
    async fn open(data: PathBuf, settings: youtube::Settings, key: String) -> anyhow::Result<Self> {
        let db = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(data.join("history.db"))
                    .create_if_missing(true)
                    .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                    .busy_timeout(Duration::from_secs(5)),
            )
            .await?;
        sqlx::raw_sql(include_str!("schema.sql"))
            .execute(&db)
            .await?;
        youtube::recover(&data, &db).await?;
        Ok(Self(Arc::new(Inner {
            data,
            db,
            settings,
            key,
            youtube: youtube::Jobs::default(),
            youtube_workers: Arc::new(Semaphore::new(2)),
            stopping: AtomicBool::new(false),
            rates: Mutex::new(HashMap::new()),
        })))
    }

    fn rate(&self, operation: &'static str, user: &str) -> Result<()> {
        let now = Instant::now();
        let mut rates = self.0.rates.lock().unwrap();
        rates.retain(|_, (until, _)| *until > now);
        let key = (operation, user.to_owned());
        if rates.len() >= 2048 && !rates.contains_key(&key) {
            return Err(youtube::capacity_error());
        }
        let (_, count) = rates
            .entry(key)
            .or_insert((now + Duration::from_secs(600), 0));
        if *count >= 12 {
            return Err(Error::new(
                StatusCode::TOO_MANY_REQUESTS,
                "Too many requests. Try again shortly.",
            ));
        }
        *count += 1;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    match cli.command {
        Command::CreateKey { path } => {
            write_private(
                &path,
                format!("{}{}\n", id().replace('-', ""), id().replace('-', "")).as_bytes(),
            )?;
            println!("Worker key saved to {}", path.display());
        }
        #[cfg(windows)]
        Command::WorkerSid => println!("{}", youtube::worker_sid()?),
        Command::Check { video_url } => {
            let data = prepare(cli.data_dir)?;
            let _lock = lock(&data)?;
            let state = State::open(data, youtube::Settings::default(), String::new()).await?;
            if let Some(url) = video_url {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&youtube::inspect_worker(&state, &url).await?)?
                );
            } else {
                youtube::verify_worker(&state).await?;
                println!("YouTube worker isolation checks passed.");
            }
        }
        Command::Serve { key_file, limits } => {
            let data = prepare(cli.data_dir)?;
            let _lock = lock(&data)?;
            let key = std::fs::read_to_string(key_file)?.trim().to_owned();
            anyhow::ensure!(
                key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "Use a key made with create-key"
            );
            let limits = limits.unwrap_or_else(|| data.join(DEFAULT_LIMITS));
            let settings = match std::fs::read(&limits) {
                Ok(bytes) => serde_json::from_slice::<youtube::Settings>(&bytes)?,
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound
                        && limits == data.join(DEFAULT_LIMITS) =>
                {
                    youtube::Settings::default()
                }
                Err(error) => return Err(error.into()),
            };
            settings.validate()?;
            let state = State::open(data, settings, key).await?;
            youtube::verify_worker(&state).await?;
            let listener = tokio::net::TcpListener::bind(ADDRESS).await?;
            let cleaner = tokio::spawn(youtube::maintain(state.clone()));
            let shutdown = state.clone();
            println!("LOKI YouTube worker listening on {ADDRESS}");
            let result = axum::serve(listener, youtube::router(state))
                .with_graceful_shutdown(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    shutdown.0.stopping.store(true, Ordering::Release);
                    youtube::stop_transfers(&shutdown);
                })
                .await;
            cleaner.abort();
            result?;
        }
    }
    Ok(())
}

fn prepare(path: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    let path = path
        .or_else(|| {
            directories::ProjectDirs::from("org", "LOKI", "YouTubeWorker")
                .map(|dirs| dirs.data_local_dir().to_owned())
        })
        .ok_or_else(|| {
            anyhow::anyhow!("Specify a private worker data directory with --data-dir")
        })?;
    std::fs::create_dir_all(&path)?;
    let path = dunce::canonicalize(path)?;
    anyhow::ensure!(
        !path.join("loki.db").exists(),
        "Choose a separate data directory for the worker"
    );
    private_directory(&path)?;
    Ok(path)
}

fn lock(data: &Path) -> anyhow::Result<std::fs::File> {
    let lock = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(data.join("worker.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock)
        .map_err(|_| anyhow::anyhow!("A worker is already using this data directory"))?;
    Ok(lock)
}

fn private_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(windows)]
    let _ = path; // Use a directory inheriting the per-account ACL.
    Ok(())
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
struct Error {
    status: StatusCode,
    message: String,
}

impl Error {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
    fn bad(message: &str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }
    fn forbidden() -> Self {
        Self::new(StatusCode::FORBIDDEN, "Access denied.")
    }
    fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "Download not found.")
    }
}

impl<E> From<E> for Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(error: E) -> Self {
        tracing::warn!(%error, "worker request failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "The worker could not complete this request.",
        )
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({"error": self.message})),
        )
            .into_response()
    }
}
