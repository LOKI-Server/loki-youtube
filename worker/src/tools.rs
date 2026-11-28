//! Finding and running yt-dlp, ffmpeg and Deno, and keeping yt-dlp up to date.
use crate::settings::Settings;
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::process::Command;

/// How long the newest yt-dlp release is remembered before asking GitHub again.
const LATEST_FOR: Duration = Duration::from_secs(6 * 3600);

#[derive(Clone, Debug, Serialize)]
pub struct Tool {
    pub path: String,
    pub version: String,
}

#[derive(Debug, Serialize)]
pub struct Status {
    pub yt_dlp: Option<Tool>,
    /// The newest yt-dlp release, when GitHub could be reached.
    pub yt_dlp_latest: Option<String>,
    pub update_available: bool,
    pub ffmpeg: Option<Tool>,
    /// yt-dlp needs a JavaScript runtime for YouTube; it finds Deno on PATH.
    pub deno: Option<Tool>,
    /// The last update, or the one running now.
    pub update: Option<Update>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Update {
    pub running: bool,
    pub ok: bool,
    pub output: String,
}

#[derive(Default)]
pub struct Tools {
    latest: Mutex<Option<(Instant, String)>>,
    update: Mutex<Option<Update>>,
}

impl Tools {
    pub async fn status(&self, settings: &Settings) -> Status {
        let (yt_dlp, ffmpeg, deno) = tokio::join!(
            describe(yt_dlp(settings), &["--version"]),
            describe(ffmpeg(settings), &["-version"]),
            describe(find("deno", None), &["--version"]),
        );
        let latest = if yt_dlp.is_some() {
            self.latest().await
        } else {
            None
        };
        let update_available = match (&yt_dlp, &latest) {
            (Some(tool), Some(latest)) => newer(latest, &tool.version),
            _ => false,
        };
        let update = self.update.lock().unwrap().clone();
        Status {
            yt_dlp,
            yt_dlp_latest: latest,
            update_available,
            ffmpeg,
            deno,
            update,
        }
    }

    /// Starts yt-dlp's own updater in the background. False when one is running.
    pub fn start_update(self: &std::sync::Arc<Self>, settings: Settings) -> bool {
        {
            let mut update = self.update.lock().unwrap();
            if update.as_ref().is_some_and(|update| update.running) {
                return false;
            }
            *update = Some(Update {
                running: true,
                ok: false,
                output: String::new(),
            });
        }
        let tools = self.clone();
        tokio::spawn(async move {
            let (ok, output) = tools.update(&settings).await;
            *tools.update.lock().unwrap() = Some(Update {
                running: false,
                ok,
                output,
            });
        });
        true
    }

    /// Runs yt-dlp's own updater and reports what it said.
    async fn update(&self, settings: &Settings) -> (bool, String) {
        let Some(path) = yt_dlp(settings) else {
            return (false, "yt-dlp is not installed on the server.".into());
        };
        self.latest.lock().unwrap().take();
        match run(&path, &["-U"], Duration::from_secs(180)).await {
            Some((ok, stdout, stderr)) => {
                let text = format!("{stdout}\n{stderr}");
                let lines: Vec<_> = text
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(|line| line.strip_prefix("ERROR: ").unwrap_or(line))
                    .collect();
                (ok, lines.join("\n").chars().take(800).collect())
            }
            None => (
                false,
                "yt-dlp did not finish updating within three minutes.".into(),
            ),
        }
    }

    async fn latest(&self) -> Option<String> {
        if let Some((at, version)) = self.latest.lock().unwrap().as_ref()
            && at.elapsed() < LATEST_FOR
        {
            return Some(version.clone());
        }
        #[derive(serde::Deserialize)]
        struct Release {
            tag_name: String,
        }
        let release: Release = reqwest::Client::builder()
            .user_agent(concat!(
                "media-downloader-worker/",
                env!("CARGO_PKG_VERSION")
            ))
            .timeout(Duration::from_secs(8))
            .build()
            .ok()?
            .get("https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest")
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .json()
            .await
            .ok()?;
        let version = release.tag_name.trim_start_matches('v').to_owned();
        *self.latest.lock().unwrap() = Some((Instant::now(), version.clone()));
        Some(version)
    }
}

pub fn yt_dlp(settings: &Settings) -> Option<PathBuf> {
    find("yt-dlp", settings.yt_dlp.as_deref())
}

pub fn ffmpeg(settings: &Settings) -> Option<PathBuf> {
    find("ffmpeg", settings.ffmpeg.as_deref())
}

/// A configured path, or the first match on PATH, or one beside the worker.
fn find(name: &str, configured: Option<&Path>) -> Option<PathBuf> {
    if let Some(path) = configured {
        return path.is_file().then(|| path.to_owned());
    }
    let names: Vec<String> = if cfg!(windows) {
        vec![
            format!("{name}.exe"),
            format!("{name}.cmd"),
            name.to_owned(),
        ]
    } else {
        vec![name.to_owned()]
    };
    let beside = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_owned));
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path)
        .chain(beside)
        .flat_map(|dir| names.iter().map(move |name| dir.join(name)))
        .find(|candidate| candidate.is_file())
}

async fn describe(path: Option<PathBuf>, args: &[&str]) -> Option<Tool> {
    let path = path?;
    let (_, stdout, _) = run(&path, args, Duration::from_secs(15)).await?;
    let first = stdout.lines().next().unwrap_or_default().trim();
    // `2025.11.12`, `ffmpeg version 7.1-full_build ...`, `deno 2.5.6 (stable ...)`
    let version = first
        .strip_prefix("ffmpeg version ")
        .or_else(|| first.strip_prefix("deno "))
        .unwrap_or(first)
        .split_whitespace()
        .next()
        .unwrap_or("unknown")
        .to_owned();
    Some(Tool {
        path: path.display().to_string(),
        version,
    })
}

/// Runs a tool to completion and returns whether it succeeded, with its output.
pub async fn run(path: &Path, args: &[&str], limit: Duration) -> Option<(bool, String, String)> {
    let mut command = command(path);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = tokio::time::timeout(limit, command.output())
        .await
        .ok()?
        .ok()?;
    Some((
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    ))
}

/// A command that never opens a console window and dies with its handle.
pub fn command(path: &Path) -> Command {
    let mut command = Command::new(path);
    command.kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    command
}

/// Whether release `latest` is newer than the installed `current`. yt-dlp versions
/// are dates, such as `2025.11.12`, and nightlies add a fourth part.
pub fn newer(latest: &str, current: &str) -> bool {
    let parts = |version: &str| -> Vec<u64> {
        version
            .split('.')
            .map(|part| part.parse().unwrap_or(0))
            .collect()
    };
    let (latest, current) = (parts(latest), parts(current));
    latest[..latest.len().min(3)] > current[..current.len().min(3)]
}

#[cfg(test)]
mod tests {
    use super::newer;

    #[test]
    fn release_dates_compare_by_part_and_nightlies_count_as_current() {
        assert!(newer("2025.11.12", "2025.10.22"));
        assert!(newer("2025.11.2", "2025.10.30"));
        assert!(!newer("2025.11.12", "2025.11.12"));
        assert!(!newer("2025.11.12", "2025.11.12.232505"));
        assert!(!newer("2025.10.22", "2025.11.12"));
    }
}
