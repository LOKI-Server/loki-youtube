//! Looking up a link with yt-dlp, and downloads in progress.
//!
//! Each download runs yt-dlp in its own folder under `jobs/`. The app collects the
//! finished file and then deletes the job, so the worker only ever holds downloads
//! that are in progress or waiting to be collected.
use crate::{Error, Result, State, settings::Settings, tools};
use axum::http::StatusCode;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap},
    net::IpAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::sync::CancellationToken;

/// Finished downloads nobody collects are removed after this long.
const UNCOLLECTED: Duration = Duration::from_secs(6 * 3600);
/// Failed and cancelled downloads are forgotten after this long.
const FORGET: Duration = Duration::from_secs(3600);
const MIB: u64 = 1024 * 1024;

pub struct Jobs {
    root: PathBuf,
    live: Mutex<HashMap<String, Job>>,
    /// Held while a download is checked against the limits and started, so two
    /// requests at once can't both squeeze under them.
    starting: tokio::sync::Mutex<()>,
}

struct Job {
    status: Status,
    percent: Option<f32>,
    error: Option<String>,
    file: Option<PathBuf>,
    size: u64,
    /// Last time the app asked about it.
    touched: Instant,
    cancel: CancellationToken,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Downloading,
    /// Merging or converting after the download.
    Processing,
    Ready,
    Failed,
    Cancelled,
}

#[derive(Serialize)]
pub struct View {
    pub status: Status,
    pub percent: Option<f32>,
    pub size: u64,
    pub ext: Option<String>,
    pub error: Option<String>,
}

impl Jobs {
    /// Starts empty: anything left from before a restart is removed.
    pub fn open(data: &Path) -> anyhow::Result<Self> {
        let root = data.join("jobs");
        if root.exists() {
            std::fs::remove_dir_all(&root)?;
        }
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            live: Mutex::new(HashMap::new()),
            starting: tokio::sync::Mutex::new(()),
        })
    }

    pub fn view(&self, id: &str) -> Result<View> {
        let mut live = self.live.lock().unwrap();
        let job = live.get_mut(id).ok_or_else(Error::not_found)?;
        job.touched = Instant::now();
        Ok(View {
            status: job.status,
            percent: job.percent,
            size: job.size,
            ext: job
                .file
                .as_ref()
                .and_then(|file| file.extension())
                .map(|ext| ext.to_string_lossy().into_owned()),
            error: job.error.clone(),
        })
    }

    /// The finished file, for the app to collect.
    pub fn file(&self, id: &str) -> Result<(PathBuf, u64)> {
        let mut live = self.live.lock().unwrap();
        let job = live.get_mut(id).ok_or_else(Error::not_found)?;
        job.touched = Instant::now();
        match (&job.status, &job.file) {
            (Status::Ready, Some(file)) => Ok((file.clone(), job.size)),
            _ => Err(Error::new(
                StatusCode::CONFLICT,
                "That download is not finished yet.",
            )),
        }
    }

    /// Stops a download if it is running and deletes its files.
    pub fn remove(&self, id: &str) {
        if let Some(job) = self.live.lock().unwrap().remove(id) {
            job.cancel.cancel();
        }
        let _ = std::fs::remove_dir_all(self.root.join(id));
    }

    pub fn cancel_all(&self) {
        for job in self.live.lock().unwrap().values() {
            job.cancel.cancel();
        }
    }

    pub fn active(&self) -> usize {
        self.live
            .lock()
            .unwrap()
            .values()
            .filter(|job| matches!(job.status, Status::Downloading | Status::Processing))
            .count()
    }

    /// Bytes on disk for every download the worker holds.
    pub fn usage(&self) -> u64 {
        fn walk(path: &Path) -> u64 {
            std::fs::read_dir(path)
                .into_iter()
                .flatten()
                .flatten()
                .map(|entry| match entry.file_type() {
                    Ok(kind) if kind.is_dir() => walk(&entry.path()),
                    Ok(kind) if kind.is_file() => entry.metadata().map_or(0, |meta| meta.len()),
                    _ => 0,
                })
                .sum()
        }
        walk(&self.root)
    }

    fn update(&self, id: &str, change: impl FnOnce(&mut Job)) {
        if let Some(job) = self.live.lock().unwrap().get_mut(id) {
            change(job);
        }
    }
}

/// Removes downloads that were never collected, and forgets old failures.
pub async fn maintain(state: State) {
    let mut ticks = tokio::time::interval(Duration::from_secs(30));
    loop {
        ticks.tick().await;
        let stale: Vec<String> = state
            .0
            .jobs
            .live
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, job)| match job.status {
                Status::Ready => job.touched.elapsed() > UNCOLLECTED,
                Status::Failed | Status::Cancelled => job.touched.elapsed() > FORGET,
                _ => false,
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            state.0.jobs.remove(&id);
        }
    }
}

/// What a link offers: its title and the qualities to choose from.
pub async fn inspect(state: &State, raw: &str) -> Result<Value> {
    let url = check_url(raw).await?;
    let settings = state.0.settings.get();
    let yt_dlp = tools::yt_dlp(&settings).ok_or_else(missing)?;
    let merge = tools::ffmpeg(&settings).is_some();
    let mut command = tools::command(&yt_dlp);
    command
        .args(common_args(&state.0.data))
        .args(["--dump-single-json", "--", url.as_str()])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // LOKI gives the app 30 seconds for an answer, so the lookup has a little less.
    let output = tokio::time::timeout(Duration::from_secs(27), command.output())
        .await
        .map_err(|_| {
            Error::new(
                StatusCode::GATEWAY_TIMEOUT,
                "Looking up that link took too long. Try again.",
            )
        })??;
    if !output.status.success() {
        return Err(Error::bad(explain(&String::from_utf8_lossy(
            &output.stderr,
        ))));
    }
    let info: Value = serde_json::from_slice(&output.stdout)
        .map_err(|_| Error::bad("yt-dlp returned something unexpected for that link."))?;
    describe(&info, &settings, merge, url.as_str())
}

/// The part of [`inspect`] that reads yt-dlp's answer, kept apart for tests.
fn describe(info: &Value, settings: &Settings, merge: bool, asked: &str) -> Result<Value> {
    if matches!(info["_type"].as_str(), Some("playlist" | "multi_video")) {
        return Err(Error::bad(
            "That link is a playlist or channel. Paste a link to one video.",
        ));
    }
    if info["is_live"] == true
        || matches!(
            info["live_status"].as_str(),
            Some("is_live" | "is_upcoming")
        )
    {
        return Err(Error::bad(
            "Live streams and premieres can't be downloaded until they have ended.",
        ));
    }
    let duration = info["duration"].as_f64();
    if duration.is_some_and(|seconds| seconds > f64::from(settings.max_minutes) * 60.0) {
        return Err(Error::bad(format!(
            "That video is longer than this server allows ({} minutes).",
            settings.max_minutes
        )));
    }
    let choices = choices(info, merge);
    if choices.is_empty() {
        return Err(Error::bad(
            "No downloadable video or audio was found at that link.",
        ));
    }
    let text = |key: &str, limit: usize| -> String {
        info[key]
            .as_str()
            .unwrap_or_default()
            .chars()
            .filter(|c| !c.is_control())
            .take(limit)
            .collect()
    };
    let url = info["webpage_url"]
        .as_str()
        .filter(|url| url.starts_with("http"))
        .unwrap_or(asked);
    Ok(json!({
        "url": url,
        "title": Some(text("title", 300)).filter(|title| !title.is_empty()).unwrap_or_else(|| "Untitled".into()),
        "uploader": text("uploader", 120),
        "site": Some(text("webpage_url_domain", 80)).filter(|site| !site.is_empty()).unwrap_or_else(|| text("extractor_key", 60)),
        "duration": duration,
        "merge": merge,
        "choices": choices,
    }))
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Choice {
    pub id: String,
    pub label: String,
    /// An estimate in bytes, when the site gives sizes.
    pub size: Option<u64>,
}

/// The qualities to offer. With ffmpeg, video and audio are fetched separately and
/// merged, which is how sites like YouTube offer anything above 360p. Without it,
/// only formats that already have both are offered.
fn choices(info: &Value, merge: bool) -> Vec<Choice> {
    let empty = Vec::new();
    let formats = info["formats"].as_array().unwrap_or(&empty);
    let codec = |format: &Value, key: &str| format[key].as_str() != Some("none");
    let size = |format: &Value| {
        format["filesize"]
            .as_u64()
            .or_else(|| format["filesize_approx"].as_u64())
    };
    let height = |format: &Value| {
        format["height"]
            .as_u64()
            .filter(|height| (1..=8640).contains(height))
    };
    let video: Vec<&Value> = formats
        .iter()
        .filter(|f| codec(f, "vcodec") && height(f).is_some())
        .collect();
    let audio_only: Vec<&Value> = formats
        .iter()
        .filter(|f| !codec(f, "vcodec") && codec(f, "acodec"))
        .collect();
    let combined: Vec<&Value> = video
        .iter()
        .copied()
        .filter(|f| codec(f, "acodec"))
        .collect();
    let best_audio = audio_only.iter().filter_map(|f| size(f)).max();

    let mut out = Vec::new();
    if formats.is_empty() && info["url"].is_string() {
        out.push(Choice {
            id: "best".into(),
            label: "Best available".into(),
            size: size(info),
        });
        return out;
    }
    let sources = if merge { &video } else { &combined };
    let heights: BTreeSet<u64> = sources.iter().filter_map(|f| height(f)).collect();
    if !sources.is_empty()
        || (!merge
            && formats
                .iter()
                .any(|f| codec(f, "vcodec") && codec(f, "acodec")))
    {
        out.push(Choice {
            id: "best".into(),
            label: "Best available".into(),
            size: None,
        });
    }
    for &h in heights.iter().rev().take(8) {
        // The largest file at this height stands in for what "best" picks.
        let estimate = sources
            .iter()
            .filter(|f| height(f) == Some(h))
            .filter_map(|f| {
                let bytes = size(f)?;
                Some(if merge && !codec(f, "acodec") {
                    bytes + best_audio.unwrap_or(0)
                } else {
                    bytes
                })
            })
            .max();
        out.push(Choice {
            id: format!("v{h}"),
            label: quality(h),
            size: estimate,
        });
    }
    if merge && (!audio_only.is_empty() || !combined.is_empty()) {
        out.push(Choice {
            id: "mp3".into(),
            label: "Audio only · MP3".into(),
            size: best_audio,
        });
        out.push(Choice {
            id: "m4a".into(),
            label: "Audio only · M4A".into(),
            size: best_audio,
        });
    } else if !audio_only.is_empty() {
        out.push(Choice {
            id: "audio".into(),
            label: "Audio only".into(),
            size: best_audio,
        });
    }
    out
}

fn quality(height: u64) -> String {
    match height {
        4320.. => "8K".into(),
        2160.. => "4K".into(),
        1440.. => "1440p".into(),
        h => format!("{h}p"),
    }
}

/// The yt-dlp options for a choice from [`choices`].
fn selection(choice: &str, merge: bool) -> Result<Vec<String>> {
    let unknown = || Error::bad("Choose one of the offered qualities.");
    let video = |filter: &str| -> Vec<String> {
        let format = if merge {
            format!("bv*{filter}+ba/b{filter}")
        } else {
            format!("b{filter}")
        };
        // At the same size and frame rate, prefer H.264 and AAC, which merge into an
        // MP4 that plays everywhere. Higher qualities that only exist as VP9 or AV1
        // are still chosen, and land in MKV when MP4 can't hold them.
        vec![
            "-f".into(),
            format,
            "-S".into(),
            "res,fps,vcodec:h264,acodec:aac".into(),
        ]
    };
    Ok(match choice {
        "best" => video(""),
        "mp3" | "m4a" if merge => vec![
            "-f".into(),
            "ba/b".into(),
            "-x".into(),
            "--audio-format".into(),
            choice.into(),
        ],
        "audio" => vec!["-f".into(), "ba".into()],
        _ => {
            let height: u32 = choice
                .strip_prefix('v')
                .and_then(|h| h.parse().ok())
                .filter(|h| (1..=8640).contains(h))
                .ok_or_else(unknown)?;
            video(&format!("[height<={height}]"))
        }
    })
}

/// Starts downloading a link in the chosen quality, and returns the job's id.
pub async fn start(state: &State, raw: &str, choice: &str) -> Result<String> {
    let url = check_url(raw).await?;
    let settings = state.0.settings.get();
    let yt_dlp = tools::yt_dlp(&settings).ok_or_else(missing)?;
    let ffmpeg = tools::ffmpeg(&settings);
    let mut args = selection(choice, ffmpeg.is_some())?;
    let _starting = state.0.jobs.starting.lock().await;
    if state.0.jobs.active() >= settings.max_active as usize {
        return Err(Error::busy(
            "The server is already downloading as much as it allows. Try again in a moment.",
        ));
    }
    if state.0.jobs.usage() + u64::from(settings.max_file_mib) * MIB
        > u64::from(settings.max_total_mib) * MIB
    {
        return Err(Error::busy(
            "The server's download space is full. Try again once other downloads are collected.",
        ));
    }
    let id = uuid::Uuid::new_v4().to_string();
    let folder = state.0.jobs.root.join(&id);
    tokio::fs::create_dir_all(&folder).await?;
    if let Some(ffmpeg) = &ffmpeg {
        args.extend(["--ffmpeg-location".into(), ffmpeg.display().to_string()]);
        args.extend([
            "--merge-output-format".into(),
            "mp4/mkv".into(),
            "--embed-metadata".into(),
        ]);
    }
    args.extend([
        "--max-filesize".into(),
        format!("{}M", settings.max_file_mib),
        "--match-filter".into(),
        format!("!is_live & duration <=? {}", settings.max_minutes * 60),
        "--newline".into(),
        "--progress".into(),
        "--progress-template".into(),
        "download:[progress] %(progress.downloaded_bytes)s %(progress.total_bytes)s %(progress.total_bytes_estimate)s".into(),
        "--no-mtime".into(),
        "-o".into(),
        folder.join("media.%(ext)s").display().to_string(),
        "--".into(),
        url.to_string(),
    ]);
    let mut command = tools::command(&yt_dlp);
    command
        .args(common_args(&state.0.data))
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    command.process_group(0);
    let child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = tokio::fs::remove_dir_all(&folder).await;
            return Err(error.into());
        }
    };
    let cancel = CancellationToken::new();
    state.0.jobs.live.lock().unwrap().insert(
        id.clone(),
        Job {
            status: Status::Downloading,
            percent: None,
            error: None,
            file: None,
            size: 0,
            touched: Instant::now(),
            cancel: cancel.clone(),
        },
    );
    tokio::spawn(run(
        state.clone(),
        id.clone(),
        folder,
        child,
        cancel,
        settings,
    ));
    Ok(id)
}

async fn run(
    state: State,
    id: String,
    folder: PathBuf,
    mut child: tokio::process::Child,
    cancel: CancellationToken,
    settings: Settings,
) {
    let group = Group::contain(&child);
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let jobs = &state.0.jobs;
    // yt-dlp reports progress on stdout and problems on stderr.
    let progress = async {
        let Some(stdout) = stdout else {
            return String::new();
        };
        let mut lines = BufReader::new(stdout).lines();
        let mut notes = String::new();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(rest) = line.strip_prefix("[progress] ") {
                let numbers: Vec<Option<f64>> =
                    rest.split_whitespace().map(|n| n.parse().ok()).collect();
                if let [Some(done), total, estimate] = numbers[..]
                    && let Some(total) = total.or(estimate).filter(|total| *total > 0.0)
                {
                    let percent = (done / total * 100.0).clamp(0.0, 100.0) as f32;
                    jobs.update(&id, |job| job.percent = Some(percent));
                }
            } else if line.starts_with("[Merger]")
                || line.starts_with("[ExtractAudio]")
                || line.starts_with("[Metadata]")
            {
                jobs.update(&id, |job| {
                    job.status = Status::Processing;
                    job.percent = None;
                });
            } else if notes.len() < 4096 {
                notes.push_str(&line);
                notes.push('\n');
            }
        }
        notes
    };
    let problems = async {
        let mut text = String::new();
        if let Some(stderr) = stderr {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if text.len() < 8192 {
                    text.push_str(&line);
                    text.push('\n');
                }
            }
        }
        text
    };
    let outcome = tokio::select! {
        _ = cancel.cancelled() => None,
        (notes, problems, exit) = async { tokio::join!(progress, problems, child.wait()) } => Some((notes, problems, exit)),
    };
    drop(group);
    let _ = child.kill().await;
    let Some((notes, problems, exit)) = outcome else {
        jobs.update(&id, |job| {
            job.status = Status::Cancelled;
            job.percent = None;
        });
        let _ = tokio::fs::remove_dir_all(&folder).await;
        return;
    };
    let finished = finished_file(&folder);
    let result = match (exit, finished) {
        (Ok(status), Some((file, size))) if status.success() => {
            if size > u64::from(settings.max_file_mib) * MIB {
                Err(format!(
                    "The file is larger than this server allows ({} MB).",
                    settings.max_file_mib
                ))
            } else {
                Ok((file, size))
            }
        }
        (Ok(status), _) if status.success() && notes.contains("does not pass filter") => {
            Err(format!(
                "That video is longer than this server allows ({} minutes).",
                settings.max_minutes
            ))
        }
        (Ok(status), _)
            if status.success()
                && (notes.contains("max-filesize") || notes.contains("larger than max")) =>
        {
            Err(format!(
                "The file is larger than this server allows ({} MB).",
                settings.max_file_mib
            ))
        }
        _ => Err(explain(&problems)),
    };
    match result {
        Ok((file, size)) => jobs.update(&id, |job| {
            job.status = Status::Ready;
            job.percent = Some(100.0);
            job.file = Some(file);
            job.size = size;
        }),
        Err(message) => {
            tracing::info!(job = %id, %message, "download failed");
            jobs.update(&id, |job| {
                job.status = Status::Failed;
                job.percent = None;
                job.error = Some(message);
            });
            let _ = tokio::fs::remove_dir_all(&folder).await;
        }
    }
}

/// The one file yt-dlp left in the folder, skipping its partial and temporary ones.
fn finished_file(folder: &Path) -> Option<(PathBuf, u64)> {
    std::fs::read_dir(folder)
        .ok()?
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            let meta = entry.metadata().ok()?;
            let partial = [".part", ".ytdl", ".temp", ".tmp"]
                .iter()
                .any(|end| name.contains(end));
            (meta.is_file() && name.starts_with("media.") && !partial && meta.len() > 0)
                .then(|| (entry.path(), meta.len()))
        })
        .max_by_key(|(_, size)| *size)
}

/// Options for every run: ignore personal yt-dlp configs, never follow playlists,
/// skip the generic extractor (which would fetch any web page, including ones on the
/// server's own network), and keep yt-dlp's cache with the worker.
fn common_args(data: &Path) -> Vec<String> {
    vec![
        "--ignore-config".into(),
        "--no-playlist".into(),
        "--ies".into(),
        "default,-generic".into(),
        "--no-warnings".into(),
        "--socket-timeout".into(),
        "30".into(),
        "--cache-dir".into(),
        data.join("cache").display().to_string(),
    ]
}

fn missing() -> Error {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "yt-dlp isn't installed on the server yet. Ask the owner to install it.",
    )
}

/// yt-dlp's last error, in words people can act on.
fn explain(stderr: &str) -> String {
    let error = stderr
        .lines()
        .rev()
        .find_map(|line| line.trim().strip_prefix("ERROR: "))
        .unwrap_or_else(|| {
            stderr
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .unwrap_or_default()
                .trim()
        });
    let lower = error.to_ascii_lowercase();
    if lower.contains("unsupported url") {
        return "That site isn't supported. yt-dlp lists the sites it supports.".into();
    }
    if lower.contains("sign in") || lower.contains("login") || lower.contains("cookies") {
        return "That site wants a signed-in account for this video, so it can't be downloaded here.".into();
    }
    if lower.contains("private video")
        || lower.contains("video unavailable")
        || lower.contains("not available")
    {
        return "That video is private, removed, or not available from the server's location."
            .into();
    }
    if lower.contains("http error 429") || lower.contains("too many requests") {
        return "The site is limiting requests from this server. Try again later.".into();
    }
    // Drop yt-dlp's "[extractor] id: " prefix and keep the message short.
    let message = match error.split_once("] ") {
        Some((head, rest)) if head.starts_with('[') => {
            rest.split_once(": ").map_or(rest, |(_, after)| after)
        }
        _ => error,
    };
    let message: String = message
        .chars()
        .filter(|c| !c.is_control())
        .take(300)
        .collect();
    if message.is_empty() {
        "yt-dlp could not download that link.".into()
    } else {
        format!("yt-dlp couldn't download it: {message}")
    }
}

/// Accepts web links whose host is on the public internet, so nobody can point the
/// downloader at the server itself or the network it sits on.
pub async fn check_url(raw: &str) -> Result<url::Url> {
    let invalid = || Error::bad("Paste a link that starts with https:// or http://.");
    let raw = raw.trim();
    if raw.is_empty() || raw.len() > 2048 {
        return Err(invalid());
    }
    let url = url::Url::parse(raw).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "https" | "http")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(invalid());
    }
    let private = || Error::bad("Links to this server or its local network aren't allowed.");
    let addresses: Vec<IpAddr> = match url.host().ok_or_else(invalid)? {
        url::Host::Ipv4(ip) => vec![IpAddr::V4(ip)],
        url::Host::Ipv6(ip) => vec![IpAddr::V6(ip)],
        url::Host::Domain(domain) => {
            let port = url.port_or_known_default().unwrap_or(443);
            tokio::time::timeout(
                Duration::from_secs(5),
                tokio::net::lookup_host((domain, port)),
            )
            .await
            .map_err(|_| Error::bad("Looking up that website took too long."))?
            .map_err(|_| Error::bad("Couldn't find that website. Check the link."))?
            .map(|socket| socket.ip())
            .collect()
        }
    };
    if addresses.is_empty() || !addresses.into_iter().all(public) {
        return Err(private());
    }
    Ok(url)
}

fn public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, _, _] = ip.octets();
            !(ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_multicast()
                || ip.is_documentation()
                || a == 0
                || a >= 240
                || (a == 100 && (64..128).contains(&b))
                || (a == 198 && (18..20).contains(&b))
                || matches!(ip.octets(), [192, 0, 0, _] | [192, 88, 99, _]))
        }
        IpAddr::V6(ip) => match ip.to_ipv4_mapped() {
            Some(v4) => public(IpAddr::V4(v4)),
            None => {
                let [first, second, ..] = ip.segments();
                (0x2000..0x4000).contains(&first)
                    && !matches!(first, 0x2002 | 0x3fff)
                    && !(first == 0x2001 && (second < 0x200 || second == 0xdb8))
            }
        },
    }
}

/// Keeps yt-dlp and the ffmpeg it starts together, so stopping a download stops both.
struct Group {
    #[cfg(windows)]
    _job: Option<std::os::windows::io::OwnedHandle>,
    #[cfg(unix)]
    pid: Option<i32>,
}

impl Group {
    fn contain(child: &tokio::process::Child) -> Self {
        #[cfg(windows)]
        {
            Self {
                _job: windows_job(child),
            }
        }
        #[cfg(unix)]
        {
            Self {
                pid: child.id().map(|pid| pid as i32),
            }
        }
        #[cfg(not(any(windows, unix)))]
        {
            let _ = child;
            Self {}
        }
    }
}

#[cfg(unix)]
impl Drop for Group {
    fn drop(&mut self) {
        if let Some(pid) = self.pid.filter(|pid| *pid > 0) {
            // The child leads its own process group (see `start`).
            unsafe {
                libc::kill(-pid, libc::SIGKILL);
            }
        }
    }
}

/// A job object that ends every process in it when the worker lets go of it.
#[cfg(windows)]
fn windows_job(child: &tokio::process::Child) -> Option<std::os::windows::io::OwnedHandle> {
    use std::os::windows::io::{FromRawHandle, OwnedHandle};
    use windows_sys::Win32::System::JobObjects::*;
    let process = child.raw_handle()?;
    unsafe {
        let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
        if job.is_null() {
            return None;
        }
        let owned = OwnedHandle::from_raw_handle(job as _);
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let set = SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            &limits as *const _ as *const _,
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        );
        (set != 0 && AssignProcessToJobObject(job, process as _) != 0).then_some(owned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn video(height: u64, vcodec: &str, acodec: &str, size: u64) -> Value {
        json!({"height": height, "vcodec": vcodec, "acodec": acodec, "filesize": size})
    }

    #[test]
    fn merging_offers_every_height_and_audio() {
        let info = json!({"formats": [
            video(360, "avc1", "mp4a", 10), video(1080, "avc1", "none", 100), video(2160, "vp9", "none", 400),
            {"vcodec": "none", "acodec": "opus", "filesize": 5},
        ]});
        let ids: Vec<_> = choices(&info, true)
            .into_iter()
            .map(|c| (c.id, c.label, c.size))
            .collect();
        assert_eq!(
            ids,
            vec![
                ("best".into(), "Best available".into(), None),
                ("v2160".into(), "4K".into(), Some(405)),
                ("v1080".into(), "1080p".into(), Some(105)),
                ("v360".into(), "360p".into(), Some(10)),
                ("mp3".into(), "Audio only · MP3".into(), Some(5)),
                ("m4a".into(), "Audio only · M4A".into(), Some(5)),
            ]
        );
    }

    #[test]
    fn without_ffmpeg_only_files_with_sound_and_picture_are_offered() {
        let info = json!({"formats": [video(360, "avc1", "mp4a", 10), video(1080, "avc1", "none", 100), {"vcodec": "none", "acodec": "opus"}]});
        let ids: Vec<_> = choices(&info, false).into_iter().map(|c| c.id).collect();
        assert_eq!(ids, ["best", "v360", "audio"]);
    }

    #[test]
    fn selections_match_choices_and_reject_anything_else() {
        let sort = ["-S", "res,fps,vcodec:h264,acodec:aac"];
        assert_eq!(
            selection("v720", true).unwrap(),
            [
                ["-f", "bv*[height<=720]+ba/b[height<=720]"].as_slice(),
                &sort
            ]
            .concat()
        );
        assert_eq!(
            selection("v720", false).unwrap(),
            [["-f", "b[height<=720]"].as_slice(), &sort].concat()
        );
        assert_eq!(
            selection("mp3", true).unwrap(),
            ["-f", "ba/b", "-x", "--audio-format", "mp3"]
        );
        for bad in ["mp3", "v0", "v99999", "--exec", "v720;rm"] {
            assert!(selection(bad, false).is_err(), "{bad}");
        }
    }

    #[test]
    fn playlists_live_streams_and_long_videos_are_refused() {
        let settings = Settings {
            max_minutes: 10,
            ..Settings::default()
        };
        let refused = |info: Value| {
            describe(&info, &settings, true, "https://example.com")
                .unwrap_err()
                .message
        };
        assert!(refused(json!({"_type": "playlist"})).contains("playlist"));
        assert!(refused(json!({"is_live": true})).contains("Live"));
        assert!(
            refused(json!({"duration": 601.0, "formats": [video(720, "avc1", "mp4a", 1)]}))
                .contains("10 minutes")
        );
        let ok = describe(&json!({"title": "Hi\u{7}", "duration": 30.0, "extractor_key": "Youtube", "webpage_url_domain": "youtube.com", "formats": [video(720, "avc1", "mp4a", 1)]}), &settings, true, "https://example.com").unwrap();
        assert_eq!(ok["title"], "Hi");
        assert_eq!(ok["site"], "youtube.com");
    }

    #[test]
    fn errors_read_as_plain_sentences() {
        assert_eq!(
            explain("ERROR: Unsupported URL: https://x.test/\n"),
            "That site isn't supported. yt-dlp lists the sites it supports."
        );
        assert!(
            explain("WARNING: x\nERROR: [youtube] abc: Sign in to confirm you're not a bot")
                .contains("signed-in")
        );
        assert_eq!(
            explain("ERROR: [vimeo] 123: Something odd happened"),
            "yt-dlp couldn't download it: Something odd happened"
        );
    }

    #[tokio::test]
    async fn links_to_the_local_network_are_refused() {
        for url in [
            "http://127.0.0.1/",
            "http://192.168.1.10/x",
            "http://[::1]/",
            "https://[::ffff:10.0.0.1]/",
            "http://localhost:8080/",
            "file:///etc/passwd",
            "https://user:pw@example.com/",
            "ftp://example.com/",
        ] {
            assert!(check_url(url).await.is_err(), "{url}");
        }
        assert!(public("8.8.8.8".parse().unwrap()));
        assert!(!public("100.64.0.1".parse().unwrap()));
    }
}
