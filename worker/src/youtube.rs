//! Download lifecycle and private temporary spool.
mod sandbox;
mod web;
mod worker;

use crate::{Error, Result, State, now};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Mutex, atomic::Ordering},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
pub(crate) use web::router;

const MIB: u64 = 1024 * 1024;
const HISTORY_DAYS: i64 = 30;
const GRACE: i64 = 300;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub duration_minutes: u32,
    pub file_mib: u32,
    pub total_mib: u32,
    pub pickup_seconds: u32,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            duration_minutes: 60,
            file_mib: 512,
            total_mib: 2048,
            pickup_seconds: 300,
        }
    }
}

impl Settings {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=1440).contains(&self.duration_minutes)
                && (1..=1024).contains(&self.file_mib)
                && (self.file_mib..=65_536).contains(&self.total_mib)
                && (15..=3600).contains(&self.pickup_seconds),
            "Choose valid duration, file, total storage, and pickup limits. Total storage must fit at least one file."
        );
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct Jobs {
    slots: Mutex<HashMap<String, Live>>,
    buffers: Mutex<HashMap<String, Probed>>,
    maintenance: tokio::sync::Mutex<()>,
}

struct Probed {
    user: String,
    video: worker::Video,
    expires: i64,
}

struct Live {
    user: String,
    bytes: u64,
    expiry: Option<i64>,
    active: u8,
    cancel: CancellationToken,
    streams: Vec<std::sync::Weak<web::TransferFile>>,
}

fn spool(data: &Path) -> PathBuf {
    data.join("youtube-spool")
}

fn file(data: &Path, id: &str) -> PathBuf {
    spool(data).join(id)
}

pub(crate) async fn recover(data: &Path, db: &sqlx::SqlitePool) -> anyhow::Result<()> {
    let spool = spool(data);
    if let Ok(meta) = spool.symlink_metadata() {
        anyhow::ensure!(
            meta.is_dir() && !meta.file_type().is_symlink(),
            "Unsafe downloader spool directory"
        );
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            anyhow::ensure!(
                meta.file_attributes() & 0x400 == 0,
                "Unsafe downloader spool reparse point"
            );
        }
    }
    std::fs::create_dir_all(&spool)?;
    crate::private_directory(&spool)?;
    for item in std::fs::read_dir(&spool)? {
        let item = item?;
        if item.file_type()?.is_file()
            && item
                .file_name()
                .to_str()
                .is_some_and(|name| uuid::Uuid::parse_str(name).is_ok())
        {
            std::fs::remove_file(item.path())?;
        }
    }
    sqlx::query("UPDATE youtube_jobs SET status=CASE status WHEN 'ready' THEN 'expired' ELSE 'interrupted' END, expires=NULL WHERE status IN('ready','downloading')").execute(db).await?;
    Ok(())
}

pub async fn maintain(state: State) {
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    let mut pruned = 0;
    loop {
        ticks.tick().await;
        let _updates = state.0.youtube.maintenance.lock().await;
        let now = now();
        let jobs: Vec<_> = state
            .0
            .youtube
            .slots
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        for id in jobs {
            let erase = {
                let mut slots = state.0.youtube.slots.lock().unwrap();
                let Some(job) = slots.get_mut(&id) else {
                    continue;
                };
                let cancelled = state.0.stopping.load(Ordering::Acquire)
                    || job.cancel.is_cancelled()
                    || job.expiry.is_some_and(|expiry| now >= expiry + GRACE);
                if cancelled {
                    job.cancel.cancel();
                }
                job.streams.retain(|stream| stream.strong_count() != 0);
                let erase = job.expiry.is_some_and(|expiry| {
                    (cancelled || now >= expiry) && (cancelled || job.active == 0)
                });
                erase.then(|| {
                    job.streams
                        .iter()
                        .filter_map(std::sync::Weak::upgrade)
                        .collect::<Vec<_>>()
                })
            };
            if let Some(streams) = erase {
                for stream in streams {
                    stream.close();
                }
                let deleted = tokio::fs::remove_file(file(&state.0.data, &id)).await;
                if deleted.is_ok()
                    || deleted
                        .as_ref()
                        .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
                {
                    if let Err(error) = sqlx::query(
                        "UPDATE youtube_jobs SET status='expired' WHERE id=? AND status='ready'",
                    )
                    .bind(&id)
                    .execute(&state.0.db)
                    .await
                    {
                        tracing::warn!(%error,"could not mark a downloader job expired");
                    }
                    state.0.youtube.slots.lock().unwrap().remove(&id);
                }
            }
        }
        state
            .0
            .youtube
            .buffers
            .lock()
            .unwrap()
            .retain(|_, value| value.expires > now);
        if now - pruned >= 300 {
            if let Err(error) = sqlx::query("DELETE FROM youtube_jobs WHERE status NOT IN('ready','downloading') AND (created<? OR id IN(SELECT id FROM(SELECT id,row_number() OVER(PARTITION BY user_id ORDER BY created DESC,rowid DESC) AS rank FROM youtube_jobs) WHERE rank>500))")
                .bind(now-HISTORY_DAYS*86400).execute(&state.0.db).await {
                tracing::warn!(%error,"could not prune downloader history");
            }
            pruned = now;
        }
    }
}

/// Checks the configured worker without enabling the downloader or contacting YouTube.
pub async fn verify_worker(state: &State) -> anyhow::Result<()> {
    worker::verify(state)
        .await
        .map_err(|error| anyhow::anyhow!(error.message))
}

/// Verifies the sandbox and inspects the formats available for a YouTube link.
pub async fn inspect_worker(state: &State, url: &str) -> anyhow::Result<serde_json::Value> {
    verify_worker(state).await?;
    let id = video_id(url).map_err(|error| anyhow::anyhow!(error.message))?;
    let settings = &state.0.settings;
    let video = worker::probe(state, &id, settings)
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?;
    Ok(
        serde_json::json!({"video_id":id,"title":video.title,"duration":video.duration,"formats":video.formats}),
    )
}

/// Creates the local AppContainer profile and returns its SID for runtime ACLs.
#[cfg(windows)]
pub fn worker_sid() -> std::io::Result<String> {
    sandbox::worker_sid()
}

pub(crate) fn available_error() -> Error {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "The isolated downloader worker is unavailable. Ask your LOKI owner to check the worker setup.",
    )
}

pub(crate) fn capacity_error() -> Error {
    Error::new(
        StatusCode::TOO_MANY_REQUESTS,
        "The downloader is busy or its temporary storage is full. Try again after a download expires.",
    )
}

fn video_id(raw: &str) -> Result<String> {
    let invalid = || Error::bad("Paste a link to one YouTube video.");
    if raw.len() > 2048 {
        return Err(invalid());
    }
    let url = url::Url::parse(raw.trim()).map_err(|_| invalid())?;
    if url.scheme() != "https"
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(invalid());
    }
    let query = url.query_pairs().collect::<Vec<_>>();
    if query.iter().any(|(key, _)| key == "list") {
        return Err(invalid());
    }
    let id = match (url.host_str(), url.path()) {
        (Some("youtu.be"), path) => path.strip_prefix('/').map(str::to_owned),
        (Some("youtube.com" | "www.youtube.com" | "m.youtube.com"), "/watch") => {
            let ids: Vec<_> = query.iter().filter(|(key, _)| key == "v").collect();
            (ids.len() == 1).then(|| ids[0].1.to_string())
        }
        (Some("youtube.com" | "www.youtube.com" | "m.youtube.com"), path) => {
            path.strip_prefix("/shorts/").map(str::to_owned)
        }
        _ => None,
    };
    id.filter(|id| {
        id.len() == 11
            && id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    })
    .ok_or_else(invalid)
}

pub fn stop_transfers(state: &State) {
    for live in state.0.youtube.slots.lock().unwrap().values() {
        live.cancel.cancel();
    }
    state.0.youtube.buffers.lock().unwrap().clear();
}
