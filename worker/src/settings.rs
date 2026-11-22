//! Limits and tool paths, kept in `settings.json` in the data directory. The app's
//! panel changes the limits; the tool paths are only for unusual installs.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::RwLock,
};

pub const FILE: &str = "settings.json";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    /// The longest video, in minutes.
    pub max_minutes: u32,
    /// The largest finished file, in MiB.
    pub max_file_mib: u32,
    /// Room for downloads in progress together, in MiB.
    pub max_total_mib: u32,
    /// Downloads running at once.
    pub max_active: u32,
    /// yt-dlp to run instead of the one on PATH.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub yt_dlp: Option<PathBuf>,
    /// ffmpeg to use instead of the one on PATH.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ffmpeg: Option<PathBuf>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            max_minutes: 180,
            max_file_mib: 2048,
            max_total_mib: 8192,
            max_active: 2,
            yt_dlp: None,
            ffmpeg: None,
        }
    }
}

impl Settings {
    pub fn validate(&self) -> Result<()> {
        if !(1..=1440).contains(&self.max_minutes)
            || !(1..=4096).contains(&self.max_file_mib)
            || !(self.max_file_mib..=1_048_576).contains(&self.max_total_mib)
            || !(1..=8).contains(&self.max_active)
        {
            return Err(Error::bad(
                "Use 1–1440 minutes, files of 1–4096 MB, room for at least one file, and 1–8 downloads at once.",
            ));
        }
        Ok(())
    }
}

/// The changes the app may make. Tool paths stay with whoever runs the worker.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_minutes: u32,
    pub max_file_mib: u32,
    pub max_total_mib: u32,
    pub max_active: u32,
}

pub struct Store {
    path: PathBuf,
    current: RwLock<Settings>,
}

impl Store {
    pub fn open(data: &Path) -> anyhow::Result<Self> {
        let path = data.join(FILE);
        let settings = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Settings>(&bytes)
                .map_err(|error| anyhow::anyhow!("{} is not valid: {error}", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Settings::default(),
            Err(error) => return Err(error.into()),
        };
        settings
            .validate()
            .map_err(|error| anyhow::anyhow!("{}: {}", path.display(), error.message))?;
        Ok(Self { path, current: RwLock::new(settings) })
    }

    pub fn get(&self) -> Settings {
        self.current.read().unwrap().clone()
    }

    pub fn set_limits(&self, limits: Limits) -> Result<Settings> {
        let mut next = self.get();
        next.max_minutes = limits.max_minutes;
        next.max_file_mib = limits.max_file_mib;
        next.max_total_mib = limits.max_total_mib;
        next.max_active = limits.max_active;
        next.validate()?;
        let bytes = serde_json::to_vec_pretty(&next).map_err(|_| Error::bad("Invalid settings."))?;
        let temporary = self.path.with_extension("json.tmp");
        std::fs::write(&temporary, bytes)?;
        std::fs::rename(&temporary, &self.path)?;
        *self.current.write().unwrap() = next.clone();
        Ok(next)
    }
}
