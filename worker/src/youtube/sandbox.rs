use super::available_error;
use crate::Result;
use std::path::PathBuf;
#[cfg(unix)]
use std::{path::Path, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};

#[cfg(windows)]
mod windows;

#[cfg(windows)]
pub(super) fn worker_sid() -> std::io::Result<String> {
    windows::worker_sid()
}

type Input = Box<dyn AsyncWrite + Send + Unpin>;
type Output = Box<dyn AsyncRead + Send + Unpin>;
const SCRIPT: &str = include_str!("extractor.py");

pub(super) struct Process {
    #[cfg(unix)]
    child: tokio::process::Child,
    #[cfg(unix)]
    group: i32,
    #[cfg(windows)]
    job: std::os::windows::io::OwnedHandle,
}

impl Process {
    pub(super) async fn stop(&mut self) {
        #[cfg(unix)]
        {
            self.kill();
            let _ = tokio::time::timeout(Duration::from_secs(2), self.child.wait()).await;
        }
        #[cfg(windows)]
        self.kill();
    }

    fn kill(&self) {
        #[cfg(unix)]
        if self.group > 0 {
            // Each worker starts in a fresh process group.
            unsafe {
                libc::kill(-self.group, libc::SIGKILL);
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            unsafe {
                windows_sys::Win32::System::JobObjects::TerminateJobObject(
                    self.job.as_raw_handle(),
                    1,
                );
            }
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.kill();
    }
}

fn runtime() -> Result<PathBuf> {
    let root = std::env::var_os("LOKI_YOUTUBE_RUNTIME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(available_error)?;
    if !root.is_dir() {
        return Err(available_error());
    }
    dunce::canonicalize(root).map_err(|_| available_error())
}

pub(super) async fn start() -> Result<(Process, Input, Output)> {
    let root = runtime()?;
    #[cfg(windows)]
    {
        windows::start(&root, SCRIPT)
    }
    #[cfg(unix)]
    {
        unix(&root).await
    }
}

#[cfg(unix)]
async fn unix(root: &Path) -> Result<(Process, Input, Output)> {
    use std::process::Stdio;
    let python = root.join("bin/python3");
    let deno = root.join("bin/deno");
    if !python.is_file() || !deno.is_file() {
        return Err(available_error());
    }

    #[cfg(target_os = "linux")]
    let mut command = {
        let bubblewrap = std::env::var_os("LOKI_BWRAP_EXECUTABLE")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/usr/bin/bwrap"));
        if !bubblewrap.is_absolute() || !bubblewrap.is_file() {
            return Err(available_error());
        }
        let mut command = tokio::process::Command::new(bubblewrap);
        command.args([
            "--unshare-all",
            "--unshare-user",
            "--disable-userns",
            "--die-with-parent",
            "--cap-drop",
            "ALL",
            "--clearenv",
            "--dev",
            "/dev",
            "--size",
            "16777216",
            "--tmpfs",
            "/tmp",
            "--setenv",
            "HOME",
            "/tmp",
            "--setenv",
            "DENO_DIR",
            "/tmp/deno",
            "--setenv",
            "LC_ALL",
            "C.UTF-8",
            "--chdir",
            "/tmp",
        ]);
        for location in ["/usr/lib", "/usr/lib64", "/lib", "/lib64"] {
            if Path::new(location).is_dir() {
                command.args(["--ro-bind", location, location]);
            }
        }
        command.arg("--ro-bind").arg(root).arg("/worker");
        command.args([
            "--",
            "/worker/bin/python3",
            "-I",
            "-S",
            "-B",
            "-c",
            SCRIPT,
            "/worker/bin/deno",
        ]);
        command
    };

    #[cfg(target_os = "macos")]
    let mut command = {
        // Provision a signed, sandboxed CPython executable and a Deno child
        // signed with the App Sandbox inheritance entitlements.
        verify_mac_signature(&python, false).await?;
        verify_mac_signature(&deno, true).await?;
        let mut command = tokio::process::Command::new(&python);
        command
            .args(["-I", "-S", "-B", "-c", SCRIPT])
            .arg(&deno)
            .current_dir(root);
        command
    };

    command
        .env_clear()
        .env("PYTHONUTF8", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .kill_on_drop(true);
    // Set resource limits before any interpreter code runs. Limit file size
    // so a compromised extractor cannot fill its scratch area with one file.
    unsafe {
        command.pre_exec(|| {
            let cpu = libc::rlimit {
                rlim_cur: 60,
                rlim_max: 60,
            };
            let output = libc::rlimit {
                rlim_cur: 16 * 1024 * 1024,
                rlim_max: 16 * 1024 * 1024,
            };
            let files = libc::rlimit {
                rlim_cur: 128,
                rlim_max: 128,
            };
            for (resource, limit) in [
                (libc::RLIMIT_CPU, cpu),
                (libc::RLIMIT_FSIZE, output),
                (libc::RLIMIT_NOFILE, files),
            ] {
                if libc::setrlimit(resource, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            #[cfg(target_os = "linux")]
            {
                // V8 reserves a large, mostly inaccessible address range for
                // its heap cage. Bound private writable mappings separately.
                for (resource, bytes) in [
                    (libc::RLIMIT_AS, 64 * 1024 * 1024 * 1024u64),
                    (libc::RLIMIT_DATA, 768 * 1024 * 1024),
                ] {
                    let limit = libc::rlimit {
                        rlim_cur: bytes as libc::rlim_t,
                        rlim_max: bytes as libc::rlim_t,
                    };
                    if libc::setrlimit(resource, &limit) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
            }
            Ok(())
        });
    }
    let mut child = command.spawn().map_err(|_| available_error())?;
    let group = child.id().ok_or_else(available_error)? as i32;
    let stdin = Box::new(child.stdin.take().expect("piped worker input"));
    let stdout = Box::new(child.stdout.take().expect("piped worker output"));
    Ok((Process { child, group }, stdin, stdout))
}

#[cfg(target_os = "macos")]
async fn verify_mac_signature(path: &Path, child: bool) -> Result<()> {
    use std::process::Stdio;
    let mut verify = tokio::process::Command::new("/usr/bin/codesign");
    verify
        .args(["--verify", "--strict"])
        .arg(path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if !tokio::time::timeout(Duration::from_secs(5), verify.status())
        .await
        .map_err(|_| available_error())??
        .success()
    {
        return Err(available_error());
    }
    let mut show = tokio::process::Command::new("/usr/bin/codesign");
    show.args(["-d", "--entitlements", "-", "--xml"])
        .arg(path)
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(5), show.output())
        .await
        .map_err(|_| available_error())??;
    let text = String::from_utf8_lossy(&output.stdout);
    let clean: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    let keys: Vec<_> = clean
        .split("<key>")
        .skip(1)
        .filter_map(|part| part.split_once("</key>"))
        .collect();
    let expected = if child { 2 } else { 1 };
    if !output.status.success()
        || keys.len() != expected
        || !keys.iter().any(|(key, value)| {
            *key == "com.apple.security.app-sandbox" && value.starts_with("<true/>")
        })
        || (child
            && !keys.iter().any(|(key, value)| {
                *key == "com.apple.security.inherit" && value.starts_with("<true/>")
            }))
    {
        return Err(available_error());
    }
    Ok(())
}
