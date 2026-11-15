# YouTube Downloader for LOKI

An installable LOKI app for videos you have permission to download. Users sign
in through LOKI, pick an available source quality, and collect prepared videos
before the countdown expires. The app keeps private, exportable metadata history.

This project builds a portable `.loki-app` and a native worker for the same
server. The WASI app handles the browser UI and uses LOKI's existing account and
access controls. The separately installed worker uses `127.0.0.1:44317` and a
shared key to accept requests from the app. Keep the worker port on loopback.

## Build

Use LOKI and `cargo-loki` 0.6 or later. Install the WASI target and build both parts
from this repository's root.

```text
rustup target add wasm32-wasip2
cargo install cargo-loki --locked
cargo build --locked --release -p loki-youtube-worker
cargo loki package -p loki-youtube
```

The executable is in `target/release/`. The package is written under
`target/loki/`.

## Provision the worker

Set up a private data directory for the worker under its OS account. Provision a
separate read-only runtime directory with current CPython 3.11 or newer and Deno
2.9.8 or newer. The runtime layout is `bin/python3` and `bin/deno` on Unix,
`bin/python.exe` and `bin/deno.exe` on Windows, and `packages/` on every OS.
Include the complete Python standard library and any required shared libraries.
Install the pinned dependencies with the Python used to provision the runtime.

```text
python -m pip install --only-binary=:all: --no-deps --require-hashes -r packaging/youtube-requirements.txt --target RUNTIME/packages
```

- **Windows** uses a no-capability AppContainer and a Job Object. Run
  `loki-youtube-worker.exe worker-sid` as the account that will run the worker.
  Grant the returned SID read and execute access to the runtime and its contents
  with `icacls RUNTIME /grant "*SID:(OI)(CI)(RX)" /T`. Keep the data directory
  private to the worker account. The AppContainer needs zero loopback exemptions.
- **Linux** uses Bubblewrap with unprivileged user namespaces, a separate network
  namespace, and a small temporary filesystem. Bubblewrap defaults to
  `/usr/bin/bwrap`. Set `LOKI_BWRAP_EXECUTABLE` to an absolute path to select
  another installation. The runtime needs to work read-only at `/worker`.
- **macOS** requires a CPython helper signed with the
  `com.apple.security.app-sandbox` entitlement and a Deno child signed with
  `com.apple.security.app-sandbox` and `com.apple.security.inherit`. Both must pass
  strict signature verification. [Apple's helper signing guide](https://developer.apple.com/documentation/xcode/embedding-a-helper-tool-in-a-sandboxed-app)

Set `LOKI_YOUTUBE_RUNTIME` to the absolute runtime directory. With the worker
stopped, run its isolation check and generate the key. Replace `DATA` and
`PRIVATE_KEY_FILE` with your paths.

```text
loki-youtube-worker --data-dir DATA check
loki-youtube-worker create-key PRIVATE_KEY_FILE
loki-youtube-worker --data-dir DATA serve --key-file PRIVATE_KEY_FILE
```

The worker checks isolation again at startup. The sandboxed extractor cannot
connect directly to the network. Its broker permits only approved YouTube HTTPS
hosts, pins public DNS results, and checks redirects. `check --video-url URL`
also probes a particular video for supported qualities without saving it.

## Install in LOKI

1. In **Applications**, import `youtube-downloader-0.1.0.loki-app` and review its
   outbound permission for `127.0.0.1:44317`.
2. Open **Manage**, enter the key file's contents in **Local worker key**, and
   save. Keep access restricted to selected accounts or groups, or choose Members
   for all approved LOKI accounts.
3. Enable and open the app. A hostname is optional for local use. For public
   hosting, configure the app hostname and HTTPS through LOKI's usual proxy.

## Limits and history

To configure limits, save this JSON as `DATA/limits.json` and restart the worker.
`serve --limits PATH` selects a different file. These are the defaults.

```json
{
  "duration_minutes": 60,
  "file_mib": 512,
  "total_mib": 2048,
  "pickup_seconds": 300
}
```

The temporary spool and active jobs share the configured total. The file limit
may be set up to 1 GiB, which fits LOKI's WASI streaming limit. LOKI also limits
each individual delivery request to 120 seconds. A partially transferred file
can be resumed by a new ranged request during the pickup window. Transfers that
start within the window have up to five minutes of worker-side cleanup grace.

The app offers source formats that already contain both audio and video. The
worker deletes temporary video files after expiry and on startup. It keeps at
most 30 days and 500 entries of metadata per account. Users can export or clear
their history from the app. Back up the worker's `history.db` separately with
the worker stopped if you want to retain history across server moves.
