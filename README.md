# Media Downloader for LOKI

Save videos and audio from YouTube and the thousands of other sites
[yt-dlp](https://github.com/yt-dlp/yt-dlp) supports, from your own LOKI server.
People paste a link, pick a quality from 144p up to 4K or audio only, and save the
finished file. Only download media you have the right to keep.

It has two parts:

- **The LOKI app** (`app/`, a `.loki-app`): the page people use, and a panel in
  LOKI's dashboard where the owner connects the worker, sees yt-dlp's status,
  updates it, and sets limits.
- **The worker** (`worker/`, `media-downloader-worker`): a small program on the
  same computer as LOKI that runs the server's yt-dlp. LOKI apps can't start
  programs themselves, so the worker does that and listens only on
  `127.0.0.1:44317`, with a key only the app knows.

## Set up

1. **Install yt-dlp** on the server so it's on the PATH. Also install:
   - **ffmpeg**, to offer 1080p and higher. Most sites, YouTube included, send
     video and sound separately at high quality, and ffmpeg joins them. Without
     it, only files that already have both are offered, which on YouTube means
     360p.
   - **Deno**, which yt-dlp needs to read YouTube's videos.

   On Windows, `winget install yt-dlp.yt-dlp`, `winget install Gyan.FFmpeg` and
   `winget install DenoLand.Deno`.
   On Linux and macOS, use your package manager or the projects' own releases.
   The standalone yt-dlp release can update itself from the panel.

2. **Run the worker** as the account that should own the downloads:

   ```text
   media-downloader-worker
   ```

   On its first start it makes a key and prints it, along with the yt-dlp,
   ffmpeg and Deno it found. `media-downloader-worker key` prints the key again,
   and `media-downloader-worker check` shows the tools without starting.
   Keep it running, for example with Task Scheduler at sign-in, a systemd user
   service or launchd. Stopping it (Ctrl+C, or SIGTERM on Linux and macOS)
   also stops any yt-dlp it started.

3. **Import the app** in LOKI's **Applications**, grant access to the people or
   groups who should use it, and enable it.

4. Open the app's **panel** in LOKI and paste the worker key. The panel then
   shows the worker, yt-dlp (and whether a newer version is out), ffmpeg and
   Deno, with **Update yt-dlp** and **Check again** buttons.

The server owner is always an admin of the app. Give others the app's `admin`
role to let them manage the panel too.

## Limits

Set these in the panel:

| Limit | Default |
| --- | --- |
| Longest video | 180 minutes |
| Largest file | 2048 MB |
| Downloads at once (whole server) | 2 |
| Worker space for downloads in progress | 8192 MB |
| Keep finished files ready for | 60 minutes |

Each person can have three downloads in progress at a time. A download that
makes no progress for 12 hours (for example because the worker stopped for
good) is marked failed. Finished files wait
in the app's storage on LOKI until they're saved or expire. That storage counts
against the app's quota, 4096 MB unless you change it from **Manage** in
Applications.

## How it works

The worker runs yt-dlp with `--ignore-config`, never follows playlists, and
leaves out yt-dlp's generic extractor, so only sites yt-dlp specifically
supports work. A pasted link whose host resolves to the server itself or its
local network is refused. That check covers the link you paste; yt-dlp then
looks the site up again and follows the site's own redirects, so give the app
only to people you trust with the server's internet connection.

LOKI gives an app request two minutes, which isn't enough to stream a large video
through it. So when the worker finishes, the app copies the file into its own
storage on LOKI a few megabytes at a time. This happens while the page is open,
and every minute in the background otherwise. LOKI then serves the file itself,
as an ordinary download that can resume and has no time limit.

The worker keeps its key, `settings.json` and in-progress downloads in its data
folder: `%LOCALAPPDATA%\LOKI\MediaDownloader\data` on Windows, and the usual local
data folder elsewhere. `--data-dir` or `MEDIA_DOWNLOADER_DATA` chooses another.
To use a yt-dlp or ffmpeg that isn't on the PATH, add `"yt_dlp"` or `"ffmpeg"`
with its full path to `settings.json`. Downloads in progress are dropped when the
worker restarts.

## Build

```text
rustup target add wasm32-wasip2
cargo install cargo-loki --version 0.9.0 --locked
cargo build --release -p media-downloader-worker
cargo loki package -p media-downloader
```

The worker is `target/release/media-downloader-worker` and the app is
`target/loki/media-downloader-0.2.1.loki-app`. The app needs LOKI 0.9 or later.

## Upgrading from the YouTube Downloader

This replaces the earlier YouTube Downloader app and its sandboxed worker.
It has a new app ID, so import it as a new app, and remove the old app and
worker.
