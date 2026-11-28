//! Media Downloader for LOKI. The worker on the server runs yt-dlp; this app is the
//! page people use, the panel the owner manages it from, and the delivery path.
//!
//! LOKI gives each app request two minutes, which is not enough to stream a large
//! video through the app. So finished files are copied from the worker into the
//! app's own storage in short steps, and LOKI then serves them itself, as ordinary
//! resumable downloads with no time limit.
use loki_sdk::{
    Database, Error, Request, Response, Result, Router, Storage, fetch,
    panel::{Action, Field, FieldKind, Panel, PanelResponse, Section},
    params,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const WORKER: &str = "http://127.0.0.1:44317";
/// Bytes asked of the worker per request while collecting a file.
const RANGE: u64 = 8 * 1024 * 1024;
/// LOKI's storage takes at most 1 MiB per append.
const PIECE: usize = 1024 * 1024;
/// How long one collect call keeps copying before it hands back.
const COLLECT_SECONDS: i64 = 45;
const ACTIVE_PER_PERSON: i64 = 3;
const ACTIVE: &str = "('downloading','processing','collecting')";
const POLICY: &str = "default-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";
const NOT_SET_UP: &str = "Media Downloader isn't connected to its worker yet. The server owner can set it up from the app's panel in LOKI.";
const UNREACHABLE: &str = "The downloader worker isn't running on the server. Ask the server owner to start media-downloader-worker.";

fn now() -> i64 {
    loki_sdk::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Settings the owner keeps in the app's storage.
#[derive(Serialize, Deserialize)]
struct Config {
    #[serde(default)]
    key: String,
    /// How long a finished file stays ready to save.
    #[serde(default = "keep_default")]
    keep_minutes: u32,
}
fn keep_default() -> u32 {
    60
}
impl Default for Config {
    fn default() -> Self {
        Self {
            key: String::new(),
            keep_minutes: keep_default(),
        }
    }
}
fn config() -> Result<Config> {
    Ok(Storage.get_json("config")?.unwrap_or_default())
}

// ---- The worker ---------------------------------------------------------------

fn call(
    method: &str,
    path: &str,
    body: Option<&Value>,
    range: Option<(u64, u64)>,
) -> Result<fetch::Fetched> {
    let key = config()?.key;
    if key.is_empty() {
        return Err(Error::new(503, NOT_SET_UP));
    }
    let mut request = fetch::Outgoing::new(method, &format!("{WORKER}{path}"))
        .header("authorization", &format!("Bearer {key}"));
    if let Some(body) = body {
        request = request.json(body)?;
    }
    if let Some((start, end)) = range {
        request = request.header("range", &format!("bytes={start}-{end}"));
    }
    let fetched = fetch::send(request).map_err(|_| Error::new(503, UNREACHABLE))?;
    if !(200..300).contains(&fetched.status) {
        let message = serde_json::from_slice::<Value>(&fetched.body)
            .ok()
            .and_then(|value| value["error"].as_str().map(str::to_owned))
            .unwrap_or_else(|| format!("The worker answered {}.", fetched.status));
        // A wrong key is a setup problem, not the person's.
        let status = if fetched.status == 403 {
            503
        } else {
            fetched.status
        };
        return Err(Error::new(status, message));
    }
    Ok(fetched)
}

fn worker(method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
    let fetched = call(method, path, body, None)?;
    serde_json::from_slice(&fetched.body)
        .map_err(|_| Error::new(502, "The worker answered something unexpected."))
}

// ---- Downloads -----------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize)]
struct Download {
    id: String,
    url: String,
    title: String,
    site: String,
    quality: String,
    #[serde(skip_serializing)]
    job: Option<String>,
    status: String,
    percent: Option<f64>,
    ext: Option<String>,
    size: Option<i64>,
    received: i64,
    error: Option<String>,
    created: i64,
    expires: Option<i64>,
}

const COLUMNS: &str =
    "id,url,title,site,quality,job,status,percent,ext,size,received,error,created,expires";

fn health(_: Request) -> Result<Response> {
    Database.execute(
        "CREATE TABLE IF NOT EXISTS downloads(id TEXT PRIMARY KEY,user_id TEXT NOT NULL,url TEXT NOT NULL,title TEXT NOT NULL,site TEXT NOT NULL DEFAULT '',quality TEXT NOT NULL,job TEXT,status TEXT NOT NULL,percent REAL,ext TEXT,size INTEGER,received INTEGER NOT NULL DEFAULT 0,error TEXT,created INTEGER NOT NULL,expires INTEGER,claim INTEGER)",
        params![],
    )?;
    Database.execute(
        "CREATE INDEX IF NOT EXISTS downloads_user ON downloads(user_id,created)",
        params![],
    )?;
    Ok(Response::text("ready"))
}

fn part(id: &str) -> String {
    format!("downloads/{id}.part")
}
fn finished(id: &str) -> String {
    format!("downloads/{id}")
}
fn forget_files(id: &str) {
    let _ = Storage.file(part(id)).delete();
    let _ = Storage.file(finished(id)).delete();
}

fn owned(id: &str, user: &str) -> Result<Download> {
    Database
        .fetch_optional(
            &format!("SELECT {COLUMNS} FROM downloads WHERE id=? AND user_id=?"),
            params![id, user],
        )?
        .ok_or_else(|| Error::not_found("That download isn't in your list."))
}

/// Marks a download in progress as failed. One the person cancelled stays cancelled.
fn mark_failed(id: &str, message: &str) -> Result<()> {
    Database.execute(
        &format!("UPDATE downloads SET status='failed',error=?,percent=NULL,claim=NULL WHERE id=? AND status IN {ACTIVE}"),
        params![message, id],
    )?;
    Ok(())
}

/// Downloads in progress for longer than this are given up on: the worker keeps
/// uncollected files for six hours, so anything older has stopped for good.
const STALE_SECONDS: i64 = 12 * 3600;
/// A download that never got a worker job (the start was interrupted).
const UNSTARTED_SECONDS: i64 = 600;

/// Fails downloads that stopped making progress, such as when the worker is gone.
fn give_up(user: Option<&str>) -> Result<()> {
    let now = now();
    let stuck: Vec<Id> = Database.fetch_all(
        &format!("SELECT id FROM downloads WHERE status IN {ACTIVE} AND (created<? OR (job IS NULL AND created<?)) AND (?3 IS NULL OR user_id=?3)"),
        params![now - STALE_SECONDS, now - UNSTARTED_SECONDS, user],
    )?;
    for Id { id } in stuck {
        forget_files(&id);
        mark_failed(&id, "The download stopped making progress. Start it again.")?;
    }
    Ok(())
}

/// Brings a download the worker is still making up to date.
fn refresh(row: &Download) -> Result<()> {
    if !matches!(row.status.as_str(), "downloading" | "processing") {
        return Ok(());
    }
    let Some(job) = &row.job else { return Ok(()) };
    let view = match worker("GET", &format!("/jobs/{job}"), None) {
        Ok(view) => view,
        Err(error) if error.status() == 404 => {
            return mark_failed(
                &row.id,
                "The worker restarted before this finished. Start it again.",
            );
        }
        // The worker may just be restarting; try again on the next check.
        Err(_) => return Ok(()),
    };
    match view["status"].as_str() {
        Some(status @ ("downloading" | "processing")) => {
            Database.execute(
                "UPDATE downloads SET status=?,percent=? WHERE id=? AND status IN ('downloading','processing')",
                params![status, view["percent"].as_f64(), row.id],
            )?;
        }
        Some("ready") => {
            Database.execute(
                "UPDATE downloads SET status='collecting',percent=0,size=?,ext=? WHERE id=? AND status IN ('downloading','processing')",
                params![view["size"].as_i64(), view["ext"].as_str().unwrap_or("bin"), row.id],
            )?;
        }
        Some("cancelled") => {
            Database.execute(
                "UPDATE downloads SET status='cancelled',percent=NULL WHERE id=? AND status IN ('downloading','processing')",
                params![row.id],
            )?;
        }
        _ => {
            let message = view["error"].as_str().unwrap_or("The download failed.");
            mark_failed(&row.id, message)?;
        }
    }
    Ok(())
}

/// Copies a finished file from the worker into the app's storage, a range at a
/// time, for up to [`COLLECT_SECONDS`]. Whoever claims the row copies it, so the
/// page and the schedule never append to the same file at once.
fn collect(id: &str) -> Result<()> {
    let started = now();
    let claimed = Database.execute(
        "UPDATE downloads SET claim=? WHERE id=? AND status='collecting' AND (claim IS NULL OR claim<?)",
        params![started + COLLECT_SECONDS + 30, id, started],
    )?;
    if claimed == 0 {
        return Ok(());
    }
    let result = copy(id, started);
    Database.execute("UPDATE downloads SET claim=NULL WHERE id=?", params![id])?;
    result
}

fn copy(id: &str, started: i64) -> Result<()> {
    let row: Download = Database.fetch_one(
        &format!("SELECT {COLUMNS} FROM downloads WHERE id=?"),
        params![id],
    )?;
    let (Some(job), Some(size)) = (&row.job, row.size) else {
        return mark_failed(id, "The worker didn't say how large the file is.");
    };
    let size = size as u64;
    let path = part(id);
    // What is already in storage is the truth, whatever an interrupted copy recorded.
    let mut received = match Storage.file(&path).info() {
        Ok(info) => info.size,
        Err(error) if error.status() == 404 => 0,
        Err(error) => return Err(error),
    };
    while received < size && now() - started < COLLECT_SECONDS {
        let end = (received + RANGE).min(size) - 1;
        let fetched = match call(
            "GET",
            &format!("/jobs/{job}/file"),
            None,
            Some((received, end)),
        ) {
            Ok(fetched) => fetched,
            Err(error) if error.status() == 404 => {
                forget_files(id);
                return mark_failed(
                    id,
                    "The worker no longer has this file. Start the download again.",
                );
            }
            Err(error) => return Err(error),
        };
        if fetched.body.len() as u64 != end - received + 1 {
            return Ok(()); // A short read; the next call carries on from the file's size.
        }
        for piece in fetched.body.chunks(PIECE) {
            if let Err(error) = Storage.append(&path, piece) {
                forget_files(id);
                let _ = call("DELETE", &format!("/jobs/{job}"), None, None);
                return mark_failed(
                    id,
                    if matches!(error.status(), 413 | 429 | 507) {
                        "There isn't enough room in Media Downloader's storage on LOKI. Save or remove other files, or ask the owner to raise the app's storage quota."
                    } else {
                        "The file couldn't be saved to LOKI."
                    },
                );
            }
        }
        received = end + 1;
        let still = Database.execute(
            "UPDATE downloads SET received=?,percent=? WHERE id=? AND status='collecting'",
            params![received as i64, received as f64 * 100.0 / size as f64, id],
        )?;
        if still == 0 {
            // Cancelled meanwhile: drop what this copy wrote after the cancel.
            forget_files(id);
            return Ok(());
        }
    }
    if received >= size {
        Storage.rename(&path, &finished(id), true)?;
        let keep = i64::from(config()?.keep_minutes) * 60;
        let ready = Database.execute(
            "UPDATE downloads SET status='ready',percent=100,received=?,expires=? WHERE id=? AND status='collecting'",
            params![size as i64, now() + keep, id],
        )?;
        if ready == 0 {
            forget_files(id);
        }
        let _ = call("DELETE", &format!("/jobs/{job}"), None, None);
    }
    Ok(())
}

/// Removes saved files whose time is up.
fn expire() -> Result<()> {
    let ids: Vec<Id> = Database.fetch_all(
        "SELECT id FROM downloads WHERE status='ready' AND expires<?",
        params![now()],
    )?;
    for Id { id } in ids {
        forget_files(&id);
        Database.execute(
            "UPDATE downloads SET status='expired',percent=NULL WHERE id=?",
            params![id],
        )?;
    }
    Ok(())
}

#[derive(Deserialize)]
struct Id {
    id: String,
}

// ---- The page's API ----------------------------------------------------------------

/// Answers JSON either way, so the page can always show the message.
fn api(result: Result<Value>) -> Result<Response> {
    match result {
        Ok(value) => Ok(Response::json(&value)?.header("cache-control", "no-store")?),
        Err(error) => {
            let message = if error.status() == 500 {
                "Something went wrong. Try again."
            } else {
                error.message()
            };
            Ok(Response::json(&json!({"error": message}))?.with_status(error.status()))
        }
    }
}

fn inspect(mut request: Request) -> Result<Response> {
    request.context().require_user()?;
    #[derive(Deserialize)]
    struct Input {
        url: String,
    }
    let input: Input = request.json(4096)?;
    api(worker("POST", "/inspect", Some(&json!({"url": input.url}))))
}

fn start(mut request: Request) -> Result<Response> {
    let user = request.context().require_user()?.id.clone();
    #[derive(Deserialize)]
    struct Input {
        url: String,
        choice: String,
        title: String,
        quality: String,
        #[serde(default)]
        site: String,
    }
    let input: Input = request.json(8192)?;
    api((|| {
        let clean = |text: &str, limit: usize| -> String {
            text.chars()
                .filter(|c| !c.is_control())
                .take(limit)
                .collect::<String>()
                .trim()
                .to_owned()
        };
        if input.url.len() > 2048 || input.choice.len() > 16 {
            return Err(Error::bad("Look up the link again, then choose a quality."));
        }
        let id = loki_sdk::random_id();
        let title = Some(clean(&input.title, 300))
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| "Untitled".into());
        // Counting and inserting in one statement, so two requests at once can't
        // both slip under the limit. The row waits for its worker job below.
        let reserved = Database.execute(
            &format!("INSERT INTO downloads(id,user_id,url,title,site,quality,status,received,created) SELECT ?1,?2,?3,?4,?5,?6,'downloading',0,?7 WHERE (SELECT count(*) FROM downloads WHERE user_id=?2 AND status IN {ACTIVE})<?8"),
            params![id, user, input.url, title, clean(&input.site, 80), clean(&input.quality, 40), now(), ACTIVE_PER_PERSON],
        )?;
        if reserved == 0 {
            return Err(Error::new(
                429,
                format!(
                    "You already have {ACTIVE_PER_PERSON} downloads in progress. Wait for one to finish."
                ),
            ));
        }
        let job = worker(
            "POST",
            "/jobs",
            Some(&json!({"url": input.url, "choice": input.choice})),
        )
        .and_then(|job| {
            job["id"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::new(502, "The worker didn't start the download."))
        });
        let job = match job {
            Ok(job) => job,
            Err(error) => {
                Database.execute("DELETE FROM downloads WHERE id=?", params![id])?;
                return Err(error);
            }
        };
        Database.execute("UPDATE downloads SET job=? WHERE id=?", params![job, id])?;
        Ok(serde_json::to_value(owned(&id, &user)?)?)
    })())
}

fn list(request: Request) -> Result<Response> {
    let user = request.context().require_user()?.id.clone();
    api((|| {
        expire()?;
        give_up(Some(&user))?;
        let active: Vec<Download> = Database.fetch_all(
            &format!("SELECT {COLUMNS} FROM downloads WHERE user_id=? AND status IN ('downloading','processing')"),
            params![user],
        )?;
        for row in &active {
            refresh(row)?;
        }
        let items: Vec<Download> = Database.fetch_all(
            &format!(
                "SELECT {COLUMNS} FROM downloads WHERE user_id=? ORDER BY created DESC LIMIT 100"
            ),
            params![user],
        )?;
        Ok(json!({"items": items, "now": now(), "keep_minutes": config()?.keep_minutes}))
    })())
}

fn collect_one(request: Request) -> Result<Response> {
    let user = request.context().require_user()?.id.clone();
    let id = request.param("id")?.to_owned();
    api((|| {
        owned(&id, &user)?;
        collect(&id)?;
        Ok(serde_json::to_value(owned(&id, &user)?)?)
    })())
}

fn remove(request: Request) -> Result<Response> {
    let user = request.context().require_user()?.id.clone();
    let id = request.param("id")?.to_owned();
    api((|| {
        let row = owned(&id, &user)?;
        if let Some(job) = &row.job
            && matches!(
                row.status.as_str(),
                "downloading" | "processing" | "collecting"
            )
        {
            let _ = call("DELETE", &format!("/jobs/{job}"), None, None);
        }
        forget_files(&id);
        if matches!(
            row.status.as_str(),
            "downloading" | "processing" | "collecting"
        ) {
            Database.execute(
                "UPDATE downloads SET status='cancelled',percent=NULL,claim=NULL WHERE id=?",
                params![id],
            )?;
        } else {
            Database.execute("DELETE FROM downloads WHERE id=?", params![id])?;
        }
        Ok(json!({"ok": true}))
    })())
}

fn clear(request: Request) -> Result<Response> {
    let user = request.context().require_user()?.id.clone();
    api((|| {
        let ids: Vec<Id> = Database.fetch_all(
            &format!("SELECT id FROM downloads WHERE user_id=? AND status NOT IN {ACTIVE}"),
            params![user],
        )?;
        for Id { id } in &ids {
            forget_files(id);
        }
        Database.execute(
            &format!("DELETE FROM downloads WHERE user_id=? AND status NOT IN {ACTIVE}"),
            params![user],
        )?;
        Ok(json!({"ok": true, "removed": ids.len()}))
    })())
}

fn export(request: Request) -> Result<Response> {
    let user = request.context().require_user()?.id.clone();
    let items: Vec<Download> = Database.fetch_all(
        &format!("SELECT {COLUMNS} FROM downloads WHERE user_id=? ORDER BY created DESC"),
        params![user],
    )?;
    let history: Vec<Value> = items
        .into_iter()
        .map(|d| json!({"title": d.title, "url": d.url, "site": d.site, "quality": d.quality, "status": d.status, "size": d.size, "created": d.created}))
        .collect();
    Response::json(&json!({"downloads": history}))?.header(
        "content-disposition",
        "attachment; filename=\"media-downloads.json\"",
    )
}

/// A finished file, which LOKI streams itself, with resuming and no time limit.
fn file(request: Request) -> Result<Response> {
    let user = request.context().require_user()?.id.clone();
    let row = owned(request.param("id")?, &user)?;
    if row.status != "ready" || row.expires.is_none_or(|expires| expires < now()) {
        return Err(Error::not_found(
            "This file has expired. Download it again.",
        ));
    }
    let name = filename(&row.title, row.ext.as_deref().unwrap_or("bin"));
    Storage.file(finished(&row.id)).download(&name)
}

/// A safe file name from a title: no path characters, and not too long.
fn filename(title: &str, ext: &str) -> String {
    let stem: String = title
        .chars()
        .map(|c| {
            if c.is_control() || r#"\/:*?"<>|"#.contains(c) {
                '_'
            } else {
                c
            }
        })
        .take(120)
        .collect();
    let stem = stem.trim().trim_matches('.');
    let ext: String = ext
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(8)
        .collect();
    format!(
        "{}.{}",
        if stem.is_empty() { "download" } else { stem },
        if ext.is_empty() { "bin" } else { &ext }
    )
}

/// Every minute: expire old files, follow downloads in progress, and keep copying
/// finished ones even when nobody has the page open.
fn maintain(_: Request) -> Result<Response> {
    expire()?;
    give_up(None)?;
    if config()?.key.is_empty() {
        return Ok(Response::text("not set up"));
    }
    let active: Vec<Download> = Database.fetch_all(
        &format!(
            "SELECT {COLUMNS} FROM downloads WHERE status IN ('downloading','processing') LIMIT 50"
        ),
        params![],
    )?;
    for row in &active {
        refresh(row)?;
    }
    let pending: Vec<Id> = Database.fetch_all(
        "SELECT id FROM downloads WHERE status='collecting' ORDER BY created LIMIT 5",
        params![],
    )?;
    let started = now();
    for Id { id } in pending {
        if now() - started >= COLLECT_SECONDS {
            break;
        }
        collect(&id)?;
    }
    Ok(Response::text("ok"))
}

// ---- The page --------------------------------------------------------------------

#[derive(askama::Template)]
#[template(path = "page.html")]
struct Page<'a> {
    csrf: &'a str,
    name: &'a str,
}

fn page(request: Request) -> Result<Response> {
    let ctx = request.context();
    let user = ctx.require_user()?;
    let name = if user.name.trim().is_empty() {
        &user.username
    } else {
        &user.name
    };
    secure(Response::render(Page {
        csrf: ctx.csrf_token(),
        name,
    })?)
}

fn script(_: Request) -> Result<Response> {
    secure(
        Response::text(include_str!("app.js"))
            .header("content-type", "text/javascript; charset=utf-8")?,
    )
}

fn style(_: Request) -> Result<Response> {
    secure(
        Response::text(include_str!("app.css"))
            .header("content-type", "text/css; charset=utf-8")?,
    )
}

fn secure(response: Response) -> Result<Response> {
    response
        .header("content-security-policy", POLICY)?
        .header("x-content-type-options", "nosniff")?
        .header("referrer-policy", "no-referrer")?
        .header("cache-control", "no-cache")
}

// ---- The owner's panel -------------------------------------------------------------

fn panel(request: Request) -> Result<Response> {
    let ctx = request.context();
    let user = ctx.require_user()?;
    let admin = user.has_role("admin");
    let mut notice = None;
    let mut error = None;
    if let Some(action) = &ctx.panel_action {
        if !admin {
            return PanelResponse::error(
                Panel::new(),
                "Only the app's admins can change these settings.",
            )
            .respond();
        }
        match panel_action(action) {
            Ok(text) => notice = Some(text),
            Err(failure) => error = Some(failure.message().to_owned()),
        }
    }
    let panel = if admin {
        admin_panel()?
    } else {
        Panel::new().section(
            Section::new("Media Downloader")
                .text("Open Media Downloader from your LOKI dashboard to save videos and audio."),
        )
    };
    match error {
        Some(error) => PanelResponse::error(panel, error).respond(),
        None => panel.respond_with(notice.as_deref()),
    }
}

fn panel_action(action: &loki_sdk::PanelAction) -> Result<String> {
    let field = |key: &str| {
        action
            .fields
            .get(key)
            .map(|value| value.trim())
            .unwrap_or_default()
    };
    match action.action.as_str() {
        "save-key" => {
            let key = field("key");
            if key.len() != 64 || !key.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(Error::bad(
                    "Paste the 64-character key the worker prints when it starts.",
                ));
            }
            let mut config = config()?;
            config.key = key.to_ascii_lowercase();
            Storage.set_json("config", &config)?;
            match worker("GET", "/health", None) {
                Ok(_) => Ok("Connected to the worker.".into()),
                Err(failure) => Err(Error::bad(format!(
                    "Saved the key, but the worker didn't accept it yet: {}",
                    failure.message()
                ))),
            }
        }
        "update" => {
            let answer = worker("POST", "/update", None)?;
            Ok(if answer["started"] == true {
                "Updating yt-dlp. Use Check again in a minute to see how it went.".into()
            } else {
                "An update is already running.".into()
            })
        }
        "refresh" => Ok("Checked the worker again.".into()),
        "limits" => {
            let number = |key: &str, label: &str| -> Result<u32> {
                field(key)
                    .parse()
                    .map_err(|_| Error::bad(format!("Enter a whole number for {label}.")))
            };
            let keep = number("keep_minutes", "how long files stay ready")?;
            if !(5..=1440).contains(&keep) {
                return Err(Error::bad("Keep finished files for 5 to 1440 minutes."));
            }
            worker(
                "PUT",
                "/settings",
                Some(&json!({
                    "max_minutes": number("max_minutes", "the longest video")?,
                    "max_file_mib": number("max_file_mib", "the largest file")?,
                    "max_total_mib": number("max_total_mib", "the worker's space")?,
                    "max_active": number("max_active", "downloads at once")?,
                })),
            )?;
            let mut config = config()?;
            config.keep_minutes = keep;
            Storage.set_json("config", &config)?;
            Ok("Limits saved.".into())
        }
        _ => Err(Error::bad("Unknown action.")),
    }
}

fn admin_panel() -> Result<Panel> {
    let config = config()?;
    let setup = |section: Section| {
        section
            .text("1. Install yt-dlp on the server. Add ffmpeg to offer 1080p and higher, and Deno for YouTube.")
            .text("2. Run media-downloader-worker on the same computer as LOKI. It prints a key when it starts; media-downloader-worker key prints it again.")
            .text("3. Paste the key here.")
            .form("save-key", [Field::new("key", "Worker key", FieldKind::Text)], "Connect")
    };
    if config.key.is_empty() {
        return Ok(Panel::new().section(setup(
            Section::new("Set up").warning("Media Downloader isn't connected to its worker yet."),
        )));
    }
    let status = match worker("GET", "/status", None) {
        Ok(status) => status,
        Err(failure) => {
            return Ok(Panel::new().section(setup(
                Section::new("Worker")
                    .warning(failure.message())
                    .buttons([Action::new("refresh", "Check again").primary()]),
            )));
        }
    };
    let tools = &status["tools"];
    let version = |tool: &Value| -> Option<String> {
        tool["version"]
            .as_str()
            .map(|v| v.split('-').next().unwrap_or(v).to_owned())
    };
    let yt_dlp = version(&tools["yt_dlp"]);
    let latest = tools["yt_dlp_latest"].as_str();
    let outdated = tools["update_available"] == true;
    let mut worker_section = Section::new("Worker").stats([
        (
            "Worker",
            status["version"].as_str().unwrap_or("?").to_owned(),
        ),
        (
            "yt-dlp",
            yt_dlp.clone().unwrap_or_else(|| "Not installed".into()),
        ),
        ("Newest yt-dlp", latest.unwrap_or("Unknown").to_owned()),
        (
            "ffmpeg",
            version(&tools["ffmpeg"]).unwrap_or_else(|| "Not installed".into()),
        ),
        (
            "Deno",
            version(&tools["deno"]).unwrap_or_else(|| "Not installed".into()),
        ),
        (
            "Downloading now",
            status["active"].as_u64().unwrap_or(0).to_string(),
        ),
    ]);
    if yt_dlp.is_none() {
        worker_section = worker_section.warning("yt-dlp isn't installed on the server, or the worker can't find it on its PATH. Install it, then use Check again.");
    } else if outdated {
        worker_section = worker_section.warning(format!(
            "yt-dlp {} is available. Sites change often, so keep yt-dlp up to date.",
            latest.unwrap_or("")
        ));
    } else if latest.is_some() {
        worker_section = worker_section.muted("yt-dlp is up to date.");
    }
    match tools["update"]["running"].as_bool() {
        Some(true) => worker_section = worker_section.text("Updating yt-dlp…"),
        Some(false) => {
            let output = tools["update"]["output"].as_str().unwrap_or_default();
            let last = output.lines().last().unwrap_or("Done.");
            worker_section = if tools["update"]["ok"] == true {
                worker_section.muted(format!("Last update: {last}"))
            } else {
                worker_section.warning(format!("The last update didn't work: {last}"))
            };
        }
        None => {}
    }
    if tools["ffmpeg"].is_null() {
        worker_section = worker_section.muted("Without ffmpeg, only files that already have sound and picture together are offered. On YouTube that's 360p.");
    }
    if tools["deno"].is_null() {
        worker_section =
            worker_section.muted("YouTube needs Deno installed for yt-dlp to read its videos.");
    }
    let mut buttons = Vec::new();
    if yt_dlp.is_some() {
        let update =
            Action::new("update", "Update yt-dlp").confirm("Update yt-dlp on the server now?");
        buttons.push(if outdated { update.primary() } else { update });
    }
    buttons.push(Action::new("refresh", "Check again"));
    worker_section = worker_section.buttons(buttons);

    let limits = &status["limits"];
    let number = |key: &str| limits[key].as_u64().unwrap_or(0);
    let limits_section = Section::new("Limits").form(
        "limits",
        [
            Field::new("max_minutes", "Longest video (minutes)", FieldKind::Number)
                .value(number("max_minutes")),
            Field::new("max_file_mib", "Largest file (MB)", FieldKind::Number)
                .value(number("max_file_mib")),
            Field::new("max_active", "Downloads at once", FieldKind::Number)
                .value(number("max_active")),
            Field::new(
                "max_total_mib",
                "Worker space for downloads in progress (MB)",
                FieldKind::Number,
            )
            .value(number("max_total_mib")),
            Field::new(
                "keep_minutes",
                "Keep finished files ready for (minutes)",
                FieldKind::Number,
            )
            .value(config.keep_minutes),
        ],
        "Save limits",
    );
    let usage = Storage.usage()?;
    let megabytes = |bytes: u64| format!("{} MB", bytes / (1024 * 1024));
    let storage = Section::new("Storage on LOKI")
        .stats([("Ready files", megabytes(usage.bytes)), ("Quota", megabytes(usage.limit_bytes))])
        .muted("Finished files wait here until they're saved or expire. Raise the quota from Manage in Applications if large files don't fit.");
    let key = Section::new("Worker key")
        .muted("If you made a new key with media-downloader-worker new-key, paste it here.")
        .form(
            "save-key",
            [Field::new("key", "Worker key", FieldKind::Text)],
            "Replace key",
        );
    Ok(Panel::new()
        .section(worker_section)
        .section(limits_section)
        .section(storage)
        .section(key))
}

fn handle(request: Request) -> Result<Response> {
    Router::new()
        .get("/health", health)
        .get("/", page)
        .get("/app.js", script)
        .get("/app.css", style)
        .post("/api/inspect", inspect)
        .get("/api/downloads", list)
        .post("/api/downloads", start)
        .route(loki_sdk::Method::Delete, "/api/downloads", clear)
        .get("/api/downloads/export", export)
        .post("/api/downloads/{id}/collect", collect_one)
        .route(loki_sdk::Method::Delete, "/api/downloads/{id}", remove)
        .get("/files/{id}", file)
        .panel(panel)
        .schedule("maintain", maintain)
        .handle(request)
}

loki_sdk::export!(handle);

#[cfg(test)]
mod tests {
    use super::filename;

    #[test]
    fn file_names_lose_path_characters() {
        assert_eq!(filename("Big Buck Bunny", "mp4"), "Big Buck Bunny.mp4");
        assert_eq!(filename("a/b\\c:d*?\"<>|", "mkv"), "a_b_c_d______.mkv");
        assert_eq!(filename("...", "mp4"), "download.mp4");
        assert_eq!(filename("x", "m4a/../"), "x.m4a");
        assert_eq!(filename(&"y".repeat(400), "mp3").len(), 124);
    }
}
