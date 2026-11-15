use super::{GRACE, HISTORY_DAYS, Live, MIB, Probed, capacity_error, file, video_id, worker};
use crate::{Error, Result, State, id, now};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State as ExtractState,
    http::{HeaderMap, Method, Request, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    io::{Read, Seek},
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio_util::sync::CancellationToken;

pub(crate) fn router(state: State) -> Router {
    Router::new().fallback(serve).with_state(state)
}

async fn serve(
    ExtractState(state): ExtractState<State>,
    request: Request<Body>,
) -> Result<Response> {
    if state.0.stopping.load(Ordering::Acquire) {
        return Err(super::available_error());
    }
    let headers = request.headers();
    if headers
        .get(header::HOST)
        .is_none_or(|host| host != crate::ADDRESS)
        || headers.contains_key(header::ORIGIN)
        || !headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|given| given.as_bytes().ct_eq(state.0.key.as_bytes()).into())
    {
        return Err(Error::forbidden());
    }
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    if method == Method::GET && path == "/health" {
        return Ok(headers_for("ready".into_response()));
    }
    let user = request
        .headers()
        .get("x-loki-account")
        .and_then(|value| value.to_str().ok())
        .filter(|user| uuid::Uuid::parse_str(user).is_ok_and(|uuid| uuid.to_string() == *user))
        .ok_or_else(Error::forbidden)?
        .to_owned();
    let user = user.as_str();
    let response = match (method, path.as_str()) {
        (Method::GET, "/api/history") => Json(history(&state, user).await?).into_response(),
        (Method::GET, "/api/history/export") => (
            [(
                header::CONTENT_DISPOSITION,
                "attachment; filename=download-history.json",
            )],
            Json(history(&state, user).await?),
        )
            .into_response(),
        (Method::DELETE, "/api/history") => {
            sqlx::query(
                "DELETE FROM youtube_jobs WHERE user_id=? AND status NOT IN('ready','downloading')",
            )
            .bind(user)
            .execute(&state.0.db)
            .await?;
            Json(json!({"ok":true})).into_response()
        }
        (Method::POST, "/api/inspect") => {
            #[derive(Deserialize)]
            struct Inspect {
                url: String,
            }
            let input: Inspect = body(request).await?;
            let config = &state.0.settings;
            let video_id = video_id(&input.url)?;
            state.rate("probe", user)?;
            let video = worker::probe(&state, &video_id, config).await?;
            let key = id();
            let response = json!({"selection":key,"title":video.title,"duration":video.duration,"formats":video.formats});
            let mut buffers = state.0.youtube.buffers.lock().unwrap();
            buffers.retain(|_, item| item.user != user && item.expires > now());
            if buffers.len() >= 128 {
                return Err(capacity_error());
            }
            buffers.insert(
                key,
                Probed {
                    user: user.to_owned(),
                    video,
                    expires: now() + 120,
                },
            );
            Json(response).into_response()
        }
        (Method::POST, "/api/downloads") => {
            let input = body(request).await?;
            Json(start_download(&state, user, input).await?).into_response()
        }
        (Method::GET, path) if path.starts_with("/download/") => {
            attachment(&state, user, &path["/download/".len()..], request.headers()).await?
        }
        _ => return Err(Error::not_found()),
    };
    Ok(headers_for(response))
}

fn headers_for(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("private, no-store"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        header::HeaderValue::from_static("no-referrer"),
    );
    headers.entry(header::CONTENT_SECURITY_POLICY).or_insert(header::HeaderValue::from_static("default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"));
    response
}

async fn body<T: serde::de::DeserializeOwned>(request: Request<Body>) -> Result<T> {
    let bytes = to_bytes(request.into_body(), 4096)
        .await
        .map_err(|_| Error::bad("Request is too large."))?;
    serde_json::from_slice(&bytes).map_err(|_| Error::bad("Invalid downloader request."))
}

async fn history(state: &State, user: &str) -> Result<Value> {
    use sqlx::Row;
    let now = now();
    let rows = sqlx::query("SELECT id,video_id,title,quality,size,created,status,expires FROM youtube_jobs WHERE user_id=? AND created>=? ORDER BY created DESC,rowid DESC LIMIT 500")
        .bind(user).bind(now-HISTORY_DAYS*86400).fetch_all(&state.0.db).await?;
    let items: Vec<_> = rows.into_iter().map(|row| {
        let expiry: Option<i64> = row.get("expires");
        let mut status: String = row.get("status");
        if status=="ready" && expiry.is_some_and(|when|when<=now) { status="expired".into(); }
        json!({"id":row.get::<String,_>("id"),"video_id":row.get::<String,_>("video_id"),"title":row.get::<String,_>("title"),"quality":row.get::<String,_>("quality"),"size":row.get::<Option<i64>,_>("size"),"created":row.get::<i64,_>("created"),"status":status,"expires":expiry})
    }).collect();
    Ok(json!({"items":items,"now":now}))
}

#[derive(Deserialize)]
struct Download {
    selection: String,
    format: Option<String>,
}

async fn start_download(state: &State, user: &str, input: Download) -> Result<Value> {
    let state = state.clone();
    let user = user.to_owned();
    tokio::spawn(async move { reserve_download(&state, &user, input).await }).await?
}

async fn reserve_download(state: &State, user: &str, input: Download) -> Result<Value> {
    let _updates = state.0.youtube.maintenance.lock().await;
    if state.0.stopping.load(Ordering::Acquire) {
        return Err(super::available_error());
    }
    let config = state.0.settings.clone();
    state.rate("download", user)?;
    let video = state
        .0
        .youtube
        .buffers
        .lock()
        .unwrap()
        .get(&input.selection)
        .filter(|entry| entry.user == user && entry.expires > now())
        .map(|entry| entry.video.clone())
        .ok_or_else(|| Error::bad("Look up the video again. The format selection expired."))?;
    if video.duration > f64::from(config.duration_minutes) * 60.0 {
        return Err(Error::bad("The video exceeds the current duration limit."));
    }
    let format = video
        .formats
        .into_iter()
        .find(|format| {
            input.format.as_ref().is_none_or(|id| id == &format.id)
                && format
                    .size
                    .is_none_or(|bytes| bytes <= u64::from(config.file_mib) * MIB)
        })
        .ok_or_else(|| Error::bad("This quality is unavailable under the current limits."))?;
    let max = u64::from(config.file_mib) * MIB;
    let id = id();
    let cancellation = CancellationToken::new();
    {
        let mut slots = state.0.youtube.slots.lock().unwrap();
        if slots.len() >= 128
            || slots.values().filter(|job| job.expiry.is_none()).count() >= 3
            || slots
                .values()
                .any(|job| job.user == user && job.expiry.is_none())
            || slots.values().map(|job| job.bytes).sum::<u64>() + max
                > u64::from(config.total_mib) * MIB
        {
            return Err(capacity_error());
        }
        slots.insert(
            id.clone(),
            Live {
                user: user.to_owned(),
                bytes: max,
                expiry: None,
                active: 0,
                cancel: cancellation.clone(),
                streams: Vec::new(),
            },
        );
    }
    let quality = format.quality();
    let result = sqlx::query("INSERT INTO youtube_jobs(id,user_id,video_id,title,quality,created,status) VALUES(?,?,?,?,?,?,'downloading')")
        .bind(&id).bind(user).bind(&video.id).bind(&video.title).bind(quality).bind(now()).execute(&state.0.db).await;
    if let Err(error) = result {
        state.0.youtube.slots.lock().unwrap().remove(&id);
        return Err(error.into());
    }
    state
        .0
        .youtube
        .buffers
        .lock()
        .unwrap()
        .remove(&input.selection);
    let job_id = id.clone();
    let state = state.clone();
    tokio::spawn(async move {
        let path = file(&state.0.data, &job_id);
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => None,
            result = tokio::time::timeout(Duration::from_secs(600),worker::download(&format,&path,max)) => result.ok(),
        };
        let _updates = state.0.youtube.maintenance.lock().await;
        if let Some(Ok(bytes)) = result.as_ref().filter(|_| !cancellation.is_cancelled()) {
            let expiry = now() + i64::from(config.pickup_seconds);
            let updated =
                sqlx::query("UPDATE youtube_jobs SET size=?,status='ready',expires=? WHERE id=?")
                    .bind(*bytes as i64)
                    .bind(expiry)
                    .bind(&job_id)
                    .execute(&state.0.db)
                    .await;
            if updated.is_ok() {
                if let Some(job) = state.0.youtube.slots.lock().unwrap().get_mut(&job_id) {
                    job.bytes = *bytes;
                    job.expiry = Some(expiry);
                }
                return;
            }
        }
        let deleted = tokio::fs::remove_file(path).await;
        if deleted.is_ok()
            || deleted
                .as_ref()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            state.0.youtube.slots.lock().unwrap().remove(&job_id);
        } else if let Some(job) = state.0.youtube.slots.lock().unwrap().get_mut(&job_id) {
            job.expiry = Some(now());
        }
        let status = if result.is_some_and(|result| {
            result.is_err_and(|error| error.status == StatusCode::PAYLOAD_TOO_LARGE)
        }) {
            "too_large"
        } else {
            "failed"
        };
        let _ = sqlx::query("UPDATE youtube_jobs SET status=? WHERE id=?")
            .bind(status)
            .bind(&job_id)
            .execute(&state.0.db)
            .await;
    });
    Ok(json!({"id":id}))
}

struct Transfer {
    state: State,
    id: String,
}

pub(super) struct TransferFile(Mutex<Option<std::fs::File>>);

impl TransferFile {
    pub(super) fn close(&self) {
        self.0.lock().unwrap().take();
    }

    async fn read(self: Arc<Self>, limit: usize) -> std::io::Result<bytes::Bytes> {
        tokio::task::spawn_blocking(move || {
            let mut file = self.0.lock().unwrap();
            let Some(file) = file.as_mut() else {
                return Ok(bytes::Bytes::new());
            };
            let mut bytes = vec![0; limit];
            let count = file.read(&mut bytes)?;
            bytes.truncate(count);
            Ok(bytes::Bytes::from(bytes))
        })
        .await?
    }
}

impl Drop for Transfer {
    fn drop(&mut self) {
        if let Some(job) = self.state.0.youtube.slots.lock().unwrap().get_mut(&self.id) {
            job.active = job.active.saturating_sub(1);
        }
    }
}

async fn attachment(state: &State, user: &str, id: &str, headers: &HeaderMap) -> Result<Response> {
    if !uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.to_string() == id) {
        return Err(Error::not_found());
    }
    if headers
        .get("sec-fetch-site")
        .is_some_and(|value| value != "same-origin" && value != "none")
    {
        return Err(Error::forbidden());
    }
    let row: Option<(String, String, String)> =
        sqlx::query_as("SELECT title,quality,status FROM youtube_jobs WHERE id=? AND user_id=?")
            .bind(id)
            .bind(user)
            .fetch_optional(&state.0.db)
            .await?;
    let (title, quality, status) = row.ok_or_else(Error::not_found)?;
    let (expiry, cancel) = {
        let mut slots = state.0.youtube.slots.lock().unwrap();
        let job = slots
            .get_mut(id)
            .filter(|job| job.user == user && !job.cancel.is_cancelled())
            .ok_or_else(gone)?;
        let expiry = job
            .expiry
            .filter(|expiry| now() < *expiry && status == "ready")
            .ok_or_else(gone)?;
        if job.active >= 2 {
            return Err(capacity_error());
        }
        job.active += 1;
        (expiry, job.cancel.clone())
    };
    let guard = Transfer {
        state: state.clone(),
        id: id.to_owned(),
    };
    let path = file(&state.0.data, id);
    if !tokio::fs::symlink_metadata(&path).await?.is_file() {
        return Err(gone());
    }
    let mut file = std::fs::File::open(path).map_err(|_| gone())?;
    let length = file.metadata()?.len();
    let (start, end, partial) = match headers.get(header::RANGE) {
        None => (0, length.saturating_sub(1), false),
        Some(value) => match value.to_str().ok().and_then(|value| range(value, length)) {
            Some((start, end)) => (start, end, true),
            None => {
                return Ok((
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    [(header::CONTENT_RANGE, format!("bytes */{length}"))],
                )
                    .into_response());
            }
        },
    };
    file.seek(std::io::SeekFrom::Start(start))?;
    let size = end - start + 1;
    let file = Arc::new(TransferFile(Mutex::new(Some(file))));
    {
        let mut slots = state.0.youtube.slots.lock().unwrap();
        let job = slots
            .get_mut(id)
            .filter(|job| !job.cancel.is_cancelled())
            .ok_or_else(gone)?;
        job.streams.push(Arc::downgrade(&file));
    }
    let ext = if quality.ends_with(" WEBM") {
        "webm"
    } else {
        "mp4"
    };
    let title: String = title
        .chars()
        .filter(|c| {
            !c.is_control() && !matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|')
        })
        .take(85)
        .collect();
    let filename = format!(
        "{}.{}",
        if title.trim().is_empty() {
            "youtube"
        } else {
            title.trim()
        },
        ext
    );
    let name = url::form_urlencoded::byte_serialize(filename.as_bytes()).collect::<String>();
    let mut response = Response::builder()
        .status(if partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        })
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, size)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"youtube.{ext}\"; filename*=UTF-8''{name}"),
        );
    if partial {
        response = response.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{length}"),
        );
    }
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs((expiry + GRACE - now()).max(0) as u64);
    let stream = futures_util::stream::unfold(
        (file, size, guard, cancel, deadline),
        |(file, left, guard, cancel, deadline)| async move {
            if left == 0 || guard.state.0.stopping.load(Ordering::Acquire) {
                return None;
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => None,
                _ = tokio::time::sleep_until(deadline) => None,
                chunk = file.clone().read(left.min(65536) as usize) => match chunk {
                    Ok(bytes) if !bytes.is_empty() => {
                        let next=left-bytes.len() as u64;
                        Some((Ok(bytes),(file,next,guard,cancel,deadline)))
                    }
                    Ok(_) => None,
                    Err(error) => Some((Err(error),(file,0,guard,cancel,deadline))),
                },
            }
        },
    );
    Ok(response.body(Body::from_stream(stream))?)
}

fn gone() -> Error {
    Error::new(
        StatusCode::GONE,
        "This download expired or is no longer available.",
    )
}

fn range(value: &str, length: u64) -> Option<(u64, u64)> {
    if length == 0 {
        return None;
    }
    let value = value.strip_prefix("bytes=")?;
    if value.contains(',') {
        return None;
    }
    let (start, end) = value.split_once('-')?;
    if start.is_empty() {
        let suffix: u64 = end.parse().ok()?;
        if suffix == 0 {
            return None;
        }
        return Some((length.saturating_sub(suffix), length - 1));
    }
    let start: u64 = start.parse().ok()?;
    let end = if end.is_empty() {
        length - 1
    } else {
        end.parse::<u64>().ok()?.min(length - 1)
    };
    if start >= length || end < start {
        return None;
    }
    Some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[tokio::test]
    async fn worker_requires_its_key_and_scopes_history_and_downloads_to_the_account() {
        let data = tempfile::tempdir().unwrap();
        let key = "a".repeat(64);
        let state = State::open(
            data.path().to_path_buf(),
            super::super::Settings::default(),
            key.clone(),
        )
        .await
        .unwrap();
        let alice = id();
        let bob = id();
        let first = id();
        let second = id();
        let expiry = now() + 60;
        for (job, user, title) in [
            (&first, &alice, "Alice's clip"),
            (&second, &bob, "Bob's clip"),
        ] {
            sqlx::query("INSERT INTO youtube_jobs(id,user_id,video_id,title,quality,size,created,status,expires) VALUES(?,?,'abcdefghijk',?,'360p MP4',13,?,'ready',?)")
                .bind(job).bind(user).bind(title).bind(now()).bind(expiry).execute(&state.0.db).await.unwrap();
            state.0.youtube.slots.lock().unwrap().insert(
                job.clone(),
                Live {
                    user: user.clone(),
                    bytes: 13,
                    expiry: Some(expiry),
                    active: 0,
                    cancel: CancellationToken::new(),
                    streams: Vec::new(),
                },
            );
            std::fs::write(file(data.path(), job), b"video content").unwrap();
        }
        let app = router(state.clone());
        let request = |path: &str, user: &str, authorized: bool| {
            let mut request = Request::builder()
                .uri(path)
                .header(header::HOST, crate::ADDRESS)
                .header("x-loki-account", user)
                .header(header::RANGE, "bytes=0-4");
            if authorized {
                request = request.header(header::AUTHORIZATION, format!("Bearer {key}"));
            }
            app.clone().oneshot(request.body(Body::empty()).unwrap())
        };
        assert_eq!(
            request("/api/history", &alice, false)
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        let history = request("/api/history/export", &alice, true).await.unwrap();
        assert_eq!(history.status(), StatusCode::OK);
        let history: Value =
            serde_json::from_slice(&to_bytes(history.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(history["items"].as_array().unwrap().len(), 1);
        assert_eq!(history["items"][0]["title"], "Alice's clip");
        assert_eq!(
            request(&format!("/download/{second}"), &alice, true)
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        let path = format!("/download/{first}");
        let active = request(&path, &alice, true).await.unwrap();
        assert_eq!(active.status(), StatusCode::PARTIAL_CONTENT);
        state
            .0
            .youtube
            .slots
            .lock()
            .unwrap()
            .get_mut(&first)
            .unwrap()
            .expiry = Some(now() - 1);
        assert_eq!(
            request(&path, &alice, true).await.unwrap().status(),
            StatusCode::GONE
        );
        assert_eq!(to_bytes(active.into_body(), 256).await.unwrap(), "video");
        let maintenance = tokio::spawn(super::super::maintain(state.clone()));
        tokio::time::timeout(Duration::from_secs(4), async {
            while file(data.path(), &first).exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(file(data.path(), &second).is_file());
        maintenance.abort();
        drop(app);
        drop(state);
        let reopened = State::open(
            data.path().to_path_buf(),
            super::super::Settings::default(),
            key,
        )
        .await
        .unwrap();
        assert!(!file(data.path(), &second).exists());
        let history = super::history(&reopened, &bob).await.unwrap();
        assert_eq!(history["items"][0]["status"], "expired");
    }
}
