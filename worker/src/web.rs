//! The worker's API. Only the LOKI app calls it, over loopback, with the shared key.
use crate::{ADDRESS, Error, Result, State, jobs, settings::Limits};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Request, State as Shared},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::Deserialize;
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub fn router(state: State) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/update", post(update))
        .route("/settings", put(limits))
        .route("/inspect", post(inspect))
        .route("/jobs", post(start))
        .route("/jobs/{id}", get(job).delete(remove))
        .route("/jobs/{id}/file", get(file))
        .layer(middleware::from_fn_with_state(state.clone(), guard))
        .with_state(state)
}

/// Requests must be addressed to the loopback port, come from no web page, and
/// carry the key the worker printed.
async fn guard(Shared(state): Shared<State>, request: Request, next: Next) -> Response {
    let headers = request.headers();
    let key = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    let allowed = headers.get(header::HOST).is_some_and(|host| host == ADDRESS)
        && !headers.contains_key(header::ORIGIN)
        && bool::from(key.as_bytes().ct_eq(state.0.key.as_bytes()));
    if !allowed {
        return Error::new(StatusCode::FORBIDDEN, "The worker key doesn't match. Paste the key the worker prints into the app's panel.").into_response();
    }
    let mut response = next.run(request).await;
    response.headers_mut().insert(header::CACHE_CONTROL, header::HeaderValue::from_static("no-store"));
    response
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true, "version": env!("CARGO_PKG_VERSION")}))
}

async fn status(Shared(state): Shared<State>) -> Json<Value> {
    let settings = state.0.settings.get();
    let tools = state.0.tools.status(&settings).await;
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "tools": tools,
        "limits": {
            "max_minutes": settings.max_minutes,
            "max_file_mib": settings.max_file_mib,
            "max_total_mib": settings.max_total_mib,
            "max_active": settings.max_active,
        },
        "active": state.0.jobs.active(),
        "used_bytes": state.0.jobs.usage(),
    }))
}

/// Starts updating yt-dlp; `/status` reports how it went.
async fn update(Shared(state): Shared<State>) -> Json<Value> {
    let started = state.0.tools.start_update(state.0.settings.get());
    Json(json!({"started": started}))
}

async fn limits(Shared(state): Shared<State>, Json(limits): Json<Limits>) -> Result<Json<Value>> {
    let settings = state.0.settings.set_limits(limits)?;
    Ok(Json(json!({"ok": true, "max_minutes": settings.max_minutes, "max_file_mib": settings.max_file_mib, "max_total_mib": settings.max_total_mib, "max_active": settings.max_active})))
}

#[derive(Deserialize)]
struct Link {
    url: String,
}

async fn inspect(Shared(state): Shared<State>, Json(link): Json<Link>) -> Result<Json<Value>> {
    Ok(Json(jobs::inspect(&state, &link.url).await?))
}

#[derive(Deserialize)]
struct Start {
    url: String,
    choice: String,
}

async fn start(Shared(state): Shared<State>, Json(input): Json<Start>) -> Result<Json<Value>> {
    let id = jobs::start(&state, &input.url, &input.choice).await?;
    Ok(Json(json!({"id": id})))
}

fn valid_id(id: &str) -> Result<()> {
    uuid::Uuid::parse_str(id)
        .ok()
        .filter(|uuid| uuid.to_string() == id)
        .map(|_| ())
        .ok_or_else(Error::not_found)
}

async fn job(Shared(state): Shared<State>, Path(id): Path<String>) -> Result<Json<jobs::View>> {
    valid_id(&id)?;
    Ok(Json(state.0.jobs.view(&id)?))
}

async fn remove(Shared(state): Shared<State>, Path(id): Path<String>) -> Result<Json<Value>> {
    valid_id(&id)?;
    state.0.jobs.remove(&id);
    Ok(Json(json!({"ok": true})))
}

/// The finished file, whole or one `bytes=START-END` range at a time.
async fn file(Shared(state): Shared<State>, Path(id): Path<String>, headers: HeaderMap) -> Result<Response> {
    valid_id(&id)?;
    let (path, size) = state.0.jobs.file(&id)?;
    let (start, end) = match headers.get(header::RANGE).and_then(|value| value.to_str().ok()) {
        Some(range) => range_of(range, size).ok_or_else(|| Error::new(StatusCode::RANGE_NOT_SATISFIABLE, "Invalid range."))?,
        None => (0, size.saturating_sub(1)),
    };
    let length = end - start + 1;
    let mut file = tokio::fs::File::open(&path).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let body = Body::from_stream(tokio_util::io::ReaderStream::with_capacity(file.take(length), 256 * 1024));
    let mut response = Response::new(body);
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/octet-stream"));
    headers.insert(header::CONTENT_LENGTH, length.into());
    if start != 0 || end + 1 != size {
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            header::HeaderValue::from_str(&format!("bytes {start}-{end}/{size}")).map_err(|_| Error::bad("Invalid range."))?,
        );
    }
    Ok(response)
}

/// One `bytes=START-END` or `bytes=START-` range within a file of `size` bytes.
fn range_of(range: &str, size: u64) -> Option<(u64, u64)> {
    let (start, end) = range.strip_prefix("bytes=")?.split_once('-')?;
    let start: u64 = start.trim().parse().ok()?;
    let end = match end.trim() {
        "" => size.checked_sub(1)?,
        end => end.parse::<u64>().ok()?.min(size.checked_sub(1)?),
    };
    (start <= end).then_some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    fn state() -> (tempfile::TempDir, State) {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().to_owned();
        let inner = crate::Inner {
            jobs: jobs::Jobs::open(&data).unwrap(),
            tools: Default::default(),
            settings: crate::settings::Store::open(&data).unwrap(),
            key: "a".repeat(64),
            data,
        };
        (dir, State(std::sync::Arc::new(inner)))
    }

    async fn call(state: &State, host: &str, key: &str, origin: bool) -> StatusCode {
        let mut request = HttpRequest::get("/health").header(header::HOST, host).header(header::AUTHORIZATION, format!("Bearer {key}"));
        if origin {
            request = request.header(header::ORIGIN, "https://evil.test");
        }
        router(state.clone()).oneshot(request.body(Body::empty()).unwrap()).await.unwrap().status()
    }

    #[tokio::test]
    async fn only_the_app_with_the_key_gets_in() {
        let (_dir, state) = state();
        let key = "a".repeat(64);
        assert_eq!(call(&state, ADDRESS, &key, false).await, StatusCode::OK);
        assert_eq!(call(&state, ADDRESS, &"b".repeat(64), false).await, StatusCode::FORBIDDEN);
        assert_eq!(call(&state, "localhost:44317", &key, false).await, StatusCode::FORBIDDEN);
        assert_eq!(call(&state, ADDRESS, &key, true).await, StatusCode::FORBIDDEN);
    }

    #[test]
    fn ranges_stay_inside_the_file() {
        assert_eq!(range_of("bytes=0-99", 1000), Some((0, 99)));
        assert_eq!(range_of("bytes=900-", 1000), Some((900, 999)));
        assert_eq!(range_of("bytes=900-5000", 1000), Some((900, 999)));
        assert_eq!(range_of("bytes=1000-", 1000), None);
        assert_eq!(range_of("bytes=-5", 1000), None);
        assert_eq!(range_of("items=0-1", 1000), None);
    }
}
