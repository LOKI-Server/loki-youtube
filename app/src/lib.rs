use loki_sdk::{__private::wasi, Context, Request, Response, Result};
use serde::Deserialize;
use wasi::http::{
    outgoing_handler,
    types::{
        ErrorCode, Fields, IncomingRequest, Method, OutgoingBody, OutgoingRequest,
        OutgoingResponse, RequestOptions, ResponseOutparam, Scheme,
    },
};
use wasi::io::streams::StreamError;

const WORKER: &str = "127.0.0.1:44317";
const POLICY: &str = "default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'";

#[derive(Deserialize)]
struct Config {
    #[serde(rename = "worker-key")]
    worker_key: String,
}

#[derive(askama::Template)]
#[template(path = "page.html")]
struct Page<'a> {
    csrf: &'a str,
    username: &'a str,
}

fn local(request: Request) -> Result<Response> {
    let ctx = request.context();
    let user = ctx.require_user()?;
    let response = match request.path() {
        "/" => Response::render(Page {
            csrf: ctx.csrf_token(),
            username: &user.username,
        })?,
        "/app.js" => Response::text(include_str!("app.js"))
            .header("content-type", "text/javascript; charset=utf-8")?,
        "/app.css" => Response::text(include_str!("app.css"))
            .header("content-type", "text/css; charset=utf-8")?,
        _ => return Ok(Response::text("Not found.").with_status(404)),
    };
    Ok(response
        .header("content-security-policy", POLICY)?
        .header("cache-control", "private, no-store")?
        .header("x-content-type-options", "nosniff")?
        .header("referrer-policy", "no-referrer")?)
}

struct App;

impl wasi::exports::http::incoming_handler::Guest for App {
    fn handle(incoming: IncomingRequest, out: ResponseOutparam) {
        let method = incoming.method();
        let path = incoming.path_with_query().unwrap_or_default();
        let health = matches!(method, Method::Get) && path == "/health";
        let private = match (&method, path.as_str()) {
            (Method::Get, "/api/history" | "/api/history/export")
            | (Method::Delete, "/api/history")
            | (Method::Post, "/api/inspect" | "/api/downloads") => true,
            (Method::Get, path) => path.strip_prefix("/download/").is_some_and(|id| {
                id.len() == 36
                    && id
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
            }),
            _ => false,
        };
        if !health && !private {
            return loki_sdk::__private::serve(incoming, out, local);
        }
        let Ok(ctx) = Context::current() else {
            return unavailable(out);
        };
        let user = ctx.user.as_ref().map(|user| user.id.as_str());
        if private && user.is_none() {
            return answer(out, 401, b"{\"error\":\"Sign in with your LOKI account.\"}");
        }
        let Ok(config) = ctx.config::<Config>() else {
            return unavailable(out);
        };
        if config.worker_key.len() != 64
            || !config
                .worker_key
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return unavailable(out);
        }
        if let Ok(response) = to_worker(incoming, method, &path, &config.worker_key, user) {
            pipe(out, response);
        } else {
            unavailable(out);
        }
    }
}

fn to_worker(
    incoming: IncomingRequest,
    method: Method,
    path: &str,
    key: &str,
    user: Option<&str>,
) -> std::result::Result<wasi::http::types::IncomingResponse, ()> {
    let mut headers = vec![(
        "authorization".to_owned(),
        format!("Bearer {key}").into_bytes(),
    )];
    if let Some(user) = user {
        headers.push(("x-loki-account".to_owned(), user.as_bytes().to_vec()));
    }
    let input_headers = incoming.headers();
    for name in ["range", "sec-fetch-site"] {
        if let [value] = input_headers.get(name).as_slice() {
            headers.push((name.to_owned(), value.clone()));
        }
    }
    drop(input_headers);
    let post = matches!(method, Method::Post);
    if post {
        headers.push(("content-type".to_owned(), b"application/json".to_vec()));
    }
    let request = OutgoingRequest::new(Fields::from_list(&headers).map_err(|_| ())?);
    request.set_scheme(Some(&Scheme::Http))?;
    request.set_authority(Some(WORKER))?;
    request.set_path_with_query(Some(path))?;
    request.set_method(&method)?;
    let output = request.body()?;
    let timeout = RequestOptions::new();
    timeout.set_connect_timeout(Some(3_000_000_000))?;
    timeout.set_first_byte_timeout(Some(30_000_000_000))?;
    let future = outgoing_handler::handle(request, Some(timeout)).map_err(|_| ())?;
    if post {
        let body = incoming.consume()?;
        let input = body.stream()?;
        let writer = output.write()?;
        let mut size = 0;
        loop {
            match input.blocking_read(4096) {
                Ok(bytes) => {
                    size += bytes.len();
                    if size > 4096 {
                        return Err(());
                    }
                    writer.blocking_write_and_flush(&bytes).map_err(|_| ())?;
                }
                Err(StreamError::Closed) => break,
                Err(_) => return Err(()),
            }
        }
    }
    OutgoingBody::finish(output, None).map_err(|_| ())?;
    future.subscribe().block();
    future.get().ok_or(())??.map_err(|_| ())
}

fn pipe(out: ResponseOutparam, incoming: wasi::http::types::IncomingResponse) {
    let mut headers: Vec<_> = incoming
        .headers()
        .entries()
        .into_iter()
        .filter(|(name, _)| {
            matches!(
                name.as_str(),
                "content-type"
                    | "content-length"
                    | "content-disposition"
                    | "content-range"
                    | "accept-ranges"
            )
        })
        .collect();
    headers.push(("cache-control".into(), b"private, no-store".to_vec()));
    headers.push(("x-content-type-options".into(), b"nosniff".to_vec()));
    let Ok(fields) = Fields::from_list(&headers) else {
        return unavailable(out);
    };
    let response = OutgoingResponse::new(fields);
    if response.set_status_code(incoming.status()).is_err() {
        return unavailable(out);
    }
    let Ok(body) = incoming.consume() else {
        return unavailable(out);
    };
    let Ok(input) = body.stream() else {
        return unavailable(out);
    };
    let Ok(output) = response.body() else {
        return unavailable(out);
    };
    ResponseOutparam::set(out, Ok(response));
    if let Ok(writer) = output.write() {
        loop {
            match input.blocking_read(65536) {
                Ok(bytes) if writer.blocking_write_and_flush(&bytes).is_ok() => {}
                Err(StreamError::Closed) => break,
                _ => return,
            }
        }
    }
    let _ = OutgoingBody::finish(output, None);
}

fn answer(out: ResponseOutparam, status: u16, bytes: &[u8]) {
    let fields = Fields::new();
    if fields
        .set("content-type", &[b"application/json".to_vec()])
        .is_err()
        || fields
            .set("cache-control", &[b"no-store".to_vec()])
            .is_err()
    {
        return ResponseOutparam::set(out, Err(ErrorCode::InternalError(None)));
    }
    let response = OutgoingResponse::new(fields);
    if response.set_status_code(status).is_err() {
        return ResponseOutparam::set(out, Err(ErrorCode::InternalError(None)));
    }
    let Ok(body) = response.body() else {
        return ResponseOutparam::set(out, Err(ErrorCode::InternalError(None)));
    };
    ResponseOutparam::set(out, Ok(response));
    if let Ok(writer) = body.write() {
        let _ = writer.blocking_write_and_flush(bytes);
    }
    let _ = OutgoingBody::finish(body, None);
}

fn unavailable(out: ResponseOutparam) {
    answer(out, 503, b"{\"error\":\"The local downloader worker is unavailable. Ask your LOKI owner to check the setup.\"}");
}

wasi::http::proxy::export!(App);
