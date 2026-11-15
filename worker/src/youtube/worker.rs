use super::{Settings, available_error, sandbox};
use crate::{Error, Result, State, id, write_private};
use axum::http::{Method, StatusCode, header};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    path::Path,
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

const MAX_WIRE: u64 = 512 * 1024;
const MAX_METADATA: usize = 4 * 1024 * 1024;
const MAX_METADATA_PER_PROBE: usize = 20 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
pub(super) struct Video {
    pub id: String,
    pub title: String,
    pub duration: f64,
    live_status: Option<String>,
    availability: Option<String>,
    pub formats: Vec<Format>,
    observed: usize,
    diagnostics: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct Format {
    pub id: String,
    pub height: u32,
    pub size: Option<u64>,
    pub ext: String,
    #[serde(skip_serializing)]
    pub url: String,
    #[serde(skip_serializing)]
    headers: BTreeMap<String, String>,
}

impl Format {
    pub fn quality(&self) -> String {
        format!("{}p {}", self.height, self.ext.to_ascii_uppercase())
    }
}

#[derive(Deserialize)]
struct BrokerRequest {
    url: String,
    method: String,
    headers: BTreeMap<String, String>,
    body: String,
}

pub(super) async fn verify(state: &State) -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let witness = state.0.data.join(format!("youtube-probe-{}", id()));
    let read = state.0.data.join(format!("youtube-probe-{}", id()));
    write_private(&read, b"LOKI worker isolation check")?;
    let test = json!({"op":"validate","loopback":listener.local_addr()?.port(),"secret":read,"write_probe":witness});
    let result = tokio::select! {
        biased;
        _ = listener.accept() => Err(available_error()),
        result = exchange(state, test, Duration::from_secs(15), false) => result,
    };
    let _ = tokio::fs::remove_file(witness).await;
    let _ = tokio::fs::remove_file(read).await;
    match result {
        Ok(value) if value["type"] == "validated" && value["ok"] == true => Ok(()),
        Ok(value) => {
            tracing::warn!(read_denied=?value["read_denied"],write_denied=?value["write_denied"],loopback=?value["loopback_result"],public=?value["public_result"],runtime=?value["runtime"],"YouTube worker isolation check failed");
            Err(available_error())
        }
        Err(error) => {
            tracing::warn!(message=%error.message,"YouTube worker did not finish its isolation check");
            Err(available_error())
        }
    }
}

pub(super) async fn probe(state: &State, id: &str, settings: &Settings) -> Result<Video> {
    let response = exchange(
        state,
        json!({"op":"probe","id":id}),
        Duration::from_secs(75),
        true,
    )
    .await?;
    let mut video: Video = serde_json::from_value(response).map_err(|_| unsupported())?;
    if video.id != id
        || !video.duration.is_finite()
        || video.duration <= 0.0
        || video
            .live_status
            .as_deref()
            .is_some_and(|status| !matches!(status, "not_live" | "was_live"))
        || video
            .availability
            .as_deref()
            .is_some_and(|status| !matches!(status, "public" | "unlisted"))
    {
        tracing::debug!(availability=?video.availability, live_status=?video.live_status, "YouTube returned an incompatible video");
        return Err(unsupported());
    }
    if video.duration > f64::from(settings.duration_minutes) * 60.0 {
        return Err(Error::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "The video exceeds the installation's duration limit.",
        ));
    }
    video.title = video
        .title
        .chars()
        .filter(|c| !c.is_control())
        .take(250)
        .collect();
    let extracted_formats = video.formats.len();
    video.formats.retain(|f| {
        !f.id.is_empty()
            && f.id.len() <= 32
            && f.id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
            && (1..=4320).contains(&f.height)
            && matches!(f.ext.as_str(), "mp4" | "webm")
            && f.size
                .is_none_or(|size| size > 0 && size <= u64::from(settings.file_mib) * super::MIB)
            && parse_url(&f.url, true).is_ok()
    });
    video.formats.sort_by(|a, b| {
        b.height
            .cmp(&a.height)
            .then_with(|| a.ext.cmp(&b.ext))
            .then_with(|| a.id.cmp(&b.id))
    });
    video
        .formats
        .dedup_by(|a, b| a.height == b.height && a.ext == b.ext);
    if video.formats.is_empty() {
        tracing::debug!(
            extracted_formats,
            observed=video.observed,
            diagnostics=?video.diagnostics,
            "YouTube supplied no eligible combined formats"
        );
        if video
            .diagnostics
            .iter()
            .any(|reason| reason == "upstream_sign_in")
        {
            return Err(sign_in_required());
        }
        return Err(unsupported());
    }
    Ok(video)
}

fn unsupported() -> Error {
    Error::bad(
        "No downloadable video with audio is available. Use a public or unlisted video within the server limits.",
    )
}

fn sign_in_required() -> Error {
    Error::bad(
        "YouTube requested sign-in for this video or this server. Try another publicly available video.",
    )
}

async fn exchange(state: &State, task: Value, time: Duration, network: bool) -> Result<Value> {
    let _slot = state
        .0
        .youtube_workers
        .clone()
        .try_acquire_owned()
        .map_err(|_| super::capacity_error())?;
    let (mut process, mut stdin, stdout) = sandbox::start().await?;
    let result = tokio::time::timeout(time, async {
        let mut stdout = BufReader::new(stdout);
        send(&mut stdin, &task).await?;
        let mut remaining = MAX_METADATA_PER_PROBE;
        for _ in 0..35 {
            let mut line = Vec::new();
            (&mut stdout)
                .take(MAX_WIRE + 1)
                .read_until(b'\n', &mut line)
                .await?;
            if line.last() != Some(&b'\n') || line.len() as u64 > MAX_WIRE {
                return Err(unsupported());
            }
            let message: Value = serde_json::from_slice(&line).map_err(|_| unsupported())?;
            match message["type"].as_str() {
                Some("validation-step") if !network && message["step"]=="runtime" => tracing::debug!("checking the isolated JavaScript runtime"),
                Some("failed") if !network => {
                    tracing::warn!(error_class=?message["error_class"],error_code=?message["error_code"],"YouTube worker failed during the isolation probe");
                    return Err(available_error());
                }
                Some("validated" | "video") => return Ok(message),
                Some("request") if network => {
                    let value = match serde_json::from_value(message) {
                        Ok(input) => match broker(input, &mut remaining).await {
                            Ok(value) => value,
                            Err(error) => {
                                tracing::debug!(status=%error.status, message=%error.message, "YouTube broker request failed");
                                json!({"ok":false})
                            }
                        },
                        Err(_) => json!({"ok":false}),
                    };
                    send(&mut stdin, &value).await?;
                }
                Some("failed") => {
                    tracing::debug!(error_class=?message["error_class"],reason=?message["reason"],"YouTube extraction failed");
                    if message["reason"] == "upstream_sign_in" {
                        return Err(sign_in_required());
                    }
                    return Err(unsupported());
                }
                _ => return Err(unsupported()),
            }
        }
        Err(unsupported())
    })
    .await;
    drop(stdin);
    process.stop().await;
    result.map_err(|_| Error::new(StatusCode::GATEWAY_TIMEOUT, "YouTube extraction timed out."))?
}

async fn send(stdin: &mut (dyn tokio::io::AsyncWrite + Unpin + Send), value: &Value) -> Result<()> {
    let mut encoded = serde_json::to_vec(value)?;
    encoded.push(b'\n');
    stdin.write_all(&encoded).await?;
    stdin.flush().await?;
    Ok(())
}

fn cdn(host: &str) -> bool {
    host == "googlevideo.com"
        || host
            .strip_suffix(".googlevideo.com")
            .is_some_and(|prefix| !prefix.is_empty())
}

fn parse_url(raw: &str, media: bool) -> Result<url::Url> {
    if raw.len() > 16384 {
        return Err(Error::forbidden());
    }
    let url = url::Url::parse(raw).map_err(|_| Error::forbidden())?;
    let host = url.host_str().ok_or_else(Error::forbidden)?;
    let ok_host = if media {
        cdn(host)
    } else {
        cdn(host)
            || matches!(
                host,
                "www.youtube.com"
                    | "youtube.com"
                    | "m.youtube.com"
                    | "music.youtube.com"
                    | "www.youtube-nocookie.com"
                    | "youtubei.googleapis.com"
            )
    };
    if url.scheme() != "https"
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || !ok_host
    {
        return Err(Error::forbidden());
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
                || a == 100 && (64..128).contains(&b)
                || a == 198 && (18..20).contains(&b)
                || matches!(ip.octets(), [192, 0, 0, _] | [192, 88, 99, _]))
        }
        IpAddr::V6(ip) => {
            let [first, second, ..] = ip.segments();
            (0x2000..0x4000).contains(&first)
                && !matches!(first, 0x2002 | 0x3fff)
                && !(first == 0x2001 && (second < 0x200 || second == 0xdb8))
        }
    }
}

async fn network(
    mut url: url::Url,
    mut method: Method,
    mut bytes: Vec<u8>,
    input: header::HeaderMap,
    media: bool,
) -> Result<reqwest::Response> {
    for _ in 0..=5 {
        url = parse_url(url.as_str(), media)?;
        let host = url.host_str().ok_or_else(Error::forbidden)?.to_owned();
        let sockets: Vec<SocketAddr> = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::net::lookup_host((host.as_str(), 443)),
        )
        .await
        .map_err(|_| unsupported())??
        .filter(|addr| public(addr.ip()))
        .take(8)
        .collect();
        if sockets.is_empty() {
            return Err(Error::forbidden());
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(20))
            .resolve_to_addrs(&host, &sockets)
            .build()?;
        let mut request = client
            .request(method.clone(), url.clone())
            .headers(input.clone());
        if !bytes.is_empty() {
            request = request.body(bytes.clone());
        }
        let response = request.send().await.map_err(|error| {
            tracing::debug!(host=%host,connect=error.is_connect(),timeout=error.is_timeout(),"YouTube request failed");
            unsupported()
        })?;
        tracing::debug!(host=%host,status=%response.status(),"YouTube upstream replied");
        if response
            .remote_addr()
            .is_none_or(|addr| !sockets.contains(&addr))
        {
            return Err(Error::forbidden());
        }
        if !matches!(response.status().as_u16(), 301..=303 | 307..=308) {
            return Ok(response);
        }
        let next = response
            .headers()
            .get(header::LOCATION)
            .and_then(|h| h.to_str().ok())
            .ok_or_else(Error::forbidden)?;
        url = url.join(next).map_err(|_| Error::forbidden())?;
        if matches!(response.status().as_u16(), 301..=303) && method == Method::POST {
            method = Method::GET;
            bytes.clear();
        }
    }
    Err(unsupported())
}

async fn broker(input: BrokerRequest, remaining: &mut usize) -> Result<Value> {
    let method = match input.method.as_str() {
        "GET" => Method::GET,
        "HEAD" => Method::HEAD,
        "POST" => Method::POST,
        _ => return Err(Error::forbidden()),
    };
    let bytes = STANDARD
        .decode(input.body)
        .map_err(|_| Error::forbidden())?;
    if bytes.len() > 64 * 1024 || (!bytes.is_empty() && method != Method::POST) {
        return Err(Error::forbidden());
    }
    let mut headers = header::HeaderMap::new();
    for (name, value) in input.headers {
        let name = name.to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "user-agent"
                | "accept"
                | "accept-language"
                | "content-type"
                | "x-youtube-client-name"
                | "x-youtube-client-version"
                | "origin"
                | "referer"
        ) && value.len() <= 2048
        {
            let key =
                header::HeaderName::from_bytes(name.as_bytes()).map_err(|_| Error::forbidden())?;
            let value = header::HeaderValue::from_str(&value).map_err(|_| Error::forbidden())?;
            headers.insert(key, value);
        }
    }
    headers.insert(
        header::ACCEPT_ENCODING,
        header::HeaderValue::from_static("identity"),
    );
    let mut response =
        network(parse_url(&input.url, false)?, method, bytes, headers, false).await?;
    let status = response.status().as_u16();
    let url = response.url().to_string();
    let header_subset: BTreeMap<_, _> = [
        header::CONTENT_TYPE,
        header::CONTENT_ENCODING,
        header::CONTENT_LENGTH,
    ]
    .into_iter()
    .filter_map(|name| {
        response
            .headers()
            .get(&name)?
            .to_str()
            .ok()
            .map(|value| (name.to_string(), value.to_owned()))
    })
    .collect();
    let max = (*remaining).min(MAX_METADATA);
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| unsupported())? {
        if chunk.len() > *remaining {
            *remaining = 0;
            return Err(unsupported());
        }
        *remaining -= chunk.len();
        if body.len() + chunk.len() > max {
            return Err(unsupported());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(
        json!({"ok":true,"status":status,"url":url,"headers":header_subset,"body":STANDARD.encode(body)}),
    )
}

pub(super) async fn download(format: &Format, path: &Path, max: u64) -> Result<u64> {
    let mut headers = header::HeaderMap::new();
    for name in [header::USER_AGENT, header::REFERER, header::ORIGIN] {
        if let Some(value) = format
            .headers
            .get(name.as_str())
            .filter(|value| value.len() <= 2048)
            && let Ok(value) = header::HeaderValue::from_str(value)
        {
            headers.insert(name, value);
        }
    }
    headers.insert(
        header::ACCEPT_ENCODING,
        header::HeaderValue::from_static("identity"),
    );
    let response = network(
        parse_url(&format.url, true)?,
        Method::GET,
        Vec::new(),
        headers,
        true,
    )
    .await?;
    if response.status() != StatusCode::OK || response.content_length() == Some(0) {
        return Err(unsupported());
    }
    let content_length = response.content_length();
    if content_length.is_some_and(|len| len > max) {
        return Err(too_large());
    }
    let mut options = tokio::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path).await?;
    let stream = response
        .bytes_stream()
        .map(|result| result.map_err(|_| unsupported()));
    let size = copy_media(stream, &mut file, max, content_length).await?;
    file.flush().await?;
    Ok(size)
}

async fn copy_media(
    stream: impl Stream<Item = Result<bytes::Bytes>>,
    output: &mut (impl tokio::io::AsyncWrite + Unpin),
    max: u64,
    content_length: Option<u64>,
) -> Result<u64> {
    tokio::pin!(stream);
    let mut size = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        size += chunk.len() as u64;
        if size > max {
            return Err(too_large());
        }
        output.write_all(&chunk).await?;
    }
    if size == 0 || content_length.is_some_and(|len| len != size) {
        return Err(unsupported());
    }
    Ok(size)
}

fn too_large() -> Error {
    Error::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "The video exceeds the server's file limit.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn media_size_cap_applies_without_a_content_length() {
        let mut output = Vec::new();
        let chunks = futures_util::stream::iter([
            Ok(bytes::Bytes::from_static(b"first")),
            Ok(bytes::Bytes::from_static(b"second")),
        ]);
        assert_eq!(
            copy_media(chunks, &mut output, 8, None)
                .await
                .unwrap_err()
                .status,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(output, b"first");
    }

    #[test]
    fn youtube_broker_has_strict_host_and_ip_boundaries() {
        assert!(parse_url("https://rr2.googlevideo.com/videoplayback", true).is_ok());
        assert!(parse_url("https://www.youtube.com/watch?v=abcdefghijk", false).is_ok());
        for url in [
            "https://youtube.com.evil.test/",
            "https://evilgooglevideo.com/",
            "https://googlevideo.com@localhost/",
            "https://192.168.1.1/",
            "http://youtube.com/",
            "https://youtube.com:81/",
            "file:///etc/passwd",
        ] {
            assert!(parse_url(url, false).is_err(), "{url}");
        }
        for ip in [
            "127.0.0.1",
            "169.254.169.254",
            "10.10.10.10",
            "100.64.0.1",
            "::1",
            "::ffff:8.8.8.8",
            "64:ff9b::a00:1",
            "fe80::1",
            "fd00::1",
        ] {
            assert!(!public(ip.parse().unwrap()), "{ip}");
        }
    }
}
