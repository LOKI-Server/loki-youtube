"""LOKI's pinned yt-dlp adapter. Network I/O is delegated over stdio."""
import base64
import errno
import io
import json
import os
import socket
import subprocess
import sys
import urllib.request
from importlib.metadata import version


wire = sys.stdout
sys.stdout = sys.stderr


def emit(message):
    wire.write(json.dumps(message, ensure_ascii=True, separators=(",", ":")) + "\n")
    wire.flush()


def receive():
    data = sys.stdin.buffer.readline(6 * 1024 * 1024)
    if not data.endswith(b"\n"):
        raise RuntimeError("The LOKI broker disconnected")
    return json.loads(data)


def socket_result(address):
    try:
        try_connect(address)
        return "connected"
    except TimeoutError:
        return "timeout"
    except OSError as error:
        return getattr(error, "winerror", None) or error.errno or "unknown"


def try_read(path):
    with open(path, "rb") as source:
        source.read(1)


def try_connect(address):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        sock.connect(address)


def main():
    os.environ["DENO_V8_FLAGS"] = "--max-old-space-size=128,--jitless"
    os.environ["DENO_NO_PROMPT"] = "1"
    os.environ["DENO_NO_UPDATE_CHECK"] = "1"
    packages = os.path.abspath(os.path.join(os.path.dirname(sys.argv[1]), "..", "packages"))
    sys.path.insert(0, packages)
    from yt_dlp import YoutubeDL
    from yt_dlp.networking import Request, Response
    from yt_dlp.networking.exceptions import HTTPError, TransportError
    from yt_dlp.version import __version__
    import yt_dlp_ejs  # The matching EJS package is provisioned with the worker.

    task = receive()
    if __version__ != "2026.10.26" or version("yt-dlp-ejs") != "0.8.0":
        raise RuntimeError("Unexpected yt-dlp version")
    if task["op"] == "validate":
        secret = task["secret"]
        try:
            try_read(secret)
            protected = False
        except (PermissionError, FileNotFoundError):
            protected = True
        write_probe = task["write_probe"]
        try:
            with open(write_probe, "xb"):
                pass
            writable = True
        except (PermissionError, FileNotFoundError):
            writable = False
        emit({"type": "validation-step", "step": "runtime"})
        runtime = subprocess.run([sys.argv[1], "--version"], stdin=subprocess.DEVNULL,
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5, check=True)
        runtime_version = tuple(int(part) for part in runtime.stdout.splitlines()[0].split()[1].split(b"."))
        javascript = subprocess.run([sys.argv[1], "run", "--no-config", "--no-code-cache", "--no-remote",
                                     "--no-npm", "--cached-only", "-"], input=b"console.log(19 + 23)",
                                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5, check=True)
        local_result = socket_result(("127.0.0.1", task["loopback"]))
        public_result = socket_result(("1.1.1.1", 443))
        policy_errors = {errno.EPERM, errno.EACCES, errno.ENETUNREACH, errno.EHOSTUNREACH, 10013, 10051, 10065}
        # The host also checks that the known-live listener accepted no connection.
        loopback_errors = policy_errors | {errno.ECONNREFUSED, 10061, "timeout"}
        network = local_result in loopback_errors and public_result in policy_errors
        runtime_ok = runtime.stdout.startswith(b"deno ") and runtime_version >= (2, 9, 8) and javascript.stdout.strip() == b"42"
        emit({"type": "validated", "ok": protected and not writable and network and runtime_ok,
              "read_denied": protected, "write_denied": not writable, "network_denied": network, "runtime": runtime_ok,
              "loopback_result": local_result, "public_result": public_result})
        return

    class BrokeredYoutube(YoutubeDL):
        def urlopen(self, request):
            if isinstance(request, str):
                request = Request(request)
            if isinstance(request, urllib.request.Request):
                request = Request(request.full_url, data=request.data,
                                  headers=dict(request.header_items()), method=request.get_method())
            data = request.data
            if data is not None and not isinstance(data, bytes):
                raise TransportError("Unsupported YouTube request body")
            headers = dict(self.params["http_headers"])
            headers.update(request.headers)
            emit({"type": "request", "url": request.url, "method": request.method,
                  "headers": headers, "body": base64.b64encode(data or b"").decode("ascii")})
            result = receive()
            if not result.get("ok"):
                raise TransportError("The LOKI network broker rejected this request")
            response = Response(io.BytesIO(base64.b64decode(result["body"], validate=True)),
                                result["url"], result["headers"], result["status"])
            if not 200 <= response.status < 300:
                raise HTTPError(response)
            return response

    class Diagnostics:
        def __init__(self):
            self.reasons = set()

        def debug(self, message):
            pass

        def error(self, message):
            self.warning(message)

        def warning(self, message):
            message = message.lower()
            for text, reason in (("sabr", "segmented_streams"), ("po token", "token_required"),
                                 ("challenge solving failed", "javascript_challenge"),
                                 ("js runtime", "javascript_runtime"), ("sign in", "upstream_sign_in")):
                if text in message:
                    self.reasons.add(reason)

    diagnostics = Diagnostics()
    options = {
        "quiet": True, "noplaylist": True, "logger": diagnostics,
        "cachedir": False, "skip_download": True, "enable_file_urls": False,
        "allowed_extractors": ["youtube"], "plugin_dirs": [], "remote_components": [],
        "js_runtimes": {"deno": {"path": sys.argv[1]}},
        "ignore_no_formats_error": True,
        "extractor_retries": 0, "socket_timeout": 15,
    }
    with BrokeredYoutube(options) as extractor:
        info = extractor.extract_info("https://www.youtube.com/watch?v=" + task["id"], download=False,
                                      ie_key="Youtube")
    if info.get("id") != task["id"] or info.get("_type", "video") != "video":
        raise RuntimeError("Unexpected video result")
    formats = []
    for item in info.get("formats", []):
        if (item.get("protocol") != "https" or not item.get("acodec") or not item.get("vcodec") or not item.get("height")
                or "none" in (item["acodec"], item["vcodec"]) or item.get("has_drm")):
            continue
        headers = {key.lower(): value for key, value in item.get("http_headers", {}).items()
                   if key.lower() in {"user-agent", "referer", "origin"}}
        formats.append({"id": str(item["format_id"]), "height": item.get("height"),
                        "size": item.get("filesize"), "ext": item.get("ext"), "url": item.get("url"),
                        "headers": headers})
    emit({"type": "video", "id": info["id"], "title": info.get("title", ""),
          "duration": info.get("duration"), "live_status": info.get("live_status"),
          "availability": info.get("availability"), "formats": formats[-100:],
          "observed": len(info.get("formats", [])), "diagnostics": sorted(diagnostics.reasons)})


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        message = str(error).lower()
        reason = ("upstream_sign_in" if "sign in" in message or "login required" in message else
                  "no_formats" if "format" in message else "upstream_unavailable")
        emit({"type": "failed", "reason": reason, "error_class": type(error).__name__,
              "error_code": getattr(error, "winerror", None)})
        sys.exit(1)
