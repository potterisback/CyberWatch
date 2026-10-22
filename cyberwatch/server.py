"""Dashboard server with an intentionally small, read-only HTTP surface."""

import json
import mimetypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .storage import Store

WEB = Path(__file__).parent / "web"


def handler(store, metrics_path="reports/evaluation.json"):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlsplit(self.path)
            try:
                if url.path == "/api/health":
                    return self.reply(200, {"status": "ok"})
                if url.path == "/api/dashboard":
                    proto = parse_qs(url.query).get("proto", [None])[0]
                    return self.reply(200, store.dashboard(proto=proto))
                if url.path == "/api/model":
                    path = Path(metrics_path)
                    return self.reply(
                        200,
                        json.loads(path.read_text())
                        if path.exists()
                        else {"available": False},
                    )
                files = {
                    "/": "index.html",
                    "/app.js": "app.js",
                    "/style.css": "style.css",
                }
                if url.path not in files:
                    return self.reply(404, {"error": "Not found"})
                file = WEB / files[url.path]
                content_type = (
                    mimetypes.guess_type(file.name)[0] or "application/octet-stream"
                )
                self.send_response(200)
                self.send_header("Content-Type", content_type + "; charset=utf-8")
                self.security_headers()
                self.end_headers()
                self.wfile.write(file.read_bytes())
            except ValueError as exc:
                self.reply(400, {"error": str(exc)})
            except Exception as exc:
                self.reply(
                    503, {"error": "Backend unavailable", "detail": str(exc)[:300]}
                )

        def security_headers(self):
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:; connect-src 'self'",
            )

        def reply(self, status, data):
            body = json.dumps(data, default=str, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.security_headers()
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            pass

    return Handler


def serve(
    db="var/cyberwatch.db",
    host="127.0.0.1",
    port=8000,
    backend="sqlite",
    trino_url="http://localhost:8080",
):
    if backend == "trino":
        from .trino_client import TrinoStore

        store = TrinoStore(trino_url)
    else:
        store = Store(db)
    server = ThreadingHTTPServer((host, port), handler(store))
    print(
        f"CyberWatch dashboard: http://{host}:{server.server_port} ({backend})",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    finally:
        server.server_close()
