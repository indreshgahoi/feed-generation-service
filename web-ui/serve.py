#!/usr/bin/env python3
"""Static file server for the web UI that also reverse-proxies API calls to
the backend services on the SAME origin/port as the page itself.

Why: the browser only reliably reaches whatever port you actually navigated
to (5173) -- in a remote/sandboxed dev environment, other ports the page's
JS tries to call directly (4001, 4002) may not be forwarded to your real
browser at all, which Chrome then reports as a blocked/opaque response
(net::ERR_BLOCKED_BY_ORB) rather than a clear connection error. Proxying
through this single port sidesteps that entirely -- no cross-origin
requests, no CORS, nothing else to forward.
"""
import http.server
import os
import sys
import urllib.error
import urllib.request

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("WEB_UI_PORT", "5173"))
INGESTION_URL = os.environ.get("INGESTION_URL", "http://localhost:4001")
FEED_URL = os.environ.get("FEED_URL", "http://localhost:4002")

PROXY_PREFIXES = {
    "/api/ingest": INGESTION_URL,
    "/api/feed": FEED_URL,
}


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if not self._proxy():
            super().do_GET()

    def do_POST(self):
        if not self._proxy():
            self.send_error(404)

    def do_OPTIONS(self):
        if not self._proxy():
            self.send_response(204)
            self.end_headers()

    def _proxy(self) -> bool:
        for prefix, target in PROXY_PREFIXES.items():
            if self.path.startswith(prefix):
                self._forward(target + self.path[len(prefix):])
                return True
        return False

    def _forward(self, url: str) -> None:
        length = int(self.headers.get("Content-Length", 0) or 0)
        body = self.rfile.read(length) if length else None
        req = urllib.request.Request(url, data=body, method=self.command)
        content_type = self.headers.get("Content-Type")
        if content_type:
            req.add_header("Content-Type", content_type)

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._relay(resp.status, resp.headers.get("Content-Type", "application/json"), resp.read())
        except urllib.error.HTTPError as e:
            self._relay(e.code, "application/json", e.read())
        except urllib.error.URLError as e:
            self._relay(502, "application/json", f'{{"error": "upstream unreachable: {e.reason}"}}'.encode())

    def _relay(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(f"[web-ui] {self.address_string()} {fmt % args}")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"web-ui serving on :{PORT} (proxying /api/ingest -> {INGESTION_URL}, /api/feed -> {FEED_URL})")
    server.serve_forever()
