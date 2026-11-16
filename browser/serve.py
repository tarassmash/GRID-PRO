#!/usr/bin/env python3
"""Отдаёт HTML-бота (index.html) по ссылке. Сам ничего не торгует: бот работает в браузере того, кто открыл страницу.
Если задана переменная APP_PASSWORD, страница спросит пароль (логин любой)."""
import base64, hmac, os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
PAGE = open(os.path.join(HERE, "index.html"), "rb").read()
PASSWORD = os.environ.get("APP_PASSWORD", "")


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _ok_auth(self):
        if not PASSWORD:
            return True
        h = self.headers.get("Authorization", "")
        try:
            return h.startswith("Basic ") and hmac.compare_digest(
                base64.b64decode(h[6:]).decode("utf-8").partition(":")[2], PASSWORD)
        except Exception:
            return False

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/api/ping":
            body, ctype = b'{"ok": true}', "application/json"
        elif not self._ok_auth():
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="Grid bot", charset="UTF-8"')
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        elif path in ("/", "/index.html", "/gridbot.html"):
            body, ctype = PAGE, "text/html; charset=utf-8"
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(os.environ.get("PORT") or 8080)
    print(f"HTML-бот: порт {port}" + (", пароль включён" if PASSWORD else ""), flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), H).serve_forever()
