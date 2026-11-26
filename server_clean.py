"""
server_clean.py - DeckLink Spike 3: USB/WiFi Feasibility Experiment

Concurrent servers:
  HTTP       0.0.0.0:8000  - static files from ./webroot only
  WebSocket  0.0.0.0:8765  - echo server

Usage (activate .venv first):
    python server_clean.py
Stop with Ctrl+C.
"""

import asyncio
import os
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote

from websockets.asyncio.server import serve as ws_serve
from websockets.exceptions import ConnectionClosed

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8000

WS_HOST = "0.0.0.0"
WS_PORT = 8765

WS_MAX_SIZE      = 64 * 1024  # 64 KiB
WS_PING_INTERVAL = 20         # seconds
WS_PING_TIMEOUT  = 10         # seconds

# Resolved once at import time so every request handler can use it.
WEBROOT: Path = (Path(__file__).parent / "webroot").resolve()


# ---------------------------------------------------------------------------
# MIME type lookup
# ---------------------------------------------------------------------------

_MIME: dict[str, str] = {
    ".html": "text/html; charset=utf-8",
    ".css":  "text/css; charset=utf-8",
    ".js":   "application/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif":  "image/gif",
    ".svg":  "image/svg+xml",
    ".ico":  "image/x-icon",
    ".txt":  "text/plain; charset=utf-8",
}


def mime_for(suffix: str) -> str:
    """Return a Content-Type string for *suffix* (e.g. '.html')."""
    return _MIME.get(suffix.lower(), "application/octet-stream")


# ---------------------------------------------------------------------------
# HTTP handler - serves files from WEBROOT only
# ---------------------------------------------------------------------------

class WebrootHandler(BaseHTTPRequestHandler):
    """Static file handler. No directory listing; full traversal protection."""

    def do_GET(self) -> None:
        # Strip query string and percent-decode the URL path.
        raw = unquote(self.path.split("?", 1)[0])

        # Collapse all '..' and '.' segments; remove leading separators so the
        # result is always a plain relative path, never an absolute one.
        rel = os.path.normpath(raw).lstrip("/\\")

        # Map bare '/' to index.html so the browser has a default document.
        if rel == "":
            rel = "index.html"

        # Resolve symlinks and canonicalise.  On Windows, resolve() raises
        # ValueError for reserved device names such as CON or NUL.
        try:
            target: Path = (WEBROOT / rel).resolve()
        except (ValueError, OSError):
            self._reply(400, b"400 Bad Request\n", "text/plain")
            return

        # Reject any path that escapes WEBROOT (catches symlink escapes).
        try:
            target.relative_to(WEBROOT)
        except ValueError:
            self._reply(403, b"403 Forbidden\n", "text/plain")
            return

        # No directory listing.
        if target.is_dir():
            self._reply(403, b"403 Forbidden - no directory listing\n", "text/plain")
            return

        # 404 for anything that is not a regular file.
        if not target.is_file():
            self._reply(404, b"404 Not Found\n", "text/plain")
            return

        # Read and serve.
        try:
            body = target.read_bytes()
        except OSError as exc:
            print(f"[HTTP ] Read error '{target.name}': {exc}")
            self._reply(500, b"500 Internal Server Error\n", "text/plain")
            return

        self._reply(200, body, mime_for(target.suffix))

    def _reply(self, code: int, body: bytes, content_type: str) -> None:
        """Write a complete HTTP response; ignore write errors (client gone)."""
        try:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except OSError:
            pass

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[HTTP ] {self.address_string()} - {fmt % args}")

    def log_error(self, fmt: str, *args: object) -> None:
        print(f"[HTTP ] ERR {self.address_string()} - {fmt % args}")


# ---------------------------------------------------------------------------
# HTTP worker thread
# ---------------------------------------------------------------------------

def http_worker(httpd: ThreadingHTTPServer, stop: threading.Event) -> None:
    """Poll for HTTP requests until *stop* is set.

    Uses handle_request() with a short timeout instead of serve_forever() so
    the loop checks the stop flag regularly and exits without needing
    httpd.shutdown(), which can deadlock if called before serve_forever()
    has started.
    """
    httpd.timeout = 0.5  # handle_request() polls every 0.5 s maximum
    print(f"[HTTP ] Listening  http://{HTTP_HOST}:{HTTP_PORT}  root={WEBROOT}")
    while not stop.is_set():
        httpd.handle_request()
    print("[HTTP ] Stopped.")


# ---------------------------------------------------------------------------
# HTTP shutdown helper
# ---------------------------------------------------------------------------

def _shutdown_http(
    httpd: ThreadingHTTPServer,
    thread: threading.Thread,
    stop: threading.Event,
) -> None:
    """Signal the HTTP worker to stop, join it, then release the socket.

    Order matters:
      1. Set *stop* so the handle_request() poll loop exits on its next
         iteration (within 0.5 s at most).
      2. Join the thread with a 5-second timeout, letting any in-flight
         handle_request() call finish naturally before the socket is closed.
      3. Close the server socket only if the worker exited; if it is still
         alive after 5 s, warn and leave the socket untouched so an active
         request is not cut off.

    Safe to call even if the worker already terminated unexpectedly:
    join() on a dead thread returns immediately, and server_close() on an
    already-closed socket is a harmless no-op.
    """
    stop.set()  # tell the poll loop to exit on its next iteration

    # Allow up to 5 s for the current handle_request() call to finish.
    thread.join(timeout=5.0)
    if thread.is_alive():
        print(
            "[MAIN ] WARNING - HTTP thread did not exit within 5 s; "
            "skipping server_close()."
        )
    else:
        # Release the listening socket only after the worker has exited.
        httpd.server_close()


# ---------------------------------------------------------------------------
# WebSocket echo handler
# ---------------------------------------------------------------------------

async def ws_handler(websocket) -> None:
    """Echo every incoming message back unchanged.  Never log message content."""
    host, port = websocket.remote_address[:2]
    peer = f"{host}:{port}"
    print(f"[WS   ] Connected     {peer}")
    try:
        async for message in websocket:
            await websocket.send(message)
    except ConnectionClosed:
        pass  # clean or unclean close - nothing to do
    except Exception as exc:
        print(f"[WS   ] Error {peer}: {exc!r}")
    finally:
        print(f"[WS   ] Disconnected  {peer}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    # Abort early if the webroot directory does not exist.
    if not WEBROOT.is_dir():
        print(f"[MAIN ] ERROR - webroot not found: {WEBROOT}")
        sys.exit(1)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        print("\n[MAIN ] Ctrl+C - shutting down ...")
        stop.set()

    # Windows does not support loop.add_signal_handler; fall back to signal.signal.
    if sys.platform == "win32":
        signal.signal(signal.SIGINT,  lambda *_: loop.call_soon_threadsafe(request_stop))
        signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(request_stop))
    else:
        loop.add_signal_handler(signal.SIGINT,  request_stop)
        loop.add_signal_handler(signal.SIGTERM, request_stop)

    print("=" * 54)
    print("  DeckLink Spike 3 - HTTP + WebSocket servers")
    print("=" * 54)

    # Bind the HTTP server socket before starting anything else so a port
    # conflict fails fast with a clear message rather than a confusing error
    # inside the worker thread.
    try:
        httpd = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), WebrootHandler)
    except OSError as exc:
        print(f"[MAIN ] ERROR - cannot bind HTTP server: {exc}")
        sys.exit(1)

    http_stop = threading.Event()
    http_thread = threading.Thread(
        target=http_worker, args=(httpd, http_stop), daemon=True, name="http",
    )
    http_thread.start()

    try:
        # Start the WebSocket server on the asyncio event loop.
        async with ws_serve(
            ws_handler,
            WS_HOST,
            WS_PORT,
            max_size=WS_MAX_SIZE,
            ping_interval=WS_PING_INTERVAL,
            ping_timeout=WS_PING_TIMEOUT,
        ):
            print(f"[WS   ] Listening  ws://{WS_HOST}:{WS_PORT}")

            # Poll every 0.5 seconds instead of blocking on stop.wait() alone.
            # This lets us detect an unexpected HTTP worker exit and initiate
            # a clean shutdown rather than leaving the WebSocket server running
            # with no HTTP counterpart.
            while not stop.is_set():
                if not http_thread.is_alive():
                    print("[MAIN ] ERROR - HTTP worker exited unexpectedly; shutting down.")
                    stop.set()
                    break
                await asyncio.sleep(0.5)

        print("[WS   ] Stopped.")

    finally:
        # Runs on normal exit, Ctrl+C, and WebSocket startup failure alike.
        _shutdown_http(httpd, http_thread, http_stop)

    print("[MAIN ] Done.")


if __name__ == "__main__":
    asyncio.run(main())
