#!/usr/bin/env python3
"""Local mock Hugging Face Hub with fault injection, for robustness tests.

Serves one repo (REPO) with one file (FILE) under HUB_DIR/<FILE>. Supports
HEAD/GET with Range, the revision and tree API endpoints hfd calls, and these
faults:
  --drop-after N   the first --drop-count body responses are cut after N bytes
  --throttle BPS   body is sent at roughly BPS bytes per second
Stdlib only. Prints "ready <port>" on stdout once listening.
"""
import argparse
import hashlib
import http.server
import json
import os
import socketserver
import sys
import threading
import time

REPO = "test/robust"
REV = "a" * 40
FILE = "model.gguf"


class State:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.drops_left = args.drop_count


def make_handler(state, data_path, size, sha):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _common(self, status=200, length=None):
            self.send_response(status)
            self.send_header("X-Repo-Commit", REV)
            self.send_header("ETag", f'"{sha}"')
            self.send_header("X-Linked-Etag", f'"{sha}"')
            self.send_header("X-Linked-Size", str(size))
            self.send_header("Accept-Ranges", "bytes")
            if length is not None:
                self.send_header("Content-Length", str(length))

        def _json(self, obj):
            body = json.dumps(obj).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?")[0]
            if path == f"/api/models/{REPO}/revision/main" or path.startswith(f"/api/models/{REPO}/revision/"):
                return self._json({"sha": REV, "id": REPO})
            if path.startswith(f"/api/models/{REPO}/tree/"):
                return self._json([{
                    "type": "file", "path": FILE, "size": size, "oid": "b" * 40,
                    "lfs": {"oid": sha, "size": size},
                }])
            if path == f"/{REPO}/resolve/{REV}/{FILE}" or path == f"/{REPO}/resolve/main/{FILE}" \
                    or path == f"/{REPO}/resolve/{REV}/{FILE}".replace("/" + REPO + "/", "/" + REPO + "/"):
                return self._file(head=False)
            self.send_error(404)

        def do_HEAD(self):
            path = self.path.split("?")[0]
            if path.startswith(f"/{REPO}/resolve/"):
                return self._file(head=True)
            self.send_error(404)

        def _file(self, head):
            start, end = 0, size - 1
            rng = self.headers.get("Range")
            status = 200
            if rng and rng.startswith("bytes="):
                a, _, b = rng[6:].partition("-")
                start = int(a) if a else 0
                end = int(b) if b else size - 1
                end = min(end, size - 1)
                status = 206
            length = end - start + 1
            self._common(status, length)
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            if head:
                return
            drop = False
            with state.lock:
                if state.args.drop_after is not None and state.drops_left > 0 and start < state.args.drop_after:
                    drop = True
                    state.drops_left -= 1
            limit = state.args.drop_after if drop else None
            sent = 0
            with open(data_path, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(65536, remaining))
                    if not chunk:
                        break
                    if limit is not None and sent + len(chunk) > limit - start:
                        chunk = chunk[: max(0, limit - start - sent)]
                        if chunk:
                            self.wfile.write(chunk)
                        self.close_connection = True
                        return
                    self.wfile.write(chunk)
                    sent += len(chunk)
                    remaining -= len(chunk)
                    if state.args.throttle:
                        time.sleep(len(chunk) / state.args.throttle)
    return Handler


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", required=True)
    p.add_argument("--port", type=int, default=0)
    p.add_argument("--drop-after", type=int, default=None)
    p.add_argument("--drop-count", type=int, default=1)
    p.add_argument("--throttle", type=float, default=0)
    args = p.parse_args()
    state = State(args)
    data_path = os.path.join(args.dir, FILE)
    size = os.path.getsize(data_path)
    h = hashlib.sha256()
    with open(data_path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    srv = Server(("127.0.0.1", args.port), make_handler(state, data_path, size, h.hexdigest()))
    print(f"ready {srv.server_address[1]}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    sys.exit(main())
