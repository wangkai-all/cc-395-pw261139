"""Stdlib HTTP server: REST API + static web UI.

Run:  python3 -m app.server --data-dir runtime/data --port 8000
"""
from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from . import messaging, service
from .db import connect, init_db

STATIC_DIR = Path(__file__).parent / "static"


class State:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.raw_dir = data_dir / "raw"
        self.evidence_dir = data_dir / "evidence"
        self.conn = connect(data_dir / "db")
        init_db(self.conn)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.evidence_dir.mkdir(parents=True, exist_ok=True)


STATE: State | None = None


class Handler(BaseHTTPRequestHandler):
    server_version = "PointwiseRevocation/1.0"

    # ------------------------------------------------------------ utilities
    def _json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, exc: Exception, status=400):
        code = 409 if isinstance(exc, service.Conflict) else status
        self._json({"error": str(exc), "type": exc.__class__.__name__}, code)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        raw = self.rfile.read(length)
        ctype = self.headers.get("Content-Type", "")
        if "application/json" in ctype:
            return json.loads(raw.decode("utf-8"))
        # raw file upload: x-import-name carries the display name
        name = self.headers.get("X-Import-Name", "upload.json")
        tmp = STATE.data_dir / "incoming" / name
        tmp.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "wb") as f:
            f.write(raw)
        return {"_uploaded": str(tmp), "name": name}

    def log_message(self, fmt, *args):
        pass  # quiet; oplog is the audit trail

    # ------------------------------------------------------------ GET
    def do_GET(self):
        u = urlparse(self.path)
        path, q = u.path, parse_qs(u.query)
        c = STATE.conn
        try:
            if path == "/" or path == "/index.html":
                return self._static("index.html", "text/html; charset=utf-8")
            if path == "/app.js":
                return self._static("app.js", "application/javascript; charset=utf-8")
            if path == "/styles.css":
                return self._static("styles.css", "text/css; charset=utf-8")
            if path == "/api/health":
                return self._json({"ok": True})
            if path == "/api/samples":
                from .sample_data import write_samples
                return self._json(write_samples(STATE.data_dir / "samples"))
            if path == "/api/overview":
                return self._json(self._overview())
            if path == "/api/imports":
                return self._json([dict(r) for r in
                                   c.execute("SELECT * FROM imports ORDER BY created_at")])
            if path == "/api/products":
                return self._json([dict(r) for r in
                                   c.execute("SELECT * FROM products ORDER BY type, account, id")])
            if path == "/api/contributions":
                return self._json([dict(r) for r in
                                   c.execute("SELECT * FROM contributions ORDER BY product_id")])
            if path == "/api/events":
                limit = int(q.get("limit", ["100"])[0])
                return self._json([dict(r) for r in
                                   c.execute("SELECT * FROM events ORDER BY seq DESC LIMIT ?",
                                             (limit,))])
            if path == "/api/consumers":
                return self._json([dict(r) for r in
                                   c.execute("SELECT * FROM consumers ORDER BY created_at")])
            if path == "/api/messages":
                return self._json(self._messages())
            if path == "/api/revocations":
                return self._json([dict(r) for r in
                                   c.execute("SELECT * FROM revocations ORDER BY created_at")])
            if path == "/api/oplog":
                return self._json([dict(r) for r in
                                   c.execute("SELECT * FROM oplog ORDER BY id DESC LIMIT 200")])
            if path == "/api/verify":
                return self._json(service.verify(c))
            if path.startswith("/api/imports/") and path.endswith("/revocation-preview"):
                import_id = path.split("/")[3]
                return self._json(service.preview_revocation(c, import_id))
            if path.startswith("/api/consumers/"):
                parts = path.strip("/").split("/")
                if len(parts) == 3:
                    return self._json(messaging.consumer_view(c, parts[2]))
            self._json({"error": "not found"}, 404)
        except Exception as e:  # noqa: BLE001
            self._error(e)

    def _static(self, name: str, ctype: str):
        p = STATIC_DIR / name
        if not p.is_file():
            return self._json({"error": "not found"}, 404)
        body = p.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _overview(self):
        c = STATE.conn
        def one(sql):
            r = c.execute(sql).fetchone()
            return r[0] if r else 0
        return {
            "imports": {
                "total": one("SELECT COUNT(*) FROM imports"),
                "processed": one("SELECT COUNT(*) FROM imports WHERE status='processed'"),
                "revoked": one("SELECT COUNT(*) FROM imports WHERE status='revoked'"),
            },
            "products": {
                "active": one("SELECT COUNT(*) FROM products WHERE status='active'"),
                "removed": one("SELECT COUNT(*) FROM products WHERE status='removed'"),
                "invalidated": one("SELECT COUNT(*) FROM products WHERE status='invalidated'"),
                "externalized": one("SELECT COUNT(*) FROM products WHERE externalized=1"),
            },
            "events": one("SELECT COUNT(*) FROM events"),
            "pending_deliveries": one("SELECT COUNT(*) FROM deliveries WHERE status='pending'"),
            "verify": service.verify(c),
        }

    def _messages(self):
        c = STATE.conn
        rows = c.execute(
            """SELECT m.*,
                      (SELECT COUNT(*) FROM deliveries d WHERE d.message_id=m.id) AS fanout,
                      (SELECT COUNT(*) FROM deliveries d WHERE d.message_id=m.id AND d.status='delivered') AS delivered,
                      (SELECT COUNT(*) FROM deliveries d WHERE d.message_id=m.id AND d.status='pending') AS pending
               FROM messages m ORDER BY m.created_at DESC""").fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ POST
    def do_POST(self):
        u = urlparse(self.path)
        path = u.path
        c = STATE.conn
        try:
            if path == "/api/imports":
                body = self._body()
                if "_uploaded" in body:
                    info = service.register_import(
                        c, body["_uploaded"], STATE.raw_dir,
                        display_name=body.get("name"))
                else:
                    # named built-in sample: resolve its materialized path
                    from .sample_data import write_samples
                    samples = write_samples(STATE.data_dir / "samples")
                    name = body.get("name", "inline.json")
                    if name in samples:
                        src = samples[name]
                    else:  # arbitrary JSON-array upload: materialize for evidence
                        src = STATE.data_dir / "incoming" / name
                        src.parent.mkdir(parents=True, exist_ok=True)
                        src.write_text(json.dumps(body.get("rows", []), ensure_ascii=False),
                                       encoding="utf-8")
                    info = service.register_import(c, src, STATE.raw_dir,
                                                   display_name=name)
                return self._json(info, 201)
            if path.startswith("/api/imports/") and path.endswith("/process"):
                import_id = path.split("/")[3]
                body = self._body()
                job = service.process_import(
                    c, import_id, chunk_size=int(body.get("chunk_size", 5)))
                return self._json(job)
            if path.startswith("/api/imports/") and path.endswith("/revoke"):
                import_id = path.split("/")[3]
                body = self._body()
                report = service.revoke_import(
                    c, import_id, body.get("strategy", "compensate"),
                    body.get("reason", "user requested revocation"),
                    STATE.evidence_dir)
                return self._json(report, 200)
            if path == "/api/consumers":
                body = self._body()
                con = messaging.create_consumer(
                    c, body["name"], body.get("account_filter", "*"),
                    body.get("status", "online"))
                return self._json(con, 201)
            if path.startswith("/api/consumers/") and path.endswith("/offline"):
                messaging.set_consumer_status(c, path.split("/")[3], "offline")
                return self._json({"ok": True})
            if path.startswith("/api/consumers/") and path.endswith("/online"):
                cid = path.split("/")[3]
                messaging.set_consumer_status(c, cid, "online")
                report = messaging.deliver_pending(c, cid, actor="reconnect")
                return self._json({"ok": True, "backlog_replay": report})
            if path.startswith("/api/consumers/") and path.endswith("/deliver"):
                return self._json(messaging.deliver_pending(c, path.split("/")[3]))
            if path.startswith("/api/messages/") and path.endswith("/redrive"):
                return self._json(messaging.redrive(c, path.split("/")[3]))
            if path == "/api/demo/run":
                body = self._body()
                from .demo import run_demo
                report = run_demo(STATE.data_dir,
                                  scenario=body.get("scenario", "full"))
                return self._json(report)
            self._json({"error": "not found"}, 404)
        except Exception as e:  # noqa: BLE001
            self._error(e)


def serve(data_dir: str, port: int):
    global STATE
    STATE = State(Path(data_dir))
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    httpd.daemon_threads = True
    print(f"web UI: http://127.0.0.1:{port}/  (data under {data_dir})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="runtime/data")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()
    serve(args.data_dir, args.port)


if __name__ == "__main__":
    main()
