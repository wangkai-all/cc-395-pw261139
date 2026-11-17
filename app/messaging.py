"""Delivery layer: at-least-once push with consumer-side dedup.

- Offline consumers keep deliveries ``pending``; nothing is lost.
- ``deliver_pending`` is safe to run repeatedly; redriven messages are
  acknowledged at most once via ``consumer_acks``.
- Any successful attempt marks the product ``externalized``: once data has
  left the system, revocation can only invalidate, never physically retract.
"""
from __future__ import annotations

import json
import sqlite3

from .db import write_lock
from .service import _oplog, _row, _rows, new_id, now


def create_consumer(conn, name: str, account_filter: str = "*",
                    status: str = "online", actor: str = "api") -> dict:
    if account_filter != "*" and not account_filter.strip():
        raise ValueError("account_filter must be '*' or an account id")
    cid = new_id("con")
    with write_lock():
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """INSERT INTO consumers(id, name, account_filter, status, created_at)
               VALUES (?,?,?,?,?)""",
            (cid, name, account_filter, status, now()),
        )
        _oplog(conn, actor, "consumer.created",
               {"consumer_id": cid, "filter": account_filter, "status": status})
        conn.execute("COMMIT")
    return get_consumer(conn, cid)


def get_consumer(conn, consumer_id: str):
    r = _row(conn, "SELECT * FROM consumers WHERE id=?", (consumer_id,))
    return dict(r) if r else None


def set_consumer_status(conn, consumer_id: str, status: str, actor: str = "api"):
    if status not in ("online", "offline"):
        raise ValueError("status must be online|offline")
    with write_lock():
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute("UPDATE consumers SET status=? WHERE id=?",
                           (status, consumer_id))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            raise ValueError(f"unknown consumer {consumer_id}")
        _oplog(conn, actor, f"consumer.{status}", {"consumer_id": consumer_id})
        conn.execute("COMMIT")


def _mark_externalized(conn, message_id: str):
    conn.execute(
        "UPDATE products SET externalized=1 WHERE id IN "
        "(SELECT product_id FROM messages WHERE id=? AND product_id IS NOT NULL)",
        (message_id,),
    )


def deliver_pending(conn, consumer_id: str, actor: str = "dispatcher") -> dict:
    """Attempt every pending delivery for one consumer.

    Returns a report that separately counts first deliveries and duplicate
    attempts — duplicates prove the idempotency path works.
    """
    consumer = _row(conn, "SELECT * FROM consumers WHERE id=?", (consumer_id,))
    if not consumer:
        raise ValueError(f"unknown consumer {consumer_id}")
    if consumer["status"] != "online":
        return {"consumer_id": consumer_id, "delivered": [], "duplicates": [],
                "skipped": "consumer offline — messages remain pending"}

    delivered, duplicates = [], []
    with write_lock():
        conn.execute("BEGIN IMMEDIATE")
        pendings = _rows(
            conn,
            """SELECT d.*, m.type, m.payload_json, m.idempotency_key AS mkey
               FROM deliveries d JOIN messages m ON m.id=d.message_id
               WHERE d.consumer_id=? AND d.status='pending'
               ORDER BY m.created_at, d.id""",
            (consumer_id,),
        )
        for d in pendings:
            conn.execute(
                """UPDATE deliveries SET attempts=attempts+1, last_attempt_at=?
                   WHERE id=?""",
                (now(), d["id"]))
            acked = _row(
                conn,
                "SELECT 1 FROM consumer_acks WHERE consumer_id=? AND message_id=?",
                (consumer_id, d["message_id"]))
            if acked:
                # redrive of an already-processed message: never applied twice
                duplicates.append(d["message_id"])
                conn.execute(
                    "UPDATE deliveries SET status='delivered', delivered_at=? WHERE id=?",
                    (now(), d["id"]))
                continue
            conn.execute(
                """INSERT INTO consumer_acks(consumer_id, message_id, ts)
                   VALUES (?,?,?)""",
                (consumer_id, d["message_id"], now()))
            conn.execute(
                "UPDATE deliveries SET status='delivered', delivered_at=? WHERE id=?",
                (now(), d["id"]))
            _mark_externalized(conn, d["message_id"])
            delivered.append({"message_id": d["message_id"],
                              "type": d["type"],
                              "idempotency_key": d["mkey"],
                              "payload": json.loads(d["payload_json"])})
        if delivered or duplicates:
            _oplog(conn, actor, "delivery.batch",
                   {"consumer_id": consumer_id,
                    "delivered": len(delivered), "duplicates": len(duplicates)})
        conn.execute("COMMIT")
    return {"consumer_id": consumer_id, "delivered": delivered,
            "duplicates": duplicates}


def deliver_all(conn, **kw) -> dict:
    out = {}
    for c in _rows(conn, "SELECT id FROM consumers WHERE status='online'"):
        out[c["id"]] = deliver_pending(conn, c["id"], **kw)
    return out


def redrive(conn, message_id: str, actor: str = "api") -> dict:
    """Force re-send of one message to every consumer that received it.
    The consumer ack ledger guarantees its effect is applied at most once."""
    msg = _row(conn, "SELECT * FROM messages WHERE id=?", (message_id,))
    if not msg:
        raise ValueError(f"unknown message {message_id}")
    with write_lock():
        conn.execute("BEGIN IMMEDIATE")
        targets = _rows(
            conn,
            "SELECT consumer_id FROM deliveries WHERE message_id=?", (message_id,))
        conn.execute(
            "UPDATE deliveries SET status='pending' WHERE message_id=?",
            (message_id,))
        _oplog(conn, actor, "message.redrive",
               {"message_id": message_id, "targets": len(targets)})
        conn.execute("COMMIT")
    results = {}
    for t in targets:
        results[t["consumer_id"]] = deliver_pending(conn, t["consumer_id"],
                                                    actor="redrive")
    return {"message_id": message_id, "results": results}


def consumer_view(conn, consumer_id: str) -> dict:
    """The consumer-side materialized inbox, derived only from acked messages."""
    consumer = get_consumer(conn, consumer_id)
    if not consumer:
        raise ValueError(f"unknown consumer {consumer_id}")
    msgs = _rows(
        conn,
        """SELECT m.*, d.status AS delivery_status, d.attempts, d.delivered_at,
                  (a.message_id IS NOT NULL) AS acked
           FROM deliveries d
           JOIN messages m ON m.id=d.message_id
           LEFT JOIN consumer_acks a
                  ON a.message_id=m.id AND a.consumer_id=d.consumer_id
           WHERE d.consumer_id=? ORDER BY m.created_at""",
        (consumer_id,),
    )
    return {"consumer": consumer, "messages": [dict(m) for m in msgs]}
