"""Core domain services: registration, chunked processing, contribution graph,
revocation with precise compensation or rebuild, and the append-only event ledger.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .db import write_lock


class ServiceError(Exception):
    pass


class Conflict(ServiceError):
    """HTTP 409 — operation conflicts with current state (e.g. job running)."""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(timezone.utc):%Y%m%d%H%M%S}_{secrets.token_hex(3)}"


# --------------------------------------------------------------------- helpers
def _row(conn: sqlite3.Connection, sql: str, params=()):
    return conn.execute(sql, params).fetchone()


def _rows(conn: sqlite3.Connection, sql: str, params=()):
    return conn.execute(sql, params).fetchall()

def _emit(conn, event_type, product_id, import_id, revoc_id, account,
          amount_delta, qty_delta, idem_key, note=None):
    """Append an event exactly once. INSERT OR IGNORE + unique idempotency key
    makes retries (duplicate compensation, double revoke) harmless."""
    # seq defaults to the autoincrement row id via an AFTER INSERT trigger:
    # atomic under concurrency, no MAX(seq)+1 read-modify-write race.
    cur = conn.execute(
        """INSERT OR IGNORE INTO events
           (ts, event_type, product_id, import_id, revoc_id, account,
            amount_delta, qty_delta, idempotency_key, note)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (now(), event_type, product_id, import_id, revoc_id, account,
         amount_delta, qty_delta, idem_key, note),
    )
    return cur.rowcount == 1


def _oplog(conn, actor: str, action: str, detail: dict):
    conn.execute(
        "INSERT INTO oplog(ts, actor, action, detail_json) VALUES (?,?,?,?)",
        (now(), actor, action, json.dumps(detail, ensure_ascii=False)),
    )


def _enqueue(conn, msg_type: str, product_id, account: str, payload: dict,
             origin: str, idem_key: str):
    """Create a message and one pending delivery per matching consumer.
    Idempotent on idem_key so repeated revocation phases never duplicate."""
    conn.execute(
        """INSERT OR IGNORE INTO messages
           (id, idempotency_key, type, product_id, account, payload_json, origin, created_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (new_id("msg"), idem_key, msg_type, product_id, account,
         json.dumps(payload, ensure_ascii=False), origin, now()),
    )
    msg = _row(conn, "SELECT id FROM messages WHERE idempotency_key=?", (idem_key,))
    consumers = _rows(
        conn,
        "SELECT id FROM consumers WHERE account_filter='*' OR account_filter=?",
        (account,),
    )
    for c in consumers:
        conn.execute(
            """INSERT OR IGNORE INTO deliveries(message_id, consumer_id)
               VALUES (?,?)""",
            (msg["id"], c["id"]),
        )


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


# ================================================================ registration
def register_import(conn, src_path: str | Path, raw_dir: str | Path,
                    actor: str = "api", display_name: str | None = None) -> dict:
    """Ingest an import file. The original bytes are copied into the evidence
    store and the copy is never modified or deleted — revocation evidence."""
    src = Path(src_path)
    if not src.is_file():
        raise ServiceError(f"file not found: {src}")
    if src.suffix.lower() != ".json":
        raise ServiceError("only .json import files are supported")
    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list) or not data:
        raise ServiceError("import file must contain a non-empty JSON array")

    required = {"record_uid", "account", "sku", "qty", "amount", "ts"}
    for i, r in enumerate(data):
        missing = required - set(r)
        if missing:
            raise ServiceError(f"row {i} missing fields: {sorted(missing)}")
        if not isinstance(r["amount"], int) or not isinstance(r["qty"], int):
            raise ServiceError(f"row {i}: amount/qty must be integer cents/units")

    import_id = new_id("imp")
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    kept = raw_dir / f"{import_id}__{display_name or src.name}"
    shutil.copy2(src, kept)  # preserve original bytes separately
    digest = _sha256(kept)

    with write_lock():
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                """INSERT INTO imports(id, name, source_path, file_sha256,
                       row_count, status, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (import_id, display_name or src.name, str(kept), digest,
                 len(data), "registered", now()),
            )
            conn.executemany(
                """INSERT INTO records(import_id, record_uid, account, sku,
                       qty, amount, ts)
                   VALUES (?,?,?,?,?,?,?)""",
                [(import_id, r["record_uid"], r["account"], r["sku"],
                  r["qty"], r["amount"], r["ts"]) for r in data],
            )
            _oplog(conn, actor, "import.registered",
                   {"import_id": import_id, "rows": len(data), "sha256": digest})
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return get_import(conn, import_id)


def get_import(conn, import_id: str):
    imp = _row(conn, "SELECT * FROM imports WHERE id=?", (import_id,))
    if not imp:
        raise ServiceError(f"unknown import {import_id}")
    return dict(imp)


# ================================================================ processing
def _upsert_aggregate(conn, account: str, amount: int, qty: int, job_id: str):
    prod = _row(conn, "SELECT * FROM products WHERE type='aggregate' AND key=?",
                (f"agg:{account}",))
    ts = now()
    if prod is None:
        pid = new_id("pa")
        conn.execute(
            """INSERT INTO products(id, type, key, account, title, status,
                   current_amount, current_qty, record_count, created_job_id,
                   created_at, updated_at)
               VALUES (?,?,?,?,?, 'active',?,?,1,?,?,?)""",
            (pid, "aggregate", f"agg:{account}", account,
             f"账户 {account} 汇总", amount, qty, job_id, ts, ts),
        )
        return pid
    conn.execute(
        """UPDATE products SET current_amount=current_amount+?,
               current_qty=current_qty+?, record_count=record_count+1,
               updated_at=? WHERE id=?""",
        (amount, qty, ts, prod["id"]),
    )
    return prod["id"]


def _touch_contribution(conn, product_id, import_id, job_id, amount, qty, qty_count):
    """Maintain the contribution graph; a second feeder downgrades FULL→PARTIAL."""
    conn.execute(
        """INSERT INTO contributions(product_id, import_id, kind, record_count,
               qty, amount, active, job_id, created_at)
           VALUES (?,?,?,?,?,?,1,?,?)
           ON CONFLICT(product_id, import_id) DO UPDATE SET
               kind=excluded.kind,
               record_count=contributions.record_count+excluded.record_count,
               qty=contributions.qty+excluded.qty,
               amount=contributions.amount+excluded.amount,
               active=1""",
        (product_id, import_id, "FULL", qty_count, qty, amount, job_id, now()),
    )
    others = _rows(
        conn,
        "SELECT import_id FROM contributions WHERE product_id=? AND active=1",
        (product_id,),
    )
    if len(others) > 1:  # merged downstream: nobody can claim FULL any more
        conn.execute(
            "UPDATE contributions SET kind='PARTIAL' WHERE product_id=? AND active=1",
            (product_id,),
        )


def process_import(conn, import_id: str, chunk_size: int = 100,
                   actor: str = "api", after_chunk=None) -> dict:
    """Process an import in separate per-chunk transactions so a concurrent
    revocation/new batch can genuinely interleave between chunks.

    ``after_chunk(chunk_index)`` is invoked after each committed chunk — the
    demo uses it to interleave a revocation while this job is still running."""
    imp = _row(conn, "SELECT * FROM imports WHERE id=?", (import_id,))
    if not imp:
        raise ServiceError(f"unknown import {import_id}")
    if imp["status"] == "processing":
        raise Conflict(f"import {import_id} is already being processed")
    if imp["status"] == "processed":
        raise Conflict(f"import {import_id} already processed")
    if imp["status"] == "revoked":
        raise Conflict(f"import {import_id} was revoked before processing")

    job_id = new_id("job")
    # status check + job creation are one atomic transaction; concurrent
    # callers serialize on BEGIN IMMEDIATE and see the updated status.
    conn.execute("BEGIN IMMEDIATE")
    cur_imp = _row(conn, "SELECT status FROM imports WHERE id=?", (import_id,))
    if cur_imp["status"] != "registered":
        conn.execute("ROLLBACK")
        raise Conflict(f"import {import_id} is {cur_imp['status']}, not registered")
    conn.execute(
        "UPDATE imports SET status='processing' WHERE id=?", (import_id,))
    conn.execute(
        """INSERT INTO jobs(id, import_id, status, chunk_size, started_at)
           VALUES (?,?,'running',?,?)""",
        (job_id, import_id, chunk_size, now()),
    )
    _oplog(conn, actor, "job.started", {"job_id": job_id, "import_id": import_id})
    conn.execute("COMMIT")

    records = _rows(
        conn,
        "SELECT * FROM records WHERE import_id=? ORDER BY id", (import_id,))
    done = 0
    try:
        for start in range(0, len(records), chunk_size):
            chunk = records[start:start + chunk_size]
            conn.execute("BEGIN IMMEDIATE")
            chunk_accounts: dict[str, dict] = {}
            for r in chunk:
                # ---- detail product (FULL dependent on this import)
                detail_id = new_id("pd")
                detail_key = f"detail:{r['record_uid']}"
                ts = now()
                conn.execute(
                    """INSERT INTO products(id, type, key, account, title,
                           status, current_amount, current_qty, record_count,
                           created_job_id, created_at, updated_at)
                       VALUES (?,?,?,?,?,'active',?,?,1,?,?,?)""",
                    (detail_id, "detail", detail_key, r["account"],
                     f"明细 {r['record_uid']} ({r['sku']})",
                     r["amount"], r["qty"], job_id, ts, ts),
                )
                _emit(conn, "ITEM_ADDED", detail_id, import_id, None,
                      r["account"], r["amount"], r["qty"],
                      f"add:{r['record_uid']}")
                _touch_contribution(conn, detail_id, import_id, job_id,
                                    r["amount"], r["qty"], 1)

                # ---- aggregate product (may merge several imports)
                agg_id = _upsert_aggregate(conn, r["account"],
                                           r["amount"], r["qty"], job_id)
                _emit(conn, "ITEM_ADDED", agg_id, import_id, None,
                      r["account"], r["amount"], r["qty"],
                      f"add:{r['record_uid']}:{agg_id}")
                _touch_contribution(conn, agg_id, import_id, job_id,
                                    r["amount"], r["qty"], 1)

                acc = chunk_accounts.setdefault(r["account"],
                                                {"amount": 0, "qty": 0, "uids": []})
                acc["amount"] += r["amount"]
                acc["qty"] += r["qty"]
                acc["uids"].append(r["record_uid"])

            done += len(chunk)
            conn.execute(
                "UPDATE jobs SET processed_rows=? WHERE id=?", (done, job_id))

            # subscribers see mid-job aggregate state per chunk
            chunk_no = start // chunk_size
            for account, agg in chunk_accounts.items():
                prod = _row(conn,
                            "SELECT * FROM products WHERE type='aggregate' AND key=?",
                            (f"agg:{account}",))
                _enqueue(
                    conn, "aggregate.updated", prod["id"], account,
                    {"account": account,
                     "current_amount": prod["current_amount"],
                     "current_qty": prod["current_qty"],
                     "job_id": job_id, "chunk": chunk_no},
                    origin="job",
                    idem_key=f"msg:aggupd:{job_id}:{chunk_no}:{account}",
                )
            conn.execute("COMMIT")
            if after_chunk is not None:
                after_chunk(start // chunk_size)

        # ---- statements: one per account for this job (FULL dependent)
        conn.execute("BEGIN IMMEDIATE")
        grouped: dict[str, dict] = {}
        for r in records:
            g = grouped.setdefault(r["account"],
                                   {"amount": 0, "qty": 0, "uids": [], "records": []})
            g["amount"] += r["amount"]
            g["qty"] += r["qty"]
            g["uids"].append(r["record_uid"])
            g["records"].append(r)
        for account, g in grouped.items():
            st_id = new_id("ps")
            ts = now()
            conn.execute(
                """INSERT INTO products(id, type, key, account, title,
                       status, current_amount, current_qty, record_count,
                       payload_json, created_job_id, created_at, updated_at)
                   VALUES (?,?,?,?,?,'active',?,?,?,?,?,?,?)""",
                (st_id, "statement", f"stmt:{job_id}:{account}", account,
                 f"对账单 {job_id} / {account}", g["amount"], g["qty"],
                 len(g["uids"]), json.dumps(g["uids"]), job_id, ts, ts),
            )
            for r in g["records"]:
                _emit(conn, "ITEM_ADDED", st_id, import_id, None, account,
                      r["amount"], r["qty"],
                      f"add:{r['record_uid']}:{st_id}")
            _touch_contribution(conn, st_id, import_id, job_id,
                                g["amount"], g["qty"], len(g["uids"]))
            _enqueue(
                conn, "statement.available", st_id, account,
                {"statement_id": st_id, "job_id": job_id, "account": account,
                 "amount": g["amount"], "qty": g["qty"],
                 "record_uids": g["uids"]},
                origin="job",
                idem_key=f"msg:stmt:{job_id}:{account}",
            )
        conn.execute(
            "UPDATE imports SET status='processed', processed_at=? WHERE id=?",
            (now(), import_id))
        conn.execute(
            "UPDATE jobs SET status='done', finished_at=? WHERE id=?",
            (now(), job_id))
        _oplog(conn, actor, "job.done",
               {"job_id": job_id, "import_id": import_id, "rows": len(records)})
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        conn.execute("UPDATE jobs SET status='failed', finished_at=?, error=? WHERE id=?",
                     (now(), "processing failed", job_id))
        raise
    return get_job(conn, job_id)


def get_job(conn, job_id: str):
    return dict(_row(conn, "SELECT * FROM jobs WHERE id=?", (job_id,)))


# ================================================================ revocation
def preview_revocation(conn, import_id: str) -> dict:
    """Classify every product that depends on the import before acting."""
    imp = get_import(conn, import_id)
    deps = _rows(
        conn,
        """SELECT p.*, c.kind, c.amount AS contrib_amount, c.record_count AS contrib_rows
           FROM contributions c JOIN products p ON p.id=c.product_id
           WHERE c.import_id=? AND c.active=1""",
        (import_id,),
    )
    full, partial, externalized = [], [], []
    for d in deps:
        item = {"product_id": d["id"], "type": d["type"], "title": d["title"],
                "account": d["account"], "kind": d["kind"],
                "contrib_amount": d["contrib_amount"],
                "contrib_rows": d["contrib_rows"],
                "externalized": bool(d["externalized"]),
                "status": d["status"]}
        (full if d["kind"] == "FULL" else partial).append(item)
        if d["externalized"]:
            externalized.append(item)
    return {
        "import": {k: imp[k] for k in
                   ("id", "name", "status", "row_count", "created_at")},
        "full_dependent": full,
        "partial_dependent": partial,
        "externalized": externalized,
        "affected_count": len(deps),
    }


def _reclassify_after_removal(conn, product_id: str):
    active = _rows(
        conn,
        "SELECT import_id FROM contributions WHERE product_id=? AND active=1",
        (product_id,),
    )
    if len(active) == 1:
        conn.execute(
            "UPDATE contributions SET kind='FULL' WHERE product_id=? AND active=1",
            (product_id,),
        )
    elif len(active) == 0:
        conn.execute(
            "UPDATE products SET status='removed', updated_at=? WHERE id=? AND status='active'",
            (now(), product_id),
        )


def _ground_truth_for_account(conn, account: str):
    r = _row(
        conn,
        """SELECT COALESCE(SUM(amount),0) AS a, COALESCE(SUM(qty),0) AS q,
                  COUNT(*) AS n
           FROM records WHERE active=1 AND account=?""",
        (account,),
    )
    return {"amount": r["a"], "qty": r["q"], "count": r["n"]}


def revoke_import(conn, import_id: str, strategy: str, reason: str,
                  evidence_dir: str | Path, actor: str = "api") -> dict:
    """Revoke an import.

    strategy='compensate' — signed reversal ADJUSTMENT events (precise, cheap).
    strategy='rebuild'     — recompute every affected product from the surviving
                             original records (strongest, self-healing).
    Already-externalized products can never be silently retracted: they are
    invalidated and correction notices are published.
    """
    if strategy not in ("compensate", "rebuild"):
        raise ServiceError("strategy must be compensate or rebuild")
    imp = _row(conn, "SELECT * FROM imports WHERE id=?", (import_id,))
    if not imp:
        raise ServiceError(f"unknown import {import_id}")
    if imp["status"] == "revoked":
        existing = _row(conn, "SELECT * FROM revocations WHERE import_id=?",
                        (import_id,))
        raise Conflict(
            f"import already revoked by {existing['id']} — no second deduction")
    if imp["status"] in ("processing",):
        raise Conflict("import is being processed; retry after the job finishes")

    preview = preview_revocation(conn, import_id)
    revoc_id = new_id("rev")

    conn.execute("BEGIN IMMEDIATE")
    try:
        # ---- phase 1: source rows + FULL detail products
        conn.execute("UPDATE records SET active=0 WHERE import_id=?", (import_id,))
        details = _rows(
            conn,
            """SELECT p.* FROM products p JOIN contributions c ON c.product_id=p.id
               WHERE c.import_id=? AND p.type='detail' AND c.active=1""",
            (import_id,),
        )
        removed_detail_ids = []
        for d in details:
            r = _row(conn, "SELECT * FROM records WHERE record_uid=? LIMIT 1",
                     (d["key"].split(":", 1)[1],))
            _emit(conn, "ITEM_REMOVED", d["id"], import_id, revoc_id,
                  d["account"], -d["current_amount"], -d["current_qty"],
                  f"revoke:{revoc_id}:rm:{d['id']}",
                  note=f"revoked import {import_id}")
            conn.execute(
                "UPDATE products SET status='removed', updated_at=? WHERE id=?",
                (now(), d["id"]))
            removed_detail_ids.append(d["id"])

        # ---- phase 2: derived products that still carry this import
        derived = _rows(
            conn,
            """SELECT p.*, c.amount AS contrib_amount, c.qty AS contrib_qty,
                      c.record_count AS contrib_rows
               FROM contributions c JOIN products p ON p.id=c.product_id
               WHERE c.import_id=? AND c.active=1 AND p.type!='detail'""",
            (import_id,),
        )
        adjusted, rebuilt, invalidated = [], [], []
        for d in derived:
            ext = bool(d["externalized"])
            if strategy == "compensate":
                delta_amount = -d["contrib_amount"]
                delta_qty = -d["contrib_qty"]
                _emit(conn, "ADJUSTMENT", d["id"], import_id, revoc_id,
                      d["account"], delta_amount, delta_qty,
                      f"revoke:{revoc_id}:adj:{d['id']}",
                      note="precise reversal of revoked contribution")
                new_amount = d["current_amount"] + delta_amount
                new_qty = d["current_qty"] + delta_qty
                conn.execute(
                    """UPDATE products SET current_amount=?, current_qty=?,
                           record_count=record_count-?, updated_at=? WHERE id=?""",
                    (new_amount, new_qty, d["contrib_rows"], now(), d["id"]))
                adjusted.append({"product_id": d["id"], "type": d["type"],
                                 "delta": delta_amount, "new_amount": new_amount})
            else:  # rebuild from surviving original records
                if d["type"] == "aggregate":
                    gt = _ground_truth_for_account(conn, d["account"])
                    target_amount, target_qty, target_count = (
                        gt["amount"], gt["qty"], gt["count"])
                else:  # statement of this job: none of its rows survive
                    target_amount = target_qty = target_count = 0
                delta = target_amount - d["current_amount"]
                delta_q = target_qty - d["current_qty"]
                _emit(conn, "SNAPSHOT", d["id"], import_id, revoc_id,
                      d["account"], delta, delta_q,
                      f"rebuild:{revoc_id}:snap:{d['id']}",
                      note=f"rebuilt from surviving records; target={target_amount}")
                conn.execute(
                    """UPDATE products SET current_amount=?, current_qty=?,
                           record_count=?, updated_at=? WHERE id=?""",
                    (target_amount, target_qty, target_count, now(), d["id"]))
                rebuilt.append({"product_id": d["id"], "type": d["type"],
                                "old_amount": d["current_amount"],
                                "new_amount": target_amount, "delta": delta})

            # contributions of this import disappear; others keep feeding
            conn.execute(
                "UPDATE contributions SET active=0 WHERE product_id=? AND import_id=?",
                (d["id"], import_id))
            _reclassify_after_removal(conn, d["id"])

            # ---- externalized results: retraction is impossible
            if ext:
                conn.execute(
                    "UPDATE products SET status='invalidated', updated_at=? WHERE id=?",
                    (now(), d["id"]))
                fresh = _row(conn, "SELECT * FROM products WHERE id=?", (d["id"],))
                invalidated.append({"product_id": d["id"], "type": d["type"],
                                    "account": d["account"],
                                    "amount_after": fresh["current_amount"]})
                _emit(conn, "INVALIDATION", d["id"], import_id, revoc_id,
                      d["account"], 0, 0,
                      f"revoke:{revoc_id}:inval:{d['id']}",
                      note="already delivered downstream; notice only")
                if d["type"] == "aggregate":
                    _enqueue(
                        conn, "aggregate.corrected", d["id"], d["account"],
                        {"account": d["account"],
                         "old_amount": d["current_amount"],
                         "corrected_amount": fresh["current_amount"],
                         "revocation_id": revoc_id, "import_id": import_id},
                        origin="revocation",
                        idem_key=f"msg:corr:{revoc_id}:{d['id']}")
                else:
                    _enqueue(
                        conn, "product.invalidated", d["id"], d["account"],
                        {"product_id": d["id"], "title": d["title"],
                         "revocation_id": revoc_id, "import_id": import_id},
                        origin="revocation",
                        idem_key=f"msg:inval:{revoc_id}:{d['id']}")

        # detail contributions also deactivate
        conn.execute(
            "UPDATE contributions SET active=0 WHERE import_id=?", (import_id,))
        for d in details:
            _reclassify_after_removal(conn, d["id"])

        conn.execute(
            "UPDATE imports SET status='revoked', revoked_at=? WHERE id=?",
            (now(), import_id))
        report = {
            "revocation_id": revoc_id,
            "import_id": import_id,
            "import_name": imp["name"],
            "strategy": strategy,
            "reason": reason,
            "preview": preview,
            "removed_details": removed_detail_ids,
            "adjusted": adjusted,
            "rebuilt": rebuilt,
            "invalidated": invalidated,
            "guarantees": _guarantee_notes(strategy, bool(invalidated)),
            "created_at": now(),
        }
        conn.execute(
            """INSERT INTO revocations(id, import_id, strategy, reason,
                   status, report_json, created_at, finished_at)
               VALUES (?,?,?,?,'done',?,?,?)""",
            (revoc_id, import_id, strategy, reason,
             json.dumps(report, ensure_ascii=False), now(), now()))
        _oplog(conn, actor, "import.revoked",
               {"revocation_id": revoc_id, "import_id": import_id,
                "strategy": strategy,
                "invalidated": len(invalidated)})
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    evidence_dir = Path(evidence_dir) / "revocations"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    with open(evidence_dir / f"{revoc_id}.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return report


def _guarantee_notes(strategy: str, has_external: bool) -> list[str]:
    notes = [
        "内部产物（未外发）的余额由事件账本重算，撤销后与原始记录 ground truth 完全一致，保证正确。",
        "所有事件带幂等键：重复撤销、补偿重发不会产生第二次扣减。",
    ]
    if strategy == "compensate":
        notes.insert(0, "精确补偿：按该导入的实际贡献生成反向 ADJUSTMENT，增量小、成本低；"
                        "依赖贡献图准确，不重算历史。")
    else:
        notes.insert(0, "重建：从所有未撤销的原始记录重新计算受影响产物，可自愈任何历史漂移，"
                        "正确性最强，但需要全量扫描。")
    if has_external:
        notes.append("已外发结果无法物理撤回：系统只能下发失效通知（product.invalidated）"
                     "和更正通知（aggregate.corrected），是否被消费者采纳以 ack 为准。")
    return notes


# ================================================================ verification
def verify(conn) -> dict:
    """Independent invariants checked against the surviving ORIGINAL records."""
    problems = []
    accounts = [r["account"] for r in _rows(
        conn, "SELECT DISTINCT account FROM records")]

    gt_total = ev_total = prod_total = 0
    for account in accounts:
        gt = _ground_truth_for_account(conn, account)
        agg = _row(conn,
                   "SELECT * FROM products WHERE type='aggregate' AND key=? AND status!='removed'",
                   (f"agg:{account}",))
        ledger = _row(
            conn,
            "SELECT COALESCE(SUM(amount_delta),0) s FROM events WHERE product_id=?",
            (agg["id"],)) if agg else {"s": 0}
        if agg:
            if agg["current_amount"] != gt["amount"] or agg["current_qty"] != gt["qty"]:
                problems.append(
                    f"aggregate {account}: product={agg['current_amount']}/{agg['current_qty']} "
                    f"ground_truth={gt['amount']}/{gt['qty']}")
            if ledger["s"] != gt["amount"]:
                problems.append(
                    f"aggregate {account}: event ledger sums {ledger['s']} != ground truth {gt['amount']}")
            if agg["record_count"] != gt["count"]:
                problems.append(
                    f"aggregate {account}: record_count={agg['record_count']} != {gt['count']}")
            gt_total += gt["amount"]
            ev_total += ledger["s"]
            prod_total += agg["current_amount"]

    # every product balance must equal its event ledger
    for p in _rows(conn, "SELECT * FROM products WHERE status!='removed'"):
        s = _row(conn,
                 "SELECT COALESCE(SUM(amount_delta),0) s, COALESCE(SUM(qty_delta),0) q FROM events WHERE product_id=?",
                 (p["id"],))
        if s["s"] != p["current_amount"] or s["q"] != p["current_qty"]:
            problems.append(
                f"product {p['id']} ({p['type']}): ledger {s['s']}/{s['q']} != "
                f"stored {p['current_amount']}/{p['current_qty']}")

    # removed details must net to zero on the ledger
    for p in _rows(conn, "SELECT * FROM products WHERE status='removed'"):
        s = _row(conn,
                 "SELECT COALESCE(SUM(amount_delta),0) s FROM events WHERE product_id=?",
                 (p["id"],))
        if s["s"] != 0:
            problems.append(f"removed product {p['id']} ledger nets {s['s']}, expected 0")

    # revoked imports must have no active contribution left
    for r in _rows(conn, "SELECT id FROM imports WHERE status='revoked'"):
        n = _row(conn,
                 "SELECT COUNT(*) n FROM contributions WHERE import_id=? AND active=1",
                 (r["id"],))["n"]
        if n:
            problems.append(f"revoked import {r['id']} still has {n} active contributions")

    return {
        "ok": not problems,
        "problems": problems,
        "ground_truth_total_cents": gt_total,
        "event_ledger_total_cents": ev_total,
        "product_total_cents": prod_total,
        "checked_at": now(),
    }
