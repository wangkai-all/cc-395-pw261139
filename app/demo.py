"""End-to-end demonstrations run against real SQLite state.

Scenarios
---------
full          — complete data flow: 3 batches, merged aggregates, an offline
                subscriber, revocation (compensate + rebuild), redrive dedup,
                ground-truth verification.
concurrency   — revocation interleaves with a still-running chunked job and a
                brand-new batch; conflict handling, then successful retry.
idempotency   — duplicate redrive, duplicate revocation attempt, offline
                backlog replay; detail and aggregate totals must never be
                deducted twice.

Every run leaves its original import files under ``raw/`` and its revocation
reports under ``evidence/revocations/`` — both retained as audit evidence.
"""
from __future__ import annotations

import json
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path

from . import messaging, sample_data, service
from .db import connect, init_db


def _fresh(data_dir: Path, name: str) -> Path:
    d = data_dir / name
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    return d


def _snapshot(conn) -> dict:
    return {
        "imports": [dict(r) for r in conn.execute(
            "SELECT id, name, status, row_count FROM imports ORDER BY id")],
        "products": [dict(r) for r in conn.execute(
            "SELECT type, account, status, current_amount, current_qty, "
            "record_count, externalized FROM products ORDER BY type, account, id")],
        "verify": service.verify(conn),
    }


# ================================================================ full flow
def scenario_full(root: Path) -> dict:
    d = _fresh(root, "full")
    raw, evidence = d / "raw", d / "evidence"
    conn = connect(d / "db")
    init_db(conn)
    log = []

    files = sample_data.write_samples(d / "incoming")
    imps = {}
    for name, path in files.items():
        imps[name] = service.register_import(conn, path, raw_dir=raw, actor="demo")
        log.append(f"registered {name} → {imps[name]['id']}")

    # two subscribers: one always online, one starts offline
    online = messaging.create_consumer(conn, "在线订阅方-全量", "*", "online")
    offline = messaging.create_consumer(conn, "离线订阅方-ACC300", "ACC-300", "offline")

    for name in ("batch_1.json", "batch_2.json", "batch_3.json"):
        service.process_import(conn, imps[name]["id"], chunk_size=5, actor="demo")
    messaging.deliver_all(conn, actor="demo")
    v1 = service.verify(conn)
    log.append(f"3 batches processed; verify ok={v1['ok']} "
               f"total={v1['ground_truth_total_cents']} cents")

    # contribution graph must show PARTIAL on merged ACC aggregates
    merged = [dict(r) for r in conn.execute(
        "SELECT * FROM contributions WHERE kind='PARTIAL' AND active=1")]
    full_stmt = [dict(r) for r in conn.execute(
        "SELECT * FROM contributions WHERE kind='FULL' AND active=1")]
    log.append(f"contribution graph: {len(merged)} PARTIAL (merged), "
               f"{len(full_stmt)} FULL links")

    totals_before = service.verify(conn)["ground_truth_total_cents"]

    # ---- revoke B1 with precise compensation (its statements were delivered)
    r1 = service.revoke_import(
        conn, imps["batch_1.json"]["id"], "compensate",
        "B1 source file corrupted at origin", evidence, actor="demo")
    messaging.deliver_all(conn, actor="demo")
    v2 = service.verify(conn)
    log.append(f"revoke B1 (compensate): invalidated={len(r1['invalidated'])} "
               f"adjusted={len(r1['adjusted'])} verify ok={v2['ok']}")

    # ---- revoke B3 with rebuild (FULL-only account, no merge)
    r2 = service.revoke_import(
        conn, imps["batch_3.json"]["id"], "rebuild",
        "B3 uploaded against wrong period", evidence, actor="demo")
    messaging.deliver_all(conn, actor="demo")
    v3 = service.verify(conn)
    log.append(f"revoke B3 (rebuild): rebuilt={len(r2['rebuilt'])} verify ok={v3['ok']}")

    # ---- duplicate redrive: consumer applies correction at most once
    corr = conn.execute(
        "SELECT id FROM messages WHERE type='aggregate.corrected' LIMIT 1").fetchone()
    redrive_report = messaging.redrive(conn, corr["id"], actor="demo") if corr else None
    v4 = service.verify(conn)
    redrive_dups = sum(len(x["duplicates"])
                       for x in (redrive_report or {"results": {}})["results"].values())
    log.append(f"redrive correction: duplicate consumer acks={redrive_dups}, "
               f"verify still ok={v4['ok']}")

    # ---- duplicate revocation must be rejected, not deducted twice
    double_revoke = None
    try:
        service.revoke_import(conn, imps["batch_1.json"]["id"], "compensate",
                              "duplicate operator request", evidence, actor="demo")
    except service.Conflict as e:
        double_revoke = str(e)
    v5 = service.verify(conn)
    log.append(f"duplicate revocation blocked: {double_revoke!r}; verify ok={v5['ok']}")

    # ---- offline subscriber comes back: whole backlog replays, nothing lost
    messaging.set_consumer_status(conn, offline["id"], "online", actor="demo")
    replay = messaging.deliver_pending(conn, offline["id"], actor="reconnect")
    inbox = messaging.consumer_view(conn, offline["id"])
    pending_left = conn.execute(
        "SELECT COUNT(*) n FROM deliveries WHERE consumer_id=? AND status='pending'",
        (offline["id"],)).fetchone()["n"]
    log.append(f"offline consumer replay: {len(replay['delivered'])} backlog messages, "
               f"pending remaining={pending_left}, inbox size={len(inbox['messages'])}")

    totals_after = service.verify(conn)["ground_truth_total_cents"]
    # independent expectation: only B2 rows remain
    expect = conn.execute(
        "SELECT COALESCE(SUM(amount),0) s FROM records WHERE active=1").fetchone()["s"]
    snap = _snapshot(conn)
    return {
        "scenario": "full",
        "log": log,
        "totals_before_cents": totals_before,
        "totals_after_cents": totals_after,
        "expected_remaining_cents": expect,
        "no_double_deduction": totals_after == expect and v5["ok"],
        "partial_links": len(merged),
        "full_links": len(full_stmt),
        "redrive_duplicates": redrive_dups,
        "duplicate_revocation_blocked": double_revoke is not None,
        "offline_replay_delivered": len(replay["delivered"]),
        "final_verify": v5,
        "snapshot": snap,
        "evidence_files": sorted(str(p.relative_to(d))
                                 for p in d.rglob("*") if p.is_file()),
    }


# ================================================================ concurrency
def scenario_concurrency(root: Path) -> dict:
    d = _fresh(root, "concurrency")
    raw, evidence = d / "raw", d / "evidence"
    conn = connect(d / "db")
    init_db(conn)
    log = []

    def thread_conn():
        # each thread gets its own connection; SQLite file locks + busy_timeout
        # serialize the BEGIN IMMEDIATE transactions for real.
        c = connect(d / "db")
        init_db(c)  # IF NOT EXISTS — harmless on an existing schema
        return c

    files = sample_data.write_samples(d / "incoming")
    b1 = service.register_import(conn, files["batch_1.json"], raw_dir=raw, actor="demo")
    b2 = service.register_import(conn, files["batch_2.json"], raw_dir=raw, actor="demo")
    b3 = service.register_import(conn, files["batch_3.json"], raw_dir=raw, actor="demo")
    messaging.create_consumer(conn, "watcher", "*", "online")

    service.process_import(conn, b1["id"], chunk_size=5, actor="demo")
    messaging.deliver_all(conn)

    conflict_observed = []
    interleaving = []

    def revoke_b2_mid_job():
        # B2 is the import whose job is mid-flight → must be rejected with 409
        tc = thread_conn()
        status = service.get_import(tc, b2["id"])["status"]
        interleaving.append(f"revocation attempt while B2 is {status}")
        try:
            service.revoke_import(tc, b2["id"], "compensate",
                                  "concurrent operator revoke", evidence,
                                  actor="thread-revoke")
            interleaving.append("revocation succeeded unexpectedly")
        except service.Conflict as e:
            conflict_observed.append(str(e))
            interleaving.append(f"revocation correctly rejected: {e}")

    def register_and_process_b4():
        # a brand-new batch lands and fully processes while B2 is mid-flight
        tc = thread_conn()
        rows = sample_data._batch(4, 5, base=300, seed_uid=300)
        p = d / "incoming" / "batch_4.json"
        p.write_text(json.dumps(rows), encoding="utf-8")
        b4 = service.register_import(tc, p, raw_dir=raw, actor="thread-import")
        interleaving.append(f"new batch registered mid-flight: {b4['id']}")
        service.process_import(tc, b4["id"], chunk_size=5, actor="thread-import")
        interleaving.append(f"new batch processed: {b4['id']}")
        return b4

    b4_holder = {}

    def hook(chunk_index):
        if chunk_index == 0:
            t1 = threading.Thread(target=revoke_b2_mid_job)
            t2 = threading.Thread(target=lambda: b4_holder.update(b4=register_and_process_b4()))
            t1.start(); t2.start()
            t1.join(); t2.join()

    service.process_import(conn, b2["id"], chunk_size=5, actor="demo",
                           after_chunk=hook)
    messaging.deliver_all(conn)
    log.extend(interleaving)

    # now that B2's job finished, revoking B1 (a different, completed import)
    # must go through even though B4 and B2 committed concurrently
    report = service.revoke_import(conn, b1["id"], "compensate",
                                   "retried after job completion", evidence,
                                   actor="demo")
    messaging.deliver_all(conn)
    log.append(f"post-job revocation succeeded: {report['revocation_id']}")

    # B3 also revoked via rebuild to exercise both strategies under concurrency
    service.revoke_import(conn, b3["id"], "rebuild",
                          "period mismatch", evidence, actor="demo")
    messaging.deliver_all(conn)

    verify = service.verify(conn)
    expect = conn.execute(
        "SELECT COALESCE(SUM(amount),0) s FROM records WHERE active=1").fetchone()["s"]
    return {
        "scenario": "concurrency",
        "log": log,
        "conflict_observed": conflict_observed,
        "revocation_retried_ok": len(conflict_observed) == 1,
        "final_verify": verify,
        "totals_match_ground_truth": verify["ground_truth_total_cents"] == expect
                                     and verify["ok"],
        "snapshot": _snapshot(conn),
        "evidence_files": sorted(str(p.relative_to(d))
                                 for p in d.rglob("*") if p.is_file()),
    }


# ================================================================ idempotency
def scenario_idempotency(root: Path) -> dict:
    d = _fresh(root, "idempotency")
    raw, evidence = d / "raw", d / "evidence"
    conn = connect(d / "db")
    init_db(conn)
    log = []

    files = sample_data.write_samples(d / "incoming")
    b1 = service.register_import(conn, files["batch_1.json"], raw_dir=raw, actor="demo")
    b2 = service.register_import(conn, files["batch_2.json"], raw_dir=raw, actor="demo")
    c1 = messaging.create_consumer(conn, "flaky-consumer", "*", "offline")
    c2 = messaging.create_consumer(conn, "steady-consumer", "*", "online")

    service.process_import(conn, b1["id"], chunk_size=4, actor="demo")
    service.process_import(conn, b2["id"], chunk_size=4, actor="demo")
    messaging.deliver_all(conn)  # steady gets everything; flaky stays pending

    before = service.verify(conn)
    log.append(f"before revoke: total={before['ground_truth_total_cents']}")

    # flaky reconnects twice with the same backlog
    messaging.set_consumer_status(conn, c1["id"], "online", actor="demo")
    first = messaging.deliver_pending(conn, c1["id"], actor="reconnect-1")
    second = messaging.deliver_pending(conn, c1["id"], actor="reconnect-2")
    log.append(f"flaky first replay={len(first['delivered'])} "
               f"second replay={len(second['delivered'])} (must be 0)")

    # redrive every delivered message: all must hit the ack ledger as duplicates
    msg_ids = [r["id"] for r in conn.execute("SELECT id FROM messages")]
    dup_total = 0
    for mid in msg_ids:
        rep = messaging.redrive(conn, mid, actor="demo")
        dup_total += sum(len(x["duplicates"]) for x in rep["results"].values())
    log.append(f"redrove {len(msg_ids)} messages; consumer-side duplicates={dup_total}")

    report = service.revoke_import(conn, b1["id"], "compensate",
                                   "idempotency stress", evidence, actor="demo")
    messaging.deliver_all(conn)
    after_revoke = service.verify(conn)

    # redrive the correction message twice more
    corr = conn.execute(
        "SELECT id FROM messages WHERE origin='revocation' ORDER BY created_at LIMIT 1"
    ).fetchone()
    messaging.redrive(conn, corr["id"], actor="demo")
    messaging.redrive(conn, corr["id"], actor="demo")
    final = service.verify(conn)
    log.append(f"after revoke+2 redrives: total={final['ground_truth_total_cents']} "
               f"ok={final['ok']}")

    # repeated revoke attempts
    blocked = []
    for _ in range(2):
        try:
            service.revoke_import(conn, b1["id"], "compensate", "dup",
                                  evidence, actor="demo")
        except service.Conflict as e:
            blocked.append(str(e))

    # event-ledger uniqueness proof
    idem_rows = conn.execute(
        "SELECT COUNT(*) n, COUNT(DISTINCT idempotency_key) d FROM events"
    ).fetchone()

    return {
        "scenario": "idempotency",
        "log": log,
        "empty_second_replay": len(second["delivered"]) == 0,
        "redrive_duplicates": dup_total,
        "duplicate_revocations_blocked": len(blocked) == 2,
        "event_keys_all_unique": idem_rows["n"] == idem_rows["d"],
        "final_verify": final,
        "no_double_deduction": final["ok"] and
            final["ground_truth_total_cents"] ==
            conn.execute("SELECT COALESCE(SUM(amount),0) s FROM records WHERE active=1"
                         ).fetchone()["s"],
        "snapshot": _snapshot(conn),
        "evidence_files": sorted(str(p.relative_to(d))
                                 for p in d.rglob("*") if p.is_file()),
    }


def run_demo(data_dir: str | Path, scenario: str = "full") -> dict:
    root = Path(data_dir) / "demo-runs"
    root.mkdir(parents=True, exist_ok=True)
    runners = {
        "full": scenario_full,
        "concurrency": scenario_concurrency,
        "idempotency": scenario_idempotency,
    }
    if scenario == "all":
        results = {name: fn(root) for name, fn in runners.items()}
    else:
        results = {scenario: runners[scenario](root)}

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = root / f"report-{ts}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    results["_report_path"] = str(out)
    return results


def main():
    import argparse
    ap = argparse.ArgumentParser(description="Run revocation/concurrency demos")
    ap.add_argument("--data-dir", default="runtime/data")
    ap.add_argument("--scenario", choices=["full", "concurrency", "idempotency", "all"],
                    default="all")
    args = ap.parse_args()
    report = run_demo(args.data_dir, args.scenario)
    print(json.dumps({k: v for k, v in report.items() if not k.startswith("_")},
                     ensure_ascii=False, indent=2))
    print("report written:", report["_report_path"])


if __name__ == "__main__":
    main()
