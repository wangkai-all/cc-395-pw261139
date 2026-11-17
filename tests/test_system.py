"""Backend integration tests: real SQLite, real threads, real file evidence.

Run:  python3 -I tests/test_system.py   (with repo root on sys.path)
Exits non-zero on any failure.
"""
import json
import shutil
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import messaging, sample_data, service  # noqa: E402
from app.db import connect, init_db  # noqa: E402

TMP = ROOT / "runtime" / "tests"
PASS, FAIL = 0, 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"PASS  {name}")
    else:
        FAIL += 1
        print(f"FAIL  {name}")


def fresh(name):
    d = TMP / name
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    conn = connect(d / "db")
    init_db(conn)
    return d, conn


def test_full_lifecycle():
    d, conn = fresh("lifecycle")
    raw, ev = d / "raw", d / "evidence"
    files = sample_data.write_samples(d / "in")
    imps = {n: service.register_import(conn, p, raw_dir=raw) for n, p in files.items()}
    check("imports retained as raw evidence",
          all((raw / f"{v['id']}__{n}").is_file() for n, v in imps.items()))

    for n in files:
        service.process_import(conn, imps[n]["id"], chunk_size=4)
    v = service.verify(conn)
    check("all products match ground truth before revoke", v["ok"])

    # merged aggregates must be PARTIAL, details FULL
    kinds = {r["kind"] for r in conn.execute(
        "SELECT DISTINCT kind FROM contributions WHERE active=1 AND product_id IN "
        "(SELECT id FROM products WHERE type='aggregate')")}
    check("merged aggregates downgraded to PARTIAL", "PARTIAL" in kinds)
    check("detail products stay FULL", conn.execute(
        "SELECT COUNT(*) FROM contributions c JOIN products p ON p.id=c.product_id "
        "WHERE p.type='detail' AND c.kind='FULL' AND c.active=1").fetchone()[0] > 0)

    # compensation path
    r1 = service.revoke_import(conn, imps["batch_1.json"]["id"], "compensate",
                               "test", ev)
    check("compensation emits ADJUSTMENT events",
          conn.execute("SELECT COUNT(*) FROM events WHERE event_type='ADJUSTMENT' "
                       "AND revoc_id=?", (r1["revocation_id"],)).fetchone()[0] > 0)
    check("compensation report persisted as evidence",
          (ev / "revocations" / f"{r1['revocation_id']}.json").is_file())
    check("verify after compensate", service.verify(conn)["ok"])

    # rebuild path
    r3 = service.revoke_import(conn, imps["batch_3.json"]["id"], "rebuild",
                               "test", ev)
    check("rebuild emits SNAPSHOT events",
          conn.execute("SELECT COUNT(*) FROM events WHERE event_type='SNAPSHOT' "
                       "AND revoc_id=?", (r3["revocation_id"],)).fetchone()[0] > 0)
    check("verify after rebuild", service.verify(conn)["ok"])

    # ground truth: only batch_2 rows remain active
    expect = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM records WHERE active=1").fetchone()[0]
    got = service.verify(conn)["ground_truth_total_cents"]
    check("final total equals surviving original records", expect == got)

    # revoked import keeps its original file and rows (marked inactive)
    check("revoked rows retained (active=0) as evidence", conn.execute(
        "SELECT COUNT(*) FROM records WHERE import_id=? AND active=0",
        (imps["batch_1.json"]["id"],)).fetchone()[0] == 12)


def test_externalized_invalidation():
    d, conn = fresh("external")
    raw, ev = d / "raw", d / "evidence"
    files = sample_data.write_samples(d / "in")
    b1 = service.register_import(conn, files["batch_1.json"], raw_dir=raw)
    c1 = messaging.create_consumer(conn, "sub", "*", "online")
    service.process_import(conn, b1["id"], chunk_size=4)
    messaging.deliver_pending(conn, c1["id"])

    ext_before = conn.execute(
        "SELECT COUNT(*) FROM products WHERE externalized=1").fetchone()[0]
    check("delivery marks products externalized", ext_before > 0)

    r = service.revoke_import(conn, b1["id"], "compensate", "test", ev)
    check("externalized products are invalidated, not silently removed",
          len(r["invalidated"]) == ext_before)
    check("invalidation notices enqueued", conn.execute(
        "SELECT COUNT(*) FROM messages WHERE type IN "
        "('product.invalidated','aggregate.corrected')").fetchone()[0] > 0)
    check("invalidated product balances still internally correct",
          service.verify(conn)["ok"])


def test_idempotency():
    d, conn = fresh("idempotent")
    raw, ev = d / "raw", d / "evidence"
    files = sample_data.write_samples(d / "in")
    b1 = service.register_import(conn, files["batch_1.json"], raw_dir=raw)
    b2 = service.register_import(conn, files["batch_2.json"], raw_dir=raw)
    c1 = messaging.create_consumer(conn, "flake", "*", "offline")
    c2 = messaging.create_consumer(conn, "steady", "*", "online")
    service.process_import(conn, b1["id"], chunk_size=4)
    service.process_import(conn, b2["id"], chunk_size=4)
    messaging.deliver_all(conn)

    # offline backlog replays exactly once
    messaging.set_consumer_status(conn, c1["id"], "online")
    first = messaging.deliver_pending(conn, c1["id"])
    second = messaging.deliver_pending(conn, c1["id"])
    check("offline backlog delivered once", len(first["delivered"]) > 0
          and len(second["delivered"]) == 0)

    # redrive every message: consumer acks must not double-apply
    before = service.verify(conn)["ground_truth_total_cents"]
    for mid in [r["id"] for r in conn.execute("SELECT id FROM messages")]:
        messaging.redrive(conn, mid)
    check("redrives do not change balances",
          service.verify(conn)["ground_truth_total_cents"] == before)

    # revocation then repeated redrives of its correction
    service.revoke_import(conn, b1["id"], "compensate", "t", ev)
    messaging.deliver_all(conn)
    after = service.verify(conn)
    corr = conn.execute(
        "SELECT id FROM messages WHERE origin='revocation' LIMIT 1").fetchone()["id"]
    messaging.redrive(conn, corr)
    messaging.redrive(conn, corr)
    check("correction redrives do not double-deduct", service.verify(conn) == after
          or service.verify(conn)["ground_truth_total_cents"] == after["ground_truth_total_cents"])

    # duplicate revocation rejected
    blocked = 0
    for _ in range(2):
        try:
            service.revoke_import(conn, b1["id"], "compensate", "t", ev)
        except service.Conflict:
            blocked += 1
    check("duplicate revocations blocked", blocked == 2)

    # event ledger idempotency keys are globally unique
    n, u = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT idempotency_key) FROM events").fetchone()
    check("every ledger event has a unique idempotency key", n == u)


def test_concurrency():
    d, conn = fresh("concurrent")
    raw, ev = d / "raw", d / "evidence"
    files = sample_data.write_samples(d / "in")
    b1 = service.register_import(conn, files["batch_1.json"], raw_dir=raw)
    b2 = service.register_import(conn, files["batch_2.json"], raw_dir=raw)
    service.process_import(conn, b1["id"], chunk_size=5)

    conflicts, done = [], []

    def worker_revoke_b2():
        tc = connect(d / "db")
        try:
            service.revoke_import(tc, b2["id"], "compensate", "mid-flight", ev)
            done.append("unexpected")
        except service.Conflict as e:
            conflicts.append(str(e))

    def worker_new_batch():
        tc = connect(d / "db")
        rows = sample_data._batch(9, 4, base=900, seed_uid=900)
        p = d / "in" / "b9.json"
        p.write_text(json.dumps(rows), encoding="utf-8")
        b9 = service.register_import(tc, p, raw_dir=raw)
        service.process_import(tc, b9["id"], chunk_size=2)
        done.append(b9["id"])

    def hook(i):
        if i == 0:
            threads = [threading.Thread(target=worker_revoke_b2),
                       threading.Thread(target=worker_new_batch)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

    service.process_import(conn, b2["id"], chunk_size=5, after_chunk=hook)
    check("revoke of in-flight import rejected with conflict", len(conflicts) == 1)
    check("concurrent new batch committed", len(done) == 1)
    check("verify after interleaved concurrency", service.verify(conn)["ok"])

    # after the job finishes, revocation succeeds and totals stay exact
    service.revoke_import(conn, b1["id"], "compensate", "after", ev)
    v = service.verify(conn)
    expect = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM records WHERE active=1").fetchone()[0]
    check("post-job revoke keeps totals exact", v["ok"]
          and v["ground_truth_total_cents"] == expect)


def test_garbage_inputs():
    d, conn = fresh("garbage")
    bad = d / "bad.json"
    bad.write_text(json.dumps([{"record_uid": "x"}]), encoding="utf-8")
    try:
        service.register_import(conn, bad, raw_dir=d / "raw")
        check("reject rows missing required fields", False)
    except service.ServiceError:
        check("reject rows missing required fields", True)

    empty = d / "empty.json"
    empty.write_text("[]", encoding="utf-8")
    try:
        service.register_import(conn, empty, raw_dir=d / "raw")
        check("reject empty import", False)
    except service.ServiceError:
        check("reject empty import", True)


if __name__ == "__main__":
    test_full_lifecycle()
    test_externalized_invalidation()
    test_idempotency()
    test_concurrency()
    test_garbage_inputs()
    print(f"\n{PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)
