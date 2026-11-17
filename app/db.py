"""SQLite schema and connection management.

All money amounts are integer cents so every aggregation is exact.
The database is the single durable state; the event ledger (``events``) is
append-only and every numeric product value must equal the sum of its events.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ---------------------------------------------------------------- imports
CREATE TABLE IF NOT EXISTS imports (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    source_path   TEXT NOT NULL,           -- original file, never deleted/moved
    file_sha256   TEXT NOT NULL,
    row_count     INTEGER NOT NULL,
    status        TEXT NOT NULL,           -- registered|processing|processed|revoked|failed
    error         TEXT,
    created_at    TEXT NOT NULL,
    processed_at  TEXT,
    revoked_at    TEXT
);

-- individual source rows, retained forever as revocation evidence
CREATE TABLE IF NOT EXISTS records (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    import_id    TEXT NOT NULL REFERENCES imports(id),
    record_uid   TEXT NOT NULL UNIQUE,     -- globally unique row identity
    account      TEXT NOT NULL,
    sku          TEXT NOT NULL,
    qty          INTEGER NOT NULL,
    amount       INTEGER NOT NULL,         -- cents
    ts           TEXT NOT NULL,
    active       INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_records_import ON records(import_id);
CREATE INDEX IF NOT EXISTS idx_records_account ON records(account);

-- ---------------------------------------------------------------- processing
CREATE TABLE IF NOT EXISTS jobs (
    id             TEXT PRIMARY KEY,
    import_id      TEXT NOT NULL REFERENCES imports(id),
    status         TEXT NOT NULL,          -- running|done|failed
    chunk_size     INTEGER NOT NULL,
    processed_rows INTEGER NOT NULL DEFAULT 0,
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    error          TEXT
);

-- ---------------------------------------------------------------- products
-- detail    : one product per source record
-- aggregate : per-account running totals (internal materialized view)
-- statement : per-job/per-account external-facing document
CREATE TABLE IF NOT EXISTS products (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,         -- detail|aggregate|statement
    key             TEXT NOT NULL UNIQUE,
    account         TEXT NOT NULL,
    title           TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',  -- active|removed|invalidated
    current_amount  INTEGER NOT NULL DEFAULT 0,
    current_qty     INTEGER NOT NULL DEFAULT 0,
    record_count    INTEGER NOT NULL DEFAULT 0,
    payload_json    TEXT,
    externalized    INTEGER NOT NULL DEFAULT 0,      -- ever delivered to a consumer
    created_job_id  TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_products_account ON products(account);
CREATE INDEX IF NOT EXISTS idx_products_type ON products(type);

-- contribution graph: which import feeds which product, and how strongly
CREATE TABLE IF NOT EXISTS contributions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id   TEXT NOT NULL REFERENCES products(id),
    import_id    TEXT NOT NULL REFERENCES imports(id),
    kind         TEXT NOT NULL,            -- FULL|PARTIAL
    record_count INTEGER NOT NULL,
    qty          INTEGER NOT NULL,         -- units contributed (signed sums)
    amount       INTEGER NOT NULL,         -- cents contributed by this import
    active       INTEGER NOT NULL DEFAULT 1,
    job_id       TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    UNIQUE(product_id, import_id)
);
CREATE INDEX IF NOT EXISTS idx_contrib_import ON contributions(import_id);

-- ---------------------------------------------------------------- event ledger
-- append-only; idempotency_key makes every mutation exactly-once even under
-- retries, duplicate compensation sends or double revocation.
CREATE TABLE IF NOT EXISTS events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    seq              INTEGER UNIQUE,       -- populated by trg_events_seq from id
    ts               TEXT NOT NULL,
    event_type       TEXT NOT NULL,        -- ITEM_ADDED|ITEM_REMOVED|ADJUSTMENT|SNAPSHOT|INVALIDATION
    product_id       TEXT NOT NULL REFERENCES products(id),
    import_id        TEXT,                 -- source import (nullable for snapshots)
    revoc_id         TEXT,                 -- revocation that produced the event
    account          TEXT NOT NULL,
    amount_delta     INTEGER NOT NULL,
    qty_delta        INTEGER NOT NULL,
    idempotency_key  TEXT NOT NULL UNIQUE,
    note             TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_product ON events(product_id);

-- seq mirrors the immutable row id automatically; safe under concurrent writers
CREATE TRIGGER IF NOT EXISTS trg_events_seq
AFTER INSERT ON events
WHEN NEW.seq IS NULL
BEGIN
    UPDATE events SET seq = NEW.id WHERE id = NEW.id;
END;

-- ---------------------------------------------------------------- revocations
CREATE TABLE IF NOT EXISTS revocations (
    id          TEXT PRIMARY KEY,
    import_id   TEXT NOT NULL UNIQUE REFERENCES imports(id),
    strategy    TEXT NOT NULL,             -- compensate|rebuild
    reason      TEXT NOT NULL,
    status      TEXT NOT NULL,             -- done|conflict
    report_json TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    finished_at TEXT NOT NULL
);

-- ---------------------------------------------------------------- subscribers
CREATE TABLE IF NOT EXISTS consumers (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    account_filter TEXT NOT NULL,          -- '*' or one account
    status         TEXT NOT NULL DEFAULT 'online', -- online|offline
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    type            TEXT NOT NULL,         -- statement.available|aggregate.updated|
                                           -- product.invalidated|aggregate.corrected
    product_id      TEXT REFERENCES products(id),
    account         TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    origin          TEXT NOT NULL,         -- job|revocation
    created_at      TEXT NOT NULL
);

-- one delivery row per (message, consumer); at most one ack ever
CREATE TABLE IF NOT EXISTS deliveries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      TEXT NOT NULL REFERENCES messages(id),
    consumer_id     TEXT NOT NULL REFERENCES consumers(id),
    status          TEXT NOT NULL DEFAULT 'pending', -- pending|delivered
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    delivered_at    TEXT,
    UNIQUE(message_id, consumer_id)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_consumer ON deliveries(consumer_id, status);

-- consumer-side dedup ledger: a redriven message is acknowledged at most once
CREATE TABLE IF NOT EXISTS consumer_acks (
    consumer_id TEXT NOT NULL REFERENCES consumers(id),
    message_id  TEXT NOT NULL REFERENCES messages(id),
    ts          TEXT NOT NULL,
    PRIMARY KEY (consumer_id, message_id)
);

CREATE TABLE IF NOT EXISTS oplog (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    actor       TEXT NOT NULL,
    action      TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
"""

_write_lock = threading.RLock()


def connect(data_dir: str | Path) -> sqlite3.Connection:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        data_dir / "ledger.db",
        timeout=15,
        isolation_level=None,  # explicit transaction control
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def write_lock() -> threading.RLock:
    """Process-wide writer lock; SQLite itself serializes writers, this also
    keeps multi-step transactions from interleaving inside one process."""
    return _write_lock
