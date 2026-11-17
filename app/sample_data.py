"""Deterministic sample import files (integer cents, globally unique uids)."""
from __future__ import annotations

import json
from pathlib import Path

ACCOUNTS = ["ACC-100", "ACC-200", "ACC-300"]
SKUS = ["SKU-A", "SKU-B", "SKU-C", "SKU-D"]


def _batch(batch_no: int, n: int, base: int, *, accounts=None, seed_uid: int = 0):
    accounts = accounts or ACCOUNTS
    rows = []
    for i in range(n):
        uid = f"UID-{seed_uid + base + i:05d}"
        rows.append({
            "record_uid": uid,
            "account": accounts[(base + i) % len(accounts)],
            "sku": SKUS[(base + i) % len(SKUS)],
            "qty": 1 + ((base + i) % 4),
            "amount": 1000 + ((base * 7 + i * 137) % 9000),  # cents
            "ts": f"2026-11-{(batch_no % 27) + 1:02d}T09:{(i * 7) % 60:02d}:00Z",
        })
    return rows


def write_samples(dir_path: str | Path) -> dict[str, str]:
    """Three batches. B1 and B2 share accounts (merged aggregates → PARTIAL);
    B3 deliberately only touches ACC-300 to keep a clean FULL example too."""
    d = Path(dir_path)
    d.mkdir(parents=True, exist_ok=True)
    batches = {
        "batch_1.json": _batch(1, 12, base=0, seed_uid=0),
        "batch_2.json": _batch(2, 10, base=100, seed_uid=100),
        "batch_3.json": _batch(3, 6, base=200, accounts=["ACC-300"], seed_uid=200),
    }
    out = {}
    for name, rows in batches.items():
        p = d / name
        with open(p, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        out[name] = str(p)
    return out
