"""
Persistence for the local/dev backend.

SQLite is used here as a stand-in for Azure Cosmos DB. The access pattern is
deliberately identical to the Cosmos one (partition by zone+sensor_type, sort
by timestamp) so the cloud implementation is a driver swap, not a rewrite.
"""
from __future__ import annotations

import sqlite3
import threading
from typing import Any, Dict, List, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS aggregates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT, fog_id TEXT, sensor_type TEXT, zone TEXT, unit TEXT,
    count INTEGER, min REAL, max REAL, mean REAL, p95 REAL, last REAL,
    window_start INTEGER, window_end INTEGER
);
CREATE INDEX IF NOT EXISTS ix_agg_key ON aggregates(sensor_type, zone, window_end);

CREATE TABLE IF NOT EXISTS anomalies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT, fog_id TEXT, sensor_id TEXT, sensor_type TEXT, zone TEXT,
    ts INTEGER, value REAL, kind TEXT, severity TEXT, detail TEXT
);
CREATE INDEX IF NOT EXISTS ix_anom_ts ON anomalies(ts DESC);

CREATE TABLE IF NOT EXISTS raw_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT, sensor_id TEXT, sensor_type TEXT, zone TEXT,
    ts INTEGER, raw REAL, smoothed REAL
);
CREATE INDEX IF NOT EXISTS ix_raw ON raw_samples(sensor_type, ts DESC);

-- Idempotency ledger: makes ingest safe to retry (at-least-once -> effectively-once)
CREATE TABLE IF NOT EXISTS processed_batches (
    batch_id TEXT PRIMARY KEY, received_at INTEGER DEFAULT (strftime('%s','now'))
);
"""


class Store:
    def __init__(self, path: str = "./backend.db") -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def already_processed(self, batch_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM processed_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        return row is not None

    def save_envelope(self, env: Dict[str, Any]) -> bool:
        """
        Persist one fog envelope. Returns False if it was a duplicate.

        The duplicate check and the writes happen under a single lock and a
        single transaction. Doing the check outside the lock would let two
        concurrent workers both pass it and double-insert the same batch --
        exactly the race that at-least-once queue delivery creates.
        """
        batch_id = env.get("batch_id", "")
        fog_id = env.get("fog_id", "")
        with self._lock:
            cur = self._conn.cursor()
            if batch_id:
                cur.execute("INSERT OR IGNORE INTO processed_batches (batch_id) VALUES (?)",
                            (batch_id,))
                if cur.rowcount == 0:          # someone already claimed this batch
                    self._conn.commit()
                    return False
            for a in env.get("aggregates", []):
                cur.execute(
                    "INSERT INTO aggregates (batch_id, fog_id, sensor_type, zone, unit,"
                    " count, min, max, mean, p95, last, window_start, window_end)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (batch_id, fog_id, a["sensor_type"], a["zone"], a.get("unit", ""),
                     a["count"], a["min"], a["max"], a["mean"], a["p95"], a["last"],
                     a.get("window_start"), a.get("window_end")),
                )
            for x in env.get("anomalies", []):
                cur.execute(
                    "INSERT INTO anomalies (batch_id, fog_id, sensor_id, sensor_type,"
                    " zone, ts, value, kind, severity, detail) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (batch_id, fog_id, x["sensor_id"], x["sensor_type"], x["zone"],
                     x["ts"], x["value"], x["kind"], x["severity"], x.get("detail", "")),
                )
            for s in env.get("raw_sample", []):
                cur.execute(
                    "INSERT INTO raw_samples (batch_id, sensor_id, sensor_type, zone,"
                    " ts, raw, smoothed) VALUES (?,?,?,?,?,?,?)",
                    (batch_id, s["sensor_id"], s["sensor_type"], s["zone"],
                     s["ts"], s["raw"], s["smoothed"]),
                )
            self._conn.commit()
        return True

    # ---- read models used by the dashboard ------------------------------
    def latest_by_type(self) -> List[Dict[str, Any]]:
        sql = ("SELECT sensor_type, zone, unit, last, mean, min, max, count, window_end"
               " FROM aggregates a WHERE window_end = ("
               "   SELECT MAX(window_end) FROM aggregates b"
               "   WHERE b.sensor_type = a.sensor_type AND b.zone = a.zone)"
               " ORDER BY sensor_type, zone")
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql).fetchall()]

    def series(self, sensor_type: str, limit: int = 120) -> List[Dict[str, Any]]:
        sql = ("SELECT zone, window_end AS ts, mean, min, max, p95, unit FROM aggregates"
               " WHERE sensor_type = ? ORDER BY window_end DESC LIMIT ?")
        with self._lock:
            rows = [dict(r) for r in self._conn.execute(sql, (sensor_type, limit)).fetchall()]
        return list(reversed(rows))

    def recent_anomalies(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._conn.execute(
                "SELECT * FROM anomalies ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()]

    def counts(self) -> Dict[str, int]:
        with self._lock:
            c = self._conn.cursor()
            return {
                "aggregates": c.execute("SELECT COUNT(*) FROM aggregates").fetchone()[0],
                "anomalies": c.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0],
                "raw_samples": c.execute("SELECT COUNT(*) FROM raw_samples").fetchone()[0],
                "batches": c.execute("SELECT COUNT(*) FROM processed_batches").fetchone()[0],
            }

    def sensor_types(self) -> List[str]:
        with self._lock:
            return [r[0] for r in self._conn.execute(
                "SELECT DISTINCT sensor_type FROM aggregates ORDER BY 1").fetchall()]
