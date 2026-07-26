"""
Store-and-forward spool.

If the cloud ingest endpoint is unreachable the fog node must not lose data --
that resilience is one of the main reasons the tier exists. Batches are
persisted to SQLite (durable across a fog-node restart, no server needed) and
replayed in order once connectivity returns.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import List, Optional, Tuple


class Spool:
    def __init__(self, path: str = "./fog_spool.db", max_rows: int = 10000) -> None:
        self.path = path
        self.max_rows = max_rows
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS spool ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " batch_id TEXT UNIQUE,"
            " payload TEXT NOT NULL,"
            " created_at INTEGER DEFAULT (strftime('%s','now')))"
        )
        self._conn.commit()

    def push(self, batch_id: str, payload: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO spool (batch_id, payload) VALUES (?, ?)",
                (batch_id, json.dumps(payload)),
            )
            # Bounded queue: shed the oldest first rather than exhausting the disk.
            self._conn.execute(
                "DELETE FROM spool WHERE id NOT IN "
                "(SELECT id FROM spool ORDER BY id DESC LIMIT ?)", (self.max_rows,)
            )
            self._conn.commit()

    def peek(self, limit: int = 10) -> List[Tuple[int, dict]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, payload FROM spool ORDER BY id ASC LIMIT ?", (limit,)
            ).fetchall()
        return [(row[0], json.loads(row[1])) for row in rows]

    def ack(self, row_id: int) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM spool WHERE id = ?", (row_id,))
            self._conn.commit()

    def depth(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM spool").fetchone()[0]

    def close(self) -> None:
        self._conn.close()
