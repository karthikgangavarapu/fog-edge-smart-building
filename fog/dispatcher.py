"""
Cloud dispatcher: ships fog batches to the backend ingest endpoint.

Reliability features, all of which matter on a flaky building network:
* HTTPS POST with an API key header (shared-secret auth at the edge).
* Exponential backoff with jitter, which avoids a thundering herd when a whole
  campus of fog nodes reconnects at once after an outage.
* Idempotency key (``batch_id``) so a retry after a lost ACK cannot duplicate
  data in the cloud store.
* Spool-first semantics: a batch is only removed from the durable spool after
  the backend has acknowledged it (at-least-once delivery).

Only the Python standard library is used so the fog node stays deployable on a
minimal gateway image.
"""
from __future__ import annotations

import json
import logging
import random
import threading
import time
import urllib.error
import urllib.request
from typing import Optional

from .spool import Spool

log = logging.getLogger("fog.dispatcher")


class CloudDispatcher:
    def __init__(self, url: str, api_key: str = "", timeout: float = 10.0,
                 max_retries: int = 5, spool: Optional[Spool] = None) -> None:
        self.url = url
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.spool = spool or Spool()
        self.sent = 0
        self.failed = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ---- transport ----------------------------------------------------
    def _post(self, payload: dict) -> bool:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Api-Key", self.api_key)
        req.add_header("X-Idempotency-Key", payload.get("batch_id", ""))
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return 200 <= resp.status < 300
        except urllib.error.HTTPError as exc:
            # 4xx (except 429) is a poison message: retrying will never help.
            if 400 <= exc.code < 500 and exc.code != 429:
                log.error("permanent ingest failure %s, dropping batch", exc.code)
                return True
            log.warning("ingest HTTP %s", exc.code)
        except Exception as exc:            # network down, DNS, TLS, timeout
            log.warning("ingest transport error: %s", exc)
        return False

    # ---- public API ---------------------------------------------------
    def enqueue(self, payload: dict) -> None:
        """Durably record the batch; the flush loop owns actual delivery."""
        self.spool.push(payload.get("batch_id", ""), payload)

    def flush_once(self) -> int:
        """Attempt to deliver spooled batches. Returns number delivered."""
        delivered = 0
        for row_id, payload in self.spool.peek(limit=20):
            ok = False
            for attempt in range(self.max_retries):
                if self._post(payload):
                    ok = True
                    break
                backoff = min(30.0, (2 ** attempt)) + random.uniform(0, 0.5)
                time.sleep(backoff)
            if ok:
                self.spool.ack(row_id)
                self.sent += 1
                delivered += 1
            else:
                self.failed += 1
                break   # preserve ordering; try again on the next cycle
        return delivered

    def start(self, interval: float = 2.0) -> None:
        self._running = True

        def loop() -> None:
            while self._running:
                try:
                    self.flush_once()
                except Exception:               # never let the loop die
                    log.exception("flush cycle failed")
                time.sleep(interval)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        try:
            self.flush_once()
        except Exception:
            pass
