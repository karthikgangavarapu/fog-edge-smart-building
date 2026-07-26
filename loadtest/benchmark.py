"""
Two measurements used in the project report.

1. DATA REDUCTION: how many bytes the fog tier stops from crossing the WAN.
   The counterfactual is a "dumb gateway" that forwards every raw reading.

2. INGEST THROUGHPUT: how many fog batches/second the backend accepts, and
   what the accept latency looks like, with the queue absorbing the write.

Usage:
    python -m loadtest.benchmark reduction
    python -m loadtest.benchmark ingest --url http://localhost:8000/api/ingest \
        --requests 500 --concurrency 32
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
import time
import urllib.request

from fog.node import FogNode
from sensors.config import load_config
from sensors.runner import SensorRunner
from sensors.transport import InProcTransport


class _CountingDispatcher:
    """Captures envelopes instead of sending them, so we can weigh them."""

    def __init__(self):
        self.envelopes = []
        self.sent = 0

        class _S:
            def depth(self_inner):
                return 0
        self.spool = _S()

    def enqueue(self, payload):
        self.envelopes.append(payload)
        self.sent += 1

    def start(self, interval=2.0):
        pass

    def stop(self):
        pass


def reduction(seconds: float = 30.0) -> None:
    cfg = load_config()
    cfg["transport"]["kind"] = "inproc"
    transport = InProcTransport()
    transport.start()

    raw_bytes = {"n": 0, "count": 0}
    disp = _CountingDispatcher()
    fog = FogNode(cfg, transport, dispatcher=disp)

    original_on_message = fog.on_message

    def spy(topic, payload):
        raw_bytes["n"] += len(json.dumps(payload).encode())
        raw_bytes["count"] += len(payload.get("readings", []))
        original_on_message(topic, payload)

    fog.on_message = spy
    fog.start()

    sensors = SensorRunner(cfg, transport)
    sensors.start()
    time.sleep(seconds)
    sensors.stop()
    time.sleep(1.0)
    fog.stop()
    env = fog.build_envelope()
    if env:
        disp.enqueue(env)
    transport.stop()

    fog_bytes = sum(len(json.dumps(e).encode()) for e in disp.envelopes)
    print(f"window                : {seconds:.0f}s")
    print(f"raw readings          : {raw_bytes['count']}")
    print(f"raw bytes (no fog)    : {raw_bytes['n']:,}")
    print(f"fog envelopes sent    : {len(disp.envelopes)}")
    print(f"fog bytes (with fog)  : {fog_bytes:,}")
    if raw_bytes["n"]:
        print(f"byte reduction        : {100 * (1 - fog_bytes / raw_bytes['n']):.1f}%")
    print(f"WAN requests avoided  : {raw_bytes['count']} readings -> "
          f"{len(disp.envelopes)} HTTPS POSTs")
    print(f"anomalies at the edge : {fog.anomalies.count}")


def ingest(url: str, api_key: str, requests_n: int, concurrency: int) -> None:
    latencies: list[float] = []
    errors = {"n": 0}
    lock = threading.Lock()

    def envelope(i: int) -> dict:
        return {
            "batch_id": f"bench-{i}", "fog_id": "bench", "schema_version": "1.0",
            "window_start": 0, "window_end": 0, "raw_sample": [], "stats": {},
            "aggregates": [{"sensor_type": "temperature", "zone": f"z{i % 8}",
                            "unit": "C", "count": 50, "min": 20.0, "max": 22.0,
                            "mean": 21.0, "p95": 21.9, "last": 21.0,
                            "window_start": 0, "window_end": 0}],
            "anomalies": [],
        }

    def worker(start: int, step: int) -> None:
        for i in range(start, requests_n, step):
            body = json.dumps(envelope(i)).encode()
            req = urllib.request.Request(url, data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("X-Api-Key", api_key)
            t0 = time.perf_counter()
            try:
                with urllib.request.urlopen(req, timeout=15) as r:
                    r.read()
                dt = (time.perf_counter() - t0) * 1000
                with lock:
                    latencies.append(dt)
            except Exception:
                with lock:
                    errors["n"] += 1

    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(k, concurrency))
               for k in range(concurrency)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    elapsed = time.perf_counter() - t0

    if not latencies:
        print("no successful requests")
        return
    latencies.sort()
    print(f"requests              : {requests_n} (concurrency {concurrency})")
    print(f"errors                : {errors['n']}")
    print(f"elapsed               : {elapsed:.2f}s")
    print(f"throughput            : {len(latencies) / elapsed:.1f} req/s")
    print(f"latency mean          : {statistics.fmean(latencies):.1f} ms")
    print(f"latency p50           : {latencies[len(latencies)//2]:.1f} ms")
    print(f"latency p95           : {latencies[int(len(latencies)*0.95)]:.1f} ms")
    print(f"latency p99           : {latencies[int(len(latencies)*0.99)]:.1f} ms")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["reduction", "ingest"])
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--url", default="http://localhost:8000/api/ingest")
    ap.add_argument("--api-key", default="local-dev-key")
    ap.add_argument("--requests", type=int, default=500)
    ap.add_argument("--concurrency", type=int, default=32)
    a = ap.parse_args()
    if a.mode == "reduction":
        reduction(a.seconds)
    else:
        ingest(a.url, a.api_key, a.requests, a.concurrency)
