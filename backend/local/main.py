"""
Local backend: a faithful, dependency-light mirror of the AWS deployment.

Cloud mapping
-------------
    FastAPI  POST /api/ingest      <->  API Gateway + Lambda "ingest"
    asyncio.Queue                  <->  Amazon SQS queue "telemetry"
    _worker() consumer task        <->  Lambda "processor" (SQS event source)
    SQLite Store                   <->  Amazon DynamoDB (single table)
    /api/* read endpoints          <->  API Gateway + Lambda "query"

Keeping the two in lock-step means the demo can be run offline and the cloud
version can be validated against the same test-suite.

Run:  uvicorn backend.local.main:app --port 8000
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from .store import Store

API_KEY = os.environ.get("FOG_API_KEY", "local-dev-key")
QUEUE_MAXSIZE = int(os.environ.get("QUEUE_MAXSIZE", "10000"))
WORKERS = int(os.environ.get("QUEUE_WORKERS", "4"))

app = FastAPI(title="Fog & Edge Smart-Building Backend", version="1.0.0")
store = Store(os.environ.get("BACKEND_DB", "./backend.db"))

# The in-process queue stands in for Amazon SQS. It is what decouples the
# latency of ingest (must be fast, the fog node is waiting) from the latency of
# persistence (can be slow, can be retried, can be scaled independently).
# Created on startup, not at import time, so it binds to the running event loop.
queue: "asyncio.Queue[Dict[str, Any]] | None" = None

METRICS = {"ingested": 0, "processed": 0, "duplicates": 0, "rejected": 0,
           "queue_high_water": 0, "started_at": time.time()}


async def _worker(worker_id: int) -> None:
    """Competing consumer. N of these == the autoscaled Functions consumer group."""
    while True:
        env = await queue.get()
        try:
            applied = await asyncio.to_thread(store.save_envelope, env)
            METRICS["processed"] += 1
            if not applied:
                METRICS["duplicates"] += 1
        except Exception:
            # On AWS this is where the message would be returned to SQS and
            # eventually dead-lettered after maxReceiveCount attempts.
            METRICS["rejected"] += 1
        finally:
            queue.task_done()


@app.on_event("startup")
async def _startup() -> None:
    global queue
    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    for i in range(WORKERS):
        asyncio.create_task(_worker(i))


@app.post("/api/ingest")
async def ingest(request: Request, x_api_key: str = Header(default="")) -> JSONResponse:
    """
    Fog-facing write path. Validates, enqueues, returns 202 immediately.

    Returning 202 rather than 201 is intentional: the contract is "accepted for
    processing", which is what lets the queue absorb bursts without the fog
    node blocking or timing out.
    """
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")
    env = await request.json()
    if "batch_id" not in env or "aggregates" not in env:
        raise HTTPException(status_code=400, detail="malformed envelope")
    try:
        queue.put_nowait(env)
    except asyncio.QueueFull:
        # Backpressure: tell the fog node to retry later rather than dropping.
        raise HTTPException(status_code=429, detail="ingest queue saturated")
    METRICS["ingested"] += 1
    METRICS["queue_high_water"] = max(METRICS["queue_high_water"], queue.qsize())
    return JSONResponse({"status": "accepted", "batch_id": env["batch_id"]}, status_code=202)


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok", "queue_depth": queue.qsize() if queue else 0,
            "uptime_s": round(time.time() - METRICS["started_at"], 1)}


@app.get("/api/metrics")
async def metrics() -> dict:
    return {**METRICS, "queue_depth": queue.qsize() if queue else 0, **store.counts()}


@app.get("/api/summary")
async def summary() -> dict:
    return {"latest": store.latest_by_type(), "types": store.sensor_types()}


@app.get("/api/series/{sensor_type}")
async def series(sensor_type: str, limit: int = 120) -> dict:
    return {"sensor_type": sensor_type, "points": store.series(sensor_type, limit)}


@app.get("/api/anomalies")
async def anomalies(limit: int = 50) -> dict:
    return {"anomalies": store.recent_anomalies(limit)}


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return (Path(__file__).parent / "static" / "dashboard.html").read_text(encoding="utf-8")
