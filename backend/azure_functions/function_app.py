"""
Azure Functions backend (Python v2 programming model).

Topology
--------
    fog node --HTTPS--> [ingest]  --output binding--> Service Bus queue
                                                            |
                                              (queue trigger, autoscaled)
                                                            v
                                                      [processor] --> Cosmos DB
                                                            ^
    browser  --HTTPS--> [query]/[dashboard] ----------------+

Why this shape
--------------
* ``ingest`` does no I/O beyond the queue send, so its execution time is flat
  and the Consumption plan can scale it out linearly with request rate.
* Service Bus gives durability, at-least-once delivery, automatic retry and a
  dead-letter queue, so a transient Cosmos failure never loses telemetry.
* ``processor`` is scaled independently by the Azure Functions target-based
  scaler, which adds instances based on queue length, so the queue is the
  autoscaling signal as well as the buffer.
* Cosmos partition key is ``/zone`` so per-zone dashboard queries are
  single-partition (cheap in RU) and writes spread across zones.

Local dev equivalent lives in backend/local/main.py.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import azure.functions as func

API_KEY = os.environ.get("FOG_API_KEY", "")
COSMOS_DB = os.environ.get("COSMOS_DB", "telemetry")
COSMOS_CONTAINER = os.environ.get("COSMOS_CONTAINER", "readings")

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# ---------------------------------------------------------------------------
# 1. INGEST: HTTP in, Service Bus out. Deliberately trivial and fast.
# ---------------------------------------------------------------------------
@app.function_name(name="ingest")
@app.route(route="ingest", methods=["POST"])
@app.service_bus_queue_output(
    arg_name="msg", queue_name="telemetry", connection="ServiceBusConnection"
)
def ingest(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    if API_KEY and req.headers.get("X-Api-Key") != API_KEY:
        return func.HttpResponse(json.dumps({"error": "unauthorized"}), status_code=401,
                                 mimetype="application/json")
    try:
        envelope: Dict[str, Any] = req.get_json()
    except ValueError:
        return func.HttpResponse(json.dumps({"error": "invalid json"}), status_code=400,
                                 mimetype="application/json")
    if "batch_id" not in envelope or "aggregates" not in envelope:
        return func.HttpResponse(json.dumps({"error": "malformed envelope"}),
                                 status_code=400, mimetype="application/json")

    envelope["_received_at"] = datetime.now(timezone.utc).isoformat()
    msg.set(json.dumps(envelope))
    logging.info("accepted batch %s from fog %s", envelope["batch_id"], envelope.get("fog_id"))
    # 202: accepted for asynchronous processing (see report, Sec. III-C).
    return func.HttpResponse(
        json.dumps({"status": "accepted", "batch_id": envelope["batch_id"]}),
        status_code=202, mimetype="application/json")


# ---------------------------------------------------------------------------
# 2. PROCESSOR: Service Bus trigger, Cosmos DB out. Scales on queue depth.
# ---------------------------------------------------------------------------
@app.function_name(name="processor")
@app.service_bus_queue_trigger(
    arg_name="msg", queue_name="telemetry", connection="ServiceBusConnection"
)
@app.cosmos_db_output(
    arg_name="documents", connection="CosmosConnection",
    database_name=COSMOS_DB, container_name=COSMOS_CONTAINER,
)
def processor(msg: func.ServiceBusMessage,
              documents: func.Out[func.DocumentList]) -> None:
    envelope = json.loads(msg.get_body().decode("utf-8"))
    batch_id = envelope.get("batch_id", "")
    docs: List[Dict[str, Any]] = []

    for a in envelope.get("aggregates", []):
        docs.append({
            # Deterministic id => a redelivered message upserts instead of
            # duplicating. This is how at-least-once becomes effectively-once.
            "id": f"agg-{batch_id}-{a['sensor_type']}-{a['zone']}",
            "doc_type": "aggregate", "batch_id": batch_id,
            "fog_id": envelope.get("fog_id"), "zone": a["zone"], **a,
        })
    for x in envelope.get("anomalies", []):
        docs.append({
            "id": f"anom-{batch_id}-{x['sensor_id']}-{x['ts']}-{x['kind']}",
            "doc_type": "anomaly", "batch_id": batch_id,
            "fog_id": envelope.get("fog_id"), "zone": x["zone"], **x,
        })
    for s in envelope.get("raw_sample", []):
        docs.append({
            "id": f"raw-{batch_id}-{s['sensor_id']}-{s['ts']}",
            "doc_type": "raw", "batch_id": batch_id, "zone": s["zone"], **s,
            # TTL: raw traces are only needed for live charts, so expire them
            # after an hour and keep the aggregates forever. Big cost lever.
            "ttl": 3600,
        })

    if docs:
        # The Cosmos output binding takes a DocumentList; one round trip per batch.
        documents.set(func.DocumentList([func.Document.from_dict(d) for d in docs]))
    logging.info("persisted %d documents for batch %s", len(docs), batch_id)


# ---------------------------------------------------------------------------
# 3. QUERY: read models for the dashboard, served straight from Cosmos.
# ---------------------------------------------------------------------------
def _cosmos_container():
    from azure.cosmos import CosmosClient  # lazy import keeps cold start small
    client = CosmosClient.from_connection_string(os.environ["CosmosConnection"])
    return client.get_database_client(COSMOS_DB).get_container_client(COSMOS_CONTAINER)


def _query(sql: str, params: List[dict] | None = None) -> List[dict]:
    container = _cosmos_container()
    return list(container.query_items(query=sql, parameters=params or [],
                                      enable_cross_partition_query=True))


@app.function_name(name="summary")
@app.route(route="summary", methods=["GET"])
def summary(req: func.HttpRequest) -> func.HttpResponse:
    rows = _query(
        "SELECT TOP 200 c.sensor_type, c.zone, c.unit, c.last, c.mean, c.min, c.max,"
        " c.count, c.window_end FROM c WHERE c.doc_type = 'aggregate'"
        " ORDER BY c.window_end DESC")
    latest: Dict[tuple, dict] = {}
    for r in rows:                       # first row per key wins (already sorted)
        latest.setdefault((r["sensor_type"], r["zone"]), r)
    payload = {"latest": list(latest.values()),
               "types": sorted({k[0] for k in latest})}
    return func.HttpResponse(json.dumps(payload), mimetype="application/json")


@app.function_name(name="series")
@app.route(route="series/{sensor_type}", methods=["GET"])
def series(req: func.HttpRequest) -> func.HttpResponse:
    stype = req.route_params.get("sensor_type")
    limit = int(req.params.get("limit", 60))
    rows = _query(
        f"SELECT TOP {limit} c.zone, c.window_end AS ts, c.mean, c.min, c.max, c.p95,"
        " c.unit FROM c WHERE c.doc_type = 'aggregate' AND c.sensor_type = @t"
        " ORDER BY c.window_end DESC", [{"name": "@t", "value": stype}])
    return func.HttpResponse(
        json.dumps({"sensor_type": stype, "points": list(reversed(rows))}),
        mimetype="application/json")


@app.function_name(name="anomalies")
@app.route(route="anomalies", methods=["GET"])
def anomalies(req: func.HttpRequest) -> func.HttpResponse:
    limit = int(req.params.get("limit", 50))
    rows = _query(
        f"SELECT TOP {limit} c.sensor_id, c.sensor_type, c.zone, c.ts, c.value,"
        " c.kind, c.severity, c.detail FROM c WHERE c.doc_type = 'anomaly'"
        " ORDER BY c.ts DESC")
    return func.HttpResponse(json.dumps({"anomalies": rows}), mimetype="application/json")


@app.function_name(name="metrics")
@app.route(route="metrics", methods=["GET"])
def metrics(req: func.HttpRequest) -> func.HttpResponse:
    counts = _query("SELECT VALUE COUNT(1) FROM c WHERE c.doc_type = 'aggregate'")
    batches = _query("SELECT VALUE COUNT(1) FROM c WHERE c.doc_type = 'anomaly'")
    return func.HttpResponse(
        json.dumps({"aggregates": counts[0] if counts else 0,
                    "anomalies": batches[0] if batches else 0,
                    "batches": counts[0] if counts else 0,
                    "duplicates": 0, "queue_depth": 0}),
        mimetype="application/json")


@app.function_name(name="dashboard")
@app.route(route="dashboard", methods=["GET"])
def dashboard(req: func.HttpRequest) -> func.HttpResponse:
    html = (Path(__file__).parent / "static" / "dashboard.html").read_text(encoding="utf-8")
    # The cloud dashboard hits /api/* on the same Function App host.
    return func.HttpResponse(html, mimetype="text/html")
