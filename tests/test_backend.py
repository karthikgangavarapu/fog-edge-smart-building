"""Integration tests for the backend read/write paths."""
import asyncio

import pytest
from fastapi.testclient import TestClient

from backend.local import main as backend_main


@pytest.fixture()
def client(tmp_path, monkeypatch):
    from backend.local.store import Store
    backend_main.store = Store(str(tmp_path / "test.db"))
    with TestClient(backend_main.app) as c:
        yield c


def envelope(batch_id="b1"):
    return {
        "batch_id": batch_id, "fog_id": "fog-test", "schema_version": "1.0",
        "window_start": 1000, "window_end": 6000,
        "aggregates": [{"sensor_type": "temperature", "zone": "z1", "unit": "C",
                        "count": 5, "min": 20.0, "max": 22.0, "mean": 21.0,
                        "p95": 21.9, "last": 21.2, "window_start": 1000,
                        "window_end": 6000}],
        "anomalies": [{"sensor_id": "t1", "sensor_type": "temperature", "zone": "z1",
                       "ts": 5000, "value": 31.0, "kind": "threshold_breach",
                       "severity": "critical", "detail": "hot"}],
        "raw_sample": [], "stats": {},
    }


def test_ingest_requires_api_key(client):
    assert client.post("/api/ingest", json=envelope()).status_code == 401


def test_ingest_accepts_and_persists(client):
    r = client.post("/api/ingest", json=envelope(),
                    headers={"X-Api-Key": "local-dev-key"})
    assert r.status_code == 202                 # accepted, not created
    for _ in range(50):                          # let the queue worker drain
        if backend_main.store.counts()["aggregates"]:
            break
        import time; time.sleep(0.05)
    assert backend_main.store.counts()["aggregates"] == 1
    assert client.get("/api/summary").json()["latest"][0]["sensor_type"] == "temperature"
    assert len(client.get("/api/anomalies").json()["anomalies"]) == 1


def test_duplicate_batch_is_idempotent(client):
    h = {"X-Api-Key": "local-dev-key"}
    client.post("/api/ingest", json=envelope("dup"), headers=h)
    client.post("/api/ingest", json=envelope("dup"), headers=h)
    import time; time.sleep(0.5)
    assert backend_main.store.counts()["aggregates"] == 1   # not 2


def test_malformed_envelope_rejected(client):
    r = client.post("/api/ingest", json={"nope": 1}, headers={"X-Api-Key": "local-dev-key"})
    assert r.status_code == 400


def test_health_and_metrics(client):
    assert client.get("/api/health").json()["status"] == "ok"
    assert "ingested" in client.get("/api/metrics").json()
