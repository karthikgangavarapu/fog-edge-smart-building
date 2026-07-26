# Smart-Building Fog & Edge Telemetry Platform

A three-tier IoT system built for the Fog & Edge Computing CA: virtual sensors
publish over MQTT, a coded fog node cleans and condenses the stream at the
edge, and a queue-backed serverless backend on Microsoft Azure stores the
result and serves live dashboards.

```
 5 sensor types            virtual fog node                  Azure
 ──────────────           ─────────────────        ──────────────────────────
 temperature  ┐                                     ┌─ Function: ingest  (HTTP)
 humidity     │  MQTT      validate → smooth →      │        ↓
 CO₂          ├─ QoS 1 ──► aggregate → detect ──►   ├─ Service Bus queue
 occupancy    │            spool → dispatch  HTTPS  │        ↓
 power        ┘                                     ├─ Function: processor
                                                    │        ↓
                                                    ├─ Cosmos DB (/zone)
                                                    └─ Function: query → dashboard
```

## Highlights

| | |
|---|---|
| Sensor types | 5 (temperature, humidity, CO₂, occupancy, power) |
| Frequency / dispatch | independently configurable per sensor (0.5 Hz – 10 Hz) |
| Fog processing | range + sequence validation, EWMA smoothing, tumbling-window aggregation, z-score + threshold anomaly detection, alert de-duplication |
| Resilience | SQLite store-and-forward spool, exponential backoff, idempotent batch IDs |
| Backend | Azure Functions + Service Bus + Cosmos DB (serverless, autoscaling) |
| Measured reduction | 78 KB → 28 KB per 30 s window (63.7%); 411 readings → 7 HTTPS POSTs |
| Measured ingest | 1673 req/s accepted, p95 24.8 ms, 0 errors at concurrency 32 |
| Tests | 23 pytest unit + integration tests, run on every push by GitHub Actions |

## Run it

```bash
pip install -r requirements.txt
uvicorn backend.local.main:app --port 8000        # terminal 1
python run_demo.py --seconds 300 \
    --ingest http://localhost:8000/api/ingest      # terminal 2
open http://localhost:8000/
```

Full installation, Docker, Azure deployment and troubleshooting instructions
are in [`readme.txt`](readme.txt); the design rationale is in
[`docs/architecture.md`](docs/architecture.md).
