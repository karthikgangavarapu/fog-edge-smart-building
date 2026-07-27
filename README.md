# Smart-Building Fog & Edge Telemetry Platform

A three-tier IoT system built for the Fog & Edge Computing CA: virtual sensors
publish over MQTT, a coded fog node cleans and condenses the stream at the
edge, and a queue-backed serverless backend on AWS stores the result and serves
live dashboards.

```
 5 sensor types            virtual fog node                  AWS
 --------------           -----------------        --------------------------
 temperature  |                                     |- API Gateway -> Lambda: ingest
 humidity     |  MQTT      validate -> smooth ->    |        v
 CO2          |- QoS 1 --> aggregate -> detect -->  |- SQS queue (+ DLQ)
 occupancy    |            spool -> dispatch  HTTPS |        v
 power        |                                     |- Lambda: processor
                                                    |        v
                                                    |- DynamoDB (PK by type+zone)
                                                    |- Lambda: query -> dashboard
```

## Highlights

| | |
|---|---|
| Sensor types | 5 (temperature, humidity, CO2, occupancy, power) |
| Frequency / dispatch | independently configurable per sensor (0.5 Hz to 10 Hz) |
| Fog processing | range and sequence validation, EWMA smoothing, tumbling-window aggregation, z-score plus threshold anomaly detection, alert de-duplication |
| Resilience | SQLite store-and-forward spool, exponential backoff, idempotent batch IDs |
| Backend | API Gateway, Lambda, SQS and DynamoDB, all serverless and autoscaling |
| Measured reduction | 78 kB to 28 kB per 30 s window (63.7%); 411 readings to 7 HTTPS POSTs |
| Measured ingest | 1673 req/s accepted, p95 24.8 ms, 0 errors at concurrency 32 |
| Tests | 31 pytest unit and integration tests, run on every push by GitHub Actions |

## Run it locally

```bash
pip install -r requirements.txt
uvicorn backend.local.main:app --port 8000        # terminal 1
python run_demo.py --seconds 300 \
    --ingest http://localhost:8000/api/ingest      # terminal 2
open http://localhost:8000/
```

## Deploy it to AWS

```bash
aws configure
./infra/deploy.sh            # or .\infra\deploy.ps1 on Windows
```

Creates the API Gateway, three Lambda functions, the SQS queue with its
dead-letter queue, and the DynamoDB table, then prints the ingest URL,
dashboard URL and API key.

Full installation, Docker, AWS deployment and troubleshooting instructions are
in [`readme.txt`](readme.txt); the design rationale is in
[`docs/architecture.md`](docs/architecture.md).
