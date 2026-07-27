================================================================================
 FOG & EDGE COMPUTING CA - SMART BUILDING TELEMETRY PLATFORM
 Sensor tier -> Fog tier -> Scalable cloud backend (Amazon Web Services)
================================================================================

CONTENTS
--------
  sensors/            Five virtual sensor types + MQTT/in-process transport
  fog/                Virtual fog node: validate, smooth, aggregate, detect
                      anomalies, spool and dispatch to the cloud
  backend/local/      FastAPI backend (local mirror of the AWS design)
  backend/aws/        Lambda handlers: ingest, processor, query + dashboard
  infra/              CloudFormation template and deploy scripts
  tests/              31 pytest unit + integration tests
  loadtest/           Data-reduction and ingest-throughput benchmarks
  config/sensors.yaml All frequency / dispatch-rate / threshold settings
  run_demo.py         One-command end-to-end demo (no broker required)
  docker-compose.yml  Four-container demo with a real Mosquitto broker


--------------------------------------------------------------------------------
1. REQUIREMENTS
--------------------------------------------------------------------------------
  * Python 3.10 or newer  (developed and tested on 3.11)
  * pip
  Optional:
  * Docker + Docker Compose  (for the MQTT-broker version of the demo)
  * AWS CLI v2               (for the cloud deployment)


--------------------------------------------------------------------------------
2. INSTALLATION
--------------------------------------------------------------------------------
  # from the folder containing this file
  python -m venv .venv

  # Windows
  .venv\Scripts\activate
  # macOS / Linux
  source .venv/bin/activate

  pip install -r requirements.txt


--------------------------------------------------------------------------------
3. QUICK START - the whole pipeline in two terminals
--------------------------------------------------------------------------------
  Terminal 1 - start the backend and dashboard:
      uvicorn backend.local.main:app --port 8000

  Terminal 2 - start the sensors and the fog node:
      python run_demo.py --seconds 300 --ingest http://localhost:8000/api/ingest

  Then open:  http://localhost:8000/

  The dashboard refreshes every 3 seconds and shows live values per sensor
  type, fog-aggregated trend charts, and anomalies detected at the fog tier.
  run_demo.py prints a summary (readings published, batches delivered,
  anomalies detected) when it finishes.


--------------------------------------------------------------------------------
4. FULL DEMO WITH A REAL MQTT BROKER (Docker)
--------------------------------------------------------------------------------
      docker compose up --build

  This starts four containers: Mosquitto broker, backend, fog node, sensors.
  Dashboard: http://localhost:8000/
  Stop with:  docker compose down


--------------------------------------------------------------------------------
5. RUNNING THE TESTS
--------------------------------------------------------------------------------
      pytest -q                    # 31 tests, expect all passing

  NOTE: run the tests from a normal local disk. SQLite does not work on some
  network/cloud-synced folders (OneDrive, Google Drive) and will report
  "disk I/O error" if the project is run directly from one.


--------------------------------------------------------------------------------
6. BENCHMARKS USED IN THE REPORT
--------------------------------------------------------------------------------
  # How much data the fog tier stops from crossing the WAN
      python -m loadtest.benchmark reduction --seconds 30

  # Backend ingest throughput and latency
      uvicorn backend.local.main:app --port 8000 &
      python -m loadtest.benchmark ingest \
          --url http://localhost:8000/api/ingest --requests 2000 --concurrency 32


--------------------------------------------------------------------------------
7. DEPLOYING TO AWS
--------------------------------------------------------------------------------
  Configure credentials once:

      aws configure

  On AWS Academy, copy the keys from "AWS Details" on the lab page into
  %USERPROFILE%\.aws\credentials (Windows) or ~/.aws/credentials, including
  the aws_session_token line. Those credentials expire when the lab session
  ends, so re-copy them if a deploy suddenly fails with an auth error.

  Windows (PowerShell):
      .\infra\deploy.ps1
      .\infra\deploy.ps1 -Region us-east-1 -StackName fogedge

  macOS / Linux:
      ./infra/deploy.sh

  On AWS Academy you cannot create IAM roles, so pass the lab role instead:
      .\infra\deploy.ps1 -LabRoleArn arn:aws:iam::<account-id>:role/LabRole
      LAB_ROLE_ARN=arn:aws:iam::<account-id>:role/LabRole ./infra/deploy.sh

  The script packages backend/aws into a zip, uploads it to S3 and deploys
  infra/template.yaml, which creates:
      - API Gateway HTTP API (public entry point)
      - Lambda "ingest"    (validates and enqueues, returns 202)
      - SQS queue "telemetry" + a dead-letter queue
      - Lambda "processor" (SQS triggered, scales with the backlog)
      - DynamoDB table (on-demand, TTL on raw samples)
      - Lambda "query"     (read models + the dashboard page)
      - CloudWatch log groups for all three functions
  ...then prints the ingest URL, the dashboard URL and the generated API key.

  Point the fog node at the cloud (PowerShell):
      $env:FOG_INGEST_URL = "https://<api-id>.execute-api.<region>.amazonaws.com/api/ingest"
      $env:FOG_API_KEY    = "<key printed by the deploy script>"
      python -m fog.node          # in one terminal
      python -m sensors.runner    # in another

  Cloud dashboard:
      https://<api-id>.execute-api.<region>.amazonaws.com/api/dashboard

  To remove everything afterwards:
      aws cloudformation delete-stack --stack-name fogedge --region us-east-1


--------------------------------------------------------------------------------
8. CONFIGURATION
--------------------------------------------------------------------------------
  Everything is in config/sensors.yaml:
      sample_hz        how often a sensor takes a reading      (frequency)
      dispatch_every   how many readings it batches before publishing
      noise            Gaussian noise standard deviation
      fault_rate       probability of injecting a sensor fault
      fog.window_seconds       aggregation window length
      fog.dispatch_interval_s  how often the fog node ships a batch
      fog.thin_raw_to          raw samples retained per window for charting
      fog.anomaly.*            z-score threshold and alert cooldown

  Environment variable overrides (used by Docker and the cloud run):
      FOG_CONFIG, FOG_INGEST_URL, FOG_API_KEY, FOG_MQTT_HOST,
      BACKEND_DB, QUEUE_WORKERS, QUEUE_MAXSIZE


--------------------------------------------------------------------------------
9. TROUBLESHOOTING
--------------------------------------------------------------------------------
  "backend unreachable" on the dashboard
        The backend is not running, or it is on a different port.

  "disk I/O error"
        The project is running from a cloud-synced folder; copy it to a local
        disk (SQLite needs real file locking).

  Fog node logs "ingest transport error"
        Expected if the backend is down. The batches are being spooled to
        fog_spool.db and will be replayed automatically once it returns.
        This is the store-and-forward behaviour and is worth demonstrating.

  AWS deploy fails with ExpiredToken
        Your AWS Academy lab session ended. Start the lab again and re-copy
        the credentials, including aws_session_token.

  AWS deploy fails with "not authorized to perform: iam:CreateRole"
        You are on AWS Academy. Re-run the deploy passing -LabRoleArn (or
        LAB_ROLE_ARN) as shown in section 7.

  Lambda code changes do not appear after a deploy
        The deploy scripts upload under a new S3 key every run precisely to
        avoid this. If you deploy by hand, change the key or CloudFormation
        will see no change and keep the old code.

  No anomalies appear
        Wait about 60 s. The simulated day is compressed to 60 real seconds,
        and thresholds are only crossed around the simulated afternoon peak.

  Port 8000 already in use
        uvicorn backend.local.main:app --port 8080
        (and pass the matching --ingest URL to run_demo.py)

================================================================================
