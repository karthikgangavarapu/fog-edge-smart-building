================================================================================
 FOG & EDGE COMPUTING CA - SMART BUILDING TELEMETRY PLATFORM
 Sensor tier -> Fog tier -> Scalable cloud backend (Microsoft Azure)
================================================================================

CONTENTS
--------
  sensors/                 Five virtual sensor types + MQTT/in-process transport
  fog/                     Virtual fog node: validate, smooth, aggregate,
                           detect anomalies, spool and dispatch to the cloud
  backend/local/           FastAPI backend (local mirror of the Azure design)
  backend/azure_functions/ Azure Functions v2 app (HTTP + Service Bus + Cosmos)
  infra/                   Bicep IaC template and deploy.sh
  tests/                   23 pytest unit + integration tests
  loadtest/                Data-reduction and ingest-throughput benchmarks
  config/sensors.yaml      All frequency / dispatch-rate / threshold settings
  run_demo.py              One-command end-to-end demo (no broker required)
  docker-compose.yml       Four-container demo with a real Mosquitto broker


--------------------------------------------------------------------------------
1. REQUIREMENTS
--------------------------------------------------------------------------------
  * Python 3.10 or newer  (developed and tested on 3.11)
  * pip
  Optional:
  * Docker + Docker Compose  (for the MQTT-broker version of the demo)
  * Azure CLI + Azure Functions Core Tools v4  (for the cloud deployment)


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
      pytest -q                    # 23 tests, expect all passing

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
7. DEPLOYING TO AZURE
--------------------------------------------------------------------------------
      az login
      export RG=rg-fogedge LOCATION=northeurope
      ./infra/deploy.sh

  The script provisions (via infra/main.bicep):
      - Function App on the Consumption plan (event-driven autoscale)
      - Service Bus namespace + "telemetry" queue (durable buffer)
      - Cosmos DB serverless account, database "telemetry", container
        "readings" partitioned on /zone
      - Application Insights + the storage account the runtime requires
  ...then publishes backend/azure_functions and prints the ingest URL, the
  dashboard URL and the generated API key.

  Point the fog node at the cloud:
      export FOG_INGEST_URL=https://<app>.azurewebsites.net/api/ingest
      export FOG_API_KEY=<key printed by deploy.sh>
      python -m fog.node          # in one terminal
      python -m sensors.runner    # in another

  Cloud dashboard: https://<app>.azurewebsites.net/api/dashboard


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

  Environment variable overrides (used by Docker and Azure):
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
        fog_spool.db and will be replayed automatically once it returns -
        this is the store-and-forward behaviour and is worth demonstrating.

  No anomalies appear
        Wait ~60 s. The simulated day is compressed to 60 real seconds, and
        thresholds are only crossed around the simulated afternoon peak.

  Port 8000 already in use
        uvicorn backend.local.main:app --port 8080
        (and pass the matching --ingest URL to run_demo.py)

================================================================================
