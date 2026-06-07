# VoltFleet — EV Fleet Predictive Analytics

A telemetry ingestion and anomaly detection platform for commercial electric vehicle fleets. Built as a portfolio project demonstrating real data engineering patterns: medallion architecture, stream processing, unsupervised machine learning, and operational compliance across three jurisdictions.

---

## What it does

VoltFleet simulates a mixed fleet of Lexus RZ500e and Toyota bZ4X AWD vehicles emitting telemetry every 5 seconds — battery state, voltage, current draw, motor temperature, GPS, and speed. A pipeline processes that stream in 30-second micro-batches through three storage layers (Bronze, Silver, Gold), then runs an IsolationForest anomaly detection model to identify vehicles likely to need maintenance before they fail.

One vehicle in the fleet (VF-001) is configured to discharge 3x faster than normal and run at higher temperature. The system detects it without being told what to look for and raises an alert within two pipeline cycles.

A full operational dashboard provides fleet overview, per-vehicle telemetry charts, live GPS map, pipeline health monitoring, and dead letter queue inspection.

---

## Architecture

Kappa architecture (single streaming pipeline) with a medallion data lake (Bronze / Silver / Gold). Full decisions and tradeoffs documented in `ADR-001-voltfleet-architecture.md`.

```
Simulator (10 EVs, async, jitter on retry)
    │  HTTP POST every 5 seconds
    ▼
Ingestion Service (Flask)
    │  Token bucket rate limiter (per vehicle)
    │  Schema and range validation
    │  Dead Letter Queue for rejected events
    ▼
Bronze (SQLite, append-only, WAL mode)
    │  30-second micro-batch
    ▼
Silver (validated, cleaned, joined with vehicle metadata)
    │  30-second micro-batch
    ▼
Gold (aggregated, rolling discharge rate, health score)
    │
    ▼
Anomaly Detection (IsolationForest, unsupervised)
    │  Production model triggers alerts
    │  Shadow model logs predictions silently
    ▼
Alerts + Dashboard (Flask API + React frontend + Leaflet map)
```

---

## Project structure

```
voltfleet/
├── config/
│   └── settings.py              Central config — all settings from .env
├── ingestion/
│   ├── app.py                   Flask endpoint (/ingest, /health, /metrics)
│   ├── rate_limiter.py          Token bucket rate limiter
│   ├── validator.py             Schema and range validation
│   └── writer.py                Bronze and DLQ writes
├── simulator/
│   ├── vehicle.py               Single vehicle — async, jitter retry
│   └── fleet.py                 Runs all vehicles via asyncio
├── pipeline/
│   ├── bronze_to_silver.py      Validation, cleaning, metadata join
│   ├── silver_to_gold.py        Aggregation, discharge rate, health score
│   └── scheduler.py             30-second micro-batch loop
├── models/
│   ├── anomaly.py               IsolationForest — train and predict
│   └── predictor.py             Predictions, alerts, shadow mode
├── dashboard/
│   ├── api.py                   Flask API serving the frontend
│   └── templates/index.html     React + Chart.js + Leaflet dashboard
├── scripts/
│   ├── init_db.py               Creates schema, seeds vehicle metadata
│   └── erase_vehicle.py         GDPR/APPI right-to-erasure with audit log
├── tests/
│   ├── test_ingestion.py        33 tests: validator, rate limiter, endpoints
│   └── test_pipeline.py         Bronze-to-Silver and Silver-to-Gold tests
├── .env.example                 Copy to .env before running
└── requirements.txt
```

---

## Quickstart

Requires Python 3.12.

```bash
git clone https://github.com/davidetfabbrica/voltfleet.git
cd voltfleet

python -m venv venv
source venv/bin/activate

pip install -r requirements.txt

cp .env.example .env

python scripts/init_db.py
```

Open four terminal windows, all with `source venv/bin/activate` inside the project folder:

```bash
# Terminal 1
python -m ingestion.app

# Terminal 2
python -m pipeline.scheduler

# Terminal 3
python -m simulator.fleet

# Terminal 4
python -m dashboard.api
```

Open `http://127.0.0.1:5002/dashboard/`

**macOS note:** if port 5000 is in use, disable AirPlay Receiver in System Settings > General > AirDrop & Handoff, or set `INGESTION_PORT=5001` in `.env`.

---

## Dashboard

Five views accessible from the top navigation:

**Overview** — fleet headline stats and a full vehicle table showing battery, temperature, discharge rate, health score, firmware version, anomaly score, and alert status. Rows are sortable by clicking. VF-001 appears at the top with a red ALERT badge.

**Vehicle** — select any vehicle from the sidebar or overview table for battery and temperature charts over the last 60 minutes, live anomaly score trend, and alert history.

**Alerts** — all raised alerts with anomaly score, top contributing features, and status.

**Pipeline** — lag in seconds for each pipeline stage (ingestion, silver, gold, prediction) so you can see immediately if something has stopped running, plus record counts across all layers.

**Map** — live GPS positions for all vehicles plotted on an OpenStreetMap base layer. Markers are colour-coded by alert status. Clicking a marker shows a popup with battery, temperature, speed, firmware version, and last seen time. The positions table below shows the same data in sortable form.

---

## Running the tests

```bash
pytest tests/ -v
```

33 tests covering the validator, rate limiter, HTTP ingestion endpoint, Bronze-to-Silver pipeline (including circuit breaker behaviour), and Silver-to-Gold aggregation.

---

## Watching it run

Check data flowing through all layers:

```bash
sqlite3 data/voltfleet.db "
SELECT
  (SELECT COUNT(*) FROM bronze_telemetry)    AS bronze,
  (SELECT COUNT(*) FROM silver_telemetry)    AS silver,
  (SELECT COUNT(*) FROM gold_vehicle_health) AS gold,
  (SELECT COUNT(*) FROM bronze_dlq)          AS dlq;
"
```

Check anomaly scores (VF-001 should score lowest):

```bash
sqlite3 data/voltfleet.db "
SELECT vehicle_id, anomaly_score, is_anomaly
FROM predictions
WHERE mode = 'production'
GROUP BY vehicle_id
HAVING predicted_at = MAX(predicted_at)
ORDER BY anomaly_score ASC;
"
```

Check alerts:

```bash
sqlite3 data/voltfleet.db "
SELECT vehicle_id, anomaly_score, top_features, status, created_at
FROM alerts;
"
```

---

## GDPR / APPI right-to-erasure

```bash
python scripts/erase_vehicle.py VF-003
```

Nulls `vehicle_id` and GPS fields across Bronze, Silver, Gold, and predictions tables, deletes the vehicle metadata record, and writes an audit entry to `erasure_log`. Bronze rows are not deleted — sensor readings are retained without any identifying link. See `PRD-001-voltfleet.md` section 7 for the full compliance rationale.

To skip the confirmation prompt (for automated DSAR workflows):

```bash
python scripts/erase_vehicle.py VF-003 --confirm --requested-by "DPO"
```

---

## Configuration

All settings live in `.env`:

| Variable | Default | Description |
|---|---|---|
| `VOLTFLEET_REGION` | `EU` | Region identifier (EU, NA, JP) |
| `INGESTION_PORT` | `5000` | Port for the ingestion service |
| `SIMULATOR_VEHICLE_COUNT` | `10` | Number of simulated vehicles |
| `SIMULATOR_EMIT_INTERVAL_SECONDS` | `5.0` | Seconds between emissions |
| `PIPELINE_INTERVAL_SECONDS` | `30` | Micro-batch interval |
| `ANOMALY_CONTAMINATION` | `0.05` | Expected anomaly fraction |
| `ANOMALY_CONSECUTIVE_WINDOWS` | `2` | Windows before alert fires |
| `ALERT_SUPPRESSION_HOURS` | `4` | Hours before repeat alert |
| `LOG_LEVEL` | `INFO` | DEBUG, INFO, WARNING, ERROR |

---

## Key design decisions

**Kappa not Lambda.** At 100 events/second there is no justification for a separate batch layer. One pipeline handles everything. Reprocessing is done by replaying Bronze through the same code.

**IsolationForest.** No labelled failure data exists. The algorithm is unsupervised — it learns what normal looks like from the data itself and flags deviations. Fast enough for 30-second inference cycles.

**Bronze is append-only.** It is the source of truth. A bug in Silver processing can be corrected by reprocessing from Bronze. A mutable Bronze layer removes that option entirely.

**SQLite stands in for a real data lake.** The Bronze/Silver/Gold structure, watermark pattern, circuit breaker, and DLQ are all production patterns. Replacing SQLite with Parquet on S3 with Apache Iceberg requires changing the storage layer only — the pipeline logic is identical.

---

## Known gaps (production delta)

- Real MQTT broker (Mosquitto) for vehicle communication
- Apache Kafka as the event backbone
- Object storage (S3) with Apache Iceberg for the data lake
- Load balancer with health-check routing per region
- Mutual TLS (mTLS) for vehicle authentication
- OAuth 2.0 / OIDC for dashboard authentication
- Automated database backups and tested restore procedure
- Horizontal scaling

All documented in `ADR-001-voltfleet-architecture.md`.

---

## Related documents

- `ADR-001-voltfleet-architecture.md` — all architecture decisions with alternatives and tradeoffs
- `PRD-001-voltfleet.md` — full product requirements including EU/US/Japan compliance

---

## Licence

MIT
