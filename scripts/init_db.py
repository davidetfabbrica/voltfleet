"""
scripts/init_db.py

Database initialisation script for VoltFleet.

Run this once before starting the application:
    python scripts/init_db.py

This script creates all tables in the SQLite database following the
medallion architecture (Bronze -> Silver -> Gold) defined in ADR-001.

It is safe to run multiple times — all CREATE TABLE statements use
IF NOT EXISTS so existing data is not affected.
"""

import sqlite3
import sys
import os
from pathlib import Path

# Add the project root to sys.path so we can import config from any location
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings, configure_logging

logger = configure_logging()


def init_db() -> None:
    """
    Create all VoltFleet database tables.

    Tables are created in dependency order: Bronze first (no dependencies),
    then Silver (references vehicle metadata), then Gold, then predictions,
    alerts, and operational tables.
    """

    # Ensure the data directory exists
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Initialising database at: {settings.db_path}")

    conn = sqlite3.connect(settings.db_path)

    # Enable WAL (Write-Ahead Log) mode.
    # WAL allows readers and writers to operate concurrently without blocking
    # each other — important when the ingestion service writes while the
    # pipeline reads. It also provides better durability on process crash.
    conn.execute("PRAGMA journal_mode=WAL")

    # Enforce foreign key constraints (SQLite disables them by default)
    conn.execute("PRAGMA foreign_keys=ON")

    cursor = conn.cursor()

    # ── Vehicle metadata table ────────────────────────────────────────────────
    # Holds reference data about each vehicle. The Silver pipeline joins
    # raw telemetry against this table to enrich records.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_metadata (
            vehicle_id          TEXT PRIMARY KEY,
            -- Human-readable fleet identifier (e.g. "VF-047")
            fleet_label         TEXT NOT NULL,
            -- Vehicle make and model for maintenance context
            make                TEXT NOT NULL,
            model               TEXT NOT NULL,
            -- Year of manufacture — older vehicles may show faster degradation
            manufacture_year    INTEGER NOT NULL,
            -- Manufacturer-rated battery capacity in kWh
            rated_capacity_kwh  REAL NOT NULL,
            -- Geographic region this vehicle operates in (EU, NA, JP)
            -- Used for data residency routing in production
            region              TEXT NOT NULL DEFAULT 'EU',
            -- ISO 8601 timestamp of when this vehicle was registered in the system
            registered_at       TEXT NOT NULL,
            -- Firmware/software version currently installed on this vehicle.
            -- Fleet managers use this to identify vehicles needing an OTA update.
            software_version    TEXT NOT NULL DEFAULT 'v2.1.4'
        )
    """)

    # ── BRONZE layer ──────────────────────────────────────────────────────────
    # Raw telemetry events exactly as received from vehicles.
    # This table is APPEND-ONLY — records are never modified or deleted after
    # initial write. It is the source of truth for reprocessing.
    # (PRD FR-007, FR-010, ADR-001-05)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze_telemetry (
            -- Auto-incrementing internal ID for ordering and watermarking
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            -- Vehicle identifier (not yet validated against vehicle_metadata here)
            vehicle_id          TEXT NOT NULL,
            -- Timestamp from the vehicle (UTC ISO 8601 string)
            event_timestamp     TEXT NOT NULL,
            -- Timestamp when this record was received by the ingestion service
            received_at         TEXT NOT NULL,
            -- Battery state as a percentage of rated capacity (0-100)
            battery_pct         REAL,
            -- Absolute state of charge in kilowatt-hours
            state_of_charge_kwh REAL,
            -- Battery pack voltage in volts
            voltage_v           REAL,
            -- Current draw in amps (positive = discharge, negative = regen)
            current_a           REAL,
            -- Motor temperature in degrees Celsius
            motor_temp_c        REAL,
            -- GPS coordinates
            latitude            REAL,
            longitude           REAL,
            -- Vehicle speed in kilometres per hour
            speed_kmh           REAL,
            -- True if a regenerative braking event occurred in this interval
            regen_braking_event INTEGER,   -- SQLite stores booleans as 0/1
            -- Raw JSON payload preserved for audit and reprocessing
            raw_payload         TEXT NOT NULL
        )
    """)

    # Index on vehicle_id + event_timestamp to support per-vehicle time-range queries.
    # This mirrors the partitioning strategy described in ADR-001-09.
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_bronze_vehicle_time
        ON bronze_telemetry (vehicle_id, event_timestamp)
    """)

    # Index on received_at so the pipeline can find unprocessed records
    # using a watermark (i.e. "all records received after this timestamp").
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_bronze_received_at
        ON bronze_telemetry (received_at)
    """)

    # ── Dead Letter Queue ─────────────────────────────────────────────────────
    # Events that failed ingestion validation land here instead of being
    # silently dropped or blocking the pipeline. (PRD FR-004, ADR-001-11)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze_dlq (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            -- When the event arrived at the ingestion service
            received_at     TEXT NOT NULL,
            -- The raw payload that was rejected (may be malformed JSON)
            raw_payload     TEXT NOT NULL,
            -- Structured error reason — e.g. "missing_field:battery_pct"
            error_reason    TEXT NOT NULL,
            -- HTTP client IP for debugging misbehaving vehicles
            client_ip       TEXT
        )
    """)

    # ── SILVER layer ──────────────────────────────────────────────────────────
    # Validated, cleaned, and enriched telemetry.
    # Records here have passed type and range checks, nulls have been handled,
    # and vehicle metadata has been joined in from vehicle_metadata.
    # (PRD FR-008, ADR-001-05)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_telemetry (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            -- Foreign key back to the originating bronze record for lineage
            bronze_id               INTEGER NOT NULL,
            vehicle_id              TEXT NOT NULL,
            event_timestamp         TEXT NOT NULL,
            -- Validated and range-checked telemetry fields
            battery_pct             REAL NOT NULL,
            state_of_charge_kwh     REAL NOT NULL,
            voltage_v               REAL NOT NULL,
            current_a               REAL NOT NULL,
            motor_temp_c            REAL NOT NULL,
            latitude                REAL NOT NULL,
            longitude               REAL NOT NULL,
            speed_kmh               REAL NOT NULL,
            regen_braking_event     INTEGER NOT NULL,
            -- Enriched fields from vehicle_metadata join
            rated_capacity_kwh      REAL NOT NULL,
            manufacture_year        INTEGER NOT NULL,
            region                  TEXT NOT NULL,
            -- When this Silver record was written
            processed_at            TEXT NOT NULL,
            FOREIGN KEY (bronze_id) REFERENCES bronze_telemetry(id)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_silver_vehicle_time
        ON silver_telemetry (vehicle_id, event_timestamp)
    """)

    # ── GOLD layer ────────────────────────────────────────────────────────────
    # Aggregated, business-ready records. One record per vehicle per pipeline
    # cycle. Contains rolling averages, computed health metrics, and anomaly
    # flags. This is what the dashboard and ML model read from. (ADR-001-05)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_vehicle_health (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id                  TEXT NOT NULL,
            -- The pipeline cycle timestamp this record represents
            window_start                TEXT NOT NULL,
            window_end                  TEXT NOT NULL,
            -- Averages over the pipeline window
            avg_battery_pct             REAL,
            avg_voltage_v               REAL,
            avg_current_a               REAL,
            avg_motor_temp_c            REAL,
            avg_speed_kmh               REAL,
            -- Rolling 5-minute battery discharge rate: how fast the battery
            -- is losing charge, as a percentage per minute.
            -- A sudden increase in this value is a key anomaly signal.
            rolling_5min_discharge_rate REAL,
            -- Simple 0-100 health score derived from battery, temp, and voltage
            -- relative to the vehicle's rated specs.
            health_score                REAL,
            -- Number of Silver records that fed into this Gold record
            record_count                INTEGER,
            -- When this Gold record was written
            aggregated_at               TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_gold_vehicle_window
        ON gold_vehicle_health (vehicle_id, window_start)
    """)

    # ── Predictions table ─────────────────────────────────────────────────────
    # Stores every output from the anomaly detection model, for both production
    # and shadow mode runs. (PRD FR-015, FR-016, ADR-001-13)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id      TEXT NOT NULL,
            -- Timestamp of the Gold record this prediction was made against
            window_start    TEXT NOT NULL,
            -- Raw anomaly score from IsolationForest.
            -- More negative = more anomalous. Threshold is typically -0.1 to 0.
            anomaly_score   REAL NOT NULL,
            -- True if the score crossed the anomaly threshold
            is_anomaly      INTEGER NOT NULL,  -- 0 or 1
            -- Which model version produced this prediction (e.g. "v1.0")
            model_version   TEXT NOT NULL,
            -- "production" or "shadow" — shadow predictions never trigger alerts
            mode            TEXT NOT NULL DEFAULT 'production',
            predicted_at    TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_predictions_vehicle_time
        ON predictions (vehicle_id, window_start)
    """)

    # ── Alerts table ──────────────────────────────────────────────────────────
    # Raised when a vehicle is anomalous across consecutive windows. (PRD FR-018)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id          TEXT NOT NULL,
            -- When the first anomalous window was detected
            first_detected_at   TEXT NOT NULL,
            -- Most recent anomaly score at the time of alert
            anomaly_score       REAL NOT NULL,
            -- JSON array of the top contributing feature names
            top_features        TEXT,
            -- "open" until acknowledged by maintenance team
            status              TEXT NOT NULL DEFAULT 'open',
            -- When a maintenance team member acknowledged the alert
            acknowledged_at     TEXT,
            acknowledged_by     TEXT,
            created_at          TEXT NOT NULL
        )
    """)

    # ── Erasure log ───────────────────────────────────────────────────────────
    # Records every GDPR/APPI right-to-erasure request that has been processed.
    # This provides the audit trail required by PRIV-EU-005 and PRIV-JP-001.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS erasure_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            -- The vehicle_id that was erased (stored here even though it has
            -- been nulled out elsewhere, as the erasure event itself is the record)
            vehicle_id      TEXT NOT NULL,
            -- How many records were affected in each table
            bronze_count    INTEGER NOT NULL DEFAULT 0,
            silver_count    INTEGER NOT NULL DEFAULT 0,
            gold_count      INTEGER NOT NULL DEFAULT 0,
            prediction_count INTEGER NOT NULL DEFAULT 0,
            -- Who requested or performed the erasure
            requested_by    TEXT NOT NULL,
            erased_at       TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()

    logger.info("Database initialisation complete. All tables created.")
    _seed_vehicle_metadata()


def _seed_vehicle_metadata() -> None:
    """
    Insert sample vehicle metadata records for the simulator to use.

    Each vehicle has a fixed home depot location (used as the starting GPS
    position for the simulator) and a software version reflecting a realistic
    mixed fleet where not all vehicles have been updated to the latest firmware.

    In production this data would come from the fleet management system.
    """
    from datetime import datetime, timezone

    # Software versions assigned per vehicle to simulate a mixed-version fleet.
    # VF-001 runs old firmware (v1.8.2) — intentionally anomalous.
    # A real fleet manager would use this view to prioritise OTA updates.
    SOFTWARE_VERSIONS = {
        "VF-001": "v1.8.2",   # End-of-life — flagged for urgent update
        "VF-002": "v2.1.4",
        "VF-003": "v2.1.4",
        "VF-004": "v2.0.1",
        "VF-005": "v2.1.4",
        "VF-006": "v2.1.4",
        "VF-007": "v2.0.1",
        "VF-008": "v2.1.4",
        "VF-009": "v1.9.0",
        "VF-010": "v2.1.4",
    }

    vehicle_count = settings.simulator_vehicle_count
    conn = sqlite3.connect(settings.db_path)
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    inserted = 0
    for i in range(1, vehicle_count + 1):
        vehicle_id = f"VF-{i:03d}"

        existing = cursor.execute(
            "SELECT 1 FROM vehicle_metadata WHERE vehicle_id = ?", (vehicle_id,)
        ).fetchone()
        if existing:
            continue

        # Alternate between Lexus RZ500e and Toyota bZ4X AWD across the fleet.
        # Odd-numbered vehicles are the Lexus (larger 71.4 kWh pack),
        # even-numbered are the Toyota (72.8 kWh pack).
        if i % 2 == 1:
            make, model, capacity = "Lexus", "RZ500e", 71.4
        else:
            make, model, capacity = "Toyota", "bZ4X AWD", 72.8

        cursor.execute("""
            INSERT INTO vehicle_metadata
                (vehicle_id, fleet_label, make, model, manufacture_year,
                 rated_capacity_kwh, region, registered_at, software_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            vehicle_id,
            f"Unit {i:03d}",
            make,
            model,
            2023 + (i % 2),   # Mix of 2023 and 2024 models
            capacity,
            settings.region,
            now,
            SOFTWARE_VERSIONS.get(vehicle_id, "v2.1.4"),
        ))
        inserted += 1

    conn.commit()
    conn.close()
    logger.info(f"Seeded {inserted} vehicle metadata records.")


if __name__ == "__main__":
    init_db()
