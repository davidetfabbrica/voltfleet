"""
pipeline/bronze_to_silver.py

Bronze to Silver pipeline job.

Reads unprocessed Bronze records (those not yet in Silver), validates and
cleans each one, joins vehicle metadata, and writes to the Silver layer.

This is the first transformation stage in the medallion architecture.
Its job is to produce a trustworthy, normalised dataset — no nulls, no
out-of-range values, units consistent, every record traceable to its
Bronze origin via bronze_id.

A watermark pattern is used to track progress: the pipeline remembers
the highest Bronze id it has processed and only reads records above that
point on the next run. This is how Spark Streaming tracks position in a
stream; we implement the same concept here with a simple SQLite query.

Circuit breaker (ADR-001-10, PRD RES-004):
If more than 10% of records in a batch fail validation, the job halts
and raises an operational alert rather than writing a corrupted Silver batch.
"""

import sqlite3
import logging
from datetime import datetime, timezone

import pandas as pd

from config.settings import settings

logger = logging.getLogger("voltfleet.pipeline.bronze_to_silver")

# ── Physical plausibility ranges ──────────────────────────────────────────────
# These mirror the validator, but here we flag rather than reject — Bronze
# records are immutable so we cannot remove them. Instead, records that
# fail Silver validation are logged and skipped (not written to Silver).
# They remain in Bronze for investigation.
RANGES = {
    "battery_pct":           (0.0,    100.0),
    "state_of_charge_kwh":   (0.0,    150.0),
    "voltage_v":             (0.0,    1000.0),
    "current_a":             (-500.0, 500.0),
    "motor_temp_c":          (-40.0,  180.0),
    "latitude":              (-90.0,  90.0),
    "longitude":             (-180.0, 180.0),
    "speed_kmh":             (0.0,    250.0),
}

# Circuit breaker threshold: halt if this fraction of a batch fails
CIRCUIT_BREAKER_THRESHOLD = 0.10


def run() -> dict:
    """
    Execute one Bronze-to-Silver pipeline cycle.

    Returns a summary dict for logging and the /metrics endpoint:
        {
            "records_read":    int,  # Bronze records fetched this cycle
            "records_written": int,  # Silver records successfully written
            "records_failed":  int,  # Records that failed Silver validation
            "circuit_open":    bool, # True if circuit breaker fired
            "watermark":       int,  # Highest Bronze id processed
        }
    """
    conn = sqlite3.connect(settings.db_path, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row

    summary = {
        "records_read": 0,
        "records_written": 0,
        "records_failed": 0,
        "circuit_open": False,
        "watermark": _get_watermark(conn),
    }

    try:
        # ── Read unprocessed Bronze records ───────────────────────────────────
        # Fetch all Bronze records with id > the current watermark.
        # Limiting to 1000 per cycle prevents a single run from taking too long
        # if the pipeline has fallen behind (e.g. after a restart).
        bronze_rows = conn.execute("""
            SELECT
                b.id, b.vehicle_id, b.event_timestamp, b.received_at,
                b.battery_pct, b.state_of_charge_kwh, b.voltage_v,
                b.current_a, b.motor_temp_c, b.latitude, b.longitude,
                b.speed_kmh, b.regen_braking_event,
                vm.rated_capacity_kwh, vm.manufacture_year, vm.region
            FROM bronze_telemetry b
            LEFT JOIN vehicle_metadata vm ON b.vehicle_id = vm.vehicle_id
            WHERE b.id > ?
            ORDER BY b.id ASC
            LIMIT 1000
        """, (summary["watermark"],)).fetchall()

        if not bronze_rows:
            logger.debug("Bronze-to-Silver: no new records to process")
            return summary

        summary["records_read"] = len(bronze_rows)
        logger.info(
            f"Bronze-to-Silver: reading {len(bronze_rows)} records "
            f"from watermark={summary['watermark']}"
        )

        # ── Process each record ───────────────────────────────────────────────
        silver_rows = []
        failed = 0

        for row in bronze_rows:
            result = _process_row(dict(row))

            if result is None:
                failed += 1
            else:
                silver_rows.append(result)

        summary["records_failed"] = failed

        # ── Circuit breaker check (PRD RES-004) ───────────────────────────────
        # If more than CIRCUIT_BREAKER_THRESHOLD fraction of this batch failed,
        # halt processing. Do not write the partial batch to Silver.
        # This prevents a systematic data quality problem (e.g. a firmware bug
        # sending malformed readings) from corrupting the Silver layer.
        if summary["records_read"] > 0:
            failure_rate = failed / summary["records_read"]
            if failure_rate > CIRCUIT_BREAKER_THRESHOLD:
                logger.error(
                    f"CIRCUIT BREAKER OPEN | "
                    f"failure_rate={failure_rate:.1%} > threshold={CIRCUIT_BREAKER_THRESHOLD:.1%} | "
                    f"halting Silver write | failed={failed}/{summary['records_read']}"
                )
                summary["circuit_open"] = True
                return summary

        # ── Write Silver records ──────────────────────────────────────────────
        if silver_rows:
            _write_silver_batch(conn, silver_rows)
            summary["records_written"] = len(silver_rows)

            # Advance the watermark to the highest Bronze id we processed.
            # This ensures we never reprocess the same Bronze records.
            summary["watermark"] = max(r["bronze_id"] for r in silver_rows)
            _save_watermark(conn, summary["watermark"])

        logger.info(
            f"Bronze-to-Silver complete | "
            f"written={summary['records_written']} | "
            f"failed={summary['records_failed']} | "
            f"watermark={summary['watermark']}"
        )

    finally:
        conn.close()

    return summary


def _process_row(row: dict) -> dict | None:
    """
    Validate and clean one Bronze record for Silver.

    Returns a Silver-ready dict, or None if the record should be skipped.
    Logs a warning for skipped records so they can be investigated.
    """

    # ── Unknown vehicle check ─────────────────────────────────────────────────
    # If the LEFT JOIN found no vehicle_metadata row, rated_capacity_kwh is None.
    # We cannot enrich this record — skip it.
    if row.get("rated_capacity_kwh") is None:
        logger.warning(
            f"Silver skip: unknown vehicle | "
            f"bronze_id={row['id']} | vehicle_id={row['vehicle_id']}"
        )
        return None

    # ── Null check for required numeric fields ────────────────────────────────
    numeric_fields = [
        "battery_pct", "state_of_charge_kwh", "voltage_v",
        "current_a", "motor_temp_c", "latitude", "longitude", "speed_kmh"
    ]
    for field in numeric_fields:
        if row.get(field) is None:
            logger.warning(
                f"Silver skip: null field | "
                f"bronze_id={row['id']} | field={field}"
            )
            return None

    # ── Range checks ──────────────────────────────────────────────────────────
    for field, (min_val, max_val) in RANGES.items():
        value = row[field]
        if not (min_val <= value <= max_val):
            logger.warning(
                f"Silver skip: out of range | "
                f"bronze_id={row['id']} | field={field} | value={value}"
            )
            return None

    # ── Build Silver record ───────────────────────────────────────────────────
    return {
        "bronze_id":            row["id"],
        "vehicle_id":           row["vehicle_id"],
        "event_timestamp":      row["event_timestamp"],
        "battery_pct":          float(row["battery_pct"]),
        "state_of_charge_kwh":  float(row["state_of_charge_kwh"]),
        "voltage_v":            float(row["voltage_v"]),
        "current_a":            float(row["current_a"]),
        "motor_temp_c":         float(row["motor_temp_c"]),
        "latitude":             float(row["latitude"]),
        "longitude":            float(row["longitude"]),
        "speed_kmh":            float(row["speed_kmh"]),
        "regen_braking_event":  int(row["regen_braking_event"] or 0),
        # Enriched from vehicle_metadata join
        "rated_capacity_kwh":   float(row["rated_capacity_kwh"]),
        "manufacture_year":     int(row["manufacture_year"]),
        "region":               row["region"],
        "processed_at":         datetime.now(timezone.utc).isoformat(),
    }


def _write_silver_batch(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """
    Write a batch of Silver records in a single transaction.

    Using a transaction means either all records commit or none do —
    no partial Silver batches. This is the ACID atomicity guarantee.
    """
    conn.executemany("""
        INSERT INTO silver_telemetry (
            bronze_id, vehicle_id, event_timestamp,
            battery_pct, state_of_charge_kwh, voltage_v, current_a,
            motor_temp_c, latitude, longitude, speed_kmh,
            regen_braking_event, rated_capacity_kwh, manufacture_year,
            region, processed_at
        ) VALUES (
            :bronze_id, :vehicle_id, :event_timestamp,
            :battery_pct, :state_of_charge_kwh, :voltage_v, :current_a,
            :motor_temp_c, :latitude, :longitude, :speed_kmh,
            :regen_braking_event, :rated_capacity_kwh, :manufacture_year,
            :region, :processed_at
        )
    """, rows)
    conn.commit()


def _get_watermark(conn: sqlite3.Connection) -> int:
    """
    Return the highest Bronze id already processed into Silver.

    If Silver is empty, returns 0 so the first run processes all Bronze records.
    """
    row = conn.execute(
        "SELECT MAX(bronze_id) FROM silver_telemetry"
    ).fetchone()[0]
    return row if row is not None else 0


def _save_watermark(conn: sqlite3.Connection, watermark: int) -> None:
    """
    Persist the watermark so it survives a pipeline restart.

    We derive it from the Silver table itself (MAX bronze_id) rather than
    storing it separately — the table IS the watermark. This avoids the
    problem of a separate watermark store going out of sync with the data.
    """
    # No separate store needed — the watermark is always derived from Silver.
    # This function exists as a hook for future optimisation (e.g. caching).
    pass
