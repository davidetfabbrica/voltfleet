"""
ingestion/writer.py

Writes validated telemetry events to the Bronze layer, and failed events
to the Dead Letter Queue.

This module is the only place in the codebase that writes to the Bronze table.
Keeping writes isolated here means if we ever want to swap SQLite for a
different storage backend (e.g. S3 Parquet), there is exactly one place to change.
"""

import sqlite3
import json
import logging
from datetime import datetime, timezone

from config.settings import settings

logger = logging.getLogger("voltfleet.writer")


def _get_connection() -> sqlite3.Connection:
    """
    Open a SQLite connection with the settings we need for safe concurrent use.

    check_same_thread=False is required because Flask may call this from a
    different thread than the one that created the connection.
    WAL mode and foreign keys are set in init_db.py at the database level,
    but we enforce foreign keys here per-connection as a safety belt.
    """
    conn = sqlite3.connect(
        settings.db_path,
        check_same_thread=False,
    )
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def write_to_bronze(validated_event: dict, raw_payload: str) -> int:
    """
    Write a validated telemetry event to the Bronze layer.

    Args:
        validated_event: The clean, type-coerced event from the validator.
        raw_payload:     The original raw JSON string. Stored alongside the
                         validated fields so the full original record is
                         always available for reprocessing.

    Returns:
        The auto-incremented Bronze row ID (used for Silver lineage tracking).
    """
    received_at = datetime.now(timezone.utc).isoformat()

    conn = _get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO bronze_telemetry (
                vehicle_id, event_timestamp, received_at,
                battery_pct, state_of_charge_kwh, voltage_v, current_a,
                motor_temp_c, latitude, longitude, speed_kmh,
                regen_braking_event, raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            validated_event["vehicle_id"],
            validated_event["event_timestamp"],
            received_at,
            validated_event["battery_pct"],
            validated_event["state_of_charge_kwh"],
            validated_event["voltage_v"],
            validated_event["current_a"],
            validated_event["motor_temp_c"],
            validated_event["latitude"],
            validated_event["longitude"],
            validated_event["speed_kmh"],
            # SQLite stores booleans as integers (0/1)
            int(validated_event["regen_braking_event"]),
            raw_payload,
        ))
        conn.commit()
        bronze_id = cursor.lastrowid

        logger.debug(
            f"Bronze write | vehicle_id={validated_event['vehicle_id']} "
            f"| bronze_id={bronze_id}"
        )
        return bronze_id

    except sqlite3.Error as e:
        logger.error(f"Bronze write failed | error={e}")
        raise

    finally:
        conn.close()


def write_to_dlq(raw_payload: str, error_reason: str, client_ip: str) -> None:
    """
    Write a failed event to the Dead Letter Queue.

    The DLQ is never the reason the ingestion service returns an error — it
    is a silent side-effect. The caller handles the HTTP response separately.

    Args:
        raw_payload:  The raw request body, even if it is not valid JSON.
        error_reason: Structured error code, e.g. "missing_field:battery_pct".
        client_ip:    IP address of the vehicle client, for debugging.
    """
    received_at = datetime.now(timezone.utc).isoformat()

    conn = _get_connection()
    try:
        conn.execute("""
            INSERT INTO bronze_dlq (received_at, raw_payload, error_reason, client_ip)
            VALUES (?, ?, ?, ?)
        """, (received_at, raw_payload, error_reason, client_ip))
        conn.commit()

        logger.warning(
            f"DLQ write | reason={error_reason} | client_ip={client_ip}"
        )

    except sqlite3.Error as e:
        # If the DLQ write itself fails, log it but do not raise.
        # The ingestion service must continue processing other events even if
        # the DLQ is temporarily unavailable.
        logger.error(f"DLQ write failed | error={e} | original_reason={error_reason}")

    finally:
        conn.close()
