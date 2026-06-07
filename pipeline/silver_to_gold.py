"""
pipeline/silver_to_gold.py

Silver to Gold pipeline job.

Reads Silver records from the current 30-second window and produces one
Gold record per vehicle containing:
  - Averages for all telemetry fields over the window
  - Rolling 5-minute battery discharge rate
  - A simple 0-100 health score

The Gold layer is what the anomaly detection model and dashboard read from.
Records here are aggregated and business-ready — no joins, no nulls,
no raw sensor noise.

Rolling discharge rate:
  This is the key feature for anomaly detection. A normal vehicle loses
  battery at roughly 0.4% per minute. An anomalous vehicle (faulty cell,
  thermal runaway starting) may lose it 2-3x faster. The rolling window
  smooths out noise so a single unusual reading does not trigger a false alert.

Health score:
  A simple composite metric on a 0-100 scale. Not a substitute for the ML
  model — it is a human-readable summary for the dashboard. A vehicle at
  100% battery, normal temperature, and normal voltage scores 100.
  Each factor that deviates pulls the score down.
"""

import sqlite3
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd

from config.settings import settings

logger = logging.getLogger("voltfleet.pipeline.silver_to_gold")

# Rolling window for discharge rate calculation: 5 minutes
DISCHARGE_WINDOW_MINUTES = 5


def run() -> dict:
    """
    Execute one Silver-to-Gold pipeline cycle.

    Returns a summary dict:
        {
            "vehicles_processed": int,
            "records_written":    int,
            "window_start":       str,
            "window_end":         str,
        }
    """
    conn = sqlite3.connect(settings.db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    now = datetime.now(timezone.utc)
    # The window covers the last pipeline_interval_seconds of Silver data
    window_seconds = settings.pipeline_interval_seconds
    window_end = now
    window_start = now - timedelta(seconds=window_seconds)

    summary = {
        "vehicles_processed": 0,
        "records_written": 0,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }

    try:
        # ── Read Silver records for the current window ────────────────────────
        # We read slightly wider than the window (5 minutes back) to have
        # enough history to compute the rolling discharge rate.
        history_start = now - timedelta(minutes=DISCHARGE_WINDOW_MINUTES)

        silver_rows = conn.execute("""
            SELECT
                vehicle_id, event_timestamp, battery_pct,
                state_of_charge_kwh, voltage_v, current_a,
                motor_temp_c, speed_kmh, regen_braking_event,
                rated_capacity_kwh, manufacture_year, region
            FROM silver_telemetry
            WHERE event_timestamp >= ?
            ORDER BY vehicle_id, event_timestamp ASC
        """, (history_start.isoformat(),)).fetchall()

        if not silver_rows:
            logger.debug("Silver-to-Gold: no Silver records in window")
            return summary

        # ── Load into pandas for aggregation ─────────────────────────────────
        # pandas is well-suited to this: group by vehicle, compute rolling
        # stats, produce one summary row per vehicle.
        df = pd.DataFrame([dict(r) for r in silver_rows])
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)

        # Filter to just the current window for the averages
        # (the wider history_start window is only for discharge rate)
        window_df = df[df["event_timestamp"] >= pd.Timestamp(window_start)]

        if window_df.empty:
            logger.debug("Silver-to-Gold: no records in current window after filtering")
            return summary

        # ── Aggregate per vehicle ─────────────────────────────────────────────
        gold_rows = []
        processed_at = now.isoformat()

        for vehicle_id, vehicle_df in window_df.groupby("vehicle_id"):

            # Simple averages over the window
            avg_battery_pct   = vehicle_df["battery_pct"].mean()
            avg_voltage_v     = vehicle_df["voltage_v"].mean()
            avg_current_a     = vehicle_df["current_a"].mean()
            avg_motor_temp_c  = vehicle_df["motor_temp_c"].mean()
            avg_speed_kmh     = vehicle_df["speed_kmh"].mean()

            # Rolling 5-minute discharge rate
            # Uses the wider history window for this vehicle
            vehicle_history = df[df["vehicle_id"] == vehicle_id].sort_values(
                "event_timestamp"
            )
            discharge_rate = _compute_discharge_rate(vehicle_history)

            # Health score
            rated_capacity = vehicle_df["rated_capacity_kwh"].iloc[0]
            health_score = _compute_health_score(
                avg_battery_pct,
                avg_motor_temp_c,
                avg_voltage_v,
                discharge_rate,
            )

            gold_rows.append({
                "vehicle_id":                vehicle_id,
                "window_start":              window_start.isoformat(),
                "window_end":                window_end.isoformat(),
                "avg_battery_pct":           round(avg_battery_pct, 2),
                "avg_voltage_v":             round(avg_voltage_v, 2),
                "avg_current_a":             round(avg_current_a, 2),
                "avg_motor_temp_c":          round(avg_motor_temp_c, 2),
                "avg_speed_kmh":             round(avg_speed_kmh, 2),
                "rolling_5min_discharge_rate": round(discharge_rate, 4),
                "health_score":              round(health_score, 1),
                "record_count":              len(vehicle_df),
                "aggregated_at":             processed_at,
            })

        # ── Write Gold records ────────────────────────────────────────────────
        if gold_rows:
            _write_gold_batch(conn, gold_rows)
            summary["vehicles_processed"] = len(gold_rows)
            summary["records_written"] = len(gold_rows)

        logger.info(
            f"Silver-to-Gold complete | "
            f"vehicles={summary['vehicles_processed']} | "
            f"window={window_start.strftime('%H:%M:%S')}→{window_end.strftime('%H:%M:%S')}"
        )

    finally:
        conn.close()

    return summary


def _compute_discharge_rate(vehicle_df: pd.DataFrame) -> float:
    """
    Calculate the battery discharge rate over the rolling 5-minute window.

    Formula: (battery_pct at start of window - battery_pct at end) / minutes

    Returns percentage points lost per minute.
    A positive value means discharging. Negative means charging (unlikely
    for a commercial van but possible during depot charging).

    If there are fewer than 2 readings in the window, returns 0.0 —
    not enough data to compute a rate.
    """
    if len(vehicle_df) < 2:
        return 0.0

    # Sort by time and take the oldest and newest readings in the window
    sorted_df = vehicle_df.sort_values("event_timestamp")
    oldest = sorted_df.iloc[0]
    newest = sorted_df.iloc[-1]

    time_delta = newest["event_timestamp"] - oldest["event_timestamp"]
    minutes_elapsed = time_delta.total_seconds() / 60.0

    if minutes_elapsed <= 0:
        return 0.0

    battery_delta = oldest["battery_pct"] - newest["battery_pct"]
    return battery_delta / minutes_elapsed


def _compute_health_score(
    avg_battery_pct: float,
    avg_motor_temp_c: float,
    avg_voltage_v: float,
    discharge_rate: float,
) -> float:
    """
    Compute a 0-100 health score for a vehicle over the current window.

    This is a heuristic, not a model prediction. It gives the dashboard
    a human-readable summary. The ML model (Phase 3) operates on the raw
    features, not this score.

    Components and their weights:
      - Battery level (40%): lower battery = lower score
      - Motor temperature (30%): higher temp = lower score
      - Voltage (20%): lower voltage relative to nominal = lower score
      - Discharge rate (10%): faster discharge = lower score
    """

    # Battery component: linear from 0 (empty) to 40 (full)
    battery_component = (avg_battery_pct / 100.0) * 40.0

    # Temperature component: 30 points at 40°C, 0 points at 120°C
    # Clamp to 0-30 range
    temp_score = max(0.0, 30.0 - max(0.0, avg_motor_temp_c - 40.0) * (30.0 / 80.0))

    # Voltage component: 20 points at 380V (full battery), 0 at 300V
    # Nominal range for the simulated vehicle: 320-380V
    voltage_score = max(0.0, min(20.0, (avg_voltage_v - 300.0) / (380.0 - 300.0) * 20.0))

    # Discharge rate component: 10 points at 0%/min, 0 points at 1.5%/min or above
    # Normal rate is ~0.4%/min. Anomalous is ~1.2%/min.
    discharge_score = max(0.0, 10.0 - (discharge_rate / 1.5) * 10.0)

    total = battery_component + temp_score + voltage_score + discharge_score
    return max(0.0, min(100.0, total))


def _write_gold_batch(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Write Gold records in a single transaction."""
    conn.executemany("""
        INSERT INTO gold_vehicle_health (
            vehicle_id, window_start, window_end,
            avg_battery_pct, avg_voltage_v, avg_current_a,
            avg_motor_temp_c, avg_speed_kmh,
            rolling_5min_discharge_rate, health_score,
            record_count, aggregated_at
        ) VALUES (
            :vehicle_id, :window_start, :window_end,
            :avg_battery_pct, :avg_voltage_v, :avg_current_a,
            :avg_motor_temp_c, :avg_speed_kmh,
            :rolling_5min_discharge_rate, :health_score,
            :record_count, :aggregated_at
        )
    """, rows)
    conn.commit()
