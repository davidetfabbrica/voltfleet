"""
dashboard/api.py

Flask API that serves data to the VoltFleet dashboard.

Endpoints:
  GET /dashboard/             Serves the React dashboard HTML
  GET /api/fleet/summary      Fleet-level headline numbers
  GET /api/fleet/vehicles     All vehicles with current status
  GET /api/vehicle/<id>       Single vehicle detail + history
  GET /api/alerts             Active and recent alerts
  GET /api/pipeline/health    Pipeline component health
  GET /api/dlq                Dead letter queue records

To run:
    python -m dashboard.api
"""

import sqlite3
import json
import logging
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, render_template, request
from pathlib import Path

from config.settings import settings, configure_logging

configure_logging()
logger = logging.getLogger("voltfleet.dashboard")

app = Flask(__name__, template_folder="templates")


def _conn():
    """Open a read-only SQLite connection with row factory set."""
    conn = sqlite3.connect(settings.db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# ── HTML shell ────────────────────────────────────────────────────────────────

@app.route("/dashboard/")
@app.route("/dashboard")
def dashboard():
    """Serve the React dashboard."""
    return render_template("index.html")


# ── Fleet summary ─────────────────────────────────────────────────────────────

@app.route("/api/fleet/summary")
def fleet_summary():
    """
    Headline numbers for the top of the dashboard.

    Returns total vehicles, online count, alert count, average battery,
    and the time of the most recent telemetry event.
    """
    conn = _conn()
    try:
        # Total vehicles registered
        total = conn.execute(
            "SELECT COUNT(*) FROM vehicle_metadata"
        ).fetchone()[0]

        # Vehicles that have sent telemetry in the last 2 minutes
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
        online = conn.execute("""
            SELECT COUNT(DISTINCT vehicle_id)
            FROM bronze_telemetry
            WHERE received_at >= ?
              AND vehicle_id != '[ERASED]'
        """, (cutoff,)).fetchone()[0]

        # Active open alerts
        active_alerts = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE status = 'open'"
        ).fetchone()[0]

        # Average battery across all vehicles' latest Gold records
        avg_battery = conn.execute("""
            SELECT ROUND(AVG(avg_battery_pct), 1)
            FROM gold_vehicle_health
            WHERE (vehicle_id, aggregated_at) IN (
                SELECT vehicle_id, MAX(aggregated_at)
                FROM gold_vehicle_health
                WHERE vehicle_id != '[ERASED]'
                GROUP BY vehicle_id
            )
        """).fetchone()[0]

        # Most recent Bronze event time
        latest = conn.execute("""
            SELECT MAX(received_at) FROM bronze_telemetry
            WHERE vehicle_id != '[ERASED]'
        """).fetchone()[0]

        # DLQ depth
        dlq_depth = conn.execute(
            "SELECT COUNT(*) FROM bronze_dlq"
        ).fetchone()[0]

        # Total Bronze events ingested
        total_events = conn.execute(
            "SELECT COUNT(*) FROM bronze_telemetry"
        ).fetchone()[0]

        return jsonify({
            "total_vehicles":  total,
            "online_vehicles": online,
            "active_alerts":   active_alerts,
            "avg_battery_pct": avg_battery or 0,
            "latest_event_at": latest,
            "dlq_depth":       dlq_depth,
            "total_events":    total_events,
            "fetched_at":      datetime.now(timezone.utc).isoformat(),
        })
    finally:
        conn.close()


# ── Vehicle list ──────────────────────────────────────────────────────────────

@app.route("/api/fleet/vehicles")
def fleet_vehicles():
    """
    All vehicles with their current Gold health metrics and alert status.
    Used to populate the vehicle list panel.
    """
    conn = _conn()
    try:
        rows = conn.execute("""
            SELECT
                vm.vehicle_id,
                vm.fleet_label,
                vm.make,
                vm.model,
                vm.manufacture_year,
                vm.software_version,
                g.avg_battery_pct,
                g.avg_motor_temp_c,
                g.health_score,
                g.rolling_5min_discharge_rate,
                g.aggregated_at,
                -- Alert status: 1 if this vehicle has an open alert
                CASE WHEN a.vehicle_id IS NOT NULL THEN 1 ELSE 0 END AS has_alert,
                a.anomaly_score,
                -- Latest prediction score
                p.anomaly_score AS latest_pred_score,
                p.is_anomaly
            FROM vehicle_metadata vm
            LEFT JOIN gold_vehicle_health g
                ON vm.vehicle_id = g.vehicle_id
                AND g.aggregated_at = (
                    SELECT MAX(aggregated_at)
                    FROM gold_vehicle_health
                    WHERE vehicle_id = vm.vehicle_id
                )
            LEFT JOIN alerts a
                ON vm.vehicle_id = a.vehicle_id
                AND a.status = 'open'
            LEFT JOIN predictions p
                ON vm.vehicle_id = p.vehicle_id
                AND p.mode = 'production'
                AND p.predicted_at = (
                    SELECT MAX(predicted_at)
                    FROM predictions
                    WHERE vehicle_id = vm.vehicle_id
                    AND mode = 'production'
                )
            ORDER BY has_alert DESC, g.health_score ASC
        """).fetchall()

        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


# ── Vehicle detail ────────────────────────────────────────────────────────────

@app.route("/api/vehicle/<vehicle_id>")
def vehicle_detail(vehicle_id):
    """
    Full detail for one vehicle: metadata, last 60 minutes of telemetry,
    recent Gold history, and prediction history.
    """
    conn = _conn()
    try:
        # Metadata
        meta = conn.execute(
            "SELECT * FROM vehicle_metadata WHERE vehicle_id = ?",
            (vehicle_id,)
        ).fetchone()

        if not meta:
            return jsonify({"error": "vehicle not found"}), 404

        # Last 60 minutes of Silver telemetry for charts
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        telemetry = conn.execute("""
            SELECT
                event_timestamp,
                battery_pct,
                voltage_v,
                motor_temp_c,
                current_a,
                speed_kmh,
                regen_braking_event
            FROM silver_telemetry
            WHERE vehicle_id = ?
              AND event_timestamp >= ?
            ORDER BY event_timestamp ASC
        """, (vehicle_id, cutoff)).fetchall()

        # Last 20 Gold health records for trend sparkline
        gold_history = conn.execute("""
            SELECT
                window_start,
                avg_battery_pct,
                avg_motor_temp_c,
                health_score,
                rolling_5min_discharge_rate
            FROM gold_vehicle_health
            WHERE vehicle_id = ?
            ORDER BY aggregated_at DESC
            LIMIT 20
        """, (vehicle_id,)).fetchall()

        # Last 20 predictions
        predictions = conn.execute("""
            SELECT
                window_start,
                anomaly_score,
                is_anomaly,
                mode,
                predicted_at
            FROM predictions
            WHERE vehicle_id = ?
            ORDER BY predicted_at DESC
            LIMIT 20
        """, (vehicle_id,)).fetchall()

        # Alert history
        alerts = conn.execute("""
            SELECT
                first_detected_at,
                anomaly_score,
                top_features,
                status,
                created_at
            FROM alerts
            WHERE vehicle_id = ?
            ORDER BY created_at DESC
            LIMIT 10
        """, (vehicle_id,)).fetchall()

        return jsonify({
            "metadata":    dict(meta),
            "telemetry":   [dict(r) for r in telemetry],
            "gold_history": [dict(r) for r in gold_history],
            "predictions": [dict(r) for r in predictions],
            "alerts":      [dict(r) for r in alerts],
        })
    finally:
        conn.close()


# ── Alerts ────────────────────────────────────────────────────────────────────

@app.route("/api/alerts")
def alerts():
    """All alerts, most recent first."""
    conn = _conn()
    try:
        rows = conn.execute("""
            SELECT
                id,
                vehicle_id,
                first_detected_at,
                anomaly_score,
                top_features,
                status,
                acknowledged_at,
                acknowledged_by,
                created_at
            FROM alerts
            ORDER BY created_at DESC
            LIMIT 50
        """).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


# ── Pipeline health ───────────────────────────────────────────────────────────

@app.route("/api/pipeline/health")
def pipeline_health():
    """
    Health of each pipeline stage, derived from data recency.

    A stage is considered healthy if its most recent output is within
    2x the expected pipeline interval.
    """
    conn = _conn()
    try:
        now = datetime.now(timezone.utc)
        threshold = settings.pipeline_interval_seconds * 2

        # Bronze: latest received_at
        latest_bronze = conn.execute(
            "SELECT MAX(received_at) FROM bronze_telemetry"
        ).fetchone()[0]

        # Silver: latest processed_at
        latest_silver = conn.execute(
            "SELECT MAX(processed_at) FROM silver_telemetry"
        ).fetchone()[0]

        # Gold: latest aggregated_at
        latest_gold = conn.execute(
            "SELECT MAX(aggregated_at) FROM gold_vehicle_health"
        ).fetchone()[0]

        # Predictions: latest predicted_at
        latest_pred = conn.execute(
            "SELECT MAX(predicted_at) FROM predictions WHERE mode = 'production'"
        ).fetchone()[0]

        def stage_status(latest_ts):
            if not latest_ts:
                return {"status": "no_data", "last_run": None, "lag_seconds": None}
            try:
                dt = datetime.fromisoformat(latest_ts.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    from datetime import timezone as tz
                    dt = dt.replace(tzinfo=tz.utc)
                lag = (now - dt).total_seconds()
                status = "ok" if lag < threshold else "stale"
                return {
                    "status":      status,
                    "last_run":    latest_ts,
                    "lag_seconds": round(lag),
                }
            except Exception:
                return {"status": "error", "last_run": latest_ts, "lag_seconds": None}

        # Record counts
        counts = {
            "bronze": conn.execute("SELECT COUNT(*) FROM bronze_telemetry").fetchone()[0],
            "silver": conn.execute("SELECT COUNT(*) FROM silver_telemetry").fetchone()[0],
            "gold":   conn.execute("SELECT COUNT(*) FROM gold_vehicle_health").fetchone()[0],
            "dlq":    conn.execute("SELECT COUNT(*) FROM bronze_dlq").fetchone()[0],
            "predictions": conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0],
            "alerts": conn.execute("SELECT COUNT(*) FROM alerts WHERE status='open'").fetchone()[0],
        }

        return jsonify({
            "stages": {
                "ingestion":  stage_status(latest_bronze),
                "silver":     stage_status(latest_silver),
                "gold":       stage_status(latest_gold),
                "prediction": stage_status(latest_pred),
            },
            "counts":    counts,
            "fetched_at": now.isoformat(),
        })
    finally:
        conn.close()


# ── DLQ viewer ────────────────────────────────────────────────────────────────

@app.route("/api/dlq")
def dlq():
    """Dead letter queue records, most recent first."""
    conn = _conn()
    try:
        rows = conn.execute("""
            SELECT id, received_at, error_reason, client_ip,
                   SUBSTR(raw_payload, 1, 200) AS raw_preview
            FROM bronze_dlq
            ORDER BY received_at DESC
            LIMIT 100
        """).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()



# ── Vehicle GPS positions ─────────────────────────────────────────────────────

@app.route("/api/fleet/positions")
def fleet_positions():
    """
    Latest GPS position for each vehicle, taken from Silver telemetry.

    Also returns software_version from vehicle_metadata so the map markers
    can be coloured by firmware version.
    """
    conn = _conn()
    try:
        rows = conn.execute("""
            SELECT
                s.vehicle_id,
                s.latitude,
                s.longitude,
                s.event_timestamp,
                s.battery_pct,
                s.motor_temp_c,
                s.speed_kmh,
                vm.software_version,
                vm.fleet_label,
                CASE WHEN a.vehicle_id IS NOT NULL THEN 1 ELSE 0 END AS has_alert
            FROM silver_telemetry s
            JOIN vehicle_metadata vm ON s.vehicle_id = vm.vehicle_id
            LEFT JOIN alerts a
                ON s.vehicle_id = a.vehicle_id AND a.status = 'open'
            WHERE s.vehicle_id != '[ERASED]'
              AND (s.vehicle_id, s.event_timestamp) IN (
                  SELECT vehicle_id, MAX(event_timestamp)
                  FROM silver_telemetry
                  WHERE vehicle_id != '[ERASED]'
                  GROUP BY vehicle_id
              )
        """).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Starting VoltFleet dashboard on port 5002")
    app.run(host="127.0.0.1", port=5002, debug=False)
