"""
models/predictor.py

Runs the anomaly detection model every pipeline cycle and writes
predictions and alerts to the database.

Responsibilities:
  - Call detector.predict_all() each cycle
  - Write production predictions to the predictions table
  - Write shadow predictions to the same table with mode='shadow'
  - Apply alert logic: raise an alert when a vehicle is anomalous
    across consecutive windows (PRD FR-018)
  - Apply alert fatigue mitigation: suppress repeat alerts (PRD FR-021)

Shadow mode (ADR-001-13):
  A second model instance runs in parallel. Its predictions are written
  to the predictions table with mode='shadow'. They never trigger alerts.

Alert logic:
  A single anomalous reading does not raise an alert. Two consecutive
  anomalous windows do. This is the consecutive_windows setting in config.
"""

import sqlite3
import json
import logging
from datetime import datetime, timezone, timedelta

from config.settings import settings
from models.anomaly import AnomalyDetector, detector as production_detector

logger = logging.getLogger("voltfleet.models.predictor")


def run() -> dict:
    """
    Execute one prediction cycle.

    Returns a summary dict:
        {
            "predictions_written": int,
            "alerts_raised":       int,
            "anomalies_detected":  int,
        }
    """
    summary = {
        "predictions_written": 0,
        "alerts_raised": 0,
        "anomalies_detected": 0,
    }

    # Ensure model is trained — on first run this trains it, afterwards is a no-op
    if not production_detector.is_trained:
        trained = production_detector.train()
        if not trained:
            logger.warning("Predictor: model not ready — skipping this cycle")
            return summary

    # Production predictions
    predictions = production_detector.predict_all()
    if not predictions:
        return summary

    # Open connection with row_factory set so all queries in this function
    # return sqlite3.Row objects (accessible by column name, not just index)
    conn = sqlite3.connect(settings.db_path, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row

    try:
        predicted_at = datetime.now(timezone.utc).isoformat()

        # Write production predictions
        _write_predictions(conn, predictions, mode="production", predicted_at=predicted_at)
        summary["predictions_written"] += len(predictions)

        # ── Shadow model predictions ──────────────────────────────────────────
        # Represents a candidate model with different hyperparameters.
        # Predictions are logged but never trigger alerts.
        shadow_detector = AnomalyDetector()
        import sklearn.ensemble
        shadow_detector._model = sklearn.ensemble.IsolationForest(
            n_estimators=100,
            contamination=settings.anomaly_contamination,
            random_state=99,  # Different seed = different tree structure
        )
        # Reuse the production scaler — same features, same scaling
        shadow_detector._scaler = production_detector._scaler
        shadow_detector.is_trained = False

        shadow_trained = shadow_detector.train()
        if shadow_trained:
            shadow_predictions = shadow_detector.predict_all()
            _write_predictions(
                conn, shadow_predictions, mode="shadow", predicted_at=predicted_at
            )
            summary["predictions_written"] += len(shadow_predictions)

        # Alert logic
        anomaly_count = sum(1 for p in predictions if p["is_anomaly"])
        summary["anomalies_detected"] = anomaly_count

        alerts_raised = _process_alerts(conn, predictions)
        summary["alerts_raised"] = alerts_raised

        logger.info(
            f"Predictor cycle complete | "
            f"predictions={len(predictions)} | "
            f"anomalies={anomaly_count} | "
            f"alerts_raised={alerts_raised}"
        )

    finally:
        conn.close()

    return summary


def _write_predictions(
    conn: sqlite3.Connection,
    predictions: list[dict],
    mode: str,
    predicted_at: str,
) -> None:
    """Write a batch of predictions to the predictions table."""
    rows = [
        {
            "vehicle_id":    p["vehicle_id"],
            "window_start":  p["window_start"],
            "anomaly_score": p["anomaly_score"],
            "is_anomaly":    int(p["is_anomaly"]),
            "model_version": p["model_version"],
            "mode":          mode,
            "predicted_at":  predicted_at,
        }
        for p in predictions
    ]

    conn.executemany("""
        INSERT INTO predictions
            (vehicle_id, window_start, anomaly_score, is_anomaly,
             model_version, mode, predicted_at)
        VALUES
            (:vehicle_id, :window_start, :anomaly_score, :is_anomaly,
             :model_version, :mode, :predicted_at)
    """, rows)
    conn.commit()


def _process_alerts(conn: sqlite3.Connection, predictions: list[dict]) -> int:
    """
    Evaluate alert conditions for each vehicle and raise alerts where needed.

    Alert logic (PRD FR-018, FR-021):
      1. A vehicle must be anomalous in N consecutive windows before alerting
         (N = settings.anomaly_consecutive_windows, default 2)
      2. Once an alert is raised, suppress further alerts for that vehicle
         for settings.alert_suppression_hours hours, unless the score
         worsens by more than 20%

    Assumes conn.row_factory = sqlite3.Row is already set on the connection.
    """
    alerts_raised = 0

    for prediction in predictions:
        if not prediction["is_anomaly"]:
            continue

        vehicle_id = prediction["vehicle_id"]

        # ── Check consecutive anomalous windows ───────────────────────────────
        # Fetch the N most recent production predictions for this vehicle.
        # All must be anomalous to raise an alert.
        recent = conn.execute("""
            SELECT is_anomaly, anomaly_score
            FROM predictions
            WHERE vehicle_id = ?
              AND mode = 'production'
            ORDER BY predicted_at DESC
            LIMIT ?
        """, (vehicle_id, settings.anomaly_consecutive_windows)).fetchall()

        # Not enough history yet to confirm consecutive anomalies
        if len(recent) < settings.anomaly_consecutive_windows:
            continue

        # All recent predictions must be anomalous
        if not all(r["is_anomaly"] for r in recent):
            continue

        # ── Check alert suppression (PRD FR-021) ──────────────────────────────
        suppression_cutoff = (
            datetime.now(timezone.utc)
            - timedelta(hours=settings.alert_suppression_hours)
        ).isoformat()

        existing_alert = conn.execute("""
            SELECT id, anomaly_score
            FROM alerts
            WHERE vehicle_id = ?
              AND status = 'open'
              AND created_at >= ?
            ORDER BY created_at DESC
            LIMIT 1
        """, (vehicle_id, suppression_cutoff)).fetchone()

        if existing_alert:
            # Only re-alert if score worsened by more than 20%
            # Scores are negative: more negative = worse
            previous_score = existing_alert["anomaly_score"]
            current_score = prediction["anomaly_score"]

            if abs(current_score) <= abs(previous_score) * 1.2:
                logger.debug(
                    f"Alert suppressed | vehicle={vehicle_id} | "
                    f"score={current_score:.3f} (previous={previous_score:.3f})"
                )
                continue

        # ── Raise alert ───────────────────────────────────────────────────────
        now = datetime.now(timezone.utc).isoformat()

        conn.execute("""
            INSERT INTO alerts
                (vehicle_id, first_detected_at, anomaly_score,
                 top_features, status, created_at)
            VALUES (?, ?, ?, ?, 'open', ?)
        """, (
            vehicle_id,
            prediction["window_start"],
            prediction["anomaly_score"],
            json.dumps(_top_features(vehicle_id, conn)),
            now,
        ))
        conn.commit()

        alerts_raised += 1
        logger.warning(
            f"ALERT RAISED | vehicle={vehicle_id} | "
            f"score={prediction['anomaly_score']:.3f}"
        )

    return alerts_raised


def _top_features(vehicle_id: str, conn: sqlite3.Connection) -> list[str]:
    """
    Return the feature names most likely contributing to the anomaly.

    Compares this vehicle's latest Gold values against the fleet average.
    Features that deviate most from the fleet mean are ranked first.

    In production you would use SHAP values for proper feature attribution.
    Assumes conn.row_factory = sqlite3.Row is already set.
    """
    from models.anomaly import FEATURES

    vehicle_row = conn.execute("""
        SELECT avg_battery_pct, avg_voltage_v, avg_current_a,
               avg_motor_temp_c, rolling_5min_discharge_rate
        FROM gold_vehicle_health
        WHERE vehicle_id = ?
        ORDER BY aggregated_at DESC
        LIMIT 1
    """, (vehicle_id,)).fetchone()

    if not vehicle_row:
        return []

    fleet_row = conn.execute("""
        SELECT
            AVG(avg_battery_pct)             AS avg_battery_pct,
            AVG(avg_voltage_v)               AS avg_voltage_v,
            AVG(avg_current_a)               AS avg_current_a,
            AVG(avg_motor_temp_c)            AS avg_motor_temp_c,
            AVG(rolling_5min_discharge_rate) AS rolling_5min_discharge_rate
        FROM gold_vehicle_health
        WHERE (vehicle_id, aggregated_at) IN (
            SELECT vehicle_id, MAX(aggregated_at)
            FROM gold_vehicle_health
            GROUP BY vehicle_id
        )
    """).fetchone()

    if not fleet_row:
        return FEATURES[:2]

    deviations = []
    for feature in FEATURES:
        vehicle_val = vehicle_row[feature]
        fleet_val = fleet_row[feature]

        if fleet_val and fleet_val != 0:
            deviation = abs((vehicle_val - fleet_val) / fleet_val)
        else:
            deviation = 0.0

        deviations.append((feature, deviation))

    deviations.sort(key=lambda x: x[1], reverse=True)
    return [f[0] for f in deviations[:3]]
