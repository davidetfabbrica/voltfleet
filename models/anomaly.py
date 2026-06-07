"""
models/anomaly.py

IsolationForest anomaly detection model for VoltFleet.

Responsibilities:
  - Train a model on recent Gold layer data
  - Score each vehicle's latest Gold record
  - Return anomaly scores and flags
  - Support shadow mode (run without triggering alerts)

Training strategy:
  The model trains on a rolling 30-day window of Gold data. In practice,
  with a 30-second pipeline cycle, that is a large number of records.
  For this implementation we train on whatever Gold data is available,
  with a minimum threshold of 10 records before training is attempted.

  In production you would retrain on a schedule (e.g. weekly) and version
  the model using a model registry. Here we retrain at startup and expose
  a retrain() method the scheduler can call periodically.

Feature set (from ADR-001-13):
  - avg_battery_pct
  - avg_voltage_v
  - avg_current_a
  - avg_motor_temp_c
  - rolling_5min_discharge_rate
"""

import sqlite3
import logging
import json
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from config.settings import settings

logger = logging.getLogger("voltfleet.models.anomaly")

# Features the model trains and predicts on.
# Order matters — must be consistent between training and inference.
FEATURES = [
    "avg_battery_pct",
    "avg_voltage_v",
    "avg_current_a",
    "avg_motor_temp_c",
    "rolling_5min_discharge_rate",
]

# Minimum Gold records needed before training is attempted.
# Below this threshold the model cannot learn meaningful patterns.
MIN_TRAINING_RECORDS = 10

# Model version string — increment when retraining with new parameters
MODEL_VERSION = "v1.0"


class AnomalyDetector:
    """
    Wraps IsolationForest with training, scoring, and state management.

    Usage:
        detector = AnomalyDetector()
        detector.train()                    # train on current Gold data
        results = detector.predict_all()    # score all vehicles
    """

    def __init__(self):
        self._model: IsolationForest | None = None
        self._scaler: StandardScaler | None = None
        self._trained_at: str | None = None
        self._training_record_count: int = 0
        self.is_trained: bool = False

    def train(self) -> bool:
        """
        Train the IsolationForest model on Gold layer data.

        Uses a rolling 30-day window so the model reflects recent fleet
        behaviour rather than historical patterns that may no longer apply.

        Returns True if training succeeded, False if there was insufficient data.
        """
        conn = sqlite3.connect(settings.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

        rows = conn.execute(f"""
            SELECT {', '.join(FEATURES)}
            FROM gold_vehicle_health
            WHERE aggregated_at >= ?
            ORDER BY aggregated_at ASC
        """, (cutoff,)).fetchall()
        conn.close()

        if len(rows) < MIN_TRAINING_RECORDS:
            logger.warning(
                f"Insufficient Gold data for training | "
                f"have={len(rows)} | need={MIN_TRAINING_RECORDS}"
            )
            return False

        # Build feature matrix
        df = pd.DataFrame([dict(r) for r in rows])[FEATURES]

        # Drop any rows with nulls — cannot train on incomplete records
        df = df.dropna()

        if len(df) < MIN_TRAINING_RECORDS:
            logger.warning(f"Too many nulls in Gold data — cannot train")
            return False

        # ── Scaling ───────────────────────────────────────────────────────────
        # IsolationForest is not distance-based so scaling is not strictly
        # required, but it improves numerical stability and makes the anomaly
        # scores more comparable across features with very different ranges
        # (e.g. voltage_v is in the hundreds, discharge_rate is near zero).
        self._scaler = StandardScaler()
        X = self._scaler.fit_transform(df.values)

        # ── Train IsolationForest ─────────────────────────────────────────────
        # n_estimators=100: number of isolation trees. More trees = more stable
        # scores but slower training. 100 is the scikit-learn default and works
        # well for datasets of this size.
        #
        # contamination: expected proportion of anomalies. Drives the threshold
        # between anomalous and normal scores. Configurable via settings.
        #
        # random_state: fixed seed for reproducibility — same data always
        # produces the same model.
        self._model = IsolationForest(
            n_estimators=100,
            contamination=settings.anomaly_contamination,
            random_state=42,
        )
        self._model.fit(X)

        self._trained_at = datetime.now(timezone.utc).isoformat()
        self._training_record_count = len(df)
        self.is_trained = True

        logger.info(
            f"Model trained | records={len(df)} | "
            f"contamination={settings.anomaly_contamination} | "
            f"version={MODEL_VERSION}"
        )
        return True

    def predict_all(self) -> list[dict]:
        """
        Score each vehicle's most recent Gold record.

        Returns a list of prediction dicts, one per vehicle:
            {
                "vehicle_id":    str,
                "window_start":  str,
                "anomaly_score": float,   # more negative = more anomalous
                "is_anomaly":    bool,
                "model_version": str,
            }

        Returns an empty list if the model has not been trained.
        """
        if not self.is_trained:
            logger.warning("predict_all called before model is trained")
            return []

        conn = sqlite3.connect(settings.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # Get the most recent Gold record for each vehicle
        rows = conn.execute(f"""
            SELECT vehicle_id, window_start, {', '.join(FEATURES)}
            FROM gold_vehicle_health
            WHERE (vehicle_id, aggregated_at) IN (
                SELECT vehicle_id, MAX(aggregated_at)
                FROM gold_vehicle_health
                GROUP BY vehicle_id
            )
        """).fetchall()
        conn.close()

        if not rows:
            logger.debug("No Gold records available for prediction")
            return []

        df = pd.DataFrame([dict(r) for r in rows])

        # Keep metadata columns separate from features
        meta = df[["vehicle_id", "window_start"]].copy()
        X_raw = df[FEATURES].fillna(0)

        # Apply the same scaler used during training
        X_scaled = self._scaler.transform(X_raw.values)

        # decision_function returns the raw anomaly score.
        # More negative = further from normal = more anomalous.
        # The threshold between anomalous and normal is near 0,
        # but the exact boundary depends on the contamination parameter.
        scores = self._model.decision_function(X_scaled)

        # predict() returns -1 for anomalies, 1 for normal points
        labels = self._model.predict(X_scaled)

        results = []
        for i, row in enumerate(rows):
            results.append({
                "vehicle_id":    row["vehicle_id"],
                "window_start":  row["window_start"],
                "anomaly_score": float(round(scores[i], 4)),
                "is_anomaly":    bool(labels[i] == -1),
                "model_version": MODEL_VERSION,
            })

        anomaly_count = sum(1 for r in results if r["is_anomaly"])
        logger.info(
            f"Predictions complete | vehicles={len(results)} | "
            f"anomalies={anomaly_count}"
        )
        return results


# Module-level singleton — one detector instance shared across the application
detector = AnomalyDetector()
