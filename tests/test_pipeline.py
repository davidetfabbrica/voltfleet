"""
tests/test_pipeline.py

Tests for the Bronze-to-Silver and Silver-to-Gold pipeline jobs.
"""

import sqlite3
import pytest
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _make_db(tmp_path) -> str:
    """
    Create a minimal test database with schema and seed data.
    Returns the db path string.
    """
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute("""
        CREATE TABLE vehicle_metadata (
            vehicle_id TEXT PRIMARY KEY, fleet_label TEXT,
            make TEXT, model TEXT, manufacture_year INTEGER,
            rated_capacity_kwh REAL, region TEXT, registered_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE bronze_telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id TEXT, event_timestamp TEXT, received_at TEXT,
            battery_pct REAL, state_of_charge_kwh REAL, voltage_v REAL,
            current_a REAL, motor_temp_c REAL, latitude REAL,
            longitude REAL, speed_kmh REAL, regen_braking_event INTEGER,
            raw_payload TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE silver_telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bronze_id INTEGER, vehicle_id TEXT, event_timestamp TEXT,
            battery_pct REAL, state_of_charge_kwh REAL, voltage_v REAL,
            current_a REAL, motor_temp_c REAL, latitude REAL,
            longitude REAL, speed_kmh REAL, regen_braking_event INTEGER,
            rated_capacity_kwh REAL, manufacture_year INTEGER,
            region TEXT, processed_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE gold_vehicle_health (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id TEXT, window_start TEXT, window_end TEXT,
            avg_battery_pct REAL, avg_voltage_v REAL, avg_current_a REAL,
            avg_motor_temp_c REAL, avg_speed_kmh REAL,
            rolling_5min_discharge_rate REAL, health_score REAL,
            record_count INTEGER, aggregated_at TEXT
        )
    """)

    # Seed vehicle metadata
    conn.execute("""
        INSERT INTO vehicle_metadata VALUES
        ('VF-001','Van 001','Renault','Kangoo',2022,45.0,'EU','2026-01-01'),
        ('VF-002','Van 002','Renault','Kangoo',2022,45.0,'EU','2026-01-01')
    """)
    conn.commit()
    conn.close()
    return db_path


def _insert_bronze_row(conn, vehicle_id="VF-001", battery_pct=75.0,
                        motor_temp_c=42.0, minutes_ago=0):
    """Helper to insert a single Bronze row for testing."""
    ts = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()
    conn.execute("""
        INSERT INTO bronze_telemetry
            (vehicle_id, event_timestamp, received_at, battery_pct,
             state_of_charge_kwh, voltage_v, current_a, motor_temp_c,
             latitude, longitude, speed_kmh, regen_braking_event, raw_payload)
        VALUES (?, ?, ?, ?, 33.75, 345.0, 95.0, ?, 51.5, -0.1, 65.0, 0, '{}')
    """, (vehicle_id, ts, ts, battery_pct, motor_temp_c))
    conn.commit()


# ── Bronze-to-Silver tests ────────────────────────────────────────────────────

class TestBronzeToSilver:

    def test_valid_records_move_to_silver(self, tmp_path):
        """Valid Bronze records should appear in Silver after the job runs."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        _insert_bronze_row(conn, "VF-001", battery_pct=75.0)
        _insert_bronze_row(conn, "VF-002", battery_pct=80.0)
        conn.close()

        with patch("pipeline.bronze_to_silver.settings.db_path", db_path):
            from pipeline.bronze_to_silver import run
            summary = run()

        assert summary["records_read"] == 2
        assert summary["records_written"] == 2
        assert summary["records_failed"] == 0
        assert summary["circuit_open"] is False

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM silver_telemetry").fetchone()[0]
        conn.close()
        assert count == 2

    def test_out_of_range_record_is_skipped(self, tmp_path):
        """A Bronze record with an out-of-range value should not reach Silver."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        _insert_bronze_row(conn, "VF-001", battery_pct=999.0)  # Invalid
        conn.close()

        with patch("pipeline.bronze_to_silver.settings.db_path", db_path):
            from pipeline.bronze_to_silver import run
            summary = run()

        assert summary["records_read"] == 1
        assert summary["records_written"] == 0
        assert summary["records_failed"] == 1

        conn = sqlite3.connect(db_path)
        silver_count = conn.execute(
            "SELECT COUNT(*) FROM silver_telemetry"
        ).fetchone()[0]
        conn.close()
        assert silver_count == 0

    def test_unknown_vehicle_is_skipped(self, tmp_path):
        """A Bronze record for an unknown vehicle_id should not reach Silver."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        _insert_bronze_row(conn, "VF-999", battery_pct=75.0)  # Not in metadata
        conn.close()

        with patch("pipeline.bronze_to_silver.settings.db_path", db_path):
            from pipeline.bronze_to_silver import run
            summary = run()

        assert summary["records_written"] == 0

    def test_watermark_advances(self, tmp_path):
        """After processing, the watermark should equal the highest Bronze id."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        _insert_bronze_row(conn, "VF-001")
        _insert_bronze_row(conn, "VF-002")
        conn.close()

        with patch("pipeline.bronze_to_silver.settings.db_path", db_path):
            from pipeline import bronze_to_silver
            # Reload to reset module state
            import importlib
            importlib.reload(bronze_to_silver)
            summary = bronze_to_silver.run()

        assert summary["watermark"] == 2

    def test_circuit_breaker_fires_on_high_failure_rate(self, tmp_path):
        """If >10% of a batch fails, the circuit breaker should open."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)

        # Insert 9 invalid records and 1 valid one — 90% failure rate
        for _ in range(9):
            _insert_bronze_row(conn, "VF-001", battery_pct=999.0)
        _insert_bronze_row(conn, "VF-001", battery_pct=75.0)
        conn.close()

        with patch("pipeline.bronze_to_silver.settings.db_path", db_path):
            from pipeline import bronze_to_silver
            import importlib
            importlib.reload(bronze_to_silver)
            summary = bronze_to_silver.run()

        assert summary["circuit_open"] is True
        # Nothing should have been written to Silver
        conn = sqlite3.connect(db_path)
        silver_count = conn.execute(
            "SELECT COUNT(*) FROM silver_telemetry"
        ).fetchone()[0]
        conn.close()
        assert silver_count == 0

    def test_idempotent_watermark(self, tmp_path):
        """Running the job twice should not duplicate Silver records."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        _insert_bronze_row(conn, "VF-001")
        conn.close()

        with patch("pipeline.bronze_to_silver.settings.db_path", db_path):
            from pipeline import bronze_to_silver
            import importlib
            importlib.reload(bronze_to_silver)
            bronze_to_silver.run()
            summary2 = bronze_to_silver.run()

        # Second run should read 0 records (watermark has advanced)
        assert summary2["records_read"] == 0

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM silver_telemetry").fetchone()[0]
        conn.close()
        assert count == 1


# ── Silver-to-Gold tests ──────────────────────────────────────────────────────

class TestSilverToGold:

    def _insert_silver_row(self, conn, vehicle_id="VF-001",
                            battery_pct=75.0, motor_temp_c=42.0,
                            minutes_ago=0):
        """Helper to insert a Silver row directly for Gold pipeline tests."""
        ts = (
            datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
        ).isoformat()
        conn.execute("""
            INSERT INTO silver_telemetry
                (bronze_id, vehicle_id, event_timestamp, battery_pct,
                 state_of_charge_kwh, voltage_v, current_a, motor_temp_c,
                 latitude, longitude, speed_kmh, regen_braking_event,
                 rated_capacity_kwh, manufacture_year, region, processed_at)
            VALUES (1, ?, ?, ?, 33.75, 345.0, 95.0, ?, 51.5, -0.1,
                    65.0, 0, 45.0, 2022, 'EU', ?)
        """, (vehicle_id, ts, battery_pct, motor_temp_c, ts))
        conn.commit()

    def test_gold_record_produced_per_vehicle(self, tmp_path):
        """One Gold record should be produced per vehicle in the window."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        self._insert_silver_row(conn, "VF-001")
        self._insert_silver_row(conn, "VF-002")
        conn.close()

        with patch("pipeline.silver_to_gold.settings.db_path", db_path), \
             patch("pipeline.silver_to_gold.settings.pipeline_interval_seconds", 300):
            from pipeline import silver_to_gold
            import importlib
            importlib.reload(silver_to_gold)
            summary = silver_to_gold.run()

        assert summary["vehicles_processed"] == 2

        conn = sqlite3.connect(db_path)
        count = conn.execute(
            "SELECT COUNT(*) FROM gold_vehicle_health"
        ).fetchone()[0]
        conn.close()
        assert count == 2

    def test_health_score_in_valid_range(self, tmp_path):
        """Health score should always be between 0 and 100."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        self._insert_silver_row(conn, "VF-001", battery_pct=90.0, motor_temp_c=38.0)
        conn.close()

        with patch("pipeline.silver_to_gold.settings.db_path", db_path), \
             patch("pipeline.silver_to_gold.settings.pipeline_interval_seconds", 300):
            from pipeline import silver_to_gold
            import importlib
            importlib.reload(silver_to_gold)
            silver_to_gold.run()

        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT health_score FROM gold_vehicle_health"
        ).fetchone()
        conn.close()

        assert row is not None
        assert 0.0 <= row[0] <= 100.0

    def test_anomalous_vehicle_scores_lower(self, tmp_path):
        """A hot, fast-discharging vehicle should score lower than a normal one."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)

        # Normal vehicle: cool, good battery
        for i in range(3):
            self._insert_silver_row(
                conn, "VF-002", battery_pct=80.0 - i,
                motor_temp_c=40.0, minutes_ago=i
            )
        # Anomalous vehicle: hot, lower battery
        for i in range(3):
            self._insert_silver_row(
                conn, "VF-001", battery_pct=60.0 - i,
                motor_temp_c=70.0, minutes_ago=i
            )
        conn.close()

        with patch("pipeline.silver_to_gold.settings.db_path", db_path), \
             patch("pipeline.silver_to_gold.settings.pipeline_interval_seconds", 300):
            from pipeline import silver_to_gold
            import importlib
            importlib.reload(silver_to_gold)
            silver_to_gold.run()

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = {
            r["vehicle_id"]: r["health_score"]
            for r in conn.execute(
                "SELECT vehicle_id, health_score FROM gold_vehicle_health"
            ).fetchall()
        }
        conn.close()

        assert rows["VF-001"] < rows["VF-002"]
