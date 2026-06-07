"""
tests/test_ingestion.py

Tests for the ingestion service: validator, rate limiter, and HTTP endpoints.

Each test is independent — it does not rely on state from other tests.
Tests that need a database use a temporary SQLite file that is deleted
after the test runs.

Running the tests:
    pytest tests/ -v
"""

import json
import sqlite3
import tempfile
import os
import sys
import pytest
from pathlib import Path
from unittest.mock import patch
from datetime import datetime, timezone

# Add project root to path so imports work when running from any directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── Validator tests ───────────────────────────────────────────────────────────

from ingestion.validator import validate_event, ValidationError

# A complete, valid event used as the baseline for most tests
VALID_EVENT = {
    "vehicle_id":           "VF-001",
    "event_timestamp":      "2026-06-07T12:00:00+00:00",
    "battery_pct":          75.0,
    "state_of_charge_kwh":  33.75,
    "voltage_v":            345.0,
    "current_a":            95.0,
    "motor_temp_c":         42.0,
    "latitude":             51.505,
    "longitude":            -0.09,
    "speed_kmh":            65.0,
    "regen_braking_event":  False,
}


class TestValidator:

    def test_valid_event_passes(self):
        """A complete, well-formed event should pass without error."""
        result = validate_event(VALID_EVENT)
        assert result["vehicle_id"] == "VF-001"
        assert result["battery_pct"] == 75.0

    def test_missing_required_field_raises(self):
        """Removing a required field should raise ValidationError."""
        event = {**VALID_EVENT}
        del event["battery_pct"]
        with pytest.raises(ValidationError) as exc_info:
            validate_event(event)
        assert "missing_field:battery_pct" in exc_info.value.reason

    def test_battery_above_max_raises(self):
        """Battery percentage above 100 is physically impossible."""
        event = {**VALID_EVENT, "battery_pct": 101.0}
        with pytest.raises(ValidationError) as exc_info:
            validate_event(event)
        assert "out_of_range:battery_pct" in exc_info.value.reason

    def test_battery_below_min_raises(self):
        """Battery percentage below 0 is physically impossible."""
        event = {**VALID_EVENT, "battery_pct": -1.0}
        with pytest.raises(ValidationError) as exc_info:
            validate_event(event)
        assert "out_of_range:battery_pct" in exc_info.value.reason

    def test_motor_temp_above_max_raises(self):
        """Motor temperature above 180°C is a sensor fault."""
        event = {**VALID_EVENT, "motor_temp_c": 200.0}
        with pytest.raises(ValidationError) as exc_info:
            validate_event(event)
        assert "out_of_range:motor_temp_c" in exc_info.value.reason

    def test_invalid_vehicle_id_format_raises(self):
        """vehicle_id must match VF-NNN format."""
        event = {**VALID_EVENT, "vehicle_id": "TRUCK-001"}
        with pytest.raises(ValidationError) as exc_info:
            validate_event(event)
        assert "invalid_vehicle_id" in exc_info.value.reason

    def test_non_dict_payload_raises(self):
        """Payload must be a JSON object, not a list or string."""
        with pytest.raises(ValidationError) as exc_info:
            validate_event(["not", "a", "dict"])
        assert "invalid_format" in exc_info.value.reason

    def test_integer_battery_coerced_to_float(self):
        """Integers should be accepted where floats are expected."""
        event = {**VALID_EVENT, "battery_pct": 75}   # int, not float
        result = validate_event(event)
        assert isinstance(result["battery_pct"], float)
        assert result["battery_pct"] == 75.0

    def test_negative_current_allowed(self):
        """Negative current is valid — it indicates regenerative braking."""
        event = {**VALID_EVENT, "current_a": -30.0}
        result = validate_event(event)
        assert result["current_a"] == -30.0

    def test_regen_braking_must_be_bool(self):
        """regen_braking_event must be a boolean, not an integer."""
        event = {**VALID_EVENT, "regen_braking_event": 1}
        with pytest.raises(ValidationError) as exc_info:
            validate_event(event)
        assert "invalid_type:regen_braking_event" in exc_info.value.reason

    def test_future_timestamp_beyond_60s_raises(self):
        """Timestamps more than 60 seconds in the future indicate a bad clock."""
        from datetime import timedelta
        future = (datetime.now(timezone.utc) + timedelta(seconds=120)).isoformat()
        event = {**VALID_EVENT, "event_timestamp": future}
        with pytest.raises(ValidationError) as exc_info:
            validate_event(event)
        assert "invalid_timestamp" in exc_info.value.reason

    def test_invalid_timestamp_format_raises(self):
        """Non-ISO 8601 timestamps should be rejected."""
        event = {**VALID_EVENT, "event_timestamp": "07/06/2026 12:00"}
        with pytest.raises(ValidationError) as exc_info:
            validate_event(event)
        assert "invalid_timestamp" in exc_info.value.reason


# ── Rate limiter tests ────────────────────────────────────────────────────────

from ingestion.rate_limiter import TokenBucket, RateLimiter


class TestTokenBucket:

    def test_full_bucket_allows_requests(self):
        """A full bucket should allow up to capacity requests."""
        bucket = TokenBucket(capacity=3, refill_rate=0.25)
        assert bucket.consume() is True
        assert bucket.consume() is True
        assert bucket.consume() is True

    def test_empty_bucket_rejects(self):
        """Once empty, the bucket should reject further requests."""
        bucket = TokenBucket(capacity=2, refill_rate=0.25)
        bucket.consume()
        bucket.consume()
        assert bucket.consume() is False

    def test_tokens_refill_over_time(self):
        """After waiting, tokens should refill."""
        import time
        # High refill rate so the test doesn't take long
        bucket = TokenBucket(capacity=1, refill_rate=10.0)
        bucket.consume()                # Empty the bucket
        assert bucket.consume() is False
        time.sleep(0.15)                # Wait 150ms — 10 tokens/s = 1.5 tokens
        assert bucket.consume() is True


class TestRateLimiter:

    def test_new_vehicle_is_allowed(self):
        """A vehicle seen for the first time should be allowed."""
        limiter = RateLimiter()
        assert limiter.is_allowed("VF-099") is True

    def test_different_vehicles_have_independent_buckets(self):
        """Rate limiting one vehicle should not affect another."""
        from config.settings import settings
        limiter = RateLimiter()
        capacity = settings.rate_limit_bucket_capacity

        # Drain VF-001's bucket
        for _ in range(capacity + 1):
            limiter.is_allowed("VF-001")

        # VF-002 should still be allowed
        assert limiter.is_allowed("VF-002") is True


# ── Ingestion endpoint tests ──────────────────────────────────────────────────

class TestIngestionEndpoint:
    """
    Integration tests for the Flask ingestion endpoint.

    Uses Flask's test client — no real HTTP server needed.
    Each test uses a temporary database so tests are isolated.
    """

    @pytest.fixture(autouse=True)
    def setup_temp_db(self, tmp_path):
        """
        Create a fresh temporary database for each test.

        autouse=True means this fixture runs automatically for every
        test in this class without needing to be explicitly requested.
        """
        db_path = str(tmp_path / "test_voltfleet.db")

        # Patch settings to use the temp database
        with patch("config.settings.settings.db_path", db_path):
            # Initialise schema in the temp database
            from scripts.init_db import init_db
            with patch("config.settings.settings.db_path", db_path):
                # Temporarily redirect init_db to the temp path
                conn = sqlite3.connect(db_path)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS bronze_telemetry (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        vehicle_id TEXT, event_timestamp TEXT,
                        received_at TEXT, battery_pct REAL,
                        state_of_charge_kwh REAL, voltage_v REAL,
                        current_a REAL, motor_temp_c REAL,
                        latitude REAL, longitude REAL,
                        speed_kmh REAL, regen_braking_event INTEGER,
                        raw_payload TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS bronze_dlq (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        received_at TEXT, raw_payload TEXT,
                        error_reason TEXT, client_ip TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS vehicle_metadata (
                        vehicle_id TEXT PRIMARY KEY,
                        fleet_label TEXT, make TEXT, model TEXT,
                        manufacture_year INTEGER, rated_capacity_kwh REAL,
                        region TEXT, registered_at TEXT
                    )
                """)
                conn.execute("""
                    INSERT INTO vehicle_metadata VALUES
                    ('VF-001','Van 001','Renault','Kangoo',2022,45.0,'EU','2026-01-01')
                """)
                conn.commit()
                conn.close()

            # Patch the db_path in all modules that use it
            with patch("ingestion.writer.settings.db_path", db_path), \
                 patch("ingestion.app.settings.db_path", db_path):
                from ingestion.app import app
                app.config["TESTING"] = True
                self.client = app.test_client()
                self.db_path = db_path
                yield

    def test_valid_event_returns_202(self):
        """A valid event should be accepted with HTTP 202."""
        response = self.client.post(
            "/ingest",
            data=json.dumps(VALID_EVENT),
            content_type="application/json"
        )
        assert response.status_code == 202
        data = response.get_json()
        assert data["status"] == "accepted"
        assert "bronze_id" in data

    def test_missing_field_returns_400(self):
        """An event missing a required field should return 400."""
        event = {**VALID_EVENT}
        del event["motor_temp_c"]
        response = self.client.post(
            "/ingest",
            data=json.dumps(event),
            content_type="application/json"
        )
        assert response.status_code == 400
        data = response.get_json()
        assert data["error"] == "validation_failed"

    def test_invalid_json_returns_400(self):
        """Malformed JSON should return 400."""
        response = self.client.post(
            "/ingest",
            data="not json {{{",
            content_type="application/json"
        )
        assert response.status_code == 400

    def test_oversized_payload_returns_413(self):
        """Payloads over 4KB should be rejected with 413."""
        big_event = {**VALID_EVENT, "padding": "x" * 5000}
        response = self.client.post(
            "/ingest",
            data=json.dumps(big_event),
            content_type="application/json"
        )
        assert response.status_code == 413

    def test_health_endpoint_returns_200(self):
        """/health should return 200 with database status ok."""
        with patch("ingestion.app.settings.db_path", self.db_path):
            response = self.client.get("/health")
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "ok"

    def test_valid_event_appears_in_bronze(self):
        """A valid event should result in a Bronze table row."""
        self.client.post(
            "/ingest",
            data=json.dumps(VALID_EVENT),
            content_type="application/json"
        )
        conn = sqlite3.connect(self.db_path)
        count = conn.execute(
            "SELECT COUNT(*) FROM bronze_telemetry WHERE vehicle_id = 'VF-001'"
        ).fetchone()[0]
        conn.close()
        assert count >= 1

    def test_invalid_event_appears_in_dlq(self):
        """A validation failure should write to the DLQ, not Bronze.

        Uses VF-002 rather than VF-001 to avoid sharing a rate limiter bucket
        with test_valid_event_appears_in_bronze, which runs first and uses VF-001.
        The rate limiter is a module-level singleton — buckets persist across
        tests within the same process.
        """
        event = {**VALID_EVENT, "vehicle_id": "VF-002", "battery_pct": 999.0}
        self.client.post(
            "/ingest",
            data=json.dumps(event),
            content_type="application/json"
        )
        conn = sqlite3.connect(self.db_path)
        dlq_count = conn.execute("SELECT COUNT(*) FROM bronze_dlq").fetchone()[0]
        bronze_count = conn.execute(
            "SELECT COUNT(*) FROM bronze_telemetry"
        ).fetchone()[0]
        conn.close()
        assert dlq_count == 1
        assert bronze_count == 0
