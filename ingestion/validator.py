"""
ingestion/validator.py

Schema validation for incoming telemetry events.

Every event is checked here before being written to Bronze. Records that fail
validation are never silently dropped — they go to the DLQ (Dead Letter Queue)
with a structured error reason so the problem can be investigated.

This mirrors the schema enforcement layer in a real Kafka pipeline, where you
would use Avro or Protobuf schemas registered in a Schema Registry. Here we
do it in plain Python for clarity.
"""

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("voltfleet.validator")

# ── Field definitions ──────────────────────────────────────────────────────────
# Each entry: (field_name, expected_type, min_value, max_value, required)
# min/max are physical plausibility ranges, not business thresholds.
# A motor temperature of 200°C is physically impossible for this vehicle type —
# that is a sensor fault and should be rejected at the ingestion boundary.

TELEMETRY_FIELDS = [
    # field_name              type    min      max      required
    ("vehicle_id",            str,    None,    None,    True),
    ("event_timestamp",       str,    None,    None,    True),
    ("battery_pct",           float,  0.0,     100.0,   True),
    ("state_of_charge_kwh",   float,  0.0,     150.0,   True),
    ("voltage_v",             float,  0.0,     1000.0,  True),
    ("current_a",             float,  -500.0,  500.0,   True),   # Negative = regen
    ("motor_temp_c",          float,  -40.0,   180.0,   True),
    ("latitude",              float,  -90.0,   90.0,    True),
    ("longitude",             float,  -180.0,  180.0,   True),
    ("speed_kmh",             float,  0.0,     250.0,   True),
    ("regen_braking_event",   bool,   None,    None,    True),
]

# Maximum payload size in bytes (PRD SEC-008: 4KB limit)
MAX_PAYLOAD_BYTES = 4096


class ValidationError(Exception):
    """Raised when a telemetry event fails validation."""
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def validate_event(payload: Any) -> dict:
    """
    Validate a telemetry event payload.

    Args:
        payload: The parsed JSON body from the HTTP request. Expected to be a dict.

    Returns:
        A clean, type-coerced dictionary of the validated event fields.

    Raises:
        ValidationError: with a structured error reason if validation fails.
            The reason string uses a machine-readable format: "category:detail"
            so it can be parsed when investigating DLQ records.
    """

    # ── Basic type check ──────────────────────────────────────────────────────
    if not isinstance(payload, dict):
        raise ValidationError("invalid_format:payload_must_be_json_object")

    clean = {}

    # ── Field-by-field validation ─────────────────────────────────────────────
    for field_name, expected_type, min_val, max_val, required in TELEMETRY_FIELDS:

        raw_value = payload.get(field_name)

        # Check required fields are present and not null
        if raw_value is None:
            if required:
                raise ValidationError(f"missing_field:{field_name}")
            else:
                clean[field_name] = None
                continue

        # Type coercion: attempt to cast to the expected type.
        # Vehicles may send integers where we expect floats (e.g. battery_pct: 75)
        # so we coerce rather than reject on type mismatch alone.
        try:
            if expected_type == bool:
                # bool must be checked before int — in Python, bool is a subclass
                # of int, so isinstance(True, int) is True. Check bool first.
                if not isinstance(raw_value, bool):
                    raise ValidationError(
                        f"invalid_type:{field_name}:expected_bool:got_{type(raw_value).__name__}"
                    )
                coerced = raw_value
            elif expected_type == float:
                coerced = float(raw_value)
            elif expected_type == str:
                if not isinstance(raw_value, str):
                    raise ValidationError(
                        f"invalid_type:{field_name}:expected_str:got_{type(raw_value).__name__}"
                    )
                coerced = raw_value.strip()
            else:
                coerced = expected_type(raw_value)

        except (ValueError, TypeError):
            raise ValidationError(
                f"invalid_type:{field_name}:cannot_coerce_to_{expected_type.__name__}"
            )

        # Range check for numeric fields
        if min_val is not None and coerced < min_val:
            raise ValidationError(
                f"out_of_range:{field_name}:{coerced}<min({min_val})"
            )
        if max_val is not None and coerced > max_val:
            raise ValidationError(
                f"out_of_range:{field_name}:{coerced}>max({max_val})"
            )

        clean[field_name] = coerced

    # ── Timestamp format check ────────────────────────────────────────────────
    # event_timestamp must be a valid ISO 8601 UTC string.
    # We reject timestamps that are more than 60 seconds in the future to
    # catch vehicles with misconfigured clocks.
    try:
        event_dt = datetime.fromisoformat(
            clean["event_timestamp"].replace("Z", "+00:00")
        )
    except ValueError:
        raise ValidationError(
            f"invalid_timestamp:event_timestamp:not_iso8601:{clean['event_timestamp']}"
        )

    now_utc = datetime.now(timezone.utc)
    delta_seconds = (event_dt.replace(tzinfo=timezone.utc) - now_utc).total_seconds()
    if delta_seconds > 60:
        raise ValidationError(
            f"invalid_timestamp:event_timestamp:more_than_60s_in_future"
        )

    # ── vehicle_id format check ───────────────────────────────────────────────
    # Must match the pattern VF-NNN (e.g. VF-001, VF-047)
    vehicle_id = clean["vehicle_id"]
    if not _is_valid_vehicle_id(vehicle_id):
        raise ValidationError(
            f"invalid_vehicle_id:{vehicle_id}:must_match_VF-NNN"
        )

    logger.debug(f"Validation passed | vehicle_id={vehicle_id}")
    return clean


def _is_valid_vehicle_id(vehicle_id: str) -> bool:
    """
    Check that vehicle_id matches the expected fleet format: VF-NNN.

    In production this would also check the ID exists in the vehicle_metadata
    table. We keep that check in the writer to avoid database access in the
    validator (separation of concerns).
    """
    import re
    return bool(re.match(r"^VF-\d{3}$", vehicle_id))
