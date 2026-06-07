"""
ingestion/app.py

VoltFleet ingestion service — Flask HTTP endpoint.

This is the entry point for all vehicle telemetry. It:
  1. Applies per-vehicle rate limiting (token bucket)
  2. Validates the event schema
  3. Writes valid events to Bronze
  4. Writes failed events to the DLQ
  5. Exposes /health and /metrics endpoints

To run the ingestion service:
    python -m ingestion.app

Architecture note: in production this would sit behind a load balancer and
API gateway that handles TLS termination, authentication, and DDoS protection.
Here Flask handles requests directly for simplicity. (ADR-001-10)
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from flask import Flask, request, jsonify

from config.settings import settings, configure_logging
from ingestion.rate_limiter import rate_limiter
from ingestion.validator import validate_event, ValidationError, MAX_PAYLOAD_BYTES
from ingestion.writer import write_to_bronze, write_to_dlq

# Initialise logging before anything else
configure_logging()
logger = logging.getLogger("voltfleet.ingestion")

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__)

# Simple in-memory counters for the /metrics endpoint.
# In production these would be emitted to Prometheus or a similar system.
_metrics = {
    "events_accepted": 0,
    "events_rejected_rate_limit": 0,
    "events_rejected_validation": 0,
    "dlq_writes": 0,
    "started_at": datetime.now(timezone.utc).isoformat(),
}


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/ingest", methods=["POST"])
def ingest():
    """
    Main telemetry ingestion endpoint.

    Accepts POST requests with a JSON body containing one telemetry event.
    Returns:
        202 Accepted  — event written to Bronze.
        400 Bad Request — event failed validation (written to DLQ).
        413 Payload Too Large — payload exceeds 4KB limit.
        429 Too Many Requests — vehicle is sending faster than the rate limit.
    """

    client_ip = request.remote_addr

    # ── Payload size check (PRD SEC-008) ──────────────────────────────────────
    # Check content length before reading the body to avoid holding large
    # payloads in memory. Flask sets content_length from the Content-Length header.
    content_length = request.content_length
    if content_length and content_length > MAX_PAYLOAD_BYTES:
        logger.warning(
            f"Payload too large | size={content_length} | client_ip={client_ip}"
        )
        return jsonify({"error": "payload_too_large", "max_bytes": MAX_PAYLOAD_BYTES}), 413

    # ── Read and parse body ───────────────────────────────────────────────────
    raw_body = request.get_data(as_text=True)

    # Guard against empty bodies
    if not raw_body:
        write_to_dlq("", "empty_body", client_ip)
        _metrics["dlq_writes"] += 1
        _metrics["events_rejected_validation"] += 1
        return jsonify({"error": "empty_body"}), 400

    # Parse JSON — if it fails, the payload is malformed
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as e:
        write_to_dlq(raw_body, f"invalid_json:{str(e)[:100]}", client_ip)
        _metrics["dlq_writes"] += 1
        _metrics["events_rejected_validation"] += 1
        return jsonify({"error": "invalid_json"}), 400

    # ── Extract vehicle_id for rate limiting ──────────────────────────────────
    # We need vehicle_id before full validation to apply the per-vehicle limit.
    # If vehicle_id is missing, reject immediately (it is required for routing).
    vehicle_id = payload.get("vehicle_id")
    if not vehicle_id or not isinstance(vehicle_id, str):
        write_to_dlq(raw_body, "missing_field:vehicle_id", client_ip)
        _metrics["dlq_writes"] += 1
        _metrics["events_rejected_validation"] += 1
        return jsonify({"error": "missing_field", "field": "vehicle_id"}), 400

    # ── Rate limiting (PRD FR-005) ────────────────────────────────────────────
    if not rate_limiter.is_allowed(vehicle_id):
        _metrics["events_rejected_rate_limit"] += 1
        return jsonify({
            "error": "rate_limit_exceeded",
            "vehicle_id": vehicle_id,
            "retry_after_seconds": 4,
        }), 429

    # ── Schema validation ─────────────────────────────────────────────────────
    try:
        validated_event = validate_event(payload)
    except ValidationError as e:
        write_to_dlq(raw_body, e.reason, client_ip)
        _metrics["dlq_writes"] += 1
        _metrics["events_rejected_validation"] += 1
        return jsonify({"error": "validation_failed", "reason": e.reason}), 400

    # ── Write to Bronze ───────────────────────────────────────────────────────
    try:
        bronze_id = write_to_bronze(validated_event, raw_body)
        _metrics["events_accepted"] += 1

        return jsonify({
            "status": "accepted",
            "bronze_id": bronze_id,
            "vehicle_id": vehicle_id,
        }), 202

    except Exception as e:
        # If the Bronze write fails, send to DLQ so the event is not lost.
        write_to_dlq(raw_body, f"bronze_write_error:{str(e)[:100]}", client_ip)
        _metrics["dlq_writes"] += 1
        logger.error(f"Bronze write error | vehicle_id={vehicle_id} | error={e}")
        return jsonify({"error": "storage_error"}), 500


@app.route("/health", methods=["GET"])
def health():
    """
    Health check endpoint (PRD OR-005).

    Returns the status of each pipeline component. Designed to be called by
    a load balancer to determine whether this instance should receive traffic.

    Returns 200 if the service is healthy, 503 if not.
    """
    status = {
        "service": "ingestion",
        "region": settings.region,
        "status": "ok",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "components": {},
    }

    # Check database connectivity
    try:
        conn = sqlite3.connect(settings.db_path)
        conn.execute("SELECT 1")
        conn.close()
        status["components"]["database"] = "ok"
    except Exception as e:
        status["components"]["database"] = f"error: {e}"
        status["status"] = "degraded"

    http_status = 200 if status["status"] == "ok" else 503
    return jsonify(status), http_status


@app.route("/metrics", methods=["GET"])
def metrics():
    """
    Operational metrics endpoint (PRD OR-006).

    Returns current event counts and DLQ depth. In production this would
    emit Prometheus-format metrics; here we return JSON for simplicity.

    Note: this endpoint should be restricted to the internal network in
    production (PRD SEC-012). For local development it is open.
    """
    # Count DLQ records directly from the database for accuracy
    try:
        conn = sqlite3.connect(settings.db_path)
        dlq_depth = conn.execute(
            "SELECT COUNT(*) FROM bronze_dlq"
        ).fetchone()[0]
        bronze_count = conn.execute(
            "SELECT COUNT(*) FROM bronze_telemetry"
        ).fetchone()[0]
        conn.close()
    except Exception:
        dlq_depth = -1
        bronze_count = -1

    return jsonify({
        **_metrics,
        "dlq_depth": dlq_depth,
        "bronze_record_count": bronze_count,
    }), 200


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info(
        f"Starting ingestion service | "
        f"host={settings.ingestion_host} | port={settings.ingestion_port}"
    )
    # debug=False in all cases — debug mode exposes an interactive debugger
    # that allows arbitrary code execution. Never use debug=True in production.
    app.run(
        host=settings.ingestion_host,
        port=settings.ingestion_port,
        debug=False,
    )
