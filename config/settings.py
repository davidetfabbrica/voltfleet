"""
config/settings.py

Central configuration for VoltFleet.

All settings are read from environment variables, which are loaded from the
.env file by python-dotenv. Nothing is hardcoded here — this file is the
single place where configuration is defined and validated.

Any module that needs a setting imports it from here:
    from config.settings import settings
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env file ────────────────────────────────────────────────────────────
# load_dotenv() looks for a .env file in the current working directory and
# loads its contents into os.environ. If the variable is already set in the
# real environment (e.g. in a Docker container), it is NOT overwritten.
load_dotenv()


# ── Helper ────────────────────────────────────────────────────────────────────

def _require(key: str) -> str:
    """Read an environment variable; raise clearly if it is missing."""
    value = os.getenv(key)
    if value is None:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Check your .env file against .env.example."
        )
    return value


# ── Settings object ───────────────────────────────────────────────────────────

class Settings:
    """
    All application settings in one place.

    Attributes are typed so that the rest of the codebase never has to
    cast strings to ints or floats itself.
    """

    def __init__(self):

        # --- Region -----------------------------------------------------------
        # Identifies which geographic deployment this instance represents.
        # Used in logs and (in production) would control data residency routing.
        self.region: str = os.getenv("VOLTFLEET_REGION", "EU")

        # --- Database ---------------------------------------------------------
        # Path to the SQLite database file, relative to the project root.
        self.db_path: str = os.getenv("DB_PATH", "data/voltfleet.db")

        # --- Ingestion service ------------------------------------------------
        self.ingestion_host: str = os.getenv("INGESTION_HOST", "127.0.0.1")
        self.ingestion_port: int = int(os.getenv("INGESTION_PORT", "5000"))

        # The URL the simulator uses to POST events to the ingestion service.
        self.ingestion_url: str = (
            f"http://{self.ingestion_host}:{self.ingestion_port}/ingest"
        )

        # --- Rate limiting ----------------------------------------------------
        # Maximum events accepted per vehicle per second.
        # Vehicles emit every 5 seconds, so 0.25 events/second (1 per 4s) gives
        # a small buffer above the expected rate before triggering a 429.
        self.rate_limit_events_per_second: float = float(
            os.getenv("RATE_LIMIT_EVENTS_PER_SECOND", "0.25")
        )

        # Token bucket capacity: how many events can burst before rate limiting
        # kicks in. A value of 3 allows a short burst on reconnect.
        self.rate_limit_bucket_capacity: int = int(
            os.getenv("RATE_LIMIT_BUCKET_CAPACITY", "3")
        )

        # --- Pipeline ---------------------------------------------------------
        # How often the Bronze->Silver and Silver->Gold pipeline jobs run.
        self.pipeline_interval_seconds: int = int(
            os.getenv("PIPELINE_INTERVAL_SECONDS", "30")
        )

        # --- Simulator --------------------------------------------------------
        self.simulator_vehicle_count: int = int(
            os.getenv("SIMULATOR_VEHICLE_COUNT", "10")
        )

        self.simulator_emit_interval_seconds: float = float(
            os.getenv("SIMULATOR_EMIT_INTERVAL_SECONDS", "5.0")
        )

        # --- Anomaly detection ------------------------------------------------
        # IsolationForest contamination: expected proportion of anomalies.
        self.anomaly_contamination: float = float(
            os.getenv("ANOMALY_CONTAMINATION", "0.05")
        )

        # Consecutive anomalous windows before raising an alert (PRD FR-018).
        self.anomaly_consecutive_windows: int = int(
            os.getenv("ANOMALY_CONSECUTIVE_WINDOWS", "2")
        )

        # Suppress repeat alerts for this many hours after one is raised.
        self.alert_suppression_hours: int = int(
            os.getenv("ALERT_SUPPRESSION_HOURS", "4")
        )

        # --- Logging ----------------------------------------------------------
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        self.log_path: str = os.getenv("LOG_PATH", "logs/voltfleet.log")

    def __repr__(self) -> str:
        return (
            f"Settings(region={self.region}, db={self.db_path}, "
            f"port={self.ingestion_port}, vehicles={self.simulator_vehicle_count})"
        )


# ── Singleton instance ────────────────────────────────────────────────────────
# Import this object everywhere rather than instantiating Settings() yourself.
settings = Settings()


# ── Logging setup ─────────────────────────────────────────────────────────────

def configure_logging() -> logging.Logger:
    """
    Configure structured logging for the application.

    Writes to both console and a log file. Format is structured so entries
    can be parsed by log aggregation tools in production.
    """
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # Ensure the logs directory exists before trying to write to it
    Path(settings.log_path).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(settings.log_path),
        ]
    )

    logger = logging.getLogger("voltfleet")
    logger.info(f"VoltFleet starting | region={settings.region} | config={settings}")
    return logger
