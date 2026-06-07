"""
simulator/vehicle.py

Simulates a single electric vehicle emitting telemetry events.

Each simulated vehicle:
  - Starts with a random battery level and slowly discharges
  - Has small random variation per reading to simulate real sensor noise
  - Can be set to "anomaly mode" to inject a fault (for testing the ML model)
  - Emits events asynchronously using asyncio with a thread pool for HTTP calls
  - Retries with jitter if the ingestion service is unavailable (ADR-001-10)

Why asyncio + threads rather than aiohttp?
  asyncio handles the concurrency (many vehicles running simultaneously).
  The actual HTTP POST is a blocking operation (requests library), so we
  run it in a thread pool via asyncio.to_thread() — this avoids blocking the
  event loop while still keeping all vehicles running concurrently.
  This is a standard pattern when mixing async code with blocking I/O libraries.

Protocol note: real vehicles would use MQTT over a mobile network. Here we
use HTTP POST which mirrors the logical behaviour without requiring a broker.
(ADR-001-02)
"""

import asyncio
import random
import json
import logging
from datetime import datetime, timezone

import requests

from config.settings import settings

logger = logging.getLogger("voltfleet.simulator.vehicle")


class Vehicle:
    """
    A simulated electric van.

    State is maintained between emissions so the battery discharges over time
    and readings are temporally coherent (not random per-event).
    """

    # Physical constants for the simulated Renault Kangoo E-Tech
    RATED_CAPACITY_KWH = 45.0
    NORMAL_DISCHARGE_RATE_PCT_PER_MIN = 0.4   # ~4% per 10 minutes at highway speed
    REGEN_PROBABILITY = 0.15                   # 15% chance of regen braking per reading

    def __init__(self, vehicle_id: str, anomaly_mode: bool = False):
        """
        Args:
            vehicle_id:   Fleet identifier, e.g. "VF-001".
            anomaly_mode: If True, this vehicle will exhibit degraded battery
                          discharge — used to verify the anomaly detection model
                          flags it correctly (Acceptance Criterion AC-002).
        """
        self.vehicle_id = vehicle_id
        self.anomaly_mode = anomaly_mode

        # Start each vehicle at a random battery level between 60% and 95%
        self.battery_pct = random.uniform(60.0, 95.0)
        self.state_of_charge_kwh = (self.battery_pct / 100.0) * self.RATED_CAPACITY_KWH

        # Base voltage varies slightly by battery level
        self.nominal_voltage_v = 350.0

        # Simulate a GPS position somewhere in the UK
        self.latitude = random.uniform(51.0, 53.5)
        self.longitude = random.uniform(-2.5, 0.5)

        self._consecutive_failures = 0

        logger.info(
            f"Vehicle initialised | id={vehicle_id} | "
            f"battery={self.battery_pct:.1f}% | anomaly_mode={anomaly_mode}"
        )

    def _update_state(self) -> None:
        """
        Advance the vehicle's physical state by one emission interval.

        Called before each emission to produce temporally coherent readings.
        """
        interval_minutes = settings.simulator_emit_interval_seconds / 60.0

        if self.anomaly_mode:
            # Anomalous vehicle discharges 3x faster — simulates a cell fault
            discharge_rate = self.NORMAL_DISCHARGE_RATE_PCT_PER_MIN * 3.0
        else:
            discharge_rate = self.NORMAL_DISCHARGE_RATE_PCT_PER_MIN

        # Gaussian noise simulates real sensor variation
        noise = random.gauss(0, 0.05)
        discharge_this_interval = (discharge_rate + noise) * interval_minutes

        self.battery_pct = max(5.0, self.battery_pct - discharge_this_interval)
        self.state_of_charge_kwh = (self.battery_pct / 100.0) * self.RATED_CAPACITY_KWH
        self.nominal_voltage_v = 320.0 + (self.battery_pct / 100.0) * 60.0

        # Vehicle is moving — drift GPS slightly
        self.latitude += random.uniform(-0.001, 0.001)
        self.longitude += random.uniform(-0.001, 0.001)

    def _build_event(self) -> dict:
        """
        Build a telemetry event dict from current vehicle state.

        Returns a dict matching the schema in ingestion/validator.py.
        """
        self._update_state()

        base_temp = 65.0 if self.anomaly_mode else 40.0
        motor_temp = base_temp + random.gauss(0, 2.0)

        regen_event = random.random() < self.REGEN_PROBABILITY
        current_a = (
            random.uniform(-50.0, -5.0) if regen_event
            else random.uniform(80.0, 140.0)
        )

        speed = 0.0 if self.battery_pct < 8.0 else random.uniform(20.0, 100.0)

        return {
            "vehicle_id": self.vehicle_id,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "battery_pct": round(self.battery_pct, 2),
            "state_of_charge_kwh": round(self.state_of_charge_kwh, 3),
            "voltage_v": round(self.nominal_voltage_v + random.gauss(0, 1.0), 2),
            "current_a": round(current_a, 2),
            "motor_temp_c": round(motor_temp, 2),
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "speed_kmh": round(speed, 1),
            "regen_braking_event": regen_event,
        }

    def _post_event(self, payload: str) -> int:
        """
        Blocking HTTP POST of one event. Runs in a thread pool.

        Separated into its own method so it can be called via
        asyncio.to_thread() without blocking the event loop.

        Returns the HTTP status code, or 0 on connection error.
        """
        try:
            response = requests.post(
                settings.ingestion_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                timeout=5.0,
            )
            return response.status_code
        except requests.exceptions.ConnectionError:
            return 0   # Ingestion service not reachable
        except requests.exceptions.Timeout:
            return 0

    async def run(self) -> None:
        """
        Main vehicle loop: emit telemetry events indefinitely.

        Uses asyncio.to_thread() to run the blocking HTTP POST in a thread
        pool, keeping the event loop free for other vehicles to run.
        """
        logger.info(f"Vehicle {self.vehicle_id} starting emission loop")

        while True:
            event = self._build_event()
            await self._emit_async(event)
            await asyncio.sleep(settings.simulator_emit_interval_seconds)

    async def _emit_async(self, event: dict) -> None:
        """
        Async wrapper around _post_event with retry and jitter logic.

        Retry strategy: exponential backoff with jitter.
        Formula: delay = base * 2^attempt * (1 + uniform(0, 0.3))
        This spreads reconnection attempts across time, preventing a
        thundering herd when the ingestion service restarts. (ADR-001-10)
        """
        payload = json.dumps(event)
        base_delay = 2.0
        max_retries = 5

        for attempt in range(max_retries):

            # Run the blocking HTTP call in a thread pool
            # asyncio.to_thread() is available in Python 3.9+
            status_code = await asyncio.to_thread(self._post_event, payload)

            if status_code == 202:
                self._consecutive_failures = 0
                logger.debug(
                    f"Event emitted | vehicle={self.vehicle_id} | "
                    f"battery={event['battery_pct']}%"
                )
                return

            elif status_code == 429:
                # Rate limited — back off for one full emission interval
                logger.warning(f"Rate limited | vehicle={self.vehicle_id}")
                await asyncio.sleep(4.0)
                return

            else:
                # Connection failure (0) or unexpected status
                self._consecutive_failures += 1

                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt) * (1 + random.uniform(0, 0.3))
                    logger.warning(
                        f"Emission failed | vehicle={self.vehicle_id} | "
                        f"status={status_code} | attempt={attempt + 1}/{max_retries} | "
                        f"retry_in={delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Emission failed after {max_retries} attempts | "
                        f"vehicle={self.vehicle_id} | event dropped"
                    )
