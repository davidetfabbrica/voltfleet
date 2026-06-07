"""
simulator/fleet.py

Runs the full simulated fleet of vehicles concurrently.

All vehicles run as async coroutines in a single event loop. asyncio.gather()
starts all of them at once — they interleave cooperatively, each yielding
control when waiting for HTTP responses or sleeping between emissions.

To start the simulator (run AFTER the ingestion service):
    python -m simulator.fleet
"""

import asyncio
import logging

from config.settings import settings, configure_logging

configure_logging()
logger = logging.getLogger("voltfleet.simulator.fleet")

from simulator.vehicle import Vehicle


def build_fleet() -> list:
    """
    Create the full fleet of simulated vehicles.

    VF-001 is always the anomalous vehicle for acceptance criterion AC-002.
    All other vehicles operate normally.
    """
    vehicles = []

    for i in range(1, settings.simulator_vehicle_count + 1):
        vehicle_id = f"VF-{i:03d}"
        anomaly_mode = (vehicle_id == "VF-001")
        vehicles.append(Vehicle(vehicle_id=vehicle_id, anomaly_mode=anomaly_mode))

    logger.info(
        f"Fleet built | total={len(vehicles)} | anomaly_vehicle=VF-001"
    )
    return vehicles


async def run_fleet() -> None:
    """
    Start all vehicle coroutines concurrently and run until interrupted.
    """
    vehicles = build_fleet()

    logger.info(f"Launching {len(vehicles)} vehicle coroutines")

    # return_exceptions=True: if one vehicle coroutine crashes, the others
    # continue running rather than the whole fleet stopping.
    await asyncio.gather(
        *[vehicle.run() for vehicle in vehicles],
        return_exceptions=True,
    )


if __name__ == "__main__":
    logger.info("VoltFleet simulator starting...")
    try:
        asyncio.run(run_fleet())
    except KeyboardInterrupt:
        logger.info("Simulator stopped.")
