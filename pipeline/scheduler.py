"""
pipeline/scheduler.py

Pipeline scheduler — runs Bronze-to-Silver, Silver-to-Gold, and the
anomaly detection predictor on a configurable interval (default: 30s).

Each cycle:
  1. Bronze-to-Silver
  2. Silver-to-Gold
  3. Anomaly detection + alerting
  4. Sleep for pipeline_interval_seconds
  5. Repeat

To run:
    python -m pipeline.scheduler
"""

import time
import logging
from datetime import datetime, timezone

from config.settings import settings, configure_logging
from pipeline import bronze_to_silver, silver_to_gold
from models import predictor

configure_logging()
logger = logging.getLogger("voltfleet.pipeline.scheduler")


def run_forever() -> None:
    """Main scheduler loop. Runs until interrupted with Ctrl+C."""
    logger.info(
        f"Pipeline scheduler starting | "
        f"interval={settings.pipeline_interval_seconds}s"
    )

    cycle = 0

    while True:
        cycle += 1
        cycle_start = datetime.now(timezone.utc)
        logger.info(f"Pipeline cycle {cycle} starting | {cycle_start.strftime('%H:%M:%S')}")

        # ── Stage 1: Bronze to Silver ─────────────────────────────────────────
        try:
            b2s_summary = bronze_to_silver.run()

            if b2s_summary["circuit_open"]:
                logger.error(
                    f"Cycle {cycle}: circuit breaker OPEN — skipping remaining stages"
                )
                _sleep_until_next_cycle(cycle_start)
                continue

            logger.info(
                f"Cycle {cycle} | Bronze-to-Silver: "
                f"read={b2s_summary['records_read']} | "
                f"written={b2s_summary['records_written']} | "
                f"failed={b2s_summary['records_failed']}"
            )

        except Exception as e:
            logger.error(f"Cycle {cycle}: Bronze-to-Silver error: {e}", exc_info=True)
            _sleep_until_next_cycle(cycle_start)
            continue

        # ── Stage 2: Silver to Gold ───────────────────────────────────────────
        try:
            s2g_summary = silver_to_gold.run()

            logger.info(
                f"Cycle {cycle} | Silver-to-Gold: "
                f"vehicles={s2g_summary['vehicles_processed']} | "
                f"window={s2g_summary['window_start'][11:19]}→"
                f"{s2g_summary['window_end'][11:19]}"
            )

        except Exception as e:
            logger.error(f"Cycle {cycle}: Silver-to-Gold error: {e}", exc_info=True)

        # ── Stage 3: Anomaly detection ────────────────────────────────────────
        try:
            pred_summary = predictor.run()

            logger.info(
                f"Cycle {cycle} | Predictor: "
                f"predictions={pred_summary['predictions_written']} | "
                f"anomalies={pred_summary['anomalies_detected']} | "
                f"alerts={pred_summary['alerts_raised']}"
            )

        except Exception as e:
            logger.error(f"Cycle {cycle}: Predictor error: {e}", exc_info=True)

        # ── Sleep until next cycle ────────────────────────────────────────────
        _sleep_until_next_cycle(cycle_start)


def _sleep_until_next_cycle(cycle_start: datetime) -> None:
    """
    Sleep for whatever time remains in the current interval.

    Keeps the cycle on a regular clock rather than drifting later each run.
    """
    from datetime import timezone
    elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
    sleep_for = settings.pipeline_interval_seconds - elapsed

    if sleep_for <= 0:
        logger.warning(
            f"Pipeline cycle took {elapsed:.1f}s — starting next cycle immediately"
        )
    else:
        logger.debug(f"Sleeping {sleep_for:.1f}s until next cycle")
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        run_forever()
    except KeyboardInterrupt:
        logger.info("Pipeline scheduler stopped.")
