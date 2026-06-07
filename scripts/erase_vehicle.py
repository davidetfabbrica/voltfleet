"""
scripts/erase_vehicle.py

GDPR / APPI right-to-erasure script.

Implements PRD requirement PRIV-EU-005 and PRIV-JP-001: the right for a
data subject (in this case, a driver assigned to a vehicle) to have their
personal data removed from the system.

What this script does:
  - Nulls out vehicle_id in Bronze, Silver, Gold, and predictions tables
  - Deletes the vehicle_metadata record
  - Writes an audit entry to the erasure_log table
  - Does NOT delete Bronze rows — Bronze is append-only by design
    (ADR-001-05). Instead we overwrite the identifying field with a
    null token. The sensor readings remain for statistical purposes
    but cannot be linked to an individual.

Why we null rather than delete from Bronze:
  Deleting from Bronze would break the lineage between Bronze and Silver
  (Silver records have a bronze_id foreign key). It would also undermine
  the audit trail. Nulling the vehicle_id satisfies the erasure obligation
  while preserving data integrity. This approach is explicitly documented
  in PRD PRIV-EU-005.

Usage:
    python scripts/erase_vehicle.py VF-001
    python scripts/erase_vehicle.py VF-001 --requested-by "DPO"
"""

import sqlite3
import sys
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings, configure_logging

configure_logging()
logger = logging.getLogger("voltfleet.scripts.erase_vehicle")


def erase_vehicle(vehicle_id: str, requested_by: str = "system") -> dict:
    """
    Erase all personal data associated with a vehicle_id.

    Args:
        vehicle_id:    The vehicle whose data should be erased.
        requested_by:  Who requested the erasure (for the audit log).

    Returns:
        A summary dict of how many records were affected in each table.
    """

    # ── Confirm vehicle exists before proceeding ──────────────────────────────
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    existing = conn.execute(
        "SELECT vehicle_id FROM vehicle_metadata WHERE vehicle_id = ?",
        (vehicle_id,)
    ).fetchone()

    if not existing:
        logger.error(f"Vehicle not found: {vehicle_id}")
        conn.close()
        return {"error": f"Vehicle {vehicle_id} not found"}

    logger.info(f"Starting erasure | vehicle_id={vehicle_id} | requested_by={requested_by}")

    summary = {
        "vehicle_id":        vehicle_id,
        "requested_by":      requested_by,
        "bronze_count":      0,
        "silver_count":      0,
        "gold_count":        0,
        "prediction_count":  0,
        "erased_at":         datetime.now(timezone.utc).isoformat(),
    }

    try:
        # ── All changes run in a single transaction ───────────────────────────
        # If anything fails, nothing is committed — the data stays intact.
        # This is the ACID atomicity guarantee applied to erasure.

        # Bronze: null out vehicle_id and GPS (personal data fields)
        # Raw payload also contains vehicle_id — overwrite it too
        result = conn.execute("""
            UPDATE bronze_telemetry
            SET vehicle_id = '[ERASED]',
                latitude   = NULL,
                longitude  = NULL,
                raw_payload = '[ERASED]'
            WHERE vehicle_id = ?
        """, (vehicle_id,))
        summary["bronze_count"] = result.rowcount

        # Silver: null out vehicle_id and GPS
        result = conn.execute("""
            UPDATE silver_telemetry
            SET vehicle_id = '[ERASED]',
                latitude   = NULL,
                longitude  = NULL
            WHERE vehicle_id = ?
        """, (vehicle_id,))
        summary["silver_count"] = result.rowcount

        # Gold: null out vehicle_id
        # Gold records contain no GPS — only aggregated metrics
        result = conn.execute("""
            UPDATE gold_vehicle_health
            SET vehicle_id = '[ERASED]'
            WHERE vehicle_id = ?
        """, (vehicle_id,))
        summary["gold_count"] = result.rowcount

        # Predictions: null out vehicle_id
        result = conn.execute("""
            UPDATE predictions
            SET vehicle_id = '[ERASED]'
            WHERE vehicle_id = ?
        """, (vehicle_id,))
        summary["prediction_count"] = result.rowcount

        # Alerts: null out vehicle_id
        conn.execute("""
            UPDATE alerts
            SET vehicle_id = '[ERASED]'
            WHERE vehicle_id = ?
        """, (vehicle_id,))

        # Vehicle metadata: delete the record entirely
        # This removes the link between vehicle_id and the physical vehicle/driver
        conn.execute(
            "DELETE FROM vehicle_metadata WHERE vehicle_id = ?",
            (vehicle_id,)
        )

        # ── Write erasure audit log (PRIV-EU-005, PRIV-JP-001) ────────────────
        # The erasure event itself is logged. Storing the vehicle_id here is
        # intentional — this is the record of what was erased, not personal
        # data in the operational sense. The ICO guidance (UK GDPR) and APPI
        # both accept this approach for accountability purposes.
        conn.execute("""
            INSERT INTO erasure_log
                (vehicle_id, bronze_count, silver_count, gold_count,
                 prediction_count, requested_by, erased_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            vehicle_id,
            summary["bronze_count"],
            summary["silver_count"],
            summary["gold_count"],
            summary["prediction_count"],
            requested_by,
            summary["erased_at"],
        ))

        conn.commit()

        logger.info(
            f"Erasure complete | vehicle_id={vehicle_id} | "
            f"bronze={summary['bronze_count']} | "
            f"silver={summary['silver_count']} | "
            f"gold={summary['gold_count']} | "
            f"predictions={summary['prediction_count']}"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"Erasure failed — rolled back | vehicle_id={vehicle_id} | error={e}")
        raise

    finally:
        conn.close()

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Erase all personal data for a vehicle (GDPR/APPI right to erasure)"
    )
    parser.add_argument(
        "vehicle_id",
        help="Vehicle ID to erase, e.g. VF-001"
    )
    parser.add_argument(
        "--requested-by",
        default="DPO",
        help="Name or role of the person requesting erasure (default: DPO)"
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip the confirmation prompt (use in automated workflows)"
    )
    args = parser.parse_args()

    # ── Confirmation prompt ───────────────────────────────────────────────────
    # Erasure is irreversible. Require explicit confirmation unless --confirm
    # is passed (for use in automated DSAR workflows).
    if not args.confirm:
        print(f"\nThis will erase all personal data for vehicle: {args.vehicle_id}")
        print("This action cannot be undone.")
        response = input("Type the vehicle ID to confirm: ").strip()
        if response != args.vehicle_id:
            print("Confirmation did not match. Erasure cancelled.")
            sys.exit(1)

    summary = erase_vehicle(args.vehicle_id, requested_by=args.requested_by)

    if "error" in summary:
        print(f"Error: {summary['error']}")
        sys.exit(1)

    print(f"\nErasure complete for {args.vehicle_id}:")
    print(f"  Bronze records nulled:     {summary['bronze_count']}")
    print(f"  Silver records nulled:     {summary['silver_count']}")
    print(f"  Gold records nulled:       {summary['gold_count']}")
    print(f"  Prediction records nulled: {summary['prediction_count']}")
    print(f"  Audit log entry written:   yes")
    print(f"  Erased at:                 {summary['erased_at']}")


if __name__ == "__main__":
    main()
