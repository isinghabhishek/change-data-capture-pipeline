"""
check_wal_lag.py — WAL replication slot lag monitor.

Queries pg_replication_slots to compute how far a given slot has fallen
behind the current WAL position. Emits a WARNING log entry when the lag
exceeds the configured threshold (default 10 GB) per Requirement 1.5.

Usage (CLI):
    python check_wal_lag.py --dsn "host=localhost dbname=mydb user=cdc_user password=secret" \
                            --slot-name debezium_slot \
                            --warn-threshold-gb 10
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass

import psycopg2
import psycopg2.extensions

logger = logging.getLogger(__name__)

# Default warning threshold: 10 GB expressed in bytes
_DEFAULT_WARN_THRESHOLD_BYTES: int = 10 * 1024 * 1024 * 1024


@dataclass
class WalLagStatus:
    """Result of a single WAL lag check.

    Attributes:
        slot_name:   Name of the replication slot that was checked.
        lag_bytes:   Lag in bytes between the current WAL position and the
                     slot's ``restart_lsn``.  ``-1`` indicates the slot was
                     not found.
        warning:     ``True`` when ``lag_bytes`` exceeds the configured
                     warning threshold.
    """

    slot_name: str
    lag_bytes: int
    warning: bool


def check_wal_lag(
    conn: psycopg2.extensions.connection,
    slot_name: str,
    warn_threshold_bytes: int = _DEFAULT_WARN_THRESHOLD_BYTES,
) -> WalLagStatus:
    """Check the WAL lag for a PostgreSQL replication slot.

    Queries ``pg_replication_slots`` for the slot's ``restart_lsn`` and
    computes the byte distance to the current WAL write position using
    ``pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)``.

    A ``WARNING`` log entry is emitted when the lag exceeds
    ``warn_threshold_bytes`` (Requirement 1.5).

    Args:
        conn:                 An open ``psycopg2`` connection to the source
                              PostgreSQL database.
        slot_name:            Name of the replication slot to inspect.
        warn_threshold_bytes: Byte threshold above which a warning is emitted.
                              Defaults to 10 GiB (10 × 1024³ bytes).

    Returns:
        A :class:`WalLagStatus` dataclass with the slot name, lag in bytes,
        and a boolean warning flag.

        If the slot does not exist, ``lag_bytes`` is set to ``-1`` and
        ``warning`` is ``False``.

    Raises:
        psycopg2.Error: On any unexpected database error (connection failure,
                        permission denied, etc.).
    """
    query = """
        SELECT
            pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
        FROM pg_replication_slots
        WHERE slot_name = %s
    """

    with conn.cursor() as cur:
        cur.execute(query, (slot_name,))
        row = cur.fetchone()

    if row is None:
        logger.warning(
            "Replication slot '%s' not found in pg_replication_slots.",
            slot_name,
        )
        return WalLagStatus(slot_name=slot_name, lag_bytes=-1, warning=False)

    lag_bytes: int = int(row[0])
    exceeds_threshold = lag_bytes > warn_threshold_bytes

    if exceeds_threshold:
        threshold_gb = warn_threshold_bytes / (1024 ** 3)
        lag_gb = lag_bytes / (1024 ** 3)
        logger.warning(
            "Replication slot '%s' WAL lag is %.2f GiB, which exceeds the "
            "%.2f GiB warning threshold. Unbounded WAL accumulation risk.",
            slot_name,
            lag_gb,
            threshold_gb,
        )
    else:
        lag_mb = lag_bytes / (1024 ** 2)
        logger.debug(
            "Replication slot '%s' WAL lag is %.2f MiB — within threshold.",
            slot_name,
            lag_mb,
        )

    return WalLagStatus(
        slot_name=slot_name,
        lag_bytes=lag_bytes,
        warning=exceeds_threshold,
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check WAL replication slot lag and emit a warning if it "
                    "exceeds the configured threshold.",
    )
    parser.add_argument(
        "--dsn",
        required=True,
        help='PostgreSQL DSN, e.g. "host=localhost dbname=mydb user=cdc_user password=secret"',
    )
    parser.add_argument(
        "--slot-name",
        required=True,
        help="Name of the replication slot to inspect (e.g. debezium_slot).",
    )
    parser.add_argument(
        "--warn-threshold-gb",
        type=float,
        default=10.0,
        metavar="GB",
        help="Warning threshold in GiB (default: 10).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Returns:
        Exit code: ``0`` if lag is within threshold, ``1`` if a warning was
        triggered, ``2`` if the slot was not found, ``3`` on database error.
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    warn_threshold_bytes = int(args.warn_threshold_gb * 1024 ** 3)

    try:
        conn = psycopg2.connect(args.dsn)
    except psycopg2.OperationalError as exc:
        logger.error("Failed to connect to PostgreSQL: %s", exc)
        return 3

    try:
        status = check_wal_lag(conn, args.slot_name, warn_threshold_bytes)
    except psycopg2.Error as exc:
        logger.error("Database error while checking WAL lag: %s", exc)
        return 3
    finally:
        conn.close()

    print(status)

    if status.lag_bytes == -1:
        return 2
    if status.warning:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
