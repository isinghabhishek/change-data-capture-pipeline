"""
create_topics.py — Kafka topic provisioning for the CDC pipeline.

Creates all required Kafka topics with the correct partition count,
replication factor, min-ISR, and retention settings per design §2.4.

Usage (CLI):
    python create_topics.py --bootstrap-servers kafka:9092
    python create_topics.py --bootstrap-servers kafka:9092 --dry-run

Requirements: 4.1, 4.2, 4.3
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retention constants (milliseconds)
# ---------------------------------------------------------------------------
_7_DAYS_MS: str = "604800000"     # 7 × 24 × 3600 × 1000
_30_DAYS_MS: str = "2592000000"   # 30 × 24 × 3600 × 1000
_1_DAY_MS: str = "86400000"       # 1 × 24 × 3600 × 1000
_INFINITE_MS: str = "-1"          # No retention limit

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def topic_name(database: str, schema: str, table: str) -> str:
    """Construct a Debezium-style Kafka topic name.

    Args:
        database: The logical database / server name (e.g. ``"db"``).
        schema:   The schema (e.g. ``"public"``).
        table:    The table name (e.g. ``"orders"``).

    Returns:
        A dotted string ``"{database}.{schema}.{table}"`` as required by
        Requirement 4.1 and design §2.4.

    Example::

        >>> topic_name("db", "public", "orders")
        'db.public.orders'
    """
    return f"{database}.{schema}.{table}"


def create_cdc_topic(
    admin_client: AdminClient,
    name: str,
    num_partitions: int,
    replication_factor: int,
    config: dict[str, str],
    *,
    dry_run: bool = False,
) -> bool:
    """Create a single Kafka topic using the AdminClient.

    If the topic already exists the error is logged and ``True`` is returned
    (idempotent — no exception raised).

    Args:
        admin_client:       A configured :class:`confluent_kafka.admin.AdminClient`.
        name:               Topic name.
        num_partitions:     Number of partitions.
        replication_factor: Replication factor.
        config:             Per-topic configuration overrides (e.g.
                            ``{"retention.ms": "604800000",
                               "min.insync.replicas": "2"}``).
        dry_run:            When ``True``, print the intended configuration
                            without making any Kafka API calls.

    Returns:
        ``True`` if the topic was created (or already exists); ``False`` if
        creation failed for any other reason.
    """
    if dry_run:
        print(
            f"[DRY-RUN] Topic: {name!r}  partitions={num_partitions}"
            f"  replication_factor={replication_factor}  config={config}"
        )
        return True

    new_topic = NewTopic(
        topic=name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        config=config,
    )

    futures: dict[str, Any] = admin_client.create_topics([new_topic])

    for topic, future in futures.items():
        try:
            future.result()  # blocks until done
            logger.info("Topic %r created successfully.", topic)
        except KafkaException as exc:
            err = exc.args[0]
            # TOPIC_ALREADY_EXISTS == error code 36
            if err.code() == 36:
                logger.info(
                    "Topic %r already exists — skipping creation.", topic
                )
            else:
                logger.error(
                    "Failed to create topic %r: %s (code %s)",
                    topic,
                    err.str(),
                    err.code(),
                )
                return False

    return True


def create_all_topics(
    bootstrap_servers: str,
    *,
    dry_run: bool = False,
) -> None:
    """Orchestrate creation of all CDC-pipeline Kafka topics.

    Creates the following topics (design §2.4):

    * ``db.public.orders``      — 6 partitions, RF 3, min ISR 2, 7-day retention
    * ``db.public.customers``   — 6 partitions, RF 3, min ISR 2, 7-day retention
    * ``db.public.products``    — 6 partitions, RF 3, min ISR 2, 7-day retention
    * ``cdc.dead-letter-queue`` — 3 partitions, RF 3, min ISR 2, 30-day retention
    * ``cdc.heartbeat``         — 1 partition,  RF 3, min ISR 2, 1-day retention
    * ``connect-offsets``       — 25 partitions, RF 3, min ISR 2, infinite retention
    * ``connect-configs``       — 1 partition,  RF 3, min ISR 2, infinite retention
    * ``connect-status``        — 5 partitions, RF 3, min ISR 2, infinite retention

    Args:
        bootstrap_servers: Kafka bootstrap servers string (e.g. ``"kafka:9092"``).
        dry_run:           When ``True``, print topic definitions without
                           creating them.
    """
    # Base config shared by all CDC data topics (Requirements 4.2, 4.3)
    _cdc_data_config = {
        "retention.ms": _7_DAYS_MS,
        "min.insync.replicas": "2",
    }

    # Topic definitions: (name, num_partitions, replication_factor, config)
    topics: list[tuple[str, int, int, dict[str, str]]] = [
        # Per-table CDC topics — 6 partitions, 7-day retention
        (topic_name("db", "public", "orders"),    6, 3, _cdc_data_config),
        (topic_name("db", "public", "customers"), 6, 3, _cdc_data_config),
        (topic_name("db", "public", "products"),  6, 3, _cdc_data_config),
        # Dead-letter queue — 3 partitions, 30-day retention
        (
            "cdc.dead-letter-queue",
            3,
            3,
            {"retention.ms": _30_DAYS_MS, "min.insync.replicas": "2"},
        ),
        # Heartbeat — 1 partition, 1-day retention
        (
            "cdc.heartbeat",
            1,
            3,
            {"retention.ms": _1_DAY_MS, "min.insync.replicas": "2"},
        ),
        # Kafka Connect internal topics — infinite retention
        (
            "connect-offsets",
            25,
            3,
            {"retention.ms": _INFINITE_MS, "min.insync.replicas": "2"},
        ),
        (
            "connect-configs",
            1,
            3,
            {"retention.ms": _INFINITE_MS, "min.insync.replicas": "2"},
        ),
        (
            "connect-status",
            5,
            3,
            {"retention.ms": _INFINITE_MS, "min.insync.replicas": "2"},
        ),
    ]

    if dry_run:
        admin_client = None  # not needed in dry-run mode
    else:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    for name, partitions, rf, cfg in topics:
        create_cdc_topic(
            admin_client,  # type: ignore[arg-type]
            name,
            num_partitions=partitions,
            replication_factor=rf,
            config=cfg,
            dry_run=dry_run,
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create all CDC-pipeline Kafka topics.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:9092",
        metavar="HOST:PORT",
        help="Kafka bootstrap servers (default: kafka:9092).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print topic configurations without creating them.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Returns:
        Exit code: ``0`` on success, ``1`` on error.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        create_all_topics(
            args.bootstrap_servers,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error during topic creation: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
