#!/usr/bin/env python3
"""
DLQ Replay Utility — Re-reads events from the Dead Letter Queue and re-submits them.

Feature: cdc-pipeline, Property 11: DLQ replay delivers events to original topic
Validates: Requirement 8.5

This utility reads all messages from the cdc.dead-letter-queue Kafka topic and
re-publishes the raw payloads to the original topic after the root cause is
resolved. It supports filtering by error type and time range, and a dry-run mode.

Usage:
    python dlq_replay.py [--dlq-topic <topic>] [--kafka-bootstrap <host:port>]
                         [--filter-error-type <type>] [--since <iso8601>]
                         [--dry-run]

Options:
    --dlq-topic           DLQ topic name (default: cdc.dead-letter-queue)
    --kafka-bootstrap     Kafka bootstrap servers (default: localhost:9092)
    --filter-error-type   Filter by error type: DESERIALIZATION_FAILURE,
                          SCHEMA_INCOMPATIBLE, TYPE_MISMATCH, PROCESSING_ERROR
    --since               Only replay events since this ISO 8601 timestamp
    --dry-run             Show what would be replayed without publishing

Exit codes:
    0  Success — all events replayed successfully
    1  Failure — one or more events failed to replay

Requirements:
    confluent-kafka >= 2.3.0
"""

import argparse
import json
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Consumer, Producer, KafkaException


# Error types as defined in design §5.4
class ErrorType:
    DESERIALIZATION_FAILURE = "DESERIALIZATION_FAILURE"
    SCHEMA_INCOMPATIBLE = "SCHEMA_INCOMPATIBLE"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    PROCESSING_ERROR = "PROCESSING_ERROR"


@dataclass
class ReplayResult:
    """Result of a replay operation."""
    total_read: int
    total_replayed: int
    total_failed: int
    failed_events: list[tuple[str, str]]  # (original_topic, error_message)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Replay events from the Dead Letter Queue"
    )
    parser.add_argument(
        "--dlq-topic",
        default="cdc.dead-letter-queue",
        help="DLQ topic name (default: cdc.dead-letter-queue)",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--filter-error-type",
        choices=[
            ErrorType.DESERIALIZATION_FAILURE,
            ErrorType.SCHEMA_INCOMPATIBLE,
            ErrorType.TYPE_MISMATCH,
            ErrorType.PROCESSING_ERROR,
        ],
        help="Filter by error type",
    )
    parser.add_argument(
        "--since",
        type=datetime.fromisoformat,
        help="Only replay events since this ISO 8601 timestamp",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be replayed without publishing",
    )
    return parser.parse_args()


def load_dlq_envelope(raw_bytes: bytes) -> dict:
    """
    Parse a DLQEnvelope from raw bytes.

    The DLQEnvelope is serialized as JSON with the following structure:
    {
        "original_topic": "db.public.orders",
        "original_partition": 0,
        "original_offset": 12345,
        "error_type": "TYPE_MISMATCH",
        "error_message": "column 'amount' type change from INT to VARCHAR is not backward-compatible",
        "error_timestamp_utc": 1729345200000,
        "raw_payload": "base64-encoded-string"
    }
    """
    try:
        data = json.loads(raw_bytes.decode("utf-8"))
        # Decode base64 raw_payload back to bytes
        data["raw_payload"] = data.get("raw_payload", "").encode("utf-8")
        return data
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in DLQ envelope: {e}")


def replay_dlq(
    dlq_topic: str,
    kafka_bootstrap: str,
    filter_error_type: Optional[str] = None,
    since: Optional[datetime] = None,
    dry_run: bool = False,
) -> ReplayResult:
    """
    Re-read events from the Dead Letter Queue and re-submit them.

    Args:
        dlq_topic: Name of the DLQ topic
        kafka_bootstrap: Kafka bootstrap servers (comma-separated)
        filter_error_type: Optional error type filter
        since: Optional minimum timestamp for events to replay
        dry_run: If True, don't actually publish events

    Returns:
        ReplayResult with statistics

    Requirements: 8.5
    """
    result = ReplayResult(
        total_read=0,
        total_replayed=0,
        total_failed=0,
        failed_events=[],
    )

    # Configure DLQ consumer
    consumer_config = {
        "bootstrap.servers": kafka_bootstrap,
        "group.id": "dlq-replay-utility",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    # Configure producer for re-publishing
    producer_config = {
        "bootstrap.servers": kafka_bootstrap,
    }

    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)

    try:
        # Subscribe to DLQ topic
        consumer.subscribe([dlq_topic])
        print(f"Subscribed to DLQ topic: {dlq_topic}")

        # Read all messages from DLQ
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())

            result.total_read += 1
            raw_bytes = msg.value()

            try:
                # Parse the DLQ envelope
                envelope = load_dlq_envelope(raw_bytes)

                # Apply filters
                if filter_error_type and envelope.get("error_type") != filter_error_type:
                    print(f"[SKIP] Event {msg.offset()}: error_type mismatch")
                    continue

                if since:
                    event_time = datetime.fromtimestamp(
                        envelope.get("error_timestamp_utc", 0) / 1000, tz=timezone.utc
                    )
                    if event_time < since:
                        print(f"[SKIP] Event {msg.offset()}: before --since timestamp")
                        continue

                # Extract original topic and raw payload
                original_topic = envelope.get("original_topic")
                original_partition = envelope.get("original_partition", 0)
                original_offset = envelope.get("original_offset", 0)
                error_type = envelope.get("error_type")
                error_message = envelope.get("error_message", "")

                print(
                    f"[READ] Event {msg.offset()}: {error_type} -> {original_topic} "
                    f"(offset {original_offset})"
                )

                if dry_run:
                    # Don't actually publish, just log
                    print(f"  [DRY-RUN] Would publish to {original_topic}")
                    result.total_replayed += 1
                else:
                    # Re-publish to original topic
                    raw_payload = envelope.get("raw_payload", b"")
                    producer.produce(
                        topic=original_topic,
                        value=raw_payload,
                        partition=original_partition,
                    )
                    producer.flush()
                    result.total_replayed += 1

                # Commit offset
                consumer.commit(msg)

            except Exception as e:
                # Log failure and continue
                error_msg = f"Failed to replay event {msg.offset()}: {e}"
                print(f"[FAIL] {error_msg}")
                result.total_failed += 1
                result.failed_events.append((original_topic or "unknown", str(e)))
                traceback.print_exc()

    finally:
        # Clean up
        consumer.close()
        producer.flush()

    return result


def main():
    """Main entry point."""
    args = parse_args()

    print("=" * 60)
    print("DLQ Replay Utility")
    print("=" * 60)
    print(f"DLQ Topic: {args.dlq_topic}")
    print(f"Kafka Bootstrap: {args.kafka_bootstrap}")
    if args.filter_error_type:
        print(f"Filter Error Type: {args.filter_error_type}")
    if args.since:
        print(f"Since: {args.since}")
    if args.dry_run:
        print("DRY-RUN mode enabled")

    result = replay_dlq(
        dlq_topic=args.dlq_topic,
        kafka_bootstrap=args.kafka_bootstrap,
        filter_error_type=args.filter_error_type,
        since=args.since,
        dry_run=args.dry_run,
    )

    print("=" * 60)
    print("Replay Summary")
    print("=" * 60)
    print(f"Total read:      {result.total_read}")
    print(f"Total replayed:  {result.total_replayed}")
    print(f"Total failed:    {result.total_failed}")

    if result.failed_events:
        print("\nFailed events:")
        for topic, error in result.failed_events:
            print(f"  - {topic}: {error}")

    # Exit with non-zero if any failures
    if result.total_failed > 0:
        print(f"\nERROR: {result.total_failed} event(s) failed to replay")
        sys.exit(1)

    print("\nSUCCESS: All events replayed successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()
