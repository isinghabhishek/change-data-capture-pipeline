"""
Unit tests for scripts/create_topics.py.

Tests cover:
- topic_name() naming convention (Requirement 4.1)
- create_cdc_topic() — correct partition count, replication factor, config
  forwarding, graceful handling of already-exists errors (Requirements 4.2, 4.3)
- create_all_topics() — all 8 topics created with the correct settings
- dry-run mode — no AdminClient calls made

No live Kafka instance is required; the AdminClient is fully mocked.
"""

from __future__ import annotations

import sys
import os
from unittest.mock import MagicMock, call, patch

import pytest
from confluent_kafka.error import KafkaException

# Make the scripts directory importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from create_topics import (
    _1_DAY_MS,
    _30_DAYS_MS,
    _7_DAYS_MS,
    _INFINITE_MS,
    create_all_topics,
    create_cdc_topic,
    topic_name,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_admin_client(fail_code: int | None = None) -> MagicMock:
    """Return a mocked AdminClient.

    Args:
        fail_code: If provided, the future returned by create_topics() raises
                   a KafkaException whose error code equals *fail_code*.
                   Pass 36 to simulate TOPIC_ALREADY_EXISTS.
                   Pass None (default) for a successful creation.
    """
    admin = MagicMock()

    if fail_code is None:
        # Successful future
        future = MagicMock()
        future.result.return_value = None
    else:
        # Failing future
        err = MagicMock()
        err.code.return_value = fail_code
        err.str.return_value = f"error_code_{fail_code}"
        exc = KafkaException(err)
        future = MagicMock()
        future.result.side_effect = exc

    admin.create_topics.return_value = {"test-topic": future}
    return admin


# ---------------------------------------------------------------------------
# topic_name()
# ---------------------------------------------------------------------------


class TestTopicName:
    """topic_name() must follow the <database>.<schema>.<table> convention."""

    def test_basic_pattern(self):
        assert topic_name("db", "public", "orders") == "db.public.orders"

    def test_customers(self):
        assert topic_name("db", "public", "customers") == "db.public.customers"

    def test_products(self):
        assert topic_name("db", "public", "products") == "db.public.products"

    def test_custom_database_name(self):
        assert topic_name("mydb", "public", "events") == "mydb.public.events"

    def test_custom_schema_name(self):
        assert topic_name("db", "finance", "transactions") == "db.finance.transactions"

    def test_all_components_present(self):
        result = topic_name("alpha", "beta", "gamma")
        parts = result.split(".")
        assert parts == ["alpha", "beta", "gamma"]

    def test_returns_string(self):
        assert isinstance(topic_name("db", "public", "orders"), str)

    def test_empty_strings_still_produce_dots(self):
        # Pure function — no validation; just confirms the format holds
        assert topic_name("", "", "") == ".."

    def test_numeric_components(self):
        assert topic_name("db1", "s2", "t3") == "db1.s2.t3"


# ---------------------------------------------------------------------------
# create_cdc_topic() — success path
# ---------------------------------------------------------------------------


class TestCreateCdcTopicSuccess:
    def test_returns_true_on_success(self):
        admin = _make_admin_client()
        admin.create_topics.return_value = {"my-topic": MagicMock(**{"result.return_value": None})}
        result = create_cdc_topic(admin, "my-topic", num_partitions=6, replication_factor=3, config={})
        assert result is True

    def test_calls_create_topics_once(self):
        admin = _make_admin_client()
        future = MagicMock()
        future.result.return_value = None
        admin.create_topics.return_value = {"my-topic": future}
        create_cdc_topic(admin, "my-topic", num_partitions=6, replication_factor=3, config={})
        admin.create_topics.assert_called_once()

    def test_new_topic_has_correct_partition_count(self):
        admin = _make_admin_client()
        future = MagicMock()
        future.result.return_value = None
        admin.create_topics.return_value = {"t": future}

        create_cdc_topic(admin, "t", num_partitions=6, replication_factor=3, config={})

        new_topic_arg = admin.create_topics.call_args[0][0][0]
        assert new_topic_arg.num_partitions == 6

    def test_new_topic_has_correct_replication_factor(self):
        admin = _make_admin_client()
        future = MagicMock()
        future.result.return_value = None
        admin.create_topics.return_value = {"t": future}

        create_cdc_topic(admin, "t", num_partitions=1, replication_factor=3, config={})

        new_topic_arg = admin.create_topics.call_args[0][0][0]
        assert new_topic_arg.replication_factor == 3

    def test_config_forwarded_to_new_topic(self):
        admin = _make_admin_client()
        future = MagicMock()
        future.result.return_value = None
        admin.create_topics.return_value = {"t": future}

        cfg = {"retention.ms": _7_DAYS_MS, "min.insync.replicas": "2"}
        create_cdc_topic(admin, "t", num_partitions=6, replication_factor=3, config=cfg)

        new_topic_arg = admin.create_topics.call_args[0][0][0]
        assert new_topic_arg.config == cfg


# ---------------------------------------------------------------------------
# create_cdc_topic() — already-exists (error code 36)
# ---------------------------------------------------------------------------


class TestCreateCdcTopicAlreadyExists:
    def test_returns_true_when_topic_already_exists(self):
        admin = _make_admin_client(fail_code=36)
        admin.create_topics.return_value = {"my-topic": MagicMock(**{"result.side_effect": _make_admin_client(fail_code=36).create_topics.return_value["test-topic"].result.side_effect})}

        # Rebuild properly
        err = MagicMock()
        err.code.return_value = 36
        err.str.return_value = "TOPIC_ALREADY_EXISTS"
        future = MagicMock()
        future.result.side_effect = KafkaException(err)
        admin.create_topics.return_value = {"my-topic": future}

        result = create_cdc_topic(admin, "my-topic", num_partitions=6, replication_factor=3, config={})
        assert result is True

    def test_does_not_raise_on_already_exists(self):
        err = MagicMock()
        err.code.return_value = 36
        err.str.return_value = "TOPIC_ALREADY_EXISTS"
        future = MagicMock()
        future.result.side_effect = KafkaException(err)

        admin = MagicMock()
        admin.create_topics.return_value = {"my-topic": future}

        # Should not raise
        create_cdc_topic(admin, "my-topic", num_partitions=6, replication_factor=3, config={})


# ---------------------------------------------------------------------------
# create_cdc_topic() — other errors
# ---------------------------------------------------------------------------


class TestCreateCdcTopicError:
    def test_returns_false_on_non_exists_error(self):
        err = MagicMock()
        err.code.return_value = 3  # some other error
        err.str.return_value = "UNKNOWN_TOPIC_OR_PART"
        future = MagicMock()
        future.result.side_effect = KafkaException(err)

        admin = MagicMock()
        admin.create_topics.return_value = {"t": future}

        result = create_cdc_topic(admin, "t", num_partitions=6, replication_factor=3, config={})
        assert result is False


# ---------------------------------------------------------------------------
# create_cdc_topic() — dry-run mode
# ---------------------------------------------------------------------------


class TestCreateCdcTopicDryRun:
    def test_dry_run_returns_true(self, capsys):
        admin = MagicMock()
        result = create_cdc_topic(
            admin,
            "my-topic",
            num_partitions=6,
            replication_factor=3,
            config={"retention.ms": _7_DAYS_MS},
            dry_run=True,
        )
        assert result is True

    def test_dry_run_does_not_call_admin(self, capsys):
        admin = MagicMock()
        create_cdc_topic(
            admin,
            "my-topic",
            num_partitions=6,
            replication_factor=3,
            config={},
            dry_run=True,
        )
        admin.create_topics.assert_not_called()

    def test_dry_run_prints_topic_info(self, capsys):
        admin = MagicMock()
        create_cdc_topic(
            admin,
            "my-topic",
            num_partitions=6,
            replication_factor=3,
            config={"retention.ms": _7_DAYS_MS},
            dry_run=True,
        )
        captured = capsys.readouterr()
        assert "my-topic" in captured.out
        assert "6" in captured.out


# ---------------------------------------------------------------------------
# create_all_topics() — topic inventory
# ---------------------------------------------------------------------------


class TestCreateAllTopics:
    """Verify that create_all_topics() provisions exactly the 8 required topics
    with the correct settings (design §2.4, Requirements 4.1, 4.2, 4.3)."""

    def _capture_new_topics(self, bootstrap_servers: str = "kafka:9092") -> list:
        """Run create_all_topics() with a patched AdminClient and return the
        list of NewTopic objects that were passed to create_topics()."""
        captured: list = []

        future = MagicMock()
        future.result.return_value = None

        def fake_create_topics(new_topics):
            captured.extend(new_topics)
            return {t.topic: future for t in new_topics}

        with patch("create_topics.AdminClient") as MockAdmin:
            instance = MockAdmin.return_value
            instance.create_topics.side_effect = fake_create_topics
            create_all_topics(bootstrap_servers)

        return captured

    # --- basic inventory ---

    def test_creates_exactly_eight_topics(self):
        topics = self._capture_new_topics()
        assert len(topics) == 8

    def test_all_expected_topic_names_present(self):
        topics = self._capture_new_topics()
        names = {t.topic for t in topics}
        expected = {
            "db.public.orders",
            "db.public.customers",
            "db.public.products",
            "cdc.dead-letter-queue",
            "cdc.heartbeat",
            "connect-offsets",
            "connect-configs",
            "connect-status",
        }
        assert names == expected

    # --- partition counts ---

    def test_cdc_data_topics_have_6_partitions(self):
        topics = self._capture_new_topics()
        data_topics = [
            t for t in topics
            if t.topic in {"db.public.orders", "db.public.customers", "db.public.products"}
        ]
        assert all(t.num_partitions == 6 for t in data_topics)

    def test_dlq_has_3_partitions(self):
        topics = self._capture_new_topics()
        dlq = next(t for t in topics if t.topic == "cdc.dead-letter-queue")
        assert dlq.num_partitions == 3

    def test_heartbeat_has_1_partition(self):
        topics = self._capture_new_topics()
        hb = next(t for t in topics if t.topic == "cdc.heartbeat")
        assert hb.num_partitions == 1

    def test_connect_offsets_has_25_partitions(self):
        topics = self._capture_new_topics()
        co = next(t for t in topics if t.topic == "connect-offsets")
        assert co.num_partitions == 25

    def test_connect_configs_has_1_partition(self):
        topics = self._capture_new_topics()
        cc = next(t for t in topics if t.topic == "connect-configs")
        assert cc.num_partitions == 1

    def test_connect_status_has_5_partitions(self):
        topics = self._capture_new_topics()
        cs = next(t for t in topics if t.topic == "connect-status")
        assert cs.num_partitions == 5

    # --- replication factor (all topics must be RF=3, Requirement 4.2) ---

    def test_all_topics_have_replication_factor_3(self):
        topics = self._capture_new_topics()
        assert all(t.replication_factor == 3 for t in topics), (
            [f"{t.topic}: RF={t.replication_factor}" for t in topics
             if t.replication_factor != 3]
        )

    # --- min ISR (all topics must have min.insync.replicas=2, Requirement 4.2) ---

    def test_all_topics_have_min_isr_2(self):
        topics = self._capture_new_topics()
        for t in topics:
            assert t.config.get("min.insync.replicas") == "2", (
                f"Topic {t.topic!r} missing min.insync.replicas=2"
            )

    # --- retention values (Requirement 4.3) ---

    def test_cdc_data_topics_have_7_day_retention(self):
        topics = self._capture_new_topics()
        data_topics = [
            t for t in topics
            if t.topic in {"db.public.orders", "db.public.customers", "db.public.products"}
        ]
        for t in data_topics:
            assert t.config.get("retention.ms") == _7_DAYS_MS, (
                f"Topic {t.topic!r} has retention {t.config.get('retention.ms')!r},"
                f" expected {_7_DAYS_MS!r}"
            )

    def test_dlq_has_30_day_retention(self):
        topics = self._capture_new_topics()
        dlq = next(t for t in topics if t.topic == "cdc.dead-letter-queue")
        assert dlq.config.get("retention.ms") == _30_DAYS_MS

    def test_heartbeat_has_1_day_retention(self):
        topics = self._capture_new_topics()
        hb = next(t for t in topics if t.topic == "cdc.heartbeat")
        assert hb.config.get("retention.ms") == _1_DAY_MS

    def test_connect_topics_have_infinite_retention(self):
        topics = self._capture_new_topics()
        connect_topics = [
            t for t in topics
            if t.topic in {"connect-offsets", "connect-configs", "connect-status"}
        ]
        for t in connect_topics:
            assert t.config.get("retention.ms") == _INFINITE_MS, (
                f"Topic {t.topic!r} should have infinite retention (-1)"
            )

    # --- retention constant values ---

    def test_7_day_retention_constant_correct(self):
        assert _7_DAYS_MS == "604800000"

    def test_30_day_retention_constant_correct(self):
        assert _30_DAYS_MS == "2592000000"

    def test_1_day_retention_constant_correct(self):
        assert _1_DAY_MS == "86400000"

    def test_infinite_retention_constant_is_minus_1(self):
        assert _INFINITE_MS == "-1"

    # --- AdminClient is constructed with correct bootstrap servers ---

    def test_admin_client_uses_bootstrap_servers(self):
        with patch("create_topics.AdminClient") as MockAdmin:
            instance = MockAdmin.return_value
            future = MagicMock()
            future.result.return_value = None
            instance.create_topics.return_value = {"t": future}
            instance.create_topics.side_effect = lambda topics: {
                t.topic: future for t in topics
            }
            create_all_topics("myhost:9092")
        MockAdmin.assert_called_once_with({"bootstrap.servers": "myhost:9092"})


# ---------------------------------------------------------------------------
# create_all_topics() — dry-run mode
# ---------------------------------------------------------------------------


class TestCreateAllTopicsDryRun:
    def test_dry_run_does_not_instantiate_admin_client(self, capsys):
        with patch("create_topics.AdminClient") as MockAdmin:
            create_all_topics("kafka:9092", dry_run=True)
        MockAdmin.assert_not_called()

    def test_dry_run_prints_all_8_topics(self, capsys):
        create_all_topics("kafka:9092", dry_run=True)
        captured = capsys.readouterr()
        expected_topics = [
            "db.public.orders",
            "db.public.customers",
            "db.public.products",
            "cdc.dead-letter-queue",
            "cdc.heartbeat",
            "connect-offsets",
            "connect-configs",
            "connect-status",
        ]
        for name in expected_topics:
            assert name in captured.out, f"Expected {name!r} in dry-run output"
