"""
Unit tests for scripts/check_wal_lag.py.

Uses unittest.mock to simulate psycopg2 connections — no live database needed.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

# Make the scripts directory importable
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from check_wal_lag import WalLagStatus, check_wal_lag, _DEFAULT_WARN_THRESHOLD_BYTES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn(fetchone_return):
    """Return a mock psycopg2 connection whose cursor returns *fetchone_return*."""
    cur = MagicMock()
    cur.fetchone.return_value = fetchone_return
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


# ---------------------------------------------------------------------------
# WalLagStatus dataclass
# ---------------------------------------------------------------------------

class TestWalLagStatus:
    def test_fields_accessible(self):
        status = WalLagStatus(slot_name="my_slot", lag_bytes=1024, warning=False)
        assert status.slot_name == "my_slot"
        assert status.lag_bytes == 1024
        assert status.warning is False

    def test_equality(self):
        a = WalLagStatus("s", 0, False)
        b = WalLagStatus("s", 0, False)
        assert a == b

    def test_default_threshold_is_10_gib(self):
        assert _DEFAULT_WARN_THRESHOLD_BYTES == 10 * 1024 * 1024 * 1024


# ---------------------------------------------------------------------------
# check_wal_lag — normal operation
# ---------------------------------------------------------------------------

class TestCheckWalLag:
    def test_returns_correct_lag_bytes(self):
        conn = _make_conn(fetchone_return=(500_000_000,))  # ~477 MiB
        status = check_wal_lag(conn, "debezium_slot")
        assert status.lag_bytes == 500_000_000
        assert status.slot_name == "debezium_slot"

    def test_no_warning_below_threshold(self):
        one_gb = 1 * 1024 ** 3
        conn = _make_conn(fetchone_return=(one_gb,))
        status = check_wal_lag(conn, "debezium_slot", warn_threshold_bytes=10 * 1024 ** 3)
        assert status.warning is False

    def test_warning_above_threshold(self):
        eleven_gb = 11 * 1024 ** 3
        conn = _make_conn(fetchone_return=(eleven_gb,))
        status = check_wal_lag(conn, "debezium_slot", warn_threshold_bytes=10 * 1024 ** 3)
        assert status.warning is True

    def test_warning_exactly_at_threshold_is_not_triggered(self):
        """Lag equal to threshold should NOT trigger a warning (strictly greater)."""
        threshold = 10 * 1024 ** 3
        conn = _make_conn(fetchone_return=(threshold,))
        status = check_wal_lag(conn, "debezium_slot", warn_threshold_bytes=threshold)
        assert status.warning is False

    def test_warning_one_byte_over_threshold(self):
        threshold = 10 * 1024 ** 3
        conn = _make_conn(fetchone_return=(threshold + 1,))
        status = check_wal_lag(conn, "debezium_slot", warn_threshold_bytes=threshold)
        assert status.warning is True

    def test_zero_lag(self):
        conn = _make_conn(fetchone_return=(0,))
        status = check_wal_lag(conn, "debezium_slot")
        assert status.lag_bytes == 0
        assert status.warning is False

    def test_custom_threshold(self):
        five_gb = 5 * 1024 ** 3
        conn = _make_conn(fetchone_return=(five_gb + 1,))
        status = check_wal_lag(conn, "debezium_slot", warn_threshold_bytes=five_gb)
        assert status.warning is True

    def test_warning_log_emitted(self, caplog):
        eleven_gb = 11 * 1024 ** 3
        conn = _make_conn(fetchone_return=(eleven_gb,))
        with caplog.at_level(logging.WARNING, logger="check_wal_lag"):
            check_wal_lag(conn, "debezium_slot", warn_threshold_bytes=10 * 1024 ** 3)
        assert any("debezium_slot" in r.message for r in caplog.records)
        assert any(r.levelno == logging.WARNING for r in caplog.records)

    def test_no_warning_log_below_threshold(self, caplog):
        one_gb = 1 * 1024 ** 3
        conn = _make_conn(fetchone_return=(one_gb,))
        with caplog.at_level(logging.WARNING, logger="check_wal_lag"):
            check_wal_lag(conn, "debezium_slot", warn_threshold_bytes=10 * 1024 ** 3)
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warning_records) == 0


# ---------------------------------------------------------------------------
# check_wal_lag — slot not found
# ---------------------------------------------------------------------------

class TestCheckWalLagSlotNotFound:
    def test_returns_lag_minus_one_when_slot_missing(self):
        conn = _make_conn(fetchone_return=None)
        status = check_wal_lag(conn, "nonexistent_slot")
        assert status.lag_bytes == -1
        assert status.slot_name == "nonexistent_slot"
        assert status.warning is False

    def test_warning_log_emitted_for_missing_slot(self, caplog):
        conn = _make_conn(fetchone_return=None)
        with caplog.at_level(logging.WARNING, logger="check_wal_lag"):
            check_wal_lag(conn, "nonexistent_slot")
        assert any("nonexistent_slot" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# check_wal_lag — SQL query correctness
# ---------------------------------------------------------------------------

class TestCheckWalLagQuery:
    def test_queries_pg_replication_slots(self):
        conn = _make_conn(fetchone_return=(0,))
        check_wal_lag(conn, "debezium_slot")
        cur = conn.cursor.return_value
        executed_sql: str = cur.execute.call_args[0][0]
        assert "pg_replication_slots" in executed_sql
        assert "pg_wal_lsn_diff" in executed_sql
        assert "pg_current_wal_lsn" in executed_sql
        assert "restart_lsn" in executed_sql

    def test_passes_slot_name_as_parameter(self):
        conn = _make_conn(fetchone_return=(0,))
        check_wal_lag(conn, "my_slot")
        cur = conn.cursor.return_value
        params = cur.execute.call_args[0][1]
        assert params == ("my_slot",)
