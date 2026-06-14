"""
Extra coverage for aggregation_job.py — save_correlation function (lines 60-110).
"""
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flink_jobs.aggregation_job import save_correlation


class TestSaveCorrelation:

    def _conn(self, raise_on_execute=False):
        conn   = MagicMock()
        cursor = MagicMock()
        if raise_on_execute:
            cursor.execute.side_effect = Exception("DB write error")
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        return conn, cursor

    def _record(self, **overrides):
        base = {
            "condition_id":           "test_001",
            "question":               "Will it rain in London?",
            "LOCATION_NAME":          "London",
            "MARKET_TYPE":            "RAIN",
            "MARKET_STATUS":          "closed",
            "price":                  0.70,
            "winner":                 True,
            "closed":                 True,
            "end_date_iso":           "2026-05-19T00:00:00Z",
            "WEATHER_TYPE":           "historical",
            "OBSERVATION_DATE":       "2026-05-19",
            "ACTUAL_TEMP_C":          13.5,
            "ACTUAL_TEMP_F":          56.3,
            "ACTUAL_PRECIP_MM":       4.2,
            "ACTUAL_RAIN_MM":         4.2,
            "ACTUAL_WEATHER_CODE":    63,
            "ACTUAL_WIND_KMH":        18.0,
            "ACTUAL_OUTCOME":         1,
            "PREDICTION_ERROR":       0.30,
            "CORRELATION_METHOD":     "winner_known",
            "CORRELATION_LATENCY_SEC": 300,
            "POLL_TIMESTAMP":         "2026-05-19T12:00:00Z",
        }
        base.update(overrides)
        return base

    def test_executes_insert_on_valid_record(self):
        conn, cursor = self._conn()
        save_correlation(self._record(), conn)
        assert cursor.execute.called
        conn.commit.assert_called_once()

    def test_rollback_on_db_error(self):
        conn, cursor = self._conn(raise_on_execute=True)
        save_correlation(self._record(), conn)  # must not raise
        conn.rollback.assert_called_once()

    def test_handles_none_values_gracefully(self):
        conn, cursor = self._conn()
        record = self._record(
            ACTUAL_TEMP_C=None, ACTUAL_TEMP_F=None,
            ACTUAL_PRECIP_MM=None, winner=None,
        )
        save_correlation(record, conn)
        assert cursor.execute.called

    def test_handles_missing_optional_fields(self):
        conn, cursor = self._conn()
        minimal = {
            "condition_id": "min_001",
            "question": "Will it rain in London?",
            "LOCATION_NAME": "London",
        }
        save_correlation(minimal, conn)
        assert cursor.execute.called
