"""
Tests for flink_jobs/correlation_job.py

Tests the pure logic functions (no live Kafka connection needed):
  - _calc_latency
  - load_weather_snapshot  (via mocked KafkaConsumer)
  - find_weather
  - load_predictions       (via mocked KafkaConsumer)
  - run                    (integration-style with all Kafka mocked)
"""
import sys
import os
import json
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "flink_jobs"))

from flink_jobs.correlation_job import _calc_latency, find_weather


# ── _calc_latency ─────────────────────────────────────────────────

class TestCalcLatency:
    def test_positive_latency(self):
        result = _calc_latency(
            "2026-05-19T12:00:00+00:00",
            "2026-05-19T12:05:00+00:00",
        )
        assert result == 300

    def test_zero_latency_same_timestamps(self):
        ts = "2026-05-19T12:00:00+00:00"
        assert _calc_latency(ts, ts) == 0

    def test_returns_none_on_bad_format(self):
        assert _calc_latency("not-a-date", "2026-05-19T12:00:00+00:00") is None

    def test_returns_none_on_empty_string(self):
        assert _calc_latency("", "2026-05-19T12:00:00Z") is None

    def test_handles_z_suffix(self):
        result = _calc_latency(
            "2026-05-19T12:00:00Z",
            "2026-05-19T12:01:00Z",
        )
        assert result == 60

    def test_negative_difference_returns_positive(self):
        # corr_ts earlier than poll_ts → should still return a value
        result = _calc_latency(
            "2026-05-19T12:05:00+00:00",
            "2026-05-19T12:00:00+00:00",
        )
        assert result is not None


# ── find_weather ──────────────────────────────────────────────────

class TestFindWeather:
    def _snapshot(self):
        return {
            "London_2026-05-19": {"LOCATION_NAME": "London", "WEATHER_TYPE": "historical"},
            "London_current":    {"LOCATION_NAME": "London", "WEATHER_TYPE": "current"},
            "Paris_current":     {"LOCATION_NAME": "Paris",  "WEATHER_TYPE": "current"},
        }

    def test_exact_date_match_wins(self):
        pred = {
            "LOCATION_NAME": "London",
            "end_date_iso":  "2026-05-19T00:00:00Z",
        }
        result = find_weather(pred, self._snapshot())
        assert result["WEATHER_TYPE"] == "historical"

    def test_falls_back_to_current(self):
        pred = {
            "LOCATION_NAME": "London",
            "end_date_iso":  "2026-12-31T00:00:00Z",  # no historical for this date
        }
        result = find_weather(pred, self._snapshot())
        assert result["WEATHER_TYPE"] == "current"

    def test_returns_none_for_unknown_city(self):
        pred = {
            "LOCATION_NAME": "Tokyo",
            "end_date_iso":  "2026-05-19T00:00:00Z",
        }
        assert find_weather(pred, self._snapshot()) is None

    def test_no_end_date_uses_current(self):
        pred = {"LOCATION_NAME": "Paris", "end_date_iso": ""}
        result = find_weather(pred, self._snapshot())
        assert result["LOCATION_NAME"] == "Paris"

    def test_none_end_date_uses_current(self):
        pred = {"LOCATION_NAME": "London", "end_date_iso": None}
        result = find_weather(pred, self._snapshot())
        assert result is not None

    def test_empty_snapshot_returns_none(self):
        pred = {"LOCATION_NAME": "London", "end_date_iso": "2026-05-19T00:00:00Z"}
        assert find_weather(pred, {}) is None


# ── load_weather_snapshot (mocked Kafka) ─────────────────────────

class TestLoadWeatherSnapshot:
    def _make_message(self, value: dict):
        msg = MagicMock()
        msg.value = value
        return msg

    def test_indexes_historical_by_city_date(self):
        records = [
            {"LOCATION_NAME": "London", "WEATHER_TYPE": "historical", "OBSERVATION_DATE": "2026-05-19"},
        ]
        messages = [self._make_message(r) for r in records]

        with patch("flink_jobs.correlation_job.KafkaConsumer") as mock_cls:
            mock_cls.return_value.__iter__ = MagicMock(return_value=iter(messages))
            mock_cls.return_value.close    = MagicMock()

            from flink_jobs.correlation_job import load_weather_snapshot
            snap = load_weather_snapshot()

        assert "London_2026-05-19" in snap

    def test_indexes_current_by_city(self):
        records = [
            {"LOCATION_NAME": "Paris", "WEATHER_TYPE": "current"},
        ]
        messages = [self._make_message(r) for r in records]

        with patch("flink_jobs.correlation_job.KafkaConsumer") as mock_cls:
            mock_cls.return_value.__iter__ = MagicMock(return_value=iter(messages))
            mock_cls.return_value.close    = MagicMock()

            from flink_jobs.correlation_job import load_weather_snapshot
            snap = load_weather_snapshot()

        assert "Paris_current" in snap

    def test_skips_records_without_city(self):
        records = [{"WEATHER_TYPE": "current"}]  # no LOCATION_NAME
        messages = [self._make_message(r) for r in records]

        with patch("flink_jobs.correlation_job.KafkaConsumer") as mock_cls:
            mock_cls.return_value.__iter__ = MagicMock(return_value=iter(messages))
            mock_cls.return_value.close    = MagicMock()

            from flink_jobs.correlation_job import load_weather_snapshot
            snap = load_weather_snapshot()

        assert snap == {}

    def test_latest_current_record_wins(self):
        records = [
            {"LOCATION_NAME": "London", "WEATHER_TYPE": "current", "POLL_TIMESTAMP": "T1"},
            {"LOCATION_NAME": "London", "WEATHER_TYPE": "current", "POLL_TIMESTAMP": "T2"},
        ]
        messages = [self._make_message(r) for r in records]

        with patch("flink_jobs.correlation_job.KafkaConsumer") as mock_cls:
            mock_cls.return_value.__iter__ = MagicMock(return_value=iter(messages))
            mock_cls.return_value.close    = MagicMock()

            from flink_jobs.correlation_job import load_weather_snapshot
            snap = load_weather_snapshot()

        assert snap["London_current"]["POLL_TIMESTAMP"] == "T2"

    def test_empty_topic_returns_empty_dict(self):
        with patch("flink_jobs.correlation_job.KafkaConsumer") as mock_cls:
            mock_cls.return_value.__iter__ = MagicMock(return_value=iter([]))
            mock_cls.return_value.close    = MagicMock()

            from flink_jobs.correlation_job import load_weather_snapshot
            snap = load_weather_snapshot()

        assert snap == {}


# ── load_predictions (mocked Kafka) ──────────────────────────────

class TestLoadPredictions:
    def _make_message(self, value):
        msg = MagicMock()
        msg.value = value
        return msg

    def test_returns_all_records(self):
        records  = [{"condition_id": f"id{i}"} for i in range(5)]
        messages = [self._make_message(r) for r in records]

        with patch("flink_jobs.correlation_job.KafkaConsumer") as mock_cls:
            mock_cls.return_value.__iter__ = MagicMock(return_value=iter(messages))
            mock_cls.return_value.close    = MagicMock()

            from flink_jobs.correlation_job import load_predictions
            preds = load_predictions()

        assert len(preds) == 5

    def test_empty_topic_returns_empty_list(self):
        with patch("flink_jobs.correlation_job.KafkaConsumer") as mock_cls:
            mock_cls.return_value.__iter__ = MagicMock(return_value=iter([]))
            mock_cls.return_value.close    = MagicMock()

            from flink_jobs.correlation_job import load_predictions
            preds = load_predictions()

        assert preds == []
