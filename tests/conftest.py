"""
Shared pytest fixtures for the prediction-market-monitor test suite.
"""
import json
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone


# ── Canonical sample data ─────────────────────────────────────────

@pytest.fixture
def sample_prediction():
    """A standard open RAIN prediction for London."""
    return {
        "condition_id":  "abc123",
        "question":      "Will it rain in London on May 20?",
        "LOCATION_NAME": "London",
        "MARKET_TYPE":   "RAIN",
        "MARKET_STATUS": "open",
        "price":         0.65,
        "winner":        None,
        "closed":        False,
        "end_date_iso":  "2026-05-20T00:00:00Z",
        "POLL_TIMESTAMP": "2026-05-19T12:00:00Z",
        "IS_ARBITRAGE":  False,
    }


@pytest.fixture
def sample_closed_prediction():
    """A resolved YES prediction for London rain."""
    return {
        "condition_id":  "closed_001",
        "question":      "Will it rain in London on May 19?",
        "LOCATION_NAME": "London",
        "MARKET_TYPE":   "RAIN",
        "MARKET_STATUS": "closed",
        "price":         0.70,
        "winner":        True,
        "closed":        True,
        "end_date_iso":  "2026-05-19T00:00:00Z",
        "POLL_TIMESTAMP": "2026-05-19T06:00:00Z",
        "IS_ARBITRAGE":  False,
    }


@pytest.fixture
def sample_temperature_prediction():
    """An open TEMPERATURE prediction for New York City."""
    return {
        "condition_id":  "temp_001",
        "question":      "Will the temperature in New York City be above 77°F on May 20?",
        "LOCATION_NAME": "New York City",
        "MARKET_TYPE":   "TEMPERATURE",
        "MARKET_STATUS": "open",
        "price":         0.45,
        "winner":        None,
        "closed":        False,
        "end_date_iso":  "2026-05-20T00:00:00Z",
        "POLL_TIMESTAMP": "2026-05-19T12:00:00Z",
        "THRESHOLD":     {"value": 77, "unit": "F", "type": "above"},
        "IS_ARBITRAGE":  False,
    }


@pytest.fixture
def sample_current_weather():
    """Current weather snapshot for London with rain."""
    return {
        "LOCATION_NAME": "London",
        "WEATHER_TYPE":  "current",
        "POLL_TIMESTAMP": "2026-05-19T12:05:00Z",
        "current": {
            "time":               "2026-05-19T12:00",
            "temperature_2m":     14.5,
            "temperature_2m_f":   58.1,
            "precipitation":      2.3,
            "rain":               2.3,
            "weather_code":       61,
            "wind_speed_10m":     18.0,
            "relative_humidity_2m": 85,
        },
    }


@pytest.fixture
def sample_clear_weather():
    """Current weather snapshot for London — clear sky."""
    return {
        "LOCATION_NAME": "London",
        "WEATHER_TYPE":  "current",
        "POLL_TIMESTAMP": "2026-05-19T12:05:00Z",
        "current": {
            "time":               "2026-05-19T12:00",
            "temperature_2m":     22.0,
            "temperature_2m_f":   71.6,
            "precipitation":      0.0,
            "rain":               0.0,
            "weather_code":       0,
            "wind_speed_10m":     8.0,
            "relative_humidity_2m": 45,
        },
    }


@pytest.fixture
def sample_historical_weather():
    """Historical weather record for London on 2026-05-19."""
    return {
        "LOCATION_NAME":   "London",
        "WEATHER_TYPE":    "historical",
        "OBSERVATION_DATE": "2026-05-19",
        "POLL_TIMESTAMP":  "2026-05-19T18:00:00Z",
        "current": {
            "time":               "2026-05-19",
            "temperature_2m":     13.2,
            "temperature_2m_f":   55.8,
            "temperature_2m_max": 16.0,
            "temperature_2m_min": 10.5,
            "precipitation":      5.1,
            "rain":               5.1,
            "weather_code":       63,
            "wind_speed_10m":     22.0,
            "relative_humidity_2m": 90,
        },
    }


@pytest.fixture
def sample_nyc_hot_weather():
    """Current weather for NYC — hot and clear (above 77°F)."""
    return {
        "LOCATION_NAME": "New York City",
        "WEATHER_TYPE":  "current",
        "POLL_TIMESTAMP": "2026-05-20T14:00:00Z",
        "current": {
            "time":               "2026-05-20T14:00",
            "temperature_2m":     26.5,
            "temperature_2m_f":   79.7,
            "precipitation":      0.0,
            "rain":               0.0,
            "weather_code":       1,
            "wind_speed_10m":     12.0,
            "relative_humidity_2m": 55,
        },
    }


# ── Aggregate / anomaly fixtures ──────────────────────────────────

@pytest.fixture
def sample_aggregate():
    """A healthy aggregate record."""
    return {
        "LOCATION_NAME":          "London",
        "MARKET_TYPE":            "RAIN",
        "WINDOW_START":           "2026-05-19T11:00:00+00:00",
        "WINDOW_END":             "2026-05-19T12:00:00+00:00",
        "total_predictions":      20,
        "correct_predictions":    17,
        "accuracy_rate":          0.85,
        "avg_prediction_error":   0.18,
        "min_prediction_error":   0.02,
        "max_prediction_error":   0.48,
        "total_volume":           1000.0,
        "volume_weighted_accuracy": 0.86,
        "bias_score":             0.05,
        "over_prediction_count":  2,
        "under_prediction_count": 1,
        "POLL_TIMESTAMP":         "2026-05-19T12:00:00Z",
    }


@pytest.fixture
def low_accuracy_aggregate(sample_aggregate):
    """Aggregate with accuracy below threshold — should trigger alert."""
    return {**sample_aggregate, "accuracy_rate": 0.55, "correct_predictions": 11}


@pytest.fixture
def high_bias_aggregate(sample_aggregate):
    """Aggregate with high positive bias — should trigger alert."""
    return {**sample_aggregate, "bias_score": 0.45}


# ── Kafka mock ────────────────────────────────────────────────────

@pytest.fixture
def mock_kafka_producer():
    """Mock KafkaProducer that captures sent messages."""
    producer = MagicMock()
    producer.sent_messages = []

    def capture_send(topic, value=None, **kwargs):
        producer.sent_messages.append({"topic": topic, "value": value})
        return MagicMock()

    producer.send.side_effect = capture_send
    return producer


@pytest.fixture
def mock_kafka_consumer_factory():
    """Factory that creates a mock KafkaConsumer from a list of records."""
    def factory(records):
        messages = []
        for rec in records:
            msg = MagicMock()
            msg.value = rec
            messages.append(msg)
        consumer = MagicMock()
        consumer.__iter__ = MagicMock(return_value=iter(messages))
        consumer.close = MagicMock()
        return consumer
    return factory


# ── DB mock ───────────────────────────────────────────────────────

@pytest.fixture
def mock_db_conn():
    """Mock psycopg2 connection with cursor context manager."""
    conn   = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
    return conn, cursor
