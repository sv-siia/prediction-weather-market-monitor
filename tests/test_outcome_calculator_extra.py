"""
Extra coverage tests for outcome_calculator.py —
covers branches missed by the existing test suite:
  - compare_threshold with missing value fields
  - determine_weather_outcome for HAIL, CLOUD, WEATHER, SNOW/SUNSHINE/WIND/FOG/FROST with no code
  - correlate: current_snapshot temperature + non-temperature market types
  - _latency edge cases
  - _weather_fields helper
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from producers.utils.outcome_calculator import (
    compare_threshold,
    determine_weather_outcome,
    calculate_prediction_error,
    correlate,
    _latency,
    _weather_fields,
)


# ── compare_threshold — missing value branches ────────────────────

class TestCompareThresholdMissingValues:
    def test_above_with_none_value_returns_none(self):
        thresh = {"type": "above", "unit": "F", "value": None}
        assert compare_threshold(75.0, thresh) is None

    def test_below_with_none_value_returns_none(self):
        thresh = {"type": "below", "unit": "F", "value": None}
        assert compare_threshold(75.0, thresh) is None

    def test_between_with_none_min_returns_none(self):
        thresh = {"type": "between", "unit": "F", "value_min": None, "value_max": 80}
        assert compare_threshold(75.0, thresh) is None

    def test_between_with_none_max_returns_none(self):
        thresh = {"type": "between", "unit": "F", "value_min": 70, "value_max": None}
        assert compare_threshold(75.0, thresh) is None


# ── determine_weather_outcome — uncovered branches ────────────────

class TestDetermineWeatherOutcomeExtra:

    def _w(self, code=None, precip=0.0, rain=0.0, wind=None, temp_c=None):
        current = {
            "weather_code":   code,
            "precipitation":  precip,
            "rain":           rain,
            "wind_speed_10m": wind,
            "temperature_2m": temp_c,
        }
        return {"current": current}

    # HAIL
    def test_hail_with_hail_code(self):
        assert determine_weather_outcome("HAIL", self._w(code=96)) == 1

    def test_hail_without_hail_code(self):
        assert determine_weather_outcome("HAIL", self._w(code=0)) == 0

    def test_hail_no_code_returns_none(self):
        assert determine_weather_outcome("HAIL", self._w()) is None

    # CLOUD
    def test_cloud_with_cloudy_code(self):
        assert determine_weather_outcome("CLOUD", self._w(code=2)) == 1

    def test_cloud_with_clear_code(self):
        assert determine_weather_outcome("CLOUD", self._w(code=0)) == 0

    def test_cloud_no_code_returns_none(self):
        assert determine_weather_outcome("CLOUD", self._w()) is None

    # WEATHER (generic)
    def test_weather_generic_rain_code(self):
        assert determine_weather_outcome("WEATHER", self._w(code=61)) == 1

    def test_weather_generic_clear_code(self):
        assert determine_weather_outcome("WEATHER", self._w(code=0)) == 0

    def test_weather_generic_no_code_with_precip(self):
        assert determine_weather_outcome("WEATHER", self._w(precip=1.0)) == 1

    def test_weather_generic_no_code_no_precip(self):
        assert determine_weather_outcome("WEATHER", self._w()) == 0

    # SNOW fallback (no weather code)
    def test_snow_no_code_returns_zero(self):
        assert determine_weather_outcome("SNOW", self._w()) == 0

    # SUNSHINE — no code
    def test_sunshine_no_code_returns_none(self):
        assert determine_weather_outcome("SUNSHINE", self._w()) is None

    # WIND — no wind data
    def test_wind_no_data_returns_none(self):
        assert determine_weather_outcome("WIND", self._w()) is None

    # FOG — no code
    def test_fog_no_code_returns_none(self):
        assert determine_weather_outcome("FOG", self._w()) is None

    # FROST — no temperature
    def test_frost_no_temp_returns_none(self):
        assert determine_weather_outcome("FROST", self._w()) is None

    # Unknown market type
    def test_unknown_market_type_returns_none(self):
        assert determine_weather_outcome("VOLCANO", self._w(code=0)) is None


# ── correlate — current_snapshot paths ───────────────────────────

class TestCorrelateCurrentSnapshot:

    def _pred(self, market_type="RAIN", price=0.7, threshold=None):
        return {
            "LOCATION_NAME": "London",
            "MARKET_TYPE":   market_type,
            "price":         price,
            "winner":        None,
            "closed":        False,
            "THRESHOLD":     threshold,
            "POLL_TIMESTAMP": "2026-05-19T12:00:00Z",
        }

    def _weather(self, code=0, wind=None, temp_f=None, temp_c=None, precip=0.0):
        return {
            "LOCATION_NAME": "London",
            "WEATHER_TYPE":  "current",
            "POLL_TIMESTAMP": "2026-05-19T12:05:00Z",
            "current": {
                "weather_code":   code,
                "wind_speed_10m": wind,
                "temperature_2m_f": temp_f,
                "temperature_2m":   temp_c,
                "precipitation":  precip,
                "rain":           0.0,
            },
        }

    def test_current_temperature_above_threshold(self):
        result = correlate(
            self._pred("TEMPERATURE", threshold={"value": 70, "unit": "F", "type": "above"}),
            self._weather(temp_f=75.0),
        )
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1
        assert result["CORRELATION_METHOD"] == "current_snapshot"

    def test_current_temperature_missing_temp_returns_none(self):
        result = correlate(
            self._pred("TEMPERATURE", threshold={"value": 70, "unit": "F", "type": "above"}),
            self._weather(temp_f=None),
        )
        assert result is None

    def test_current_temperature_bad_threshold_returns_none(self):
        result = correlate(
            self._pred("TEMPERATURE", threshold={"value": None, "unit": "F", "type": "above"}),
            self._weather(temp_f=75.0),
        )
        assert result is None

    def test_current_snow_outcome(self):
        result = correlate(
            self._pred("SNOW"),
            self._weather(code=71),
        )
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1

    def test_current_wind_outcome(self):
        result = correlate(
            self._pred("WIND"),
            self._weather(wind=40.0),
        )
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1

    def test_current_unknown_market_type_returns_none(self):
        result = correlate(
            self._pred("VOLCANO"),
            self._weather(code=0),
        )
        assert result is None

    def test_current_temperature_no_threshold_returns_none(self):
        result = correlate(
            self._pred("TEMPERATURE", threshold=None),
            self._weather(temp_f=75.0),
        )
        assert result is None


# ── correlate — historical paths ─────────────────────────────────

class TestCorrelateHistoricalExtra:

    def _pred(self, market_type="RAIN", price=0.6, threshold=None):
        return {
            "LOCATION_NAME": "London",
            "MARKET_TYPE":   market_type,
            "price":         price,
            "winner":        None,
            "closed":        False,
            "THRESHOLD":     threshold,
            "POLL_TIMESTAMP": "2026-05-19T06:00:00Z",
        }

    def _hist(self, code=63, temp_f=55.0, temp_c=13.0, precip=3.0, wind=20.0):
        return {
            "LOCATION_NAME":   "London",
            "WEATHER_TYPE":    "historical",
            "OBSERVATION_DATE": "2026-05-19",
            "POLL_TIMESTAMP":  "2026-05-19T18:00:00Z",
            "current": {
                "weather_code":   code,
                "temperature_2m_f": temp_f,
                "temperature_2m":   temp_c,
                "precipitation":  precip,
                "rain":           precip,
                "wind_speed_10m": wind,
            },
        }

    def test_historical_unknown_market_type_returns_none(self):
        result = correlate(self._pred("VOLCANO"), self._hist())
        assert result is None

    def test_historical_temperature_missing_temp_returns_none(self):
        result = correlate(
            self._pred("TEMPERATURE", threshold={"value": 60, "unit": "F", "type": "above"}),
            {**self._hist(), "current": {"weather_code": 0}},
        )
        assert result is None

    def test_historical_wind_outcome(self):
        result = correlate(self._pred("WIND"), self._hist(wind=50.0))
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1

    def test_historical_fog_outcome(self):
        result = correlate(self._pred("FOG"), self._hist(code=45))
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1

    def test_historical_hail_outcome(self):
        result = correlate(self._pred("HAIL"), self._hist(code=96))
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1

    def test_historical_cloud_outcome(self):
        result = correlate(self._pred("CLOUD"), self._hist(code=3))
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1

    def test_historical_frost_outcome(self):
        result = correlate(self._pred("FROST"), self._hist(temp_c=-2.0))
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1

    def test_historical_weather_generic(self):
        result = correlate(self._pred("WEATHER"), self._hist(code=61))
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1


# ── _weather_fields helper ────────────────────────────────────────

class TestWeatherFields:
    def test_extracts_all_fields(self):
        weather = {
            "WEATHER_TYPE":    "current",
            "OBSERVATION_DATE": "2026-05-19",
            "POLL_TIMESTAMP":  "2026-05-19T12:00:00Z",
            "current": {
                "temperature_2m":   14.0,
                "temperature_2m_f": 57.2,
                "precipitation":    1.5,
                "rain":             1.5,
                "weather_code":     61,
                "wind_speed_10m":   20.0,
            },
        }
        result = _weather_fields(weather)
        assert result["WEATHER_TYPE"]       == "current"
        assert result["ACTUAL_TEMP_C"]      == 14.0
        assert result["ACTUAL_TEMP_F"]      == 57.2
        assert result["ACTUAL_PRECIP_MM"]   == 1.5
        assert result["ACTUAL_WEATHER_CODE"] == 61
        assert result["ACTUAL_WIND_KMH"]    == 20.0

    def test_missing_current_uses_none(self):
        result = _weather_fields({"WEATHER_TYPE": "current"})
        assert result["ACTUAL_TEMP_C"] is None


# ── _latency edge cases ───────────────────────────────────────────

class TestLatencyExtra:
    def test_missing_poll_timestamp_returns_none(self):
        pred    = {}
        weather = {"POLL_TIMESTAMP": "2026-05-19T12:00:00Z"}
        assert _latency(pred, weather) is None

    def test_missing_weather_poll_timestamp_returns_none(self):
        pred    = {"POLL_TIMESTAMP": "2026-05-19T12:00:00Z"}
        weather = {}
        assert _latency(pred, weather) is None

    def test_completely_malformed_returns_none(self):
        pred    = {"POLL_TIMESTAMP": "not-a-date"}
        weather = {"POLL_TIMESTAMP": "also-not-a-date"}
        assert _latency(pred, weather) is None

    def test_exception_in_latency_returns_none(self):
        # Passing non-string to trigger exception branch
        pred    = {"POLL_TIMESTAMP": 12345}
        weather = {"POLL_TIMESTAMP": object()}
        assert _latency(pred, weather) is None


# ── correlate — weather_outcome=None branches ─────────────────────

class TestCorrelateWeatherOutcomeNone:
    """Covers lines where determine_weather_outcome returns None → correlate returns None."""

    def _pred(self, market_type="WIND", price=0.5):
        return {
            "LOCATION_NAME": "London",
            "MARKET_TYPE":   market_type,
            "price":         price,
            "winner":        None,
            "closed":        False,
            "THRESHOLD":     None,
            "POLL_TIMESTAMP": "2026-05-19T12:00:00Z",
        }

    def _weather(self, weather_type="current", code=None, wind=None, temp_c=None):
        return {
            "LOCATION_NAME": "London",
            "WEATHER_TYPE":  weather_type,
            "POLL_TIMESTAMP": "2026-05-19T12:05:00Z",
            "current": {
                "weather_code":   code,
                "wind_speed_10m": wind,
                "temperature_2m": temp_c,
                "precipitation":  0.0,
                "rain":           0.0,
            },
        }

    def test_current_wind_no_speed_returns_none(self):
        # WIND with no wind_speed_10m → determine_weather_outcome returns None
        result = correlate(self._pred("WIND"), self._weather("current", wind=None))
        assert result is None

    def test_current_sunshine_no_code_returns_none(self):
        result = correlate(self._pred("SUNSHINE"), self._weather("current"))
        assert result is None

    def test_current_fog_no_code_returns_none(self):
        result = correlate(self._pred("FOG"), self._weather("current"))
        assert result is None

    def test_historical_wind_no_speed_returns_none(self):
        result = correlate(self._pred("WIND"), self._weather("historical", wind=None))
        assert result is None

    def test_historical_sunshine_no_code_returns_none(self):
        result = correlate(self._pred("SUNSHINE"), self._weather("historical"))
        assert result is None
