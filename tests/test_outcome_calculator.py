"""
test_outcome_calculator.py
---------------------------
Unit tests for outcome_calculator.py

Tests cover:
  - Temperature conversion (F↔C)
  - Threshold comparison (above/below/between)
  - Prediction error calculation
  - Rain/Snow/Weather outcome determination
  - Full correlate() function
"""

import pytest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producers.utils.outcome_calculator import (
    fahrenheit_to_celsius,
    celsius_to_fahrenheit,
    compare_threshold,
    calculate_prediction_error,
    determine_weather_outcome,
    correlate,
)


# ── Temperature conversion tests ──────────────────────────────────
class TestTemperatureConversion:

    def test_fahrenheit_to_celsius_freezing(self):
        assert fahrenheit_to_celsius(32) == 0.0

    def test_fahrenheit_to_celsius_boiling(self):
        assert fahrenheit_to_celsius(212) == 100.0

    def test_fahrenheit_to_celsius_body_temp(self):
        assert fahrenheit_to_celsius(98.6) == 37.0

    def test_fahrenheit_to_celsius_negative(self):
        assert fahrenheit_to_celsius(-40) == -40.0

    def test_fahrenheit_to_celsius_77(self):
        assert fahrenheit_to_celsius(77) == 25.0

    def test_celsius_to_fahrenheit_zero(self):
        assert celsius_to_fahrenheit(0) == 32.0

    def test_celsius_to_fahrenheit_100(self):
        assert celsius_to_fahrenheit(100) == 212.0

    def test_celsius_to_fahrenheit_25(self):
        assert celsius_to_fahrenheit(25) == 77.0

    def test_celsius_to_fahrenheit_negative(self):
        assert celsius_to_fahrenheit(-40) == -40.0

    def test_roundtrip(self):
        """Converting F→C→F should return original value."""
        original = 72.5
        result = celsius_to_fahrenheit(fahrenheit_to_celsius(original))
        assert abs(result - original) < 0.1


# ── Threshold comparison tests ────────────────────────────────────
class TestCompareThreshold:

    # Above threshold
    def test_above_passes(self):
        threshold = {"value": 77, "unit": "F", "type": "above"}
        assert compare_threshold(80.0, threshold) == 1

    def test_above_fails(self):
        threshold = {"value": 77, "unit": "F", "type": "above"}
        assert compare_threshold(72.5, threshold) == 0

    def test_above_exact_boundary(self):
        threshold = {"value": 77, "unit": "F", "type": "above"}
        assert compare_threshold(77.0, threshold) == 1

    # Below threshold
    def test_below_passes(self):
        threshold = {"value": 35, "unit": "F", "type": "below"}
        assert compare_threshold(30.0, threshold) == 1

    def test_below_fails(self):
        threshold = {"value": 35, "unit": "F", "type": "below"}
        assert compare_threshold(40.0, threshold) == 0

    def test_below_exact_boundary(self):
        threshold = {"value": 35, "unit": "F", "type": "below"}
        assert compare_threshold(35.0, threshold) == 1

    # Between threshold
    def test_between_passes(self):
        threshold = {"value_min": 40, "value_max": 41, "unit": "F", "type": "between"}
        assert compare_threshold(40.5, threshold) == 1

    def test_between_fails_low(self):
        threshold = {"value_min": 40, "value_max": 41, "unit": "F", "type": "between"}
        assert compare_threshold(25.7, threshold) == 0

    def test_between_fails_high(self):
        threshold = {"value_min": 40, "value_max": 41, "unit": "F", "type": "between"}
        assert compare_threshold(45.5, threshold) == 0

    def test_between_min_boundary(self):
        threshold = {"value_min": 40, "value_max": 41, "unit": "F", "type": "between"}
        assert compare_threshold(40.0, threshold) == 1

    def test_between_max_boundary(self):
        threshold = {"value_min": 40, "value_max": 41, "unit": "F", "type": "between"}
        assert compare_threshold(41.0, threshold) == 1

    # Celsius threshold
    def test_celsius_threshold_above(self):
        threshold = {"value": 25, "unit": "C", "type": "above"}
        # 77°F = 25°C → should pass (>=25)
        assert compare_threshold(77.0, threshold) == 1

    def test_celsius_threshold_below(self):
        threshold = {"value": 0, "unit": "C", "type": "below"}
        # 30°F = -1.1°C → below 0°C
        assert compare_threshold(30.0, threshold) == 1

    # Edge cases
    def test_none_temp_returns_none(self):
        threshold = {"value": 77, "unit": "F", "type": "above"}
        assert compare_threshold(None, threshold) is None

    def test_none_threshold_returns_none(self):
        assert compare_threshold(77.0, None) is None

    def test_empty_threshold_returns_none(self):
        assert compare_threshold(77.0, {}) is None

    def test_unknown_type_returns_none(self):
        threshold = {"value": 77, "unit": "F", "type": "unknown"}
        assert compare_threshold(77.0, threshold) is None


# ── Prediction error tests ────────────────────────────────────────
class TestCalculatePredictionError:

    def test_perfect_prediction_yes(self):
        assert calculate_prediction_error(1.0, 1) == 0.0

    def test_perfect_prediction_no(self):
        assert calculate_prediction_error(0.0, 0) == 0.0

    def test_worst_prediction(self):
        assert calculate_prediction_error(1.0, 0) == 1.0

    def test_typical_error(self):
        assert calculate_prediction_error(0.15, 0) == 0.15

    def test_typical_error_yes(self):
        assert calculate_prediction_error(0.80, 1) == 0.20

    def test_50_50_yes(self):
        assert calculate_prediction_error(0.5, 1) == 0.5

    def test_50_50_no(self):
        assert calculate_prediction_error(0.5, 0) == 0.5

    def test_rounding(self):
        result = calculate_prediction_error(0.333333, 0)
        assert len(str(result).split(".")[-1]) <= 4


# ── Weather outcome tests ─────────────────────────────────────────
class TestDetermineWeatherOutcome:

    def _make_weather(self, weather_code=None, precipitation=0.0, rain=0.0, wind=0.0):
        return {
            "WEATHER_TYPE": "historical",
            "LOCATION_NAME": "London",
            "current": {
                "weather_code": weather_code,
                "precipitation": precipitation,
                "rain": rain,
                "wind_speed_10m": wind,
                "temperature_2m": 15.0,
                "temperature_2m_f": 59.0,
            }
        }

    # RAIN tests
    def test_rain_with_rain_code(self):
        w = self._make_weather(weather_code=61)
        assert determine_weather_outcome("RAIN", w) == 1

    def test_rain_with_drizzle_code(self):
        w = self._make_weather(weather_code=51)
        assert determine_weather_outcome("RAIN", w) == 1

    def test_rain_with_clear_code(self):
        w = self._make_weather(weather_code=0)
        assert determine_weather_outcome("RAIN", w) == 0

    def test_rain_fallback_precipitation(self):
        w = self._make_weather(weather_code=None, precipitation=2.5)
        assert determine_weather_outcome("RAIN", w) == 1

    def test_rain_fallback_no_precipitation(self):
        w = self._make_weather(weather_code=None, precipitation=0.0)
        assert determine_weather_outcome("RAIN", w) == 0

    # SNOW tests
    def test_snow_with_snow_code(self):
        w = self._make_weather(weather_code=73)
        assert determine_weather_outcome("SNOW", w) == 1

    def test_snow_with_rain_code(self):
        w = self._make_weather(weather_code=61)
        assert determine_weather_outcome("SNOW", w) == 0

    def test_snow_with_clear_code(self):
        w = self._make_weather(weather_code=0)
        assert determine_weather_outcome("SNOW", w) == 0

    # SUNSHINE tests
    def test_sunshine_clear(self):
        w = self._make_weather(weather_code=0)
        assert determine_weather_outcome("SUNSHINE", w) == 1

    def test_sunshine_cloudy(self):
        w = self._make_weather(weather_code=3)
        assert determine_weather_outcome("SUNSHINE", w) == 0

    # WIND tests
    def test_wind_strong(self):
        w = self._make_weather(wind=35.0)
        assert determine_weather_outcome("WIND", w) == 1

    def test_wind_weak(self):
        w = self._make_weather(wind=10.0)
        assert determine_weather_outcome("WIND", w) == 0

    # FOG tests
    def test_fog_with_fog_code(self):
        w = self._make_weather(weather_code=45)
        assert determine_weather_outcome("FOG", w) == 1

    def test_fog_without_fog(self):
        w = self._make_weather(weather_code=0)
        assert determine_weather_outcome("FOG", w) == 0

    # FROST tests
    def _make_frost_weather(self, temp_c):
        return {
            "WEATHER_TYPE": "historical",
            "LOCATION_NAME": "London",
            "current": {
                "weather_code": 0,
                "precipitation": 0.0,
                "rain": 0.0,
                "wind_speed_10m": 5.0,
                "temperature_2m": temp_c,
                "temperature_2m_f": celsius_to_fahrenheit(temp_c),
            }
        }

    def test_frost_below_zero(self):
        w = self._make_frost_weather(-5.0)
        assert determine_weather_outcome("FROST", w) == 1

    def test_frost_above_zero(self):
        w = self._make_frost_weather(5.0)
        assert determine_weather_outcome("FROST", w) == 0


# ── Correlate function tests ──────────────────────────────────────
class TestCorrelate:

    def _make_prediction(self, city="London", price=0.15, winner=False,
                         closed=True, market_type="TEMPERATURE",
                         threshold=None, end_date="2025-01-22"):
        return {
            "LOCATION_NAME":       city,
            "price":               price,
            "winner":              winner,
            "closed":              closed,
            "MARKET_TYPE":         market_type,
            "THRESHOLD":           threshold,
            "end_date_iso":        f"{end_date}T00:00:00Z",
            "game_start_time":     f"{end_date}T00:00:00Z",
            "POLL_TIMESTAMP":      "2026-05-25T10:00:00.000000+00:00",
            "NEEDS_WEATHER_CHECK": not closed,
            "question":            "Will the temp in London be 77°F+?",
        }

    def _make_weather(self, city="London", weather_type="historical",
                      temp_c=7.5, temp_f=45.5, rain=0.0,
                      weather_code=0, obs_date="2025-01-22"):
        return {
            "LOCATION_NAME":    city,
            "WEATHER_TYPE":     weather_type,
            "OBSERVATION_DATE": obs_date,
            "POLL_TIMESTAMP":   "2026-05-25T10:05:00.000000+00:00",
            "current": {
                "time":              obs_date,
                "temperature_2m":    temp_c,
                "temperature_2m_f":  temp_f,
                "precipitation":     rain,
                "rain":              rain,
                "weather_code":      weather_code,
                "wind_speed_10m":    10.0,
                "relative_humidity_2m": 70,
            }
        }

    # Scenario 1: winner_known
    def test_closed_market_winner_yes(self):
        pred = self._make_prediction(price=0.8, winner=True, closed=True)
        weather = self._make_weather()
        result = correlate(pred, weather)
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1
        assert result["PREDICTION_ERROR"] == 0.2
        assert result["CORRELATION_METHOD"] == "winner_known"

    def test_closed_market_winner_no(self):
        pred = self._make_prediction(price=0.15, winner=False, closed=True)
        weather = self._make_weather()
        result = correlate(pred, weather)
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 0
        assert result["PREDICTION_ERROR"] == 0.15
        assert result["CORRELATION_METHOD"] == "winner_known"

    def test_closed_rain_market_winner_yes(self):
        pred = self._make_prediction(
            price=0.82, winner=True, closed=True, market_type="RAIN"
        )
        weather = self._make_weather(weather_code=61, rain=5.0)
        result = correlate(pred, weather)
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1
        assert result["CORRELATION_METHOD"] == "winner_known"

    # Scenario 2: historical_weather
    def test_historical_rain_market(self):
        pred = self._make_prediction(
            price=0.7, winner=None, closed=False,
            market_type="RAIN"
        )
        weather = self._make_weather(
            weather_type="historical", weather_code=61, rain=5.0
        )
        result = correlate(pred, weather)
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1
        assert result["CORRELATION_METHOD"] == "historical_weather"

    def test_historical_no_rain(self):
        pred = self._make_prediction(
            price=0.3, winner=None, closed=False,
            market_type="RAIN"
        )
        weather = self._make_weather(
            weather_type="historical", weather_code=0, rain=0.0
        )
        result = correlate(pred, weather)
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 0
        assert result["CORRELATION_METHOD"] == "historical_weather"

    def test_historical_temperature_above(self):
        threshold = {"value": 40, "unit": "F", "type": "above"}
        pred = self._make_prediction(
            price=0.5, winner=None, closed=False,
            market_type="TEMPERATURE", threshold=threshold
        )
        weather = self._make_weather(
            weather_type="historical", temp_c=7.5, temp_f=45.5
        )
        result = correlate(pred, weather)
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1  # 45.5°F > 40°F

    # Scenario 3: current_snapshot
    def test_current_snapshot_rain(self):
        pred = self._make_prediction(
            price=0.6, winner=None, closed=False,
            market_type="RAIN"
        )
        weather = self._make_weather(
            weather_type="current", weather_code=61, rain=2.0
        )
        result = correlate(pred, weather)
        assert result is not None
        assert result["ACTUAL_OUTCOME"] == 1
        assert result["CORRELATION_METHOD"] == "current_snapshot"

    # City mismatch
    def test_city_mismatch_returns_none(self):
        pred = self._make_prediction(city="London")
        weather = self._make_weather(city="Paris")
        result = correlate(pred, weather)
        assert result is None

    # Fields in result
    def test_result_has_required_fields(self):
        pred = self._make_prediction(price=0.15, winner=False, closed=True)
        weather = self._make_weather()
        result = correlate(pred, weather)
        assert result is not None
        assert "ACTUAL_OUTCOME" in result
        assert "PREDICTION_ERROR" in result
        assert "CORRELATION_METHOD" in result
        assert "CORRELATION_TIMESTAMP" not in result  # added by job not correlate
        assert "ACTUAL_TEMP_F" in result
        assert "ACTUAL_PRECIP_MM" in result
        assert "WEATHER_TYPE" in result