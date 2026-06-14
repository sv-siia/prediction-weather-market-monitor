"""
outcome_calculator.py
----------------------
Pure functions for calculating prediction accuracy.
Used by Flink correlation job and unit tests.

Supports:
  - TEMPERATURE markets (threshold comparison in °F or °C)
  - RAIN markets (weather_code + precipitation)
  - SNOW markets (weather_code)
  - SUNSHINE, WIND, FOG, HAIL markets (weather_code based)
  - Closed markets with known winner (direct outcome)

No external dependencies — only Python standard library.
"""

# ── Weather code constants ────────────────────────────────────────
RAIN_CODES    = [51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82]
SNOW_CODES    = [71, 73, 75, 77, 85, 86]
THUNDER_CODES = [95, 96, 99]
FOG_CODES     = [45, 48]
HAIL_CODES    = [96, 99]
CLEAR_CODES   = [0, 1]
CLOUDY_CODES  = [2, 3]
ALL_PRECIP_CODES = RAIN_CODES + SNOW_CODES

# ── Temperature conversion ────────────────────────────────────────
def fahrenheit_to_celsius(f: float) -> float:
    """Convert Fahrenheit to Celsius."""
    return round((f - 32) * 5/9, 2)

def celsius_to_fahrenheit(c: float) -> float:
    """Convert Celsius to Fahrenheit."""
    return round(c * 9/5 + 32, 1)

# ── Threshold comparison ──────────────────────────────────────────
def compare_threshold(actual_temp_f: float, threshold: dict) -> int | None:
    """
    Compares actual temperature against market threshold.
    Returns 1 (Yes won) or 0 (No won) or None if cannot determine.

    threshold examples:
      {"value": 77, "unit": "F", "type": "above"}
      {"value": 35, "unit": "F", "type": "below"}
      {"value_min": 40, "value_max": 41, "unit": "F", "type": "between"}
    """
    if not threshold or actual_temp_f is None:
        return None

    unit        = threshold.get("unit", "F")
    thresh_type = threshold.get("type")

    # Convert to matching unit
    if unit == "C":
        actual = fahrenheit_to_celsius(actual_temp_f)
    else:
        actual = actual_temp_f

    if thresh_type == "above":
        value = threshold.get("value")
        if value is None:
            return None
        return 1 if actual >= value else 0

    elif thresh_type == "below":
        value = threshold.get("value")
        if value is None:
            return None
        return 1 if actual <= value else 0

    elif thresh_type == "between":
        value_min = threshold.get("value_min")
        value_max = threshold.get("value_max")
        if value_min is None or value_max is None:
            return None
        return 1 if value_min <= actual <= value_max else 0

    return None

# ── Rain/Snow/Weather outcome ─────────────────────────────────────
def determine_weather_outcome(market_type: str, weather: dict) -> int | None:
    """
    Determines ACTUAL_OUTCOME for non-temperature weather markets.

    Uses weather_code first, falls back to precipitation values.

    Returns 1 if the weather condition occurred, 0 if not, None if unknown.
    """
    current       = weather.get("current", {})
    weather_code  = current.get("weather_code")
    precipitation = current.get("precipitation") or 0
    rain          = current.get("rain") or 0

    if market_type == "RAIN":
        if weather_code is not None:
            return 1 if weather_code in RAIN_CODES else 0
        # Fallback: any precipitation > 0.1mm counts as rain
        return 1 if (precipitation > 0.1 or rain > 0.1) else 0

    elif market_type == "SNOW":
        if weather_code is not None:
            return 1 if weather_code in SNOW_CODES else 0
        return 0

    elif market_type == "SUNSHINE":
        if weather_code is not None:
            return 1 if weather_code in CLEAR_CODES else 0
        return None

    elif market_type == "WIND":
        wind_speed = current.get("wind_speed_10m")
        if wind_speed is not None:
            # "Windy" = wind speed > 30 km/h
            return 1 if wind_speed > 30 else 0
        return None

    elif market_type == "FOG":
        if weather_code is not None:
            return 1 if weather_code in FOG_CODES else 0
        return None

    elif market_type == "HAIL":
        if weather_code is not None:
            return 1 if weather_code in HAIL_CODES else 0
        return None

    elif market_type == "CLOUD":
        if weather_code is not None:
            return 1 if weather_code in CLOUDY_CODES else 0
        return None

    elif market_type == "FROST":
        temp_c = current.get("temperature_2m")
        if temp_c is not None:
            return 1 if temp_c <= 0 else 0
        return None

    elif market_type == "WEATHER":
        # Generic weather — use precipitation as proxy
        if weather_code is not None:
            return 1 if weather_code in ALL_PRECIP_CODES else 0
        return 1 if precipitation > 0.1 else 0

    return None

# ── Prediction error ──────────────────────────────────────────────
def calculate_prediction_error(
        predicted_probability: float,
        actual_outcome: int) -> float:
    """
    Calculates absolute difference between predicted probability
    and actual outcome.

    Examples:
      predicted=0.15, actual=0 → error=0.15
      predicted=0.80, actual=1 → error=0.20
      predicted=0.50, actual=1 → error=0.50
    """
    return round(abs(predicted_probability - actual_outcome), 4)

# ── Main correlation logic ────────────────────────────────────────
def correlate(prediction: dict, weather: dict) -> dict | None:
    """
    Main function: takes one prediction and one weather record,
    returns correlated record with ACTUAL_OUTCOME and PREDICTION_ERROR.

    Returns None if correlation cannot be performed.

    Three scenarios:
      1. Closed market → winner already known → use directly
      2. Archived market → use historical weather data
      3. Open market → use current weather snapshot
    """
    city         = prediction.get("LOCATION_NAME")
    weather_city = weather.get("LOCATION_NAME")
    threshold    = prediction.get("THRESHOLD")
    yes_price    = prediction.get("price", 0.5)
    winner       = prediction.get("winner")
    closed       = bool(prediction.get("closed") or prediction.get("CLOSED"))
    market_type  = prediction.get("MARKET_TYPE", "WEATHER")
    weather_type = weather.get("WEATHER_TYPE", "current")
    current      = weather.get("current", {})

    # Cities must match
    if city != weather_city:
        return None

    # ── Scenario 1: Closed market — winner is known ───────────────
    # Works for ALL market types including RAIN, SNOW, TEMPERATURE
    if closed and winner is not None:
        actual_outcome   = 1 if winner else 0
        prediction_error = calculate_prediction_error(
            yes_price, actual_outcome
        )
        return {
            **prediction,
            **_weather_fields(weather),
            "ACTUAL_OUTCOME":          actual_outcome,
            "PREDICTION_ERROR":        prediction_error,
            "CORRELATION_METHOD":      "winner_known",
            "CORRELATION_LATENCY_SEC": _latency(prediction, weather),
        }

    # ── Scenario 2: Archived/Historical market ────────────────────
    if weather_type == "historical":

        # TEMPERATURE market → use threshold comparison
        if market_type == "TEMPERATURE" and threshold:
            temp_f = current.get("temperature_2m_f")
            if temp_f is None:
                return None
            actual_outcome = compare_threshold(temp_f, threshold)
            if actual_outcome is None:
                return None

        # RAIN/SNOW/SUNSHINE/WIND/FOG/etc → use weather codes
        elif market_type in [
            "RAIN", "SNOW", "SUNSHINE", "WIND",
            "FOG", "HAIL", "CLOUD", "FROST", "WEATHER"
        ]:
            actual_outcome = determine_weather_outcome(market_type, weather)
            if actual_outcome is None:
                return None

        else:
            return None

        prediction_error = calculate_prediction_error(
            yes_price, actual_outcome
        )
        return {
            **prediction,
            **_weather_fields(weather),
            "ACTUAL_OUTCOME":          actual_outcome,
            "PREDICTION_ERROR":        prediction_error,
            "CORRELATION_METHOD":      "historical_weather",
            "CORRELATION_LATENCY_SEC": _latency(prediction, weather),
        }

    # ── Scenario 3: Open market — current weather snapshot ────────
    if weather_type == "current":

        # TEMPERATURE market → use threshold comparison
        if market_type == "TEMPERATURE" and threshold:
            temp_f = current.get("temperature_2m_f")
            if temp_f is None:
                return None
            actual_outcome = compare_threshold(temp_f, threshold)
            if actual_outcome is None:
                return None

        # RAIN/SNOW/SUNSHINE/WIND/FOG/etc → use weather codes
        elif market_type in [
            "RAIN", "SNOW", "SUNSHINE", "WIND",
            "FOG", "HAIL", "CLOUD", "FROST", "WEATHER"
        ]:
            actual_outcome = determine_weather_outcome(market_type, weather)
            if actual_outcome is None:
                return None

        else:
            return None

        prediction_error = calculate_prediction_error(
            yes_price, actual_outcome
        )
        return {
            **prediction,
            **_weather_fields(weather),
            "ACTUAL_OUTCOME":          actual_outcome,
            "PREDICTION_ERROR":        prediction_error,
            "CORRELATION_METHOD":      "current_snapshot",
            "CORRELATION_LATENCY_SEC": _latency(prediction, weather),
        }

    return None

# ── Helper functions ──────────────────────────────────────────────
def _weather_fields(weather: dict) -> dict:
    """Extracts key weather fields for the correlated record."""
    current = weather.get("current", {})
    return {
        "WEATHER_TYPE":       weather.get("WEATHER_TYPE"),
        "OBSERVATION_DATE":   weather.get("OBSERVATION_DATE"),
        "ACTUAL_TEMP_C":      current.get("temperature_2m"),
        "ACTUAL_TEMP_F":      current.get("temperature_2m_f"),
        "ACTUAL_PRECIP_MM":   current.get("precipitation"),
        "ACTUAL_RAIN_MM":     current.get("rain"),
        "ACTUAL_WEATHER_CODE": current.get("weather_code"),
        "ACTUAL_WIND_KMH":    current.get("wind_speed_10m"),
        "WEATHER_POLL_TIME":  weather.get("POLL_TIMESTAMP"),
    }

def _latency(prediction: dict, weather: dict) -> int | None:
    """
    Calculates seconds between prediction poll and weather observation.
    """
    try:
        from datetime import datetime

        pred_time    = prediction.get("POLL_TIMESTAMP", "")
        weather_time = weather.get("POLL_TIMESTAMP", "")

        if not pred_time or not weather_time:
            return None

        formats = [
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%dT%H:%M:%S.%fZ",
        ]

        t1 = t2 = None
        for fmt in formats:
            try:
                t1 = datetime.strptime(pred_time[:26], fmt[:26])
                break
            except Exception:
                continue

        for fmt in formats:
            try:
                t2 = datetime.strptime(weather_time[:26], fmt[:26])
                break
            except Exception:
                continue

        if t1 and t2:
            return abs(int((t2 - t1).total_seconds()))
        return None

    except Exception:
        return None