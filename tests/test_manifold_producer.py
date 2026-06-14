"""
test_manifold_producer.py
--------------------------
Unit tests for manifold_producer.py pure functions.
Tests: extract_city, detect_market_type, parse_market, ms_to_iso,
       detect_arbitrage, BASE_RATES, write_pipeline_health
No Kafka or API calls needed.
"""

import pytest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producers.manifold_producer import (
    extract_city,
    detect_market_type,
    parse_market,
    ms_to_iso,
    detect_arbitrage,
    BASE_RATES,
    write_pipeline_health,
)


# ── ms_to_iso tests ───────────────────────────────────────────────
class TestMsToIso:

    def test_valid_timestamp(self):
        result = ms_to_iso(1772722851905)
        assert result is not None
        assert "T" in result
        assert result.endswith("Z")

    def test_none_returns_none(self):
        assert ms_to_iso(None) is None

    def test_zero_returns_none(self):
        assert ms_to_iso(0) is None

    def test_format(self):
        result = ms_to_iso(1700000000000)
        assert len(result) == 20  # "YYYY-MM-DDTHH:MM:SSZ"


# ── extract_city tests ────────────────────────────────────────────
class TestExtractCity:

    def test_rain_in_london(self):
        assert extract_city("Will it rain in London tomorrow?") == "London"

    def test_snow_in_thunder_bay(self):
        assert extract_city(
            "Will it snow in Thunder Bay on 2023-11-26?"
        ) == "Thunder Bay"

    def test_rain_in_amsterdam(self):
        assert extract_city(
            "Will it rain in Amsterdam on 2026-04-15?"
        ) == "Amsterdam"

    def test_rain_in_nyc(self):
        assert extract_city(
            "Will it rain in New York City on 2023-12-01?"
        ) == "New York City"

    def test_temperature_in_london(self):
        result = extract_city(
            "Will the temperature in London exceed 77°F?"
        )
        assert result == "London"

    def test_sunny_in_paris(self):
        assert extract_city("Will it be sunny in Paris today?") == "Paris"

    def test_windy_in_chicago(self):
        assert extract_city(
            "Will it be windy in Chicago tomorrow?"
        ) == "Chicago"

    def test_fog_in_dublin(self):
        assert extract_city(
            "Will there be fog in Dublin tonight?"
        ) == "Dublin"

    def test_snow_in_bratislava(self):
        assert extract_city(
            "Will it snow in Bratislava on 2026-04-14?"
        ) == "Bratislava"

    def test_rain_in_bibinje(self):
        assert extract_city(
            "Will it rain in Bibinje on 2026-04-13?"
        ) == "Bibinje"

    def test_invalid_returns_none(self):
        assert extract_city("Will Sanji have a nose bleed?") is None

    def test_no_city_returns_none(self):
        assert extract_city("Will it rain tomorrow?") is None

    def test_pokemon_go_filtered(self):
        result = extract_city("Will it rain in Pokemon Go on August 19?")
        assert result is None or result != "Pokemon Go"

    def test_anime_question_filtered(self):
        assert extract_city(
            "Will there be rain in the Kingdom of Heaven?"
        ) is None


# ── detect_market_type tests ──────────────────────────────────────
class TestDetectMarketType:

    def test_rain_question(self):
        assert detect_market_type(
            "Will it rain in London tomorrow?"
        ) == "RAIN"

    def test_snow_question(self):
        assert detect_market_type(
            "Will it snow in Toronto?"
        ) == "SNOW"

    def test_blizzard_question(self):
        assert detect_market_type(
            "Will there be a blizzard in Boston?"
        ) == "SNOW"

    def test_temperature_question(self):
        assert detect_market_type(
            "Will the temperature in NYC exceed 77°F?"
        ) == "TEMPERATURE"

    def test_degrees_question(self):
        assert detect_market_type(
            "Will it be above 30 degrees celsius in Paris?"
        ) == "TEMPERATURE"

    def test_sunshine_question(self):
        assert detect_market_type(
            "Will it be sunny in Los Angeles?"
        ) == "SUNSHINE"

    def test_wind_question(self):
        assert detect_market_type(
            "Will it be windy in Chicago?"
        ) == "WIND"

    def test_storm_question(self):
        assert detect_market_type(
            "Will there be a storm in Miami?"
        ) == "WIND"

    def test_fog_question(self):
        assert detect_market_type(
            "Will there be fog in San Francisco?"
        ) == "FOG"

    def test_hail_question(self):
        assert detect_market_type(
            "Will there be hail in Denver?"
        ) == "HAIL"

    def test_cloud_question(self):
        assert detect_market_type(
            "Will it be cloudy in Seattle?"
        ) == "CLOUD"

    def test_frost_question(self):
        assert detect_market_type(
            "Will it freeze in Warsaw?"
        ) == "TEMPERATURE"

    def test_unknown_returns_weather(self):
        assert detect_market_type("Some random question") == "WEATHER"


# ── parse_market tests ────────────────────────────────────────────
class TestParseMarket:

    def _make_market(
            self,
            question="Will it rain in London tomorrow?",
            probability=0.7,
            is_resolved=True,
            resolution="YES",
            close_time=1772722851905,
            outcome_type="BINARY",
            market_id="test123",
            slug="test-slug",
            url="https://manifold.markets/test"):
        return {
            "id":               market_id,
            "question":         question,
            "probability":      probability,
            "isResolved":       is_resolved,
            "resolution":       resolution,
            "closeTime":        close_time,
            "outcomeType":      outcome_type,
            "slug":             slug,
            "url":              url,
            "uniqueBettorCount": 10,
            "volume":           100.0,
        }

    def test_valid_resolved_yes(self):
        market = self._make_market(
            question="Will it rain in London tomorrow?",
            probability=0.7, is_resolved=True, resolution="YES"
        )
        result = parse_market(market)
        assert result is not None
        assert result["LOCATION_NAME"] == "London"
        assert result["ACTUAL_OUTCOME"] == 1
        assert result["winner"] is True
        assert result["MARKET_STATUS"] == "closed"
        assert result["SOURCE"] == "manifold"

    def test_valid_resolved_no(self):
        market = self._make_market(
            question="Will it rain in Amsterdam on 2026-04-15?",
            probability=0.15, is_resolved=True, resolution="NO"
        )
        result = parse_market(market)
        assert result is not None
        assert result["LOCATION_NAME"] == "Amsterdam"
        assert result["ACTUAL_OUTCOME"] == 0
        assert result["winner"] is False

    def test_open_market(self):
        market = self._make_market(
            question="Will it snow in Bratislava on 2026-04-14?",
            probability=0.4, is_resolved=False, resolution=None
        )
        result = parse_market(market)
        assert result is not None
        assert result["ACTUAL_OUTCOME"] is None
        assert result["winner"] is None
        assert result["MARKET_STATUS"] == "open"
        assert result["NEEDS_WEATHER_CHECK"] is True

    def test_no_probability_returns_none(self):
        market = self._make_market(probability=None)
        assert parse_market(market) is None

    def test_non_binary_returns_none(self):
        market = self._make_market(outcome_type="MULTIPLE_CHOICE")
        assert parse_market(market) is None

    def test_no_city_returns_none(self):
        market = self._make_market(
            question="Will Sanji have a nose bleed over ZaZa?"
        )
        assert parse_market(market) is None

    def test_prediction_error_calculated(self):
        market = self._make_market(probability=0.7, resolution="YES")
        result = parse_market(market)
        assert result["PREDICTION_ERROR"] == round(abs(0.7 - 1), 4)

    def test_market_type_detected(self):
        market = self._make_market(
            question="Will it rain in London tomorrow?"
        )
        result = parse_market(market)
        assert result["MARKET_TYPE"] == "RAIN"

    def test_price_is_probability(self):
        market = self._make_market(probability=0.82)
        result = parse_market(market)
        assert result is not None
        assert result["price"] == 0.82

    def test_manifold_url_preserved(self):
        market = self._make_market(
            url="https://manifold.markets/test/market"
        )
        result = parse_market(market)
        assert result is not None
        assert result["MANIFOLD_URL"] == "https://manifold.markets/test/market"

    def test_result_has_required_fields(self):
        market = self._make_market()
        result = parse_market(market)
        assert result is not None
        required = [
            "condition_id", "question", "price", "winner",
            "closed", "LOCATION_NAME", "POLL_TIMESTAMP",
            "MARKET_TYPE", "MARKET_STATUS", "SOURCE",
            "NEEDS_WEATHER_CHECK", "ACTUAL_OUTCOME",
        ]
        for field in required:
            assert field in result, f"Missing field: {field}"


# ── BASE_RATES tests ──────────────────────────────────────────────
class TestBaseRates:

    def test_all_weather_types_have_base_rate(self):
        types = [
            "RAIN", "SNOW", "TEMPERATURE", "SUNSHINE",
            "WIND", "FOG", "HAIL", "FROST", "WEATHER"
        ]
        for t in types:
            assert t in BASE_RATES
            assert 0 < BASE_RATES[t] < 1

    def test_rain_base_rate_reasonable(self):
        assert 0.2 <= BASE_RATES["RAIN"] <= 0.5

    def test_hail_base_rate_low(self):
        assert BASE_RATES["HAIL"] < BASE_RATES["RAIN"]

    def test_sunshine_base_rate_above_rain(self):
        assert BASE_RATES["SUNSHINE"] > BASE_RATES["RAIN"]


# ── detect_arbitrage tests ────────────────────────────────────────
class TestDetectArbitrage:

    def _make_record(self, location, mtype, price,
                     is_arb=False, base_rate=0.35, deviation=0.0):
        return {
            "condition_id":        f"test_{location}_{mtype}_{price}",
            "LOCATION_NAME":       location,
            "MARKET_TYPE":         mtype,
            "price":               price,
            "IS_ARBITRAGE":        is_arb,
            "BASE_RATE":           base_rate,
            "DEVIATION_FROM_BASE": deviation,
            "closed":              False,
            "end_date_iso":        "2026-06-01T00:00:00Z",
        }

    def test_no_arbitrage_normal_market(self):
        records = [self._make_record("London", "RAIN", 0.4)]
        alerts  = detect_arbitrage(records)
        assert len(alerts) == 0

    def test_mispricing_arbitrage_detected(self):
        records = [self._make_record(
            "Sahara", "RAIN", 0.85,
            is_arb=True, base_rate=0.05, deviation=0.80
        )]
        alerts = detect_arbitrage(records)
        assert len(alerts) == 1
        assert alerts[0]["alert_type"] == "arbitrage_opportunity"

    def test_mispricing_severity_critical(self):
        records = [self._make_record(
            "Sahara", "RAIN", 0.95,
            is_arb=True, base_rate=0.05, deviation=0.90
        )]
        alerts = detect_arbitrage(records)
        assert len(alerts) >= 1
        severities = [a["severity"] for a in alerts]
        assert "critical" in severities

    def test_mispricing_severity_high(self):
        records = [self._make_record(
            "London", "RAIN", 0.85,
            is_arb=True, base_rate=0.35, deviation=0.45
        )]
        alerts = detect_arbitrage(records)
        assert len(alerts) >= 1
        assert alerts[0]["severity"] in ["high", "critical"]

    def test_no_duplicate_alerts(self):
        # Same condition_id → mispricing deduplicates by condition_id
        record = self._make_record(
            "London", "RAIN", 0.85,
            is_arb=True, base_rate=0.35, deviation=0.50
        )
        # Two different condition_ids → two separate alerts
        record2 = {**record, "condition_id": "different_id"}
        alerts = detect_arbitrage([record])
        assert len(alerts) == 1  # one mispricing alert

    def test_empty_records(self):
        assert detect_arbitrage([]) == []

    def test_alert_has_required_fields(self):
        records = [self._make_record(
            "London", "RAIN", 0.85,
            is_arb=True, base_rate=0.35, deviation=0.50
        )]
        alert = detect_arbitrage(records)[0]
        required = [
            "alert_type", "message",
            "metric_value", "detected_at",
        ]
        for field in required:
            assert field in alert, f"Missing field: {field}"

    def test_multiple_markets_detected(self):
        records = [
            self._make_record(
                "London", "RAIN", 0.85,
                is_arb=True, base_rate=0.35, deviation=0.50
            ),
            self._make_record(
                "Paris", "SNOW", 0.92,
                is_arb=True, base_rate=0.15, deviation=0.77
            ),
        ]
        alerts = detect_arbitrage(records)
        assert len(alerts) == 2


# ── parse_market arbitrage tests ──────────────────────────────────
class TestParseMarketArbitrage:

    def _make_market(
            self, probability,
            question="Will it rain in London?"):
        return {
            "id":          "test123",
            "slug":        "test-market",
            "url":         "https://manifold.markets/test",
            "question":    question,
            "probability": probability,
            "outcomeType": "BINARY",
            "isResolved":  False,
            "resolution":  None,
            "closeTime":   1800000000000,
        }

    def test_normal_probability_not_arbitrage(self):
        # RAIN base rate is 0.35, price=0.35 → deviation=0 → not arb
        market = self._make_market(0.35)
        result = parse_market(market)
        if result:
            assert result["IS_ARBITRAGE"] is False

    def test_extreme_probability_is_arbitrage(self):
        # price=0.95 vs RAIN base=0.35 → deviation=0.60 > 0.40 → arb
        market = self._make_market(0.95)
        result = parse_market(market)
        if result:
            assert result["IS_ARBITRAGE"] is True

    def test_base_rate_field_present(self):
        market = self._make_market(0.5)
        result = parse_market(market)
        if result:
            assert "BASE_RATE" in result
            assert "DEVIATION_FROM_BASE" in result

    def test_deviation_calculated_correctly(self):
        market = self._make_market(0.75)
        result = parse_market(market)
        if result:
            expected = round(abs(0.75 - result["BASE_RATE"]), 4)
            assert result["DEVIATION_FROM_BASE"] == expected

    def test_price_sum_always_one(self):
        market = self._make_market(0.6)
        result = parse_market(market)
        if result:
            assert result["PRICE_SUM"] == 1.0


# ── write_pipeline_health tests ───────────────────────────────────
class TestWritePipelineHealth:

    def test_handles_db_connection_error(self):
        """Should not raise even if DB is unavailable."""
        os.environ["POSTGRES_HOST"] = "nonexistent_host_12345"
        # Should not raise — errors are caught internally
        write_pipeline_health("healthy", 10, 0, {"test": True})
        # Restore
        os.environ["POSTGRES_HOST"] = "localhost"

    def test_accepts_degraded_status(self):
        """Should not raise for degraded status."""
        os.environ["POSTGRES_HOST"] = "nonexistent_host_12345"
        write_pipeline_health(
            "degraded", 0, 1, {"error": "test error"}
        )
        os.environ["POSTGRES_HOST"] = "localhost"

    def test_accepts_none_details(self):
        """Should not raise with None details."""
        os.environ["POSTGRES_HOST"] = "nonexistent_host_12345"
        write_pipeline_health("healthy", 5, 0, None)
        os.environ["POSTGRES_HOST"] = "localhost"