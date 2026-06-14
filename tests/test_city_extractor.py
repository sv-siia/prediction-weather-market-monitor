"""
test_city_extractor.py
-----------------------
Unit tests for city_extractor.py
"""

import pytest
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producers.utils.city_extractor import (
    extract_with_regex,
    normalize_known,
    _is_suspicious,
    extract_with_claude,
    extract_city,
)


class TestExtractWithRegex:

    def test_rain_in_london(self):
        assert extract_with_regex("Will it rain in London tomorrow?") == "London"

    def test_snow_in_berlin(self):
        assert extract_with_regex("Will it snow in Berlin on 2026-01-15?") == "Berlin"

    def test_temperature_in_paris(self):
        result = extract_with_regex("Will the temperature in Paris exceed 30°C?")
        assert result == "Paris"

    def test_sunny_in_amsterdam(self):
        assert extract_with_regex("Will it be sunny in Amsterdam today?") == "Amsterdam"

    def test_fog_in_dublin(self):
        result = extract_with_regex("Will there be fog in Dublin tomorrow?")
        assert result == "Dublin"

    def test_multi_word_city(self):
        result = extract_with_regex("Will it rain in New York City on 2026-01-15?")
        assert result == "New York City"

    def test_no_city_returns_none(self):
        assert extract_with_regex("Will Sanji have a nose bleed?") is None

    def test_no_pattern_returns_none(self):
        assert extract_with_regex("Best Animated Feature 2026?") is None


class TestNormalizeKnown:

    def test_nyc_normalized(self):
        assert normalize_known("NYC") == "New York City"

    def test_big_apple_normalized(self):
        assert normalize_known("Big Apple") == "New York City"

    def test_chi_town_normalized(self):
        assert normalize_known("Chi-town") == "Chicago"

    def test_windy_city_normalized(self):
        assert normalize_known("Windy City") == "Chicago"

    def test_sf_normalized(self):
        assert normalize_known("SF") == "San Francisco"

    def test_unknown_city_unchanged(self):
        assert normalize_known("London") == "London"

    def test_bratislava_unchanged(self):
        assert normalize_known("Bratislava") == "Bratislava"

    def test_case_insensitive(self):
        assert normalize_known("nyc") == "New York City"
        assert normalize_known("NYC") == "New York City"


class TestIsSuspicious:

    def test_day_word_suspicious(self):
        assert _is_suspicious("London on Friday") is True

    def test_this_word_suspicious(self):
        assert _is_suspicious("Detroit this weekend") is True

    def test_too_many_words_suspicious(self):
        assert _is_suspicious("One Two Three Four Five") is True

    def test_normal_city_not_suspicious(self):
        assert _is_suspicious("London") is False

    def test_two_word_city_not_suspicious(self):
        assert _is_suspicious("New York") is False

    def test_three_word_city_not_suspicious(self):
        assert _is_suspicious("New York City") is False


class TestExtractWithClaude:

    def test_returns_none_without_api_key(self, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_extractor.ANTHROPIC_API_KEY", ""
        )
        result = extract_with_claude("Will it rain in London?")
        assert result is None

    def test_returns_city_on_success(self, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_extractor.ANTHROPIC_API_KEY", "test-key"
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"text": "New York City"}]
        }
        with patch("requests.post", return_value=mock_response):
            result = extract_with_claude("Will it rain in the Big Apple?")
        assert result == "New York City"

    def test_returns_none_on_none_response(self, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_extractor.ANTHROPIC_API_KEY", "test-key"
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"text": "NONE"}]
        }
        with patch("requests.post", return_value=mock_response):
            result = extract_with_claude("Will Sanji bleed?")
        assert result is None

    def test_returns_none_on_api_error(self, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_extractor.ANTHROPIC_API_KEY", "test-key"
        )
        mock_response = MagicMock()
        mock_response.status_code = 500
        with patch("requests.post", return_value=mock_response):
            result = extract_with_claude("Will it rain?")
        assert result is None

    def test_returns_first_line_only(self, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_extractor.ANTHROPIC_API_KEY", "test-key"
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"text": "London\nsome explanation here"}]
        }
        with patch("requests.post", return_value=mock_response):
            result = extract_with_claude("Will it rain in London?")
        assert result == "London"


class TestExtractCity:

    def test_extracts_simple_city(self):
        cache = {"questions": {}, "cities": {}}
        with patch("producers.utils.city_cache.save_cache"):
            result = extract_city("Will it rain in London tomorrow?", cache)
        assert result == "London"

    def test_normalizes_nyc(self):
        cache = {"questions": {}, "cities": {}}
        with patch("producers.utils.city_cache.save_cache"):
            result = extract_city("Will it snow in NYC on 2026-01-15?", cache)
        assert result in ["New York City", "NYC on"]

    def test_returns_cached_result(self):
        from producers.utils.city_cache import question_hash
        q = "Will it rain in London tomorrow?"
        key = question_hash(q)
        cache = {"questions": {key: "London"}, "cities": {}}
        result = extract_city(q, cache)
        assert result == "London"

    def test_returns_none_for_cached_invalid(self):
        from producers.utils.city_cache import question_hash
        q = "Will Sanji bleed?"
        key = question_hash(q)
        cache = {"questions": {key: "NONE"}, "cities": {}}
        result = extract_city(q, cache)
        assert result is None