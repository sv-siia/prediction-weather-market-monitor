"""
Extra coverage for city_validator.py — is_valid_city_with_coords function.
"""
import sys
import os
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestIsValidCityWithCoords:
    """Tests for is_valid_city_with_coords — lines 45-53."""

    def _mock_cache(self):
        return {}

    def test_returns_none_when_city_not_extracted(self):
        from producers.utils.city_validator import is_valid_city_with_coords
        with patch("producers.utils.city_validator.extract_city", return_value=None):
            result = is_valid_city_with_coords("random garbage question ???")
        assert result is None

    def test_returns_none_when_geocoding_fails(self):
        from producers.utils.city_validator import is_valid_city_with_coords
        with patch("producers.utils.city_validator.extract_city", return_value="London"), \
             patch("producers.utils.city_validator.geocode_city", return_value=None):
            result = is_valid_city_with_coords("Will it rain in London?")
        assert result is None

    def test_returns_city_and_coords_on_success(self):
        from producers.utils.city_validator import is_valid_city_with_coords
        coords = {"lat": 51.5, "lon": -0.12, "tz": "Europe/London"}
        with patch("producers.utils.city_validator.extract_city", return_value="London"), \
             patch("producers.utils.city_validator.geocode_city", return_value=coords):
            result = is_valid_city_with_coords("Will it rain in London?")
        assert result is not None
        city, c = result
        assert city == "London"
        assert c["lat"] == 51.5

    def test_passes_cache_to_functions(self):
        from producers.utils.city_validator import is_valid_city_with_coords
        cache  = {"questions": {}, "cities": {}}
        coords = {"lat": 48.8, "lon": 2.3, "tz": "Europe/Paris"}
        with patch("producers.utils.city_validator.extract_city", return_value="Paris") as mock_ext, \
             patch("producers.utils.city_validator.geocode_city", return_value=coords):
            is_valid_city_with_coords("Will it snow in Paris?", cache=cache)
            mock_ext.assert_called_once_with("Will it snow in Paris?", cache)

    def test_uses_load_cache_when_no_cache_passed(self):
        from producers.utils.city_validator import is_valid_city_with_coords
        empty_cache = {"questions": {}, "cities": {}}
        with patch("producers.utils.city_validator.load_cache", return_value=empty_cache) as mock_load, \
             patch("producers.utils.city_validator.extract_city", return_value=None):
            is_valid_city_with_coords("some question")
            mock_load.assert_called_once()
