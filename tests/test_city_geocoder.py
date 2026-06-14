"""
test_city_geocoder.py
----------------------
Unit tests for city_geocoder.py
"""

import pytest
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producers.utils.city_geocoder import (
    get_timezone,
    geocode_city,
    KNOWN_TIMEZONES,
)


class TestGetTimezone:

    def test_known_city_timezone(self):
        assert get_timezone(51.5, -0.1, "London") == "Europe/London"

    def test_known_city_new_york(self):
        assert get_timezone(40.7, -74.0, "New York City") == "America/New_York"

    def test_unknown_city_uses_timezonefinder(self):
        result = get_timezone(51.5074, -0.1278, "Unknown City")
        assert result is not None
        assert isinstance(result, str)

    def test_fallback_to_utc(self):
        with patch("producers.utils.city_geocoder.TimezoneFinder") as mock_tf:
            mock_tf.return_value.timezone_at.return_value = None
            result = get_timezone(0, 0, "Unknown")
        assert result == "UTC"


class TestGeocodeCity:

    def _make_location(self, lat, lon,
                       place_class="place", place_type="city"):
        location = MagicMock()
        location.latitude  = lat
        location.longitude = lon
        location.raw = {"class": place_class, "type": place_type}
        return location

    def test_returns_cached_valid_city(self):
        cache = {
            "questions": {},
            "cities": {
                "London": {
                    "lat": 51.5074, "lon": -0.1278,
                    "tz": "Europe/London", "verified": True,
                    "source": "nominatim"
                }
            }
        }
        result = geocode_city("London", cache)
        assert result is not None
        assert result["lat"] == 51.5074
        assert result["verified"] is True

    def test_returns_none_for_cached_invalid(self):
        cache = {
            "questions": {},
            "cities": {"Pokemon Go": {"verified": False}}
        }
        result = geocode_city("Pokemon Go", cache)
        assert result is None

    def test_geocodes_new_city(self):
        cache = {"questions": {}, "cities": {}}
        location = self._make_location(51.5074, -0.1278)

        with patch("producers.utils.city_geocoder.Nominatim") as mock_nom, \
             patch("producers.utils.city_geocoder.time.sleep"), \
             patch("producers.utils.city_cache.save_cache"):
            mock_nom.return_value.geocode.return_value = location
            result = geocode_city("London", cache)

        assert result is not None
        assert result["lat"] == 51.5074
        assert result["verified"] is True
        assert result["source"] == "nominatim"

    def test_returns_none_for_not_found(self):
        cache = {"questions": {}, "cities": {}}

        with patch("producers.utils.city_geocoder.Nominatim") as mock_nom, \
             patch("producers.utils.city_geocoder.time.sleep"), \
             patch("producers.utils.city_cache.save_cache"):
            mock_nom.return_value.geocode.return_value = None
            result = geocode_city("ZaZa Rain God", cache)

        assert result is None

    def test_rejects_shop_type(self):
        cache = {"questions": {}, "cities": {}}
        location = self._make_location(50.4, 30.6, "shop", "kiosk")

        with patch("producers.utils.city_geocoder.Nominatim") as mock_nom, \
             patch("producers.utils.city_geocoder.time.sleep"), \
             patch("producers.utils.city_cache.save_cache"):
            mock_nom.return_value.geocode.return_value = location
            result = geocode_city("Pokemon Go", cache)

        assert result is None

    def test_rejects_highway_type(self):
        cache = {"questions": {}, "cities": {}}
        location = self._make_location(41.0, -73.8, "highway", "residential")

        with patch("producers.utils.city_geocoder.Nominatim") as mock_nom, \
             patch("producers.utils.city_geocoder.time.sleep"), \
             patch("producers.utils.city_cache.save_cache"):
            mock_nom.return_value.geocode.return_value = location
            result = geocode_city("Some Road Name", cache)

        assert result is None

    def test_accepts_boundary_type(self):
        cache = {"questions": {}, "cities": {}}
        location = self._make_location(51.5, -0.1, "boundary", "administrative")

        with patch("producers.utils.city_geocoder.Nominatim") as mock_nom, \
             patch("producers.utils.city_geocoder.time.sleep"), \
             patch("producers.utils.city_cache.save_cache"):
            mock_nom.return_value.geocode.return_value = location
            result = geocode_city("Greater London", cache)

        assert result is not None
        assert result["verified"] is True

    def test_handles_geocoding_exception(self):
        cache = {"questions": {}, "cities": {}}
        from geopy.exc import GeocoderTimedOut

        with patch("producers.utils.city_geocoder.Nominatim") as mock_nom, \
             patch("producers.utils.city_geocoder.time.sleep"):
            mock_nom.return_value.geocode.side_effect = GeocoderTimedOut()
            result = geocode_city("London", cache)

        assert result is None