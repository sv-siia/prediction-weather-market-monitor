"""
city_geocoder.py
-----------------
Geocoding layer using Nominatim (OpenStreetMap).

Converts city names to coordinates (lat, lon, timezone).
Results are cached to avoid repeated API calls.

Rate limit: 1 request/second (Nominatim policy)
"""

import time
import logging
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from timezonefinder import TimezoneFinder

from producers.utils.city_cache import (
    load_cache, get_cached_city, cache_city, cache_invalid_city
)

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
USER_AGENT    = "prediction_market_monitor_v2"
GEOCODE_DELAY = 1.1  # seconds between requests (Nominatim policy)

# ── Valid place types for geocoding ──────────────────────────────
VALID_CLASSES = {"place", "boundary", "administrative"}

INVALID_TYPES = {
    "kiosk", "shop", "footway", "highway", "street",
    "road", "building", "house", "hotel", "restaurant",
    "cafe", "bar", "pub", "park", "garden", "parking",
    "bus_stop", "station", "airport", "ferry_terminal",
}

# ── Known timezones for common cities ────────────────────────────
KNOWN_TIMEZONES = {
    "London":        "Europe/London",
    "New York":      "America/New_York",
    "New York City": "America/New_York",
    "NYC":           "America/New_York",
    "Paris":         "Europe/Paris",
    "Berlin":        "Europe/Berlin",
    "Tokyo":         "Asia/Tokyo",
    "Sydney":        "Australia/Sydney",
    "Toronto":       "America/Toronto",
    "Warsaw":        "Europe/Warsaw",
    "Amsterdam":     "Europe/Amsterdam",
    "Moscow":        "Europe/Moscow",
    "Dubai":         "Asia/Dubai",
    "Singapore":     "Asia/Singapore",
    "Chicago":       "America/Chicago",
    "Los Angeles":   "America/Los_Angeles",
    "San Francisco": "America/Los_Angeles",
    "Miami":         "America/New_York",
    "Seattle":       "America/Los_Angeles",
    "Denver":        "America/Denver",
    "Istanbul":      "Europe/Istanbul",
    "Dublin":        "Europe/Dublin",
}


def get_timezone(lat: float, lon: float, city_name: str = "") -> str:
    """
    Returns timezone string for coordinates.
    Uses known timezones first, then timezonefinder.
    Falls back to UTC if all else fails.
    """
    if city_name in KNOWN_TIMEZONES:
        return KNOWN_TIMEZONES[city_name]

    try:
        tf = TimezoneFinder()
        tz = tf.timezone_at(lat=lat, lng=lon)
        if tz:
            return tz
    except Exception:
        pass

    return "UTC"


def geocode_city(city_name: str, cache: dict = None) -> dict | None:
    """
    Geocodes a city name to coordinates.

    Returns:
      {lat, lon, tz, verified, source} if found and valid
      None if not found or not a real city

    Pipeline:
      1. Check cache
      2. Call Nominatim API
      3. Validate place type (reject shops, roads, etc.)
      4. Save to cache
    """
    if cache is None:
        cache = load_cache()

    # Step 1: Check cache
    cached = get_cached_city(city_name, cache)
    if cached is not None:
        if cached.get("verified"):
            log.debug(f"Cache hit: '{city_name}'")
            return cached
        else:
            log.debug(f"Cache hit (invalid): '{city_name}'")
            return None

    # Step 2: Geocode via Nominatim
    log.info(f"Geocoding: '{city_name}'")
    try:
        geolocator = Nominatim(
            user_agent=USER_AGENT,
            timeout=10
        )
        time.sleep(GEOCODE_DELAY)

        location = geolocator.geocode(city_name, language="en")

        # Step 3: Not found
        if not location:
            log.warning(f"Not found: '{city_name}'")
            cache_invalid_city(city_name, cache)
            return None

        # Step 4: Validate place type
        raw         = location.raw
        place_class = raw.get("class", "")
        place_type  = raw.get("type", "")

        if place_class not in VALID_CLASSES or place_type in INVALID_TYPES:
            log.warning(
                f"Rejected '{city_name}': "
                f"class={place_class} type={place_type}"
            )
            cache_invalid_city(city_name, cache)
            return None

        # Step 5: Get coordinates and timezone
        lat = location.latitude
        lon = location.longitude
        tz  = get_timezone(lat, lon, city_name)

        log.info(
            f"Found: '{city_name}' → "
            f"({lat:.4f}, {lon:.4f}) tz={tz}"
        )

        # Step 6: Save to cache
        cache_city(city_name, lat, lon, tz, "nominatim", cache)

        return {
            "lat":      lat,
            "lon":      lon,
            "tz":       tz,
            "verified": True,
            "source":   "nominatim",
        }

    except (GeocoderTimedOut, GeocoderServiceError) as e:
        log.error(f"Geocoding error for '{city_name}': {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error for '{city_name}': {e}")
        return None