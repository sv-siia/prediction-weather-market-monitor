"""
city_validator.py
------------------
Orchestrator for city validation pipeline.

Combines:
  - city_extractor.py  (Claude + regex extraction)
  - city_geocoder.py   (Nominatim geocoding)
  - city_cache.py      (two-level cache)

Public API:
  is_valid_city(city, cache)              → bool
  is_valid_city_with_coords(question, cache) → (city, coords) | None
  validate_cities_batch(cities, cache)    → dict
"""

from producers.utils.city_extractor import extract_city
from producers.utils.city_geocoder import geocode_city
from producers.utils.city_cache import load_cache


def is_valid_city(city: str, geocache: dict = None) -> bool:
    if not city:
        return False
    # Basic checks — no geocoding needed
    if len(city) < 2 or len(city) > 50:
        return False
    if not city[0].isupper():
        return False
    if any(c.isdigit() for c in city):
        return False
    if len(city.split()) > 5:
        return False
    if geocache is None:
        geocache = load_cache()
    coords = geocode_city(city, geocache)
    return coords is not None

def is_valid_city_with_coords(
        question: str, cache: dict = None) -> tuple[str, dict] | None:
    """
    Extracts city from question and returns coordinates.
    Returns (city_name, coords) or None.
    """
    if cache is None:
        cache = load_cache()
    city = extract_city(question, cache)
    if not city:
        return None
    coords = geocode_city(city, cache)
    if not coords:
        return None
    return city, coords


def validate_cities_batch(
        cities: list, geocache: dict = None) -> dict:
    """
    Validates a batch of city names.
    Returns dict: {city: True/False}
    """
    if geocache is None:
        geocache = load_cache()
    return {city: is_valid_city(city, geocache) for city in cities}