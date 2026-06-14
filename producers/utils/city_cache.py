"""
city_cache.py
--------------
Unified cache management for city validation pipeline.

Two-level cache:
  Level 1: Question cache  → question text → normalized city name
  Level 2: City cache      → city name → {lat, lon, tz, verified}

Cache file: data/city_cache.json
"""

import os
import json
import logging
import hashlib

log = logging.getLogger(__name__)

CACHE_FILE = "data/city_cache.json"

# ── Cache structure ───────────────────────────────────────────────
DEFAULT_CACHE = {
    "questions": {},  # question_hash → city_name or None
    "cities":    {},  # city_name → {lat, lon, tz, verified, source}
}

# ── Load / Save ───────────────────────────────────────────────────
def load_cache() -> dict:
    """Loads cache from disk. Returns empty cache if not found."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Ensure both keys exist
                if "questions" not in data:
                    data["questions"] = {}
                if "cities" not in data:
                    data["cities"] = {}
                log.info(
                    f"Loaded cache: "
                    f"{len(data['questions'])} questions, "
                    f"{len(data['cities'])} cities"
                )
                return data
        except Exception as e:
            log.warning(f"Could not load cache: {e}")
    return dict(DEFAULT_CACHE)


def save_cache(cache: dict) -> None:
    """Saves cache to disk."""
    try:
        os.makedirs("data", exist_ok=True)
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.warning(f"Could not save cache: {e}")


# ── Question cache ────────────────────────────────────────────────
def question_hash(question: str) -> str:
    import re
    normalized = question.lower()
    normalized = re.sub(
        r'\b(today|tomorrow|tonight|yesterday|'
        r'monday|tuesday|wednesday|thursday|friday|saturday|sunday|'
        r'january|february|march|april|may|june|july|august|'
        r'september|october|november|december|'
        r'this week|next week|this weekend|next weekend|'
        r'on \d{4}-\d{2}-\d{2}|'
        r'on [a-z]+ \d+|'
        r'\d{4})\b',
        '', normalized
    )
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return hashlib.md5(normalized.encode()).hexdigest()[:12]


def get_cached_question(question: str, cache: dict) -> str | None:
    """
    Returns cached city name for a question.
    Returns None if not cached.
    Returns "INVALID" if question was previously rejected.
    """
    key = question_hash(question)
    return cache["questions"].get(key)


def cache_question(question: str, city_name: str | None, cache: dict) -> None:
    """
    Caches question → city_name mapping.
    city_name=None means question has no valid city.
    """
    key = question_hash(question)
    cache["questions"][key] = city_name
    save_cache(cache)


# ── City cache ────────────────────────────────────────────────────
def get_cached_city(city_name: str, cache: dict) -> dict | None:
    """
    Returns cached city data: {lat, lon, tz, verified, source}
    Returns None if not cached.
    Returns {"verified": False} if city was previously rejected.
    """
    return cache["cities"].get(city_name)


def cache_city(
    city_name: str,
    lat: float,
    lon: float,
    tz: str = "UTC",
    source: str = "geocoder",
    cache: dict = None
) -> None:
    """Caches city coordinates."""
    if cache is None:
        return
    cache["cities"][city_name] = {
        "lat":      lat,
        "lon":      lon,
        "tz":       tz,
        "verified": True,
        "source":   source,
    }
    save_cache(cache)


def cache_invalid_city(city_name: str, cache: dict) -> None:
    """Marks a city as invalid (failed geocoding)."""
    cache["cities"][city_name] = {"verified": False}
    save_cache(cache)


# ── Stats ─────────────────────────────────────────────────────────
def cache_stats(cache: dict) -> dict:
    """Returns cache statistics."""
    cities      = cache.get("cities", {})
    verified    = sum(1 for c in cities.values() if c.get("verified"))
    invalid     = sum(1 for c in cities.values() if not c.get("verified"))
    return {
        "total_questions": len(cache.get("questions", {})),
        "total_cities":    len(cities),
        "verified_cities": verified,
        "invalid_cities":  invalid,
    }