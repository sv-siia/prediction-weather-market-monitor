"""
city_extractor.py
------------------
Extracts and normalizes city names from weather market questions.

Two-stage extraction:
  Stage 1: Regex extraction (fast, free)
  Stage 2: Claude analysis (full context, only if needed)

Claude analyzes the FULL question context to determine:
  - Is this a real weather market question?
  - What is the actual city name?
  - Normalize nicknames and abbreviations

Examples:
  "Will it rain in London tomorrow?"        → "London"
  "Will it snow in NYC on Friday?"          → "New York City"
  "Will it rain in the Big Apple?"          → "New York City"
  "Will it rain in January?"                → None (month, not city)
  "Will Sanji bleed in ZaZa Rain God?"      → None (not weather)
  "Will Amazon Rainforest flood?"           → None (not a city)
  "Will it rain in Rain City, Germany?"     → "Rain City"
"""

import re
import os
import logging
import requests

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_URL     = "https://api.anthropic.com/v1/messages"

# ── Known normalizations (instant, no API) ────────────────────────
KNOWN_NORMALIZATIONS = {
    "nyc":            "New York City",
    "new york":       "New York City",
    "the big apple":  "New York City",
    "big apple":      "New York City",
    "la":             "Los Angeles",
    "chi-town":       "Chicago",
    "the windy city": "Chicago",
    "windy city":     "Chicago",
    "frisco":         "San Francisco",
    "sf":             "San Francisco",
    "dc":             "Washington",
}

# ── Claude prompt ─────────────────────────────────────────────────
CLAUDE_PROMPT = """You are analyzing weather prediction market questions.

Your task: Extract the city name from the question.

Rules:
1. Return ONLY the standard English city name
2. Normalize nicknames: "Big Apple" → "New York City", "Chi-town" → "Chicago"
3. Normalize abbreviations: "NYC" → "New York City", "LA" → "Los Angeles"
4. Remove country/state suffix: "London, UK" → "London"
5. Return NONE if:
   - No real city in the question
   - The "city" is actually a concept (Rain, January, Pokemon, etc.)
   - It's not a weather market question
   - The location is too vague (e.g. "Europe", "the world")

Examples:
Q: "Will it rain in London tomorrow?" → London
Q: "Will it snow in NYC on Friday?" → New York City
Q: "Will it rain in the Big Apple?" → New York City
Q: "Will it rain in January?" → NONE (January is a month)
Q: "Will Sanji bleed in ZaZa Rain God?" → NONE (not weather)
Q: "Will Amazon Rainforest flood?" → NONE (not a city)
Q: "Will it rain in Rain City, Germany?" → Rain City
Q: "Will it be cold in Chi-town?" → Chicago
Q: "Will there be 40 inches of rain in Central Park this year?" → NONE (not a city)
Q: "Will it snow in Banská Štiavnica on 2026-04-16?" → Banská Štiavnica

Question: "{question}"
Answer with ONLY the city name or ONLY the word NONE. No explanation.
City name:"""


# ── Stage 1: Regex extraction ─────────────────────────────────────
def extract_with_regex(question: str) -> str | None:
    """
    Fast regex extraction for standard patterns.
    Returns raw extracted text — may need normalization.
    """
    q = question.strip()

    patterns = [
        r"[Ww]ill it (?:rain|snow|precipitate|freeze) in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\s+tomorrow|\s+tonight|\?|,|\s+in\s+\d)",
        r"[Ww]ill there be (?:rain|snow|precipitation|frost|fog|hail|sunshine|a storm|a blizzard|a hurricane|wind) in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\s+tomorrow|\?|,)",
        r"[Tt]emperature in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\s+exceed|\s+be|\?|,)",
        r"[Ww]ill the temperature in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\s+exceed|\s+be|\?|,)",
        r"[Ww]ill it be (?:sunny|cloudy|overcast|hot|cold|warm|windy|foggy|above|below) in ([A-Z][^,\?\.]+?)(?:\s+on\s+\d|\s+today|\s+tomorrow|\?|,)",
        r"\bin ([A-Z][a-zA-Z\s\-]+?)(?:\s+on\s+\d|\s+today|\s+tomorrow|\?|,)",
    ]

    for pattern in patterns:
        match = re.search(pattern, q)
        if match:
            city = match.group(1).strip()
            city = re.sub(r'\s+', ' ', city)
            city = re.sub(
                r'\s+(today|tomorrow|tonight|on|in|at|this|the|before|during)$',
                '', city, flags=re.IGNORECASE
            ).strip()
            if len(city) >= 2:
                return city

    return None


def normalize_known(city: str) -> str:
    """Normalizes known nicknames instantly."""
    return KNOWN_NORMALIZATIONS.get(city.lower(), city)


def _is_suspicious(city: str) -> bool:
    """
    Returns True if extracted city looks suspicious
    and should be verified by Claude.
    """
    words = city.split()

    # Too many words
    if len(words) > 4:
        return True

    # Contains day/time words
    TIME_WORDS = {
        "monday", "tuesday", "wednesday", "thursday", "friday",
        "saturday", "sunday", "today", "tomorrow", "tonight",
        "this", "next", "last", "weekend",
    }
    if any(w.lower() in TIME_WORDS for w in words):
        return True

    return False


# ── Stage 2: Claude extraction ────────────────────────────────────
def extract_with_claude(question: str) -> str | None:
    """
    Claude analyzes the FULL question context.
    Returns normalized city name or None.
    """
    if not ANTHROPIC_API_KEY:
        log.debug("No ANTHROPIC_API_KEY — skipping Claude")
        return None

    try:
        log.info(f"Claude analyzing: '{question[:60]}'")

        response = requests.post(
            ANTHROPIC_URL,
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 10,
                "messages":   [{
                    "role":    "user",
                    "content": CLAUDE_PROMPT.format(question=question)
                }]
            },
            timeout=10
        )

        if response.status_code != 200:
            log.warning(f"Claude API error: {response.status_code}")
            return None

        city = response.json()["content"][0]["text"].strip()
        # Take only first line — ignore any explanation
        city = city.split('\n')[0].strip()
        log.info(f"Claude returned: '{city}'")

        if not city or city.upper() == "NONE":
            return None

        return city

    except Exception as e:
        log.warning(f"Claude extraction failed: {e}")
        return None


# ── Main extraction function ──────────────────────────────────────
def extract_city(question: str, cache: dict = None) -> str | None:
    """
    Main function: extracts normalized city name from question.

    Pipeline:
      1. Check question cache
      2. Regex extraction
      3. Normalize known nicknames
      4. If suspicious → Claude verifies
      5. If regex failed → Claude extracts
      6. Cache result

    Returns normalized city name or None.
    """
    from producers.utils.city_cache import (
        get_cached_question, cache_question
    )

    # Step 1: Check question cache
    if cache is not None:
        cached = get_cached_question(question, cache)
        if cached is not None:
            log.debug(f"Question cache hit: '{cached}'")
            return cached if cached != "NONE" else None

    # Step 2: Regex extraction
    city = extract_with_regex(question)

    if city:
        # Step 3: Normalize known nicknames
        normalized = normalize_known(city)
        if normalized != city:
            log.info(f"Normalized: '{city}' → '{normalized}'")
            city = normalized
            if cache is not None:
                cache_question(question, city, cache)
            return city

        # Step 4: If suspicious → Claude verifies full context
        if _is_suspicious(city):
            log.info(f"Suspicious '{city}' → Claude verifying")
            claude_city = extract_with_claude(question)
            city = claude_city  # could be None

        if cache is not None:
            cache_question(question, city or "NONE", cache)
        return city

    # Step 5: Regex failed → Claude extracts
    log.info(f"Regex failed → Claude for: '{question[:60]}'")
    city = extract_with_claude(question)

    # Step 6: Cache result
    if cache is not None:
        cache_question(question, city or "NONE", cache)

    return city