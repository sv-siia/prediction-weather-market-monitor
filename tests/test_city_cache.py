"""
test_city_cache.py
-------------------
Unit tests for city_cache.py
"""

import pytest
import sys
import os
import json
import tempfile

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producers.utils.city_cache import (
    load_cache, save_cache, question_hash,
    get_cached_question, cache_question,
    get_cached_city, cache_city, cache_invalid_city,
    cache_stats, DEFAULT_CACHE
)


@pytest.fixture
def empty_cache():
    return {"questions": {}, "cities": {}}


@pytest.fixture
def cache_with_data():
    return {
        "questions": {
            "abc123def456": "London",
            "xyz789uvw012": "NONE",
        },
        "cities": {
            "London": {
                "lat": 51.5074, "lon": -0.1278,
                "tz": "Europe/London", "verified": True,
                "source": "nominatim"
            },
            "Pokemon Go": {"verified": False},
        }
    }


class TestLoadCache:

    def test_returns_empty_cache_if_no_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_cache.CACHE_FILE",
            str(tmp_path / "nonexistent.json")
        )
        cache = load_cache()
        assert cache == DEFAULT_CACHE

    def test_loads_existing_cache(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "city_cache.json"
        data = {"questions": {"abc": "London"}, "cities": {}}
        cache_file.write_text(json.dumps(data))
        monkeypatch.setattr(
            "producers.utils.city_cache.CACHE_FILE", str(cache_file)
        )
        cache = load_cache()
        assert cache["questions"]["abc"] == "London"

    def test_adds_missing_keys(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "city_cache.json"
        cache_file.write_text(json.dumps({"questions": {}}))
        monkeypatch.setattr(
            "producers.utils.city_cache.CACHE_FILE", str(cache_file)
        )
        cache = load_cache()
        assert "cities" in cache

    def test_handles_corrupt_file(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "city_cache.json"
        cache_file.write_text("not valid json{{{")
        monkeypatch.setattr(
            "producers.utils.city_cache.CACHE_FILE", str(cache_file)
        )
        cache = load_cache()
        assert cache == DEFAULT_CACHE


class TestSaveCache:

    def test_saves_cache(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "city_cache.json"
        monkeypatch.setattr(
            "producers.utils.city_cache.CACHE_FILE", str(cache_file)
        )
        cache = {"questions": {"abc": "London"}, "cities": {}}
        save_cache(cache)
        assert cache_file.exists()
        loaded = json.loads(cache_file.read_text())
        assert loaded["questions"]["abc"] == "London"


class TestQuestionHash:

    def test_same_question_same_hash(self):
        q = "Will it rain in London tomorrow?"
        assert question_hash(q) == question_hash(q)

    def test_different_questions_different_hash(self):
        q1 = "Will it rain in London?"
        q2 = "Will it snow in Paris?"
        assert question_hash(q1) != question_hash(q2)

    def test_hash_length(self):
        q = "Will it rain in London tomorrow?"
        assert len(question_hash(q)) == 12

    def test_similar_questions_same_hash(self):
        # Same question → always same hash
        q = "Will it rain in London tomorrow?"
        assert question_hash(q) == question_hash(q)


class TestQuestionCache:

    def test_get_cached_question_miss(self, empty_cache):
        result = get_cached_question("Will it rain in London?", empty_cache)
        assert result is None

    def test_cache_and_get_question(self, empty_cache, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_cache.CACHE_FILE",
            str(tmp_path / "cache.json")
        )
        cache_question("Will it rain in London?", "London", empty_cache)
        result = get_cached_question("Will it rain in London?", empty_cache)
        assert result == "London"

    def test_cache_none_question(self, empty_cache, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_cache.CACHE_FILE",
            str(tmp_path / "cache.json")
        )
        cache_question("Will Sanji bleed?", None, empty_cache)
        result = get_cached_question("Will Sanji bleed?", empty_cache)
        assert result is None

    def test_cache_invalid_question(self, empty_cache, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_cache.CACHE_FILE",
            str(tmp_path / "cache.json")
        )
        cache_question("Not a weather question", "NONE", empty_cache)
        result = get_cached_question("Not a weather question", empty_cache)
        assert result == "NONE"


class TestCityCache:

    def test_get_cached_city_miss(self, empty_cache):
        result = get_cached_city("London", empty_cache)
        assert result is None

    def test_cache_and_get_city(self, empty_cache, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_cache.CACHE_FILE",
            str(tmp_path / "cache.json")
        )
        cache_city("London", 51.5074, -0.1278, "Europe/London",
                   "nominatim", empty_cache)
        result = get_cached_city("London", empty_cache)
        assert result is not None
        assert result["lat"] == 51.5074
        assert result["lon"] == -0.1278
        assert result["tz"] == "Europe/London"
        assert result["verified"] is True
        assert result["source"] == "nominatim"

    def test_cache_invalid_city(self, empty_cache, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "producers.utils.city_cache.CACHE_FILE",
            str(tmp_path / "cache.json")
        )
        cache_invalid_city("Pokemon Go", empty_cache)
        result = get_cached_city("Pokemon Go", empty_cache)
        assert result is not None
        assert result["verified"] is False

    def test_cache_city_none_cache(self):
        # Should not raise
        cache_city("London", 51.5, -0.1, "UTC", "test", None)

    def test_get_cached_city_verified(self, cache_with_data):
        result = get_cached_city("London", cache_with_data)
        assert result["verified"] is True

    def test_get_cached_city_invalid(self, cache_with_data):
        result = get_cached_city("Pokemon Go", cache_with_data)
        assert result["verified"] is False


class TestCacheStats:

    def test_empty_cache_stats(self, empty_cache):
        stats = cache_stats(empty_cache)
        assert stats["total_questions"] == 0
        assert stats["total_cities"] == 0
        assert stats["verified_cities"] == 0
        assert stats["invalid_cities"] == 0

    def test_cache_stats_with_data(self, cache_with_data):
        stats = cache_stats(cache_with_data)
        assert stats["total_questions"] == 2
        assert stats["total_cities"] == 2
        assert stats["verified_cities"] == 1
        assert stats["invalid_cities"] == 1