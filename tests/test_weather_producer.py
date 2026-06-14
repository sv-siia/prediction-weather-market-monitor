"""
test_weather_producer.py
-------------------------
Unit tests for weather_producer.py pure functions.
Tests: should_fetch_historical
No Kafka or API calls needed.
"""

import pytest
import sys
import os
from datetime import date, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producers.weather_producer import should_fetch_historical


# ── should_fetch_historical tests ─────────────────────────────────
class TestShouldFetchHistorical:

    def test_past_date_returns_true(self):
        past = (date.today() - timedelta(days=30)).isoformat()
        assert should_fetch_historical(past) is True

    def test_yesterday_returns_true(self):
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        assert should_fetch_historical(yesterday) is True

    def test_future_date_returns_false(self):
        future = (date.today() + timedelta(days=30)).isoformat()
        assert should_fetch_historical(future) is False

    def test_tomorrow_returns_false(self):
        tomorrow = (date.today() + timedelta(days=1)).isoformat()
        assert should_fetch_historical(tomorrow) is False

    def test_current_string_returns_false(self):
        assert should_fetch_historical("current") is False

    def test_none_returns_false(self):
        assert should_fetch_historical(None) is False

    def test_empty_string_returns_false(self):
        assert should_fetch_historical("") is False

    def test_invalid_date_returns_false(self):
        assert should_fetch_historical("not-a-date") is False

    def test_specific_past_date(self):
        assert should_fetch_historical("2025-01-22") is True

    def test_specific_future_date(self):
        assert should_fetch_historical("2027-12-31") is False

    def test_today_returns_false(self):
        today = date.today().isoformat()
        assert should_fetch_historical(today) is False