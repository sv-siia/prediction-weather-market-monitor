"""Tests for SchemaRegistryClient."""
import io
import json
import struct
from unittest.mock import MagicMock, patch

import pytest

from producers.utils.schema_registry import SchemaRegistryClient, TOPIC_SCHEMAS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(available: bool = True) -> SchemaRegistryClient:
    """Create a client without making real HTTP requests."""
    with patch.object(SchemaRegistryClient, "_check_available", return_value=available):
        client = SchemaRegistryClient(url="http://fake-registry:8081")
    return client


# ---------------------------------------------------------------------------
# _check_available
# ---------------------------------------------------------------------------

class TestCheckAvailable:
    def test_returns_true_on_200(self):
        with patch("producers.utils.schema_registry.requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200)
            client = SchemaRegistryClient.__new__(SchemaRegistryClient)
            client.url = "http://fake:8081"
            client._schema_cache = {}
            assert client._check_available() is True

    def test_returns_false_on_connection_error(self):
        with patch("producers.utils.schema_registry.requests.get", side_effect=ConnectionError):
            client = SchemaRegistryClient.__new__(SchemaRegistryClient)
            client.url = "http://fake:8081"
            client._schema_cache = {}
            assert client._check_available() is False

    def test_returns_false_on_non_200(self):
        with patch("producers.utils.schema_registry.requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=503)
            client = SchemaRegistryClient.__new__(SchemaRegistryClient)
            client.url = "http://fake:8081"
            client._schema_cache = {}
            assert client._check_available() is False


# ---------------------------------------------------------------------------
# serialize — JSON fallback
# ---------------------------------------------------------------------------

class TestSerializeFallback:
    def test_json_fallback_when_unavailable(self):
        client = _make_client(available=False)
        record = {"CONDITION_ID": "abc", "QUESTION": "Will it rain?"}
        result = client.serialize("polymarket-predictions-raw", record)
        assert result == json.dumps(record).encode("utf-8")

    def test_json_fallback_on_register_error(self):
        client = _make_client(available=True)
        client._available = True
        with patch.object(client, "_register", side_effect=Exception("network error")):
            record = {"CONDITION_ID": "abc"}
            result = client.serialize("polymarket-predictions-raw", record)
            assert result == json.dumps(record).encode("utf-8")


# ---------------------------------------------------------------------------
# serialize — Avro wire format
# ---------------------------------------------------------------------------

class TestSerializeAvro:
    def _minimal_prediction(self):
        return {
            "CONDITION_ID": "test-id-001",
            "QUESTION": "Will it snow?",
            "LOCATION_NAME": "Vancouver",
            "MARKET_TYPE": "SNOW",
            "MARKET_STATUS": "open",
            "YES_PRICE": 0.45,
            "NO_PRICE": None,
            "WINNER": None,
            "CLOSED": False,
            "END_DATE_ISO": None,
            "WEATHER_TYPE": "SNOW",
            "IS_ARBITRAGE": False,
            "POLL_TIMESTAMP": "2026-06-13T10:00:00",
        }

    def test_avro_wire_format_magic_byte(self):
        client = _make_client(available=True)
        schema_id = 42
        import fastavro
        from pathlib import Path
        raw = json.loads((Path("schemas") / "prediction.avsc").read_text())
        parsed = fastavro.parse_schema(raw)
        client._schema_cache["polymarket-predictions-raw"] = (schema_id, parsed)

        result = client.serialize("polymarket-predictions-raw", self._minimal_prediction())
        assert result[0:1] == b"\x00"  # magic byte
        assert struct.unpack(">I", result[1:5])[0] == schema_id

    def test_avro_wire_format_decodable(self):
        import fastavro
        from pathlib import Path
        client = _make_client(available=True)
        raw = json.loads((Path("schemas") / "prediction.avsc").read_text())
        parsed = fastavro.parse_schema(raw)
        client._schema_cache["polymarket-predictions-raw"] = (99, parsed)

        record = self._minimal_prediction()
        wire = client.serialize("polymarket-predictions-raw", record)

        # skip 5-byte header, decode avro body
        body = io.BytesIO(wire[5:])
        decoded = fastavro.schemaless_reader(body, parsed)
        assert decoded["CONDITION_ID"] == record["CONDITION_ID"]
        assert decoded["YES_PRICE"] == pytest.approx(0.45)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

class TestValidate:
    def test_valid_record_no_errors(self):
        client = _make_client(available=False)
        record = {
            "CONDITION_ID": "x",
            "QUESTION": "Q",
            "LOCATION_NAME": "City",
            "MARKET_TYPE": "RAIN",
            "MARKET_STATUS": "open",
            "YES_PRICE": 0.5,
            "CLOSED": False,
            "WEATHER_TYPE": "RAIN",
            "IS_ARBITRAGE": False,
            "POLL_TIMESTAMP": "2026-06-13T00:00:00",
        }
        errors = client.validate("polymarket-predictions-raw", record)
        assert errors == []

    def test_missing_required_field(self):
        client = _make_client(available=False)
        record = {"QUESTION": "Q"}  # missing CONDITION_ID, etc.
        errors = client.validate("polymarket-predictions-raw", record)
        assert any("CONDITION_ID" in e for e in errors)

    def test_none_required_field(self):
        client = _make_client(available=False)
        record = {
            "CONDITION_ID": None,
            "QUESTION": "Q",
            "LOCATION_NAME": "City",
            "MARKET_TYPE": "RAIN",
            "MARKET_STATUS": "open",
            "YES_PRICE": 0.5,
            "CLOSED": False,
            "WEATHER_TYPE": "RAIN",
            "IS_ARBITRAGE": False,
            "POLL_TIMESTAMP": "2026-06-13T00:00:00",
        }
        errors = client.validate("polymarket-predictions-raw", record)
        assert any("CONDITION_ID" in e for e in errors)

    def test_unknown_topic_no_errors(self):
        client = _make_client(available=False)
        errors = client.validate("unknown-topic", {"any": "field"})
        assert errors == []

    def test_nullable_fields_not_flagged(self):
        """NO_PRICE, WINNER, END_DATE_ISO are nullable — absent is OK."""
        client = _make_client(available=False)
        record = {
            "CONDITION_ID": "x",
            "QUESTION": "Q",
            "LOCATION_NAME": "City",
            "MARKET_TYPE": "RAIN",
            "MARKET_STATUS": "open",
            "YES_PRICE": 0.5,
            "CLOSED": False,
            "WEATHER_TYPE": "RAIN",
            "IS_ARBITRAGE": False,
            "POLL_TIMESTAMP": "2026-06-13T00:00:00",
            # NO_PRICE, WINNER, END_DATE_ISO intentionally absent
        }
        errors = client.validate("polymarket-predictions-raw", record)
        assert not any("NO_PRICE" in e or "WINNER" in e or "END_DATE_ISO" in e for e in errors)


# ---------------------------------------------------------------------------
# TOPIC_SCHEMAS constant
# ---------------------------------------------------------------------------

class TestTopicSchemas:
    def test_all_five_topics_present(self):
        expected = {
            "polymarket-predictions-raw",
            "weather-actuals-raw",
            "market-weather-correlations",
            "market-accuracy-aggregates",
            "arbitrage-alerts",
        }
        assert set(TOPIC_SCHEMAS.keys()) == expected

    def test_schema_files_exist(self):
        from pathlib import Path
        schemas_dir = Path("schemas")
        for topic, (schema_file, _) in TOPIC_SCHEMAS.items():
            assert (schemas_dir / schema_file).exists(), f"Missing schema file for {topic}: {schema_file}"
