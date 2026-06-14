"""
Schema Registry client — registers Avro schemas and serializes messages
using Confluent wire format: [0x00][4-byte schema ID][avro bytes]
"""
import io
import json
import logging
import struct
from pathlib import Path
from typing import Any

import fastavro
import requests

log = logging.getLogger(__name__)

SCHEMAS_DIR = Path(__file__).parent.parent.parent / "schemas"

# topic → (schema_file, subject_suffix)
TOPIC_SCHEMAS = {
    "polymarket-predictions-raw":  ("prediction.avsc",  "value"),
    "weather-actuals-raw":         ("weather.avsc",     "value"),
    "market-weather-correlations": ("correlation.avsc", "value"),
    "market-accuracy-aggregates":  ("aggregate.avsc",   "value"),
    "arbitrage-alerts":            ("alert.avsc",       "value"),
}


class SchemaRegistryClient:
    """
    Thin client for Confluent Schema Registry.
    - Registers schemas on first use
    - Serializes records to Confluent Avro wire format
    - Falls back to plain JSON if Schema Registry is unreachable
    """

    def __init__(self, url: str = "http://localhost:8085"):
        self.url = url.rstrip("/")
        self._schema_cache: dict[str, tuple[int, Any]] = {}  # topic → (id, parsed_schema)
        self._available = self._check_available()

    def _check_available(self) -> bool:
        try:
            r = requests.get(f"{self.url}/subjects", timeout=3)
            if r.status_code == 200:
                log.info("Schema Registry connected ✅  %s", self.url)
                return True
        except Exception:
            pass
        log.warning("Schema Registry unavailable — falling back to JSON serialization")
        return False

    def _load_schema(self, schema_file: str) -> dict:
        path = SCHEMAS_DIR / schema_file
        with open(path) as f:
            return json.load(f)

    def _register(self, topic: str) -> tuple[int, Any]:
        """Register schema for topic, return (schema_id, parsed_schema)."""
        if topic in self._schema_cache:
            return self._schema_cache[topic]

        schema_file, suffix = TOPIC_SCHEMAS[topic]
        subject = f"{topic}-{suffix}"
        raw_schema = self._load_schema(schema_file)
        parsed = fastavro.parse_schema(raw_schema)

        payload = {"schema": json.dumps(raw_schema)}
        r = requests.post(
            f"{self.url}/subjects/{subject}/versions",
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=5,
        )
        r.raise_for_status()
        schema_id = r.json()["id"]
        self._schema_cache[topic] = (schema_id, parsed)
        log.info("Registered schema for %s → id=%d", subject, schema_id)
        return schema_id, parsed

    def serialize(self, topic: str, record: dict) -> bytes:
        """
        Serialize record to Confluent Avro wire format.
        Falls back to UTF-8 JSON if Schema Registry is unavailable.
        """
        if not self._available:
            return json.dumps(record).encode("utf-8")

        try:
            schema_id, parsed_schema = self._register(topic)
            # Keep only fields defined in the schema (avro rejects extra fields)
            schema_fields = {f["name"] for f in parsed_schema["fields"]}
            avro_record = {k: v for k, v in record.items() if k in schema_fields}
            buf = io.BytesIO()
            buf.write(b"\x00")                        # magic byte
            buf.write(struct.pack(">I", schema_id))   # 4-byte schema id
            fastavro.schemaless_writer(buf, parsed_schema, avro_record)
            return buf.getvalue()
        except Exception as e:
            log.warning("Avro serialization failed (%s) — falling back to JSON", e)
            return json.dumps(record).encode("utf-8")

    def validate(self, topic: str, record: dict) -> list[str]:
        """
        Validate record fields against schema.
        Returns list of error messages (empty = valid).
        """
        if topic not in TOPIC_SCHEMAS:
            return []
        schema_file, _ = TOPIC_SCHEMAS[topic]
        raw_schema = self._load_schema(schema_file)
        errors = []

        required = [
            f["name"] for f in raw_schema["fields"]
            if not (isinstance(f["type"], list) and f["type"][0] == "null")
        ]
        for field in required:
            if field not in record or record[field] is None:
                errors.append(f"Missing required field: {field}")

        return errors
