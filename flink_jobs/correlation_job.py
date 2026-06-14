"""
correlation_job.py
-------------------
Flink Snapshot Correlation Job.

Reads from two Kafka topics:
  - polymarket-predictions-raw
  - weather-actuals-raw

Performs snapshot join:
  - Matches predictions with weather by LOCATION_NAME + date
  - Calculates ACTUAL_OUTCOME and PREDICTION_ERROR
  - Calculates CORRELATION_LATENCY_SEC

Produces to:
  - market-weather-correlations
"""

import io
import json
import os
import logging
import struct
import sys
from datetime import datetime, timezone
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer


def _kafka_deserialize(v):
    """Deserialize Kafka message: tries JSON first, then Confluent Avro wire format."""
    try:
        return json.loads(v.decode("utf-8"))
    except Exception:
        try:
            from pathlib import Path
            import fastavro
            # Detect which schema by peeking at topic context is not available here,
            # so try both schemas
            for schema_name in ("weather.avsc", "prediction.avsc", "correlation.avsc"):
                try:
                    schema_path = Path(__file__).parent.parent / "schemas" / schema_name
                    parsed = fastavro.parse_schema(json.loads(schema_path.read_text()))
                    buf = io.BytesIO(v[5:])
                    return fastavro.schemaless_reader(buf, parsed)
                except Exception:
                    continue
        except Exception:
            pass
        return None

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producers.utils.outcome_calculator import correlate
from producers.utils.metrics import (
    start_metrics_server,
    correlation_processed_total, correlation_matched_total,
    correlation_unmatched_total, correlation_join_coverage,
    correlation_latency_seconds, correlation_prediction_error,
    kafka_messages_produced_total,
)
from datetime import date as date_type

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER       = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PREDICTIONS  = os.getenv("KAFKA_TOPIC_PREDICTIONS",  "polymarket-predictions-raw")
TOPIC_WEATHER      = os.getenv("KAFKA_TOPIC_WEATHER",       "weather-actuals-raw")
TOPIC_CORRELATIONS = os.getenv("KAFKA_TOPIC_CORRELATIONS",  "market-weather-correlations")


# ── Helper: calculate latency ─────────────────────────────────────
def _calc_latency(poll_ts: str, corr_ts: str) -> int | None:
    """
    Calculates seconds between poll timestamp and correlation timestamp.
    Returns integer seconds or None if parsing fails.
    """
    try:
        t1 = datetime.fromisoformat(poll_ts)
        t2 = datetime.fromisoformat(corr_ts)
        return int((t2 - t1).total_seconds())
    except Exception:
        return None


# ── Load all weather data into memory (snapshot) ──────────────────
def load_weather_snapshot() -> dict:
    """
    Reads ALL weather records from Kafka into memory.
    Builds a snapshot dictionary:
      key = "London_2025-01-22" → historical weather record
      key = "London_current"    → latest current weather record
    """
    log.info("Loading weather snapshot from Kafka...")
    snapshot = {}

    consumer = KafkaConsumer(
        TOPIC_WEATHER,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=_kafka_deserialize,
        group_id=None,
    )

    count = 0
    for message in consumer:
        w = message.value
        if w is None:
            continue
        city  = w.get("LOCATION_NAME")
        wtype = w.get("WEATHER_TYPE", "current")

        if not city:
            continue

        if wtype == "historical":
            date_str = w.get("OBSERVATION_DATE", "")
            if date_str:
                key = f"{city}_{date_str}"
                snapshot[key] = w
        else:
            key = f"{city}_current"
            snapshot[key] = w

        count += 1

    consumer.close()
    log.info(f"Loaded {count} weather records into snapshot")
    log.info(
        f"Snapshot keys: {len(snapshot)} unique city+date combinations"
    )
    return snapshot


# ── Find matching weather for a prediction ────────────────────────
def find_weather(prediction: dict, snapshot: dict) -> dict | None:
    """
    Finds the best matching weather record for a prediction.

    Strategy:
      1. Closed market → historical or current (winner already known)
      2. Past date → historical only (no fallback to current)
      3. Today → current snapshot
      4. Future date → None (skip, no data yet)
    """
    from datetime import date as _date
    city     = prediction.get("LOCATION_NAME")
    end_date = prediction.get("end_date_iso", "")
    date_str = end_date[:10] if end_date else ""
    closed   = bool(prediction.get("closed") or prediction.get("CLOSED"))

    # Try exact historical match first
    if date_str:
        key = f"{city}_{date_str}"
        if key in snapshot:
            return snapshot[key]

    # Fall back to current snapshot for any market (closed, open, or no date)
    current_key = f"{city}_current"
    if current_key in snapshot:
        return snapshot[current_key]

    return None


# ── Load predictions into memory ──────────────────────────────────
def load_predictions() -> list:
    """
    Reads ALL predictions from Kafka into memory.
    """
    log.info("Loading predictions from Kafka...")
    predictions = []

    consumer = KafkaConsumer(
        TOPIC_PREDICTIONS,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=_kafka_deserialize,
        group_id=None,
    )

    for message in consumer:
        if message.value is not None:
            predictions.append(message.value)

    consumer.close()
    log.info(f"Loaded {len(predictions)} predictions")
    return predictions


# ── Main correlation logic ────────────────────────────────────────
def run():
    start_metrics_server(8002, "correlation_job")
    log.info("Starting Correlation Job")
    log.info(f"Kafka broker: {KAFKA_BROKER}")
    log.info(f"Input:  {TOPIC_PREDICTIONS} + {TOPIC_WEATHER}")
    log.info(f"Output: {TOPIC_CORRELATIONS}")

    # Step 1: Load weather snapshot
    snapshot = load_weather_snapshot()
    if not snapshot:
        log.error("No weather data! Run weather_producer first.")
        return

    # Step 2: Load all predictions
    predictions = load_predictions()
    if not predictions:
        log.error("No predictions! Run manifold_producer first.")
        return

    # Step 3: Connect output producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        compression_type="snappy",
        max_in_flight_requests_per_connection=1,
    )
    log.info("Connected to Kafka output ✅")

    # Step 4: Deduplicate predictions by condition_id (keep latest/closed version)
    dedup = {}
    for p in predictions:
        cid = p.get("condition_id") or p.get("CONDITION_ID") or p.get("condition_id")
        if not cid:
            continue
        existing = dedup.get(cid)
        # Prefer closed markets over open ones
        if existing is None or (not existing.get("closed") and p.get("closed")):
            dedup[cid] = p
    predictions = list(dedup.values())
    log.info(f"Deduplicated to {len(predictions)} unique markets")

    # Step 5: Correlate predictions with weather
    log.info(f"Correlating {len(predictions)} predictions...")

    total      = 0
    correlated = 0
    no_weather = 0
    failed     = 0
    by_method  = defaultdict(int)

    for prediction in predictions:
        total += 1

        try:
            correlation_processed_total.inc()
            # Find matching weather
            weather = find_weather(prediction, snapshot)
            if not weather:
                no_weather += 1
                correlation_unmatched_total.inc()
                continue

            # Correlate prediction with weather
            result = correlate(prediction, weather)
            if not result:
                failed += 1
                continue

            # Add required output stream fields
            corr_ts = datetime.now(timezone.utc).isoformat()
            result["CORRELATION_TIMESTAMP"]  = corr_ts
            latency_sec = _calc_latency(prediction.get("POLL_TIMESTAMP"), corr_ts)
            result["CORRELATION_LATENCY_SEC"] = latency_sec

            # Send to output topic
            producer.send(TOPIC_CORRELATIONS, value=result)
            correlated += 1
            method = result.get("CORRELATION_METHOD", "unknown")
            by_method[method] += 1
            correlation_matched_total.labels(method=method).inc()
            kafka_messages_produced_total.labels(topic=TOPIC_CORRELATIONS).inc()
            if latency_sec is not None:
                correlation_latency_seconds.observe(latency_sec)
            if result.get("PREDICTION_ERROR") is not None:
                correlation_prediction_error.set(result["PREDICTION_ERROR"])

            # Log progress every 200
            if correlated % 200 == 0:
                log.info(
                    f"Progress: {correlated}/{total} | "
                    f"Methods: {dict(by_method)}"
                )

        except Exception as e:
            log.error(f"Correlation error: {e}")
            failed += 1

    producer.flush()
    producer.close()

    # Update coverage gauge
    if total > 0:
        correlation_join_coverage.set(correlated / total)

    # Step 5: Final stats
    log.info("=" * 55)
    log.info("CORRELATION JOB COMPLETE")
    log.info(f"Total predictions:  {total}")
    log.info(f"Correlated:         {correlated}")
    log.info(f"No weather match:   {no_weather}")
    log.info(f"Failed:             {failed}")
    if total > 0:
        log.info(
            f"Success rate:       {round(correlated/total*100,1)}%"
        )
    log.info("Methods breakdown:")
    for method, count in by_method.items():
        log.info(f"  {method}: {count}")
    log.info("=" * 55)

    # Step 6: Show sample results
    log.info("Sample correlations from output topic:")
    sample_consumer = KafkaConsumer(
        TOPIC_CORRELATIONS,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=3000,
        value_deserializer=_kafka_deserialize,
        group_id=None,
    )

    samples = 0
    for msg in sample_consumer:
        r = msg.value
        if r is None:
            continue
        log.info(
            f"  {r.get('LOCATION_NAME')} | "
            f"{r.get('end_date_iso','')[:10]} | "
            f"price={r.get('price')} | "
            f"temp={r.get('ACTUAL_TEMP_F')}°F | "
            f"outcome={r.get('ACTUAL_OUTCOME')} | "
            f"error={r.get('PREDICTION_ERROR')} | "
            f"latency={r.get('CORRELATION_LATENCY_SEC')}s | "
            f"method={r.get('CORRELATION_METHOD')}"
        )
        samples += 1
        if samples >= 5:
            break

    sample_consumer.close()
    log.info("Done! ✅")


if __name__ == "__main__":
    run()