"""
aggregation_job.py
-------------------
Reads from market-weather-correlations Kafka topic.
Computes hourly accuracy aggregates per location + market type.
Writes results to:
  - market-accuracy-aggregates (Kafka topic)
  - market_accuracy_aggregates (PostgreSQL table)

Metrics computed:
  - accuracy_rate
  - avg_prediction_error
  - volume_weighted_accuracy
  - bias_score
"""

import io
import json
import os
import logging
import struct
from datetime import datetime, timezone, timedelta


def _kafka_deserialize(v):
    try:
        return json.loads(v.decode("utf-8"))
    except Exception:
        try:
            from pathlib import Path
            import fastavro
            for schema_name in ("correlation.avsc", "aggregate.avsc", "prediction.avsc", "weather.avsc"):
                try:
                    schema_path = Path(__file__).parent.parent / "schemas" / schema_name
                    parsed = fastavro.parse_schema(json.loads(schema_path.read_text()))
                    return fastavro.schemaless_reader(io.BytesIO(v[5:]), parsed)
                except Exception:
                    continue
        except Exception:
            pass
        return None
import sys
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producers.utils.metrics import (
    start_metrics_server,
    aggregation_windows_computed_total, aggregation_accuracy_rate,
    aggregation_bias_score, aggregation_records_processed_total,
    kafka_messages_produced_total,
)

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER       = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IN           = os.getenv("KAFKA_TOPIC_CORRELATIONS", "market-weather-correlations")
TOPIC_OUT          = os.getenv("KAFKA_TOPIC_AGGREGATES",   "market-accuracy-aggregates")
POSTGRES_HOST      = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT      = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB        = os.getenv("POSTGRES_DB",       "prediction_market")
POSTGRES_USER      = os.getenv("POSTGRES_USER",     "admin")
POSTGRES_PASSWORD  = os.getenv("POSTGRES_PASSWORD", "changeme")
WINDOW_HOURS       = 1   # 1-hour aggregation window
SLIDE_MINUTES      = 15  # slide every 15 minutes


# ── PostgreSQL connection ─────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# ── Save correlation to PostgreSQL ────────────────────────────────
def save_correlation(record: dict, conn):
    """Saves raw correlation record to market_correlations table."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_correlations (
                    condition_id, question, location_name, market_type,
                    market_status, yes_price, winner, closed, end_date_iso,
                    weather_type, observation_date, actual_temp_c,
                    actual_temp_f, actual_precip_mm, actual_rain_mm,
                    actual_weather_code, actual_wind_kmh,
                    actual_outcome, prediction_error, correlation_method,
                    correlation_latency, poll_timestamp
                ) VALUES (
                    %(condition_id)s, %(question)s, %(LOCATION_NAME)s,
                    %(MARKET_TYPE)s, %(MARKET_STATUS)s, %(price)s,
                    %(winner)s, %(closed)s, %(end_date_iso)s,
                    %(WEATHER_TYPE)s, %(OBSERVATION_DATE)s,
                    %(ACTUAL_TEMP_C)s, %(ACTUAL_TEMP_F)s,
                    %(ACTUAL_PRECIP_MM)s, %(ACTUAL_RAIN_MM)s,
                    %(ACTUAL_WEATHER_CODE)s, %(ACTUAL_WIND_KMH)s,
                    %(ACTUAL_OUTCOME)s, %(PREDICTION_ERROR)s,
                    %(CORRELATION_METHOD)s, %(CORRELATION_LATENCY_SEC)s,
                    %(POLL_TIMESTAMP)s
                )
                ON CONFLICT (condition_id) DO NOTHING
            """, {
                "condition_id":          record.get("condition_id"),
                "question":              record.get("question"),
                "LOCATION_NAME":         record.get("LOCATION_NAME"),
                "MARKET_TYPE":           record.get("MARKET_TYPE"),
                "MARKET_STATUS":         record.get("MARKET_STATUS"),
                "price":                 record.get("price"),
                "winner":                record.get("winner"),
                "closed":                record.get("closed"),
                "end_date_iso":          record.get("end_date_iso"),
                "WEATHER_TYPE":          record.get("WEATHER_TYPE"),
                "OBSERVATION_DATE":      record.get("OBSERVATION_DATE"),
                "ACTUAL_TEMP_C":         record.get("ACTUAL_TEMP_C"),
                "ACTUAL_TEMP_F":         record.get("ACTUAL_TEMP_F"),
                "ACTUAL_PRECIP_MM":      record.get("ACTUAL_PRECIP_MM"),
                "ACTUAL_RAIN_MM":        record.get("ACTUAL_RAIN_MM"),
                "ACTUAL_WEATHER_CODE":   record.get("ACTUAL_WEATHER_CODE"),
                "ACTUAL_WIND_KMH":       record.get("ACTUAL_WIND_KMH"),
                "ACTUAL_OUTCOME":        record.get("ACTUAL_OUTCOME"),
                "PREDICTION_ERROR":      record.get("PREDICTION_ERROR"),
                "CORRELATION_METHOD":    record.get("CORRELATION_METHOD"),
                "CORRELATION_LATENCY_SEC": record.get("CORRELATION_LATENCY_SEC"),
                "POLL_TIMESTAMP":        record.get("POLL_TIMESTAMP"),
            })
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error(f"Failed to save correlation: {e}")


# ── Compute aggregates ────────────────────────────────────────────
def compute_aggregates(records: list) -> dict:
    """
    Computes accuracy metrics for a list of correlation records.

    Returns:
      accuracy_rate, avg_prediction_error, volume_weighted_accuracy,
      bias_score, correct_predictions, total_predictions
    """
    if not records:
        return {}

    total          = len(records)
    # Exclude current_snapshot from accuracy — today's weather ≠ market's target date
    reliable       = [r for r in records
                      if r.get("CORRELATION_METHOD") != "current_snapshot"]
    reliable_total = len(reliable) if reliable else total
    correct        = sum(1 for r in reliable
                        if r.get("ACTUAL_OUTCOME") is not None
                        and r.get("price") is not None
                        and (
                            (r["ACTUAL_OUTCOME"] == 1 and r["price"] >= 0.5)
                            or
                            (r["ACTUAL_OUTCOME"] == 0 and r["price"] < 0.5)
                        ))
    errors         = [r["PREDICTION_ERROR"]
                      for r in records
                      if r.get("PREDICTION_ERROR") is not None]
    volumes        = [r.get("VOLUME", 1) for r in records]
    outcomes       = [r.get("ACTUAL_OUTCOME") for r in records
                      if r.get("ACTUAL_OUTCOME") is not None]
    prices         = [r.get("price", 0.5) for r in records]

    accuracy_rate  = correct / reliable_total if reliable_total > 0 else 0
    avg_error      = sum(errors) / len(errors) if errors else 0
    min_error      = min(errors) if errors else 0
    max_error      = max(errors) if errors else 0

    # Volume weighted accuracy
    total_volume   = sum(volumes)
    if total_volume > 0:
        weighted_correct = sum(
            v for r, v in zip(records, volumes)
            if r.get("ACTUAL_OUTCOME") is not None
            and r.get("price") is not None
            and (
                (r["ACTUAL_OUTCOME"] == 1 and r["price"] >= 0.5)
                or
                (r["ACTUAL_OUTCOME"] == 0 and r["price"] < 0.5)
            )
        )
        vol_weighted_acc = weighted_correct / total_volume
    else:
        vol_weighted_acc = accuracy_rate

    # Bias score: positive = over-predicting YES, negative = under-predicting
    if outcomes and prices:
        avg_predicted = sum(prices[:len(outcomes)]) / len(outcomes)
        avg_actual    = sum(outcomes) / len(outcomes)
        bias_score    = round(avg_predicted - avg_actual, 4)
    else:
        bias_score = 0

    over_predictions  = sum(
        1 for r in records
        if r.get("price", 0) > 0.5
        and r.get("ACTUAL_OUTCOME") == 0
    )
    under_predictions = sum(
        1 for r in records
        if r.get("price", 0) <= 0.5
        and r.get("ACTUAL_OUTCOME") == 1
    )

    return {
        "total_predictions":        total,
        "correct_predictions":      correct,
        "accuracy_rate":            round(accuracy_rate, 4),
        "avg_prediction_error":     round(avg_error, 4),
        "min_prediction_error":     round(min_error, 4),
        "max_prediction_error":     round(max_error, 4),
        "total_volume":             round(total_volume, 2),
        "volume_weighted_accuracy": round(vol_weighted_acc, 4),
        "bias_score":               bias_score,
        "over_prediction_count":    over_predictions,
        "under_prediction_count":   under_predictions,
    }


# ── Save aggregates to PostgreSQL ─────────────────────────────────
def save_aggregate(
        location: str, market_type: str,
        window_start: datetime, window_end: datetime,
        metrics: dict, conn) -> None:
    """Upserts aggregate record to PostgreSQL."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_accuracy_aggregates (
                    location_name, market_type,
                    window_start, window_end,
                    total_predictions, correct_predictions,
                    accuracy_rate, avg_prediction_error,
                    min_prediction_error, max_prediction_error,
                    total_volume, volume_weighted_accuracy,
                    bias_score, over_prediction_count,
                    under_prediction_count, updated_at
                ) VALUES (
                    %(location)s, %(market_type)s,
                    %(window_start)s, %(window_end)s,
                    %(total_predictions)s, %(correct_predictions)s,
                    %(accuracy_rate)s, %(avg_prediction_error)s,
                    %(min_prediction_error)s, %(max_prediction_error)s,
                    %(total_volume)s, %(volume_weighted_accuracy)s,
                    %(bias_score)s, %(over_prediction_count)s,
                    %(under_prediction_count)s, NOW()
                )
                ON CONFLICT (location_name, market_type, window_start)
                DO UPDATE SET
                    total_predictions        = EXCLUDED.total_predictions,
                    correct_predictions      = EXCLUDED.correct_predictions,
                    accuracy_rate            = EXCLUDED.accuracy_rate,
                    avg_prediction_error     = EXCLUDED.avg_prediction_error,
                    volume_weighted_accuracy = EXCLUDED.volume_weighted_accuracy,
                    bias_score               = EXCLUDED.bias_score,
                    updated_at               = NOW()
            """, {
                "location":                 location,
                "market_type":              market_type,
                "window_start":             window_start,
                "window_end":               window_end,
                **metrics,
            })
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error(f"Failed to save aggregate: {e}")


# ── Main job ──────────────────────────────────────────────────────
def run():
    start_metrics_server(8003, "aggregation_job")
    log.info("Starting Aggregation Job")
    log.info(f"Kafka broker:   {KAFKA_BROKER}")
    log.info(f"Input topic:    {TOPIC_IN}")
    log.info(f"Output topic:   {TOPIC_OUT}")
    log.info(f"PostgreSQL:     {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    log.info(f"Window:         {WINDOW_HOURS}h / {SLIDE_MINUTES}min slide")

    # Connect to PostgreSQL
    conn = get_db_connection()
    log.info("Connected to PostgreSQL ✅")

    # Connect to Kafka
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=_kafka_deserialize,
        group_id=None,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )
    log.info("Connected to Kafka ✅")

    # Read all correlations and deduplicate by condition_id
    log.info("Reading correlations from Kafka...")
    dedup = {}
    for message in consumer:
        record = message.value
        if record is None:
            continue
        cid = record.get("condition_id") or record.get("CONDITION_ID")
        if cid:
            existing = dedup.get(cid)
            # Prefer winner_known over other methods
            if existing is None:
                dedup[cid] = record
            elif (existing.get("CORRELATION_METHOD") != "winner_known"
                  and record.get("CORRELATION_METHOD") == "winner_known"):
                dedup[cid] = record
        else:
            dedup[id(record)] = record

    records = list(dedup.values())
    consumer.close()
    aggregation_records_processed_total.inc(len(records))
    log.info(f"Loaded {len(records)} unique correlation records (deduplicated)")

    # Save to PostgreSQL — skip current_snapshot (open markets with today's weather
    # have no meaningful accuracy since the market hasn't resolved yet)
    for record in records:
        if record.get("CORRELATION_METHOD") != "current_snapshot":
            save_correlation(record, conn)

    # Filter out current_snapshot before aggregation — open markets vs today's
    # weather have no meaningful accuracy signal
    records = [r for r in records if r.get("CORRELATION_METHOD") != "current_snapshot"]
    log.info(f"Records after filtering current_snapshot: {len(records)}")

    if not records:
        log.warning("No records found — exiting")
        return

    # Group by location + market_type
    groups = defaultdict(list)
    for r in records:
        key = (
            r.get("LOCATION_NAME", "unknown"),
            r.get("MARKET_TYPE", "WEATHER")
        )
        groups[key].append(r)

    log.info(f"Groups: {len(groups)} location+type combinations")

    # Compute aggregates per group
    now          = datetime.now(timezone.utc)
    window_end   = now
    window_start = now - timedelta(hours=WINDOW_HOURS)

    sent = 0
    for (location, market_type), group_records in groups.items():
        metrics = compute_aggregates(group_records)
        if not metrics:
            continue

        # Save to PostgreSQL
        save_aggregate(
            location, market_type,
            window_start, window_end,
            metrics, conn
        )

        # Produce to Kafka
        aggregate = {
            "LOCATION_NAME":          location,
            "MARKET_TYPE":            market_type,
            "WINDOW_START":           window_start.isoformat(),
            "WINDOW_END":             window_end.isoformat(),
            "POLL_TIMESTAMP":         now.isoformat(),
            **metrics,
        }
        producer.send(TOPIC_OUT, value=aggregate)
        sent += 1
        aggregation_windows_computed_total.inc()
        aggregation_accuracy_rate.set(metrics["accuracy_rate"])
        aggregation_bias_score.set(metrics["bias_score"])
        kafka_messages_produced_total.labels(topic=TOPIC_OUT).inc()

        log.info(
            f"{location} | {market_type} | "
            f"accuracy={metrics['accuracy_rate']*100:.1f}% | "
            f"predictions={metrics['total_predictions']} | "
            f"error={metrics['avg_prediction_error']:.4f} | "
            f"bias={metrics['bias_score']:+.4f}"
        )

    producer.flush()
    producer.close()
    conn.close()

    log.info("=" * 55)
    log.info("AGGREGATION JOB COMPLETE")
    log.info(f"Aggregates computed: {sent}")
    log.info(f"Window: {window_start} → {window_end}")
    log.info("=" * 55)


if __name__ == "__main__":
    run()