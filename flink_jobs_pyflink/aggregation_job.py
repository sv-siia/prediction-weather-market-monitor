"""
aggregation_job.py  (PyFlink version)
---------------------------------------
Computes rolling accuracy aggregates per location + market type.

Key differences from flink_jobs/aggregation_job.py:
  - Sliding windows: 1-hour window, 15-minute slide (event-time)
  - WatermarkStrategy with 15-min late data tolerance
  - Stateful incremental aggregation via AggregateFunction
  - Results written to market-accuracy-aggregates Kafka topic + PostgreSQL
"""

import json
import os
import logging
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)
from pyflink.common import WatermarkStrategy, Duration, Types, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import AggregateFunction, WindowFunction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IN          = os.getenv("KAFKA_TOPIC_CORRELATIONS", "market-weather-correlations")
TOPIC_OUT         = os.getenv("KAFKA_TOPIC_AGGREGATES",   "market-accuracy-aggregates")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",    "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",      "prediction_market")
POSTGRES_USER     = os.getenv("POSTGRES_USER",    "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD","changeme")

WINDOW_HOURS   = 1
SLIDE_MINUTES  = 15
LATE_DATA_MIN  = 15


# ── Timestamp assigner ────────────────────────────────────────────
class CorrelationTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        try:
            ts = value.get("POLL_TIMESTAMP") or value.get("poll_timestamp", "")
            if ts:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
        except Exception:
            pass
        return record_timestamp


# ── Accumulator for incremental aggregation ───────────────────────
class AccuracyAccumulator:
    __slots__ = [
        "total", "correct", "error_sum", "error_count",
        "price_sum", "outcome_sum", "outcome_count",
        "over_pred", "under_pred", "volume_sum", "weighted_correct"
    ]

    def __init__(self):
        self.total = 0
        self.correct = 0
        self.error_sum = 0.0
        self.error_count = 0
        self.price_sum = 0.0
        self.outcome_sum = 0.0
        self.outcome_count = 0
        self.over_pred = 0
        self.under_pred = 0
        self.volume_sum = 0.0
        self.weighted_correct = 0.0


# ── AggregateFunction ─────────────────────────────────────────────
class AccuracyAggregateFunction(AggregateFunction):
    """Incrementally computes accuracy metrics over a sliding window."""

    def create_accumulator(self):
        return AccuracyAccumulator()

    def add(self, record: dict, acc: AccuracyAccumulator):
        acc.total += 1

        outcome = record.get("ACTUAL_OUTCOME")
        price   = record.get("price") or record.get("yes_price")
        volume  = record.get("VOLUME", 1.0) or 1.0
        error   = record.get("PREDICTION_ERROR")
        method  = record.get("CORRELATION_METHOD", "")

        if method == "current_snapshot":
            return acc  # exclude current_snapshot from accuracy

        if outcome is not None and price is not None:
            price   = float(price)
            outcome = int(outcome)
            correct = (outcome == 1 and price >= 0.5) or (outcome == 0 and price < 0.5)
            if correct:
                acc.correct += 1
                acc.weighted_correct += volume

            acc.price_sum    += price
            acc.outcome_sum  += outcome
            acc.outcome_count += 1
            acc.volume_sum   += volume

            if price > 0.5 and outcome == 0:
                acc.over_pred += 1
            elif price <= 0.5 and outcome == 1:
                acc.under_pred += 1

        if error is not None:
            acc.error_sum   += float(error)
            acc.error_count += 1

        return acc

    def get_result(self, acc: AccuracyAccumulator) -> dict:
        total    = acc.total or 1
        reliable = acc.outcome_count or 1

        accuracy  = round(acc.correct / reliable, 4)
        avg_error = round(acc.error_sum / acc.error_count, 4) if acc.error_count else 0.0
        vol_acc   = round(acc.weighted_correct / acc.volume_sum, 4) if acc.volume_sum else accuracy

        avg_price  = acc.price_sum / acc.outcome_count if acc.outcome_count else 0.5
        avg_outcome = acc.outcome_sum / acc.outcome_count if acc.outcome_count else 0.5
        bias_score  = round(avg_price - avg_outcome, 4)

        return {
            "total_predictions":        acc.total,
            "correct_predictions":      acc.correct,
            "accuracy_rate":            accuracy,
            "avg_prediction_error":     avg_error,
            "volume_weighted_accuracy": vol_acc,
            "bias_score":               bias_score,
            "over_prediction_count":    acc.over_pred,
            "under_prediction_count":   acc.under_pred,
        }

    def merge(self, acc1: AccuracyAccumulator, acc2: AccuracyAccumulator):
        acc1.total            += acc2.total
        acc1.correct          += acc2.correct
        acc1.error_sum        += acc2.error_sum
        acc1.error_count      += acc2.error_count
        acc1.price_sum        += acc2.price_sum
        acc1.outcome_sum      += acc2.outcome_sum
        acc1.outcome_count    += acc2.outcome_count
        acc1.over_pred        += acc2.over_pred
        acc1.under_pred       += acc2.under_pred
        acc1.volume_sum       += acc2.volume_sum
        acc1.weighted_correct += acc2.weighted_correct
        return acc1


# ── WindowFunction: adds window metadata ─────────────────────────
class WindowMetadataFunction(WindowFunction):
    def apply(self, key, window, inputs, out):
        metrics = list(inputs)[0]
        location, market_type = key

        record = {
            "LOCATION_NAME":          location,
            "MARKET_TYPE":            market_type,
            "WINDOW_START":           datetime.fromtimestamp(
                                          window.start / 1000, tz=timezone.utc
                                      ).isoformat(),
            "WINDOW_END":             datetime.fromtimestamp(
                                          window.end / 1000, tz=timezone.utc
                                      ).isoformat(),
            "POLL_TIMESTAMP":         datetime.now(timezone.utc).isoformat(),
            **metrics,
        }
        out.collect(json.dumps(record))


# ── PostgreSQL sink ───────────────────────────────────────────────
class PostgresSinkFunction:
    def __init__(self):
        self._conn = None

    def _get_conn(self):
        if self._conn is None or self._conn.closed:
            import psycopg2
            self._conn = psycopg2.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT,
                dbname=POSTGRES_DB, user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
        return self._conn

    def invoke(self, record_json: str):
        record = json.loads(record_json)
        conn   = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_accuracy_aggregates (
                        location_name, market_type, window_start, window_end,
                        total_predictions, correct_predictions, accuracy_rate,
                        avg_prediction_error, volume_weighted_accuracy,
                        bias_score, over_prediction_count, under_prediction_count,
                        updated_at
                    ) VALUES (
                        %(LOCATION_NAME)s, %(MARKET_TYPE)s,
                        %(WINDOW_START)s,  %(WINDOW_END)s,
                        %(total_predictions)s, %(correct_predictions)s,
                        %(accuracy_rate)s, %(avg_prediction_error)s,
                        %(volume_weighted_accuracy)s, %(bias_score)s,
                        %(over_prediction_count)s, %(under_prediction_count)s,
                        NOW()
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
                """, record)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error("Failed to save aggregate: %s", e)


# ── Main job ──────────────────────────────────────────────────────
def run():
    log.info("Starting PyFlink Aggregation Job")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        "file:///app/jars/flink-connector-kafka.jar",
        "file:///app/jars/kafka-clients.jar",
    )
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.enable_checkpointing(60_000)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(LATE_DATA_MIN))
        .with_timestamp_assigner(CorrelationTimestampAssigner())
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(TOPIC_IN)
        .set_group_id("pyflink-aggregation")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = (
        env.from_source(source, watermark_strategy, "Correlations Source")
        .map(lambda s: {k: str(v) if v is not None else "" for k, v in json.loads(s).items()}, output_type=Types.MAP(Types.STRING(), Types.STRING()))
    )

    aggregated = (
        stream
        .key_by(lambda r: (
            r.get("LOCATION_NAME", "unknown"),
            r.get("MARKET_TYPE", "WEATHER")
        ))
        .window(SlidingEventTimeWindows.of(
            Time.hours(WINDOW_HOURS),
            Time.minutes(SLIDE_MINUTES)
        ))
        .aggregate(AccuracyAggregateFunction(), WindowMetadataFunction())
    )

    # Sink: Kafka
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_OUT)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    aggregated.map(lambda r: json.dumps(r), output_type=Types.STRING()).sink_to(kafka_sink)

    # Sink: PostgreSQL
    pg_sink = PostgresSinkFunction()
    aggregated.map(pg_sink.invoke)

    log.info("Submitting PyFlink Aggregation Job...")
    env.execute("PyFlink Aggregation Job")


if __name__ == "__main__":
    run()
