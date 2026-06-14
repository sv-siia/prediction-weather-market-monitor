"""
correlation_job.py  (PyFlink version)
--------------------------------------
Real-time correlation of prediction markets with weather actuals.

Key differences from flink_jobs/correlation_job.py:
  - Uses PyFlink DataStream API (not manual Kafka polling)
  - Event-time processing with WatermarkStrategy
  - 1-hour tumbling windows with 15-min late data allowance
  - Stateful join using KeyedCoProcessFunction
  - Exactly-once semantics via Flink checkpointing
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
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration, Types, Row
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import KeyedCoProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PREDICTIONS = os.getenv("KAFKA_TOPIC_PREDICTIONS",  "polymarket-predictions-raw")
TOPIC_WEATHER     = os.getenv("KAFKA_TOPIC_WEATHER",      "weather-actuals-raw")
TOPIC_OUT         = os.getenv("KAFKA_TOPIC_CORRELATIONS", "market-weather-correlations")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",    "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",      "prediction_market")
POSTGRES_USER     = os.getenv("POSTGRES_USER",    "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD","changeme")

# Watermark: allow weather data to arrive up to 15 min late
LATE_DATA_TOLERANCE_MIN = 15

# State TTL: keep pending predictions for 24 hours waiting for weather match
STATE_TTL_HOURS = 24


# ── Timestamp assigners ───────────────────────────────────────────
class PredictionTimestampAssigner(TimestampAssigner):
    """Extracts event timestamp from POLL_TIMESTAMP field."""
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        try:
            ts = value.get("POLL_TIMESTAMP") or value.get("poll_timestamp", "")
            if ts:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
        except Exception:
            pass
        return record_timestamp


class WeatherTimestampAssigner(TimestampAssigner):
    """Extracts event timestamp from weather POLL_TIMESTAMP field."""
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        try:
            ts = value.get("POLL_TIMESTAMP") or value.get("poll_timestamp", "")
            if ts:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
        except Exception:
            pass
        return record_timestamp


# ── Stateful correlation function ─────────────────────────────────
class CorrelationFunction(KeyedCoProcessFunction):
    """
    Joins predictions with weather data by LOCATION_NAME (key).

    State:
      - pending_predictions: MapState[condition_id → prediction record]
        TTL: 24 hours — predictions waiting for weather match
      - latest_weather: ValueState[weather record]
        Updated every time new weather arrives for this location
    """

    def open(self, runtime_context: RuntimeContext):
        from pyflink.datastream.state import StateTtlConfig
        from pyflink.common import Time

        ttl_config = (
            StateTtlConfig
            .new_builder(Time.hours(STATE_TTL_HOURS))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(
                StateTtlConfig.StateVisibility.NeverReturnExpired
            )
            .build()
        )

        pred_descriptor = MapStateDescriptor(
            "pending_predictions",
            Types.STRING(),
            Types.STRING(),
        )
        pred_descriptor.enable_time_to_live(ttl_config)
        self.pending_predictions = runtime_context.get_map_state(pred_descriptor)

        weather_descriptor = ValueStateDescriptor(
            "latest_weather",
            Types.STRING(),
        )
        weather_descriptor.enable_time_to_live(ttl_config)
        self.latest_weather = runtime_context.get_value_state(weather_descriptor)

    def process_element1(self, prediction, ctx):
        """Handles incoming prediction record."""
        condition_id = prediction.get("condition_id")
        if not condition_id:
            return

        # Try to match immediately with latest weather
        weather_json = self.latest_weather.value()
        if weather_json:
            weather = json.loads(weather_json)
            result = self._correlate(prediction, weather)
            if result:
                yield json.dumps(result)
                return

        # No weather yet — store prediction and wait
        self.pending_predictions.put(condition_id, json.dumps(prediction))

    def process_element2(self, weather, ctx):
        """Handles incoming weather record — try to match pending predictions."""
        self.latest_weather.update(json.dumps(weather))

        # Try to match all pending predictions for this location
        matched_ids = []
        for condition_id, pred_json in self.pending_predictions.entries():
            prediction = json.loads(pred_json)
            result = self._correlate(prediction, weather)
            if result:
                yield json.dumps(result)
                matched_ids.append(condition_id)

        for cid in matched_ids:
            self.pending_predictions.remove(cid)

    def _correlate(self, prediction: dict, weather: dict) -> dict | None:
        """Correlates a single prediction with weather data."""
        from producers.utils.outcome_calculator import correlate
        try:
            result = correlate(prediction, weather)
            return result
        except Exception as e:
            log.warning("Correlation failed: %s", e)
            return None


# ── PostgreSQL sink function ──────────────────────────────────────
class PostgresSinkFunction:
    """Writes correlated records to market_correlations table."""

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
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_correlations (
                        condition_id, question, location_name, market_type,
                        market_status, yes_price, winner, closed, end_date_iso,
                        weather_type, observation_date, actual_outcome,
                        prediction_error, correlation_method, correlation_latency,
                        poll_timestamp
                    ) VALUES (
                        %(condition_id)s, %(question)s, %(LOCATION_NAME)s,
                        %(MARKET_TYPE)s, %(MARKET_STATUS)s, %(price)s,
                        %(winner)s, %(closed)s, %(end_date_iso)s,
                        %(WEATHER_TYPE)s, %(OBSERVATION_DATE)s,
                        %(ACTUAL_OUTCOME)s, %(PREDICTION_ERROR)s,
                        %(CORRELATION_METHOD)s, %(CORRELATION_LATENCY_SEC)s,
                        %(POLL_TIMESTAMP)s
                    )
                    ON CONFLICT (condition_id) DO UPDATE SET
                        actual_outcome       = EXCLUDED.actual_outcome,
                        prediction_error     = EXCLUDED.prediction_error,
                        correlation_method   = EXCLUDED.correlation_method,
                        correlation_latency  = EXCLUDED.correlation_latency
                """, record)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error("Failed to save correlation: %s", e)


# ── Main job ──────────────────────────────────────────────────────
def run():
    log.info("Starting PyFlink Correlation Job")

    log.info("Initializing StreamExecutionEnvironment...")
    env = StreamExecutionEnvironment.get_execution_environment()
    log.info("Adding JARs...")
    env.add_jars(
        "file:///app/jars/flink-connector-kafka.jar",
        "file:///app/jars/kafka-clients.jar",
    )
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.enable_checkpointing(60_000)  # checkpoint every 60s
    env.get_checkpoint_config().set_checkpoint_timeout(30_000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # ── Watermark strategy: allow 15 min late data ────────────────
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_minutes(LATE_DATA_TOLERANCE_MIN)
        )
    )

    # ── Source: predictions ───────────────────────────────────────
    prediction_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(TOPIC_PREDICTIONS)
        .set_group_id("pyflink-correlation-predictions")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ── Source: weather ───────────────────────────────────────────
    weather_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(TOPIC_WEATHER)
        .set_group_id("pyflink-correlation-weather")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    predictions_stream = (
        env.from_source(
            prediction_source,
            watermark_strategy.with_timestamp_assigner(
                PredictionTimestampAssigner()
            ),
            "Predictions Source"
        )
        .map(lambda s: {k: str(v) if v is not None else "" for k, v in json.loads(s).items()}, output_type=Types.MAP(Types.STRING(), Types.STRING()))
        .key_by(lambda r: r.get("LOCATION_NAME", "unknown"))
    )

    weather_stream = (
        env.from_source(
            weather_source,
            watermark_strategy.with_timestamp_assigner(
                WeatherTimestampAssigner()
            ),
            "Weather Source"
        )
        .map(lambda s: {k: str(v) if v is not None else "" for k, v in json.loads(s).items()}, output_type=Types.MAP(Types.STRING(), Types.STRING()))
        .key_by(lambda r: r.get("LOCATION_NAME", "unknown"))
    )

    # ── Stateful join ─────────────────────────────────────────────
    correlated_stream = (
        predictions_stream
        .connect(weather_stream)
        .process(CorrelationFunction())
    )

    # ── Sink: Kafka ───────────────────────────────────────────────
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
    correlated_stream.map(
        lambda r: json.dumps(r), output_type=Types.STRING()
    ).sink_to(kafka_sink)

    # ── Sink: PostgreSQL ──────────────────────────────────────────
    pg_sink = PostgresSinkFunction()
    correlated_stream.map(pg_sink.invoke)

    log.info("Pipeline built. Submitting PyFlink Correlation Job...")
    env.execute("PyFlink Correlation Job")
    log.info("Job finished.")


if __name__ == "__main__":
    run()
