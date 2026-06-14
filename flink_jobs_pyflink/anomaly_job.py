"""
anomaly_job.py  (PyFlink version)
-----------------------------------
Detects anomalies in accuracy aggregates using event-time processing.

Key differences from flink_jobs/anomaly_job.py:
  - Reads from market-accuracy-aggregates as a continuous stream
  - Uses KeyedProcessFunction with stateful 7-day rolling baseline
  - WatermarkStrategy for event-time anomaly detection
  - State TTL: 8 days (covers 7-day baseline + buffer)
  - Produces alerts to arbitrage-alerts topic continuously
"""

import json
import os
import logging
import math
from datetime import datetime, timezone, timedelta

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)
from pyflink.common import WatermarkStrategy, Duration, Types, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor, StateTtlConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IN          = os.getenv("KAFKA_TOPIC_AGGREGATES",  "market-accuracy-aggregates")
TOPIC_ALERTS      = os.getenv("KAFKA_TOPIC_ALERTS",      "arbitrage-alerts")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",    "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",      "prediction_market")
POSTGRES_USER     = os.getenv("POSTGRES_USER",    "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD","changeme")

ACCURACY_DROP_THRESHOLD = 0.80
BIAS_THRESHOLD          = 0.30
MIN_PREDICTIONS         = 10
BASELINE_DAYS           = 7
SIGMA_MULTIPLIER        = 2.0
LATE_DATA_MIN           = 15


# ── Timestamp assigner ────────────────────────────────────────────
class AggregateTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        try:
            ts = value.get("POLL_TIMESTAMP") or value.get("WINDOW_END", "")
            if ts:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
        except Exception:
            pass
        return record_timestamp


# ── Stateful anomaly detection ────────────────────────────────────
class AnomalyDetectionFunction(KeyedProcessFunction):
    """
    Maintains 7-day rolling baseline of accuracy_rate and bias_score per
    location+market_type key. Detects anomalies using 2σ threshold.

    State:
      - accuracy_history: ListState[float] — last 7 days of accuracy values
      - bias_history:     ListState[float] — last 7 days of bias values
    Both have TTL of 8 days to auto-clean old data.
    """

    def open(self, runtime_context: RuntimeContext):
        ttl_config = (
            StateTtlConfig
            .new_builder(Time.days(8))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(
                StateTtlConfig.StateVisibility.NeverReturnExpired
            )
            .build()
        )

        acc_descriptor = ListStateDescriptor("accuracy_history", Types.FLOAT())
        acc_descriptor.enable_time_to_live(ttl_config)
        self.accuracy_history = runtime_context.get_list_state(acc_descriptor)

        bias_descriptor = ListStateDescriptor("bias_history", Types.FLOAT())
        bias_descriptor.enable_time_to_live(ttl_config)
        self.bias_history = runtime_context.get_list_state(bias_descriptor)

    def process_element(self, aggregate: dict, ctx, out):
        accuracy     = float(aggregate.get("accuracy_rate", 0))
        bias         = float(aggregate.get("bias_score", 0))
        total        = int(aggregate.get("total_predictions", 0))
        location     = aggregate.get("LOCATION_NAME", "unknown")
        market_type  = aggregate.get("MARKET_TYPE", "WEATHER")
        now          = datetime.now(timezone.utc).isoformat()

        if total < MIN_PREDICTIONS:
            return

        # Update rolling history
        self.accuracy_history.add(accuracy)
        self.bias_history.add(bias)

        # Compute baseline stats
        acc_values  = list(self.accuracy_history.get())
        bias_values = list(self.bias_history.get())

        acc_threshold, bias_threshold, source = self._compute_thresholds(
            acc_values, bias_values
        )

        # Detect accuracy drop
        if accuracy < acc_threshold:
            deviation = round(acc_threshold - accuracy, 4)
            severity  = "critical" if accuracy < acc_threshold * 0.7 else "high"
            alert = {
                "alert_type":    "accuracy_drop",
                "severity":      severity,
                "location_name": location,
                "market_type":   market_type,
                "message": (
                    f"Accuracy drop [{source}]: {location} {market_type} "
                    f"accuracy={accuracy*100:.1f}% < threshold={acc_threshold*100:.1f}%"
                ),
                "metric_value":    accuracy,
                "threshold_value": acc_threshold,
                "deviation":       deviation,
                "detected_at":     now,
            }
            out.collect(json.dumps(alert))
            log.warning("ACCURACY DROP: %s/%s %.1f%% < %.1f%%",
                        location, market_type, accuracy*100, acc_threshold*100)

        # Detect bias
        if abs(bias) > bias_threshold:
            deviation = round(abs(bias) - bias_threshold, 4)
            severity  = "high" if abs(bias) > bias_threshold * 1.5 else "medium"
            direction = "over-predicting" if bias > 0 else "under-predicting"
            alert = {
                "alert_type":    "bias_detected",
                "severity":      severity,
                "location_name": location,
                "market_type":   market_type,
                "message": (
                    f"Bias [{source}]: {location} {market_type} "
                    f"{direction} bias={bias:+.3f} threshold=±{bias_threshold:.3f}"
                ),
                "metric_value":    bias,
                "threshold_value": bias_threshold,
                "deviation":       deviation,
                "detected_at":     now,
            }
            out.collect(json.dumps(alert))
            log.warning("BIAS: %s/%s %+.3f", location, market_type, bias)

    def _compute_thresholds(
        self, acc_values: list, bias_values: list
    ) -> tuple[float, float, str]:
        """Returns (acc_threshold, bias_threshold, source)."""
        if len(acc_values) >= 3:
            n       = len(acc_values)
            acc_mean = sum(acc_values) / n
            acc_std  = math.sqrt(
                sum((x - acc_mean) ** 2 for x in acc_values) / n
            )
            bias_mean = sum(bias_values) / len(bias_values) if bias_values else 0
            bias_std  = math.sqrt(
                sum((x - bias_mean) ** 2 for x in bias_values) / len(bias_values)
            ) if bias_values else 0

            acc_threshold  = max(0.0, acc_mean - SIGMA_MULTIPLIER * acc_std)
            bias_threshold = bias_mean + SIGMA_MULTIPLIER * bias_std
            return acc_threshold, bias_threshold, "statistical"

        return ACCURACY_DROP_THRESHOLD, BIAS_THRESHOLD, "static"


# ── PostgreSQL sink for alerts ────────────────────────────────────
class AlertPostgresSink:
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

    def invoke(self, alert_json: str):
        alert = json.loads(alert_json)
        conn  = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO anomaly_alerts (
                        alert_type, severity, location_name, market_type,
                        message, metric_value, threshold_value, deviation,
                        detected_at
                    ) VALUES (
                        %(alert_type)s, %(severity)s, %(location_name)s,
                        %(market_type)s, %(message)s, %(metric_value)s,
                        %(threshold_value)s, %(deviation)s, %(detected_at)s
                    )
                """, alert)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error("Failed to save alert: %s", e)


# ── Main job ──────────────────────────────────────────────────────
def run():
    log.info("Starting PyFlink Anomaly Detection Job")

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
        .with_timestamp_assigner(AggregateTimestampAssigner())
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(TOPIC_IN)
        .set_group_id("pyflink-anomaly")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    alerts_stream = (
        env
        .from_source(source, watermark_strategy, "Aggregates Source")
        .map(lambda s: {k: str(v) if v is not None else "" for k, v in json.loads(s).items()}, output_type=Types.MAP(Types.STRING(), Types.STRING()))
        .key_by(lambda r: (
            r.get("LOCATION_NAME", "unknown"),
            r.get("MARKET_TYPE", "WEATHER")
        ))
        .process(AnomalyDetectionFunction())
    )

    # Sink: Kafka
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_ALERTS)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    alerts_stream.map(lambda r: json.dumps(r), output_type=Types.STRING()).sink_to(kafka_sink)

    # Sink: PostgreSQL
    pg_sink = AlertPostgresSink()
    alerts_stream.map(pg_sink.invoke)

    log.info("Submitting PyFlink Anomaly Detection Job...")
    env.execute("PyFlink Anomaly Detection Job")


if __name__ == "__main__":
    run()
