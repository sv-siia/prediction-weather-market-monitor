"""
anomaly_job.py
---------------
Reads from market-accuracy-aggregates Kafka topic.
Detects anomalies and arbitrage opportunities.
Writes alerts to:
  - arbitrage-alerts (Kafka topic)
  - anomaly_alerts (PostgreSQL table)

Detects:
  - Accuracy drop > 20% (accuracy_rate < 0.8)
  - High bias (|bias_score| > 0.3)
  - Mispricing arbitrage (IS_ARBITRAGE=True from producer)
  - Cross-market arbitrage (from manifold_producer detect_arbitrage)
"""

import io
import json
import os
import logging
import math
import struct
from datetime import datetime, timezone, timedelta


def _kafka_deserialize(v):
    try:
        return json.loads(v.decode("utf-8"))
    except Exception:
        try:
            from pathlib import Path
            import fastavro
            for schema_name in ("prediction.avsc", "aggregate.avsc", "alert.avsc", "correlation.avsc"):
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
from kafka import KafkaConsumer, KafkaProducer
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producers.utils.metrics import (
    start_metrics_server,
    anomaly_alerts_fired_total, anomaly_active_alerts,
    anomaly_arbitrage_opportunities,
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
TOPIC_IN           = os.getenv("KAFKA_TOPIC_AGGREGATES",    "market-accuracy-aggregates")
TOPIC_ALERTS       = os.getenv("KAFKA_TOPIC_ALERTS",        "arbitrage-alerts")
TOPIC_CORRELATIONS = os.getenv("KAFKA_TOPIC_CORRELATIONS",  "market-weather-correlations")
TOPIC_PREDICTIONS  = os.getenv("KAFKA_TOPIC_PREDICTIONS",   "polymarket-predictions-raw")
POSTGRES_HOST      = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT      = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB        = os.getenv("POSTGRES_DB",       "prediction_market")
POSTGRES_USER      = os.getenv("POSTGRES_USER",     "admin")
POSTGRES_PASSWORD  = os.getenv("POSTGRES_PASSWORD", "changeme")

# ── Thresholds (fallback when insufficient historical data) ───────
ACCURACY_DROP_THRESHOLD = 0.80
BIAS_THRESHOLD          = 0.30
MIN_PREDICTIONS         = 10
BASELINE_DAYS           = 7
BASELINE_MIN_POINTS     = 3   # minimum data points to use statistical baseline
SIGMA_MULTIPLIER        = 2.0 # mean ± 2σ

# ── Producer health config ────────────────────────────────────────
PRODUCER_TIMEOUT_MINUTES = 10   # alert if producer silent for this long
KNOWN_PRODUCERS = ["manifold_producer", "weather_producer"]


# ── Statistical baseline ──────────────────────────────────────────
def _mean_std(values: list[float]) -> tuple[float, float]:
    """Returns (mean, std) for a list of values."""
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    return mean, math.sqrt(variance)


def get_7day_baseline(conn, location: str, market_type: str) -> dict:
    """
    Queries last 7 days of aggregates for (location, market_type).
    Returns dynamic thresholds based on mean ± 2σ.
    Falls back to static thresholds if fewer than BASELINE_MIN_POINTS records.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=BASELINE_DAYS)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT accuracy_rate, bias_score, total_predictions
                FROM market_accuracy_aggregates
                WHERE location_name = %s
                  AND market_type   = %s
                  AND created_at   >= %s
                ORDER BY created_at
            """, (location, market_type, cutoff))
            rows = cur.fetchall()
    except Exception as e:
        log.warning("Baseline query failed for %s/%s: %s", location, market_type, e)
        rows = []

    if len(rows) < BASELINE_MIN_POINTS:
        return {
            "accuracy_threshold": ACCURACY_DROP_THRESHOLD,
            "bias_threshold":     BIAS_THRESHOLD,
            "source":             "static_fallback",
            "n":                  len(rows),
        }

    acc_values  = [float(r[0]) for r in rows if r[0] is not None]
    bias_values = [abs(float(r[1])) for r in rows if r[1] is not None]

    acc_mean,  acc_std  = _mean_std(acc_values)
    bias_mean, bias_std = _mean_std(bias_values)

    # Accuracy alert when current drops more than 2σ below historical mean
    acc_threshold  = max(0.0, acc_mean  - SIGMA_MULTIPLIER * acc_std)
    # Bias alert when current exceeds historical mean + 2σ
    bias_threshold = bias_mean + SIGMA_MULTIPLIER * bias_std

    return {
        "accuracy_threshold": acc_threshold,
        "bias_threshold":     bias_threshold,
        "acc_mean":           round(acc_mean,  4),
        "acc_std":            round(acc_std,   4),
        "bias_mean":          round(bias_mean, 4),
        "bias_std":           round(bias_std,  4),
        "source":             "7day_statistical",
        "n":                  len(rows),
    }


# ── PostgreSQL connection ─────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# ── Save alert to PostgreSQL ──────────────────────────────────────
def save_alert(alert: dict, conn) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO anomaly_alerts (
                    alert_type, severity, location_name,
                    market_type, message, metric_value,
                    threshold_value, deviation,
                    price_sum, arbitrage_margin, detected_at
                ) VALUES (
                    %(alert_type)s, %(severity)s, %(location_name)s,
                    %(market_type)s, %(message)s, %(metric_value)s,
                    %(threshold_value)s, %(deviation)s,
                    %(price_sum)s, %(arbitrage_margin)s, %(detected_at)s
                )
                ON CONFLICT (alert_type, location_name, market_type)
                DO UPDATE SET
                    severity = EXCLUDED.severity,
                    message = EXCLUDED.message,
                    metric_value = EXCLUDED.metric_value,
                    detected_at = EXCLUDED.detected_at
            """, alert)
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error(f"Failed to save alert: {e}")


# ── Detect anomalies from aggregates ─────────────────────────────
def detect_anomalies(aggregate: dict, baseline: dict) -> list:
    """
    Analyzes one aggregate record and returns list of alerts.
    Uses dynamic thresholds from 7-day baseline when available,
    falls back to static thresholds otherwise.
    """
    alerts   = []
    location = aggregate.get("LOCATION_NAME", "unknown")
    mtype    = aggregate.get("MARKET_TYPE", "WEATHER")
    accuracy = aggregate.get("accuracy_rate", 1.0)
    bias     = aggregate.get("bias_score", 0.0)
    total    = aggregate.get("total_predictions", 0)
    now      = datetime.now(timezone.utc).isoformat()

    if total < MIN_PREDICTIONS:
        return []

    acc_threshold  = baseline["accuracy_threshold"]
    bias_threshold = baseline["bias_threshold"]
    baseline_src   = baseline["source"]
    n_points       = baseline["n"]

    # Check 1: Accuracy drop
    if accuracy < acc_threshold:
        severity = "critical" if accuracy < acc_threshold * 0.6 else "high"
        baseline_info = (
            f"baseline=mean-2σ({baseline.get('acc_mean',0)*100:.1f}%±{baseline.get('acc_std',0)*100:.1f}%, n={n_points})"
            if baseline_src == "7day_statistical"
            else f"baseline=static({acc_threshold*100:.0f}%)"
        )
        alerts.append({
            "alert_type":       "accuracy_drop",
            "severity":         severity,
            "location_name":    location,
            "market_type":      mtype,
            "message":          (
                f"Accuracy drop: {location} {mtype} "
                f"{accuracy*100:.1f}% < threshold {acc_threshold*100:.1f}% "
                f"[{baseline_info}]"
            ),
            "metric_value":     accuracy,
            "threshold_value":  round(acc_threshold, 4),
            "deviation":        round(acc_threshold - accuracy, 4),
            "price_sum":        None,
            "arbitrage_margin": None,
            "detected_at":      now,
        })
        log.warning(
            "🚨 ACCURACY DROP: %s | %s | %.1f%% < %.1f%% [%s, n=%d] [%s]",
            location, mtype, accuracy * 100, acc_threshold * 100,
            baseline_src, n_points, severity,
        )

    # Check 2: High bias
    if abs(bias) > bias_threshold:
        direction = "over-predicting" if bias > 0 else "under-predicting"
        severity  = "high" if abs(bias) > bias_threshold * 1.5 else "medium"
        baseline_info = (
            f"baseline=mean+2σ({baseline.get('bias_mean',0):.3f}±{baseline.get('bias_std',0):.3f}, n={n_points})"
            if baseline_src == "7day_statistical"
            else f"baseline=static({bias_threshold:.2f})"
        )
        alerts.append({
            "alert_type":       "bias_detected",
            "severity":         severity,
            "location_name":    location,
            "market_type":      mtype,
            "message":          (
                f"Bias: {location} {mtype} {direction} "
                f"bias={bias:+.3f} > threshold {bias_threshold:.3f} "
                f"[{baseline_info}]"
            ),
            "metric_value":     bias,
            "threshold_value":  round(bias_threshold, 4),
            "deviation":        round(abs(bias) - bias_threshold, 4),
            "price_sum":        None,
            "arbitrage_margin": None,
            "detected_at":      now,
        })
        log.warning(
            "⚠️  BIAS: %s | %s | bias=%+.3f > %.3f [%s, n=%d] [%s]",
            location, mtype, bias, bias_threshold,
            baseline_src, n_points, severity,
        )

    return alerts


# ── Detect producer health timeouts ──────────────────────────────
JOIN_COVERAGE_THRESHOLD = 0.95  # alert if < 95% predictions matched


def detect_join_coverage(conn) -> list:
    """
    Checks what % of predictions in market_correlations have actual_outcome set.
    Alerts if join coverage drops below JOIN_COVERAGE_THRESHOLD (95%).
    """
    alerts = []
    now    = datetime.now(timezone.utc)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                         AS total,
                    COUNT(*) FILTER (WHERE actual_outcome IS NOT NULL) AS matched
                FROM market_correlations
            """)
            row = cur.fetchone()
            if not row or not row[0]:
                return []
            total, matched = int(row[0]), int(row[1])
    except Exception as e:
        log.warning("Could not compute join coverage: %s", e)
        return []

    if total == 0:
        return []

    coverage = matched / total
    log.info(
        "Join coverage: %d/%d = %.1f%% (threshold: %.0f%%)",
        matched, total, coverage * 100, JOIN_COVERAGE_THRESHOLD * 100
    )

    if coverage < JOIN_COVERAGE_THRESHOLD:
        severity = "critical" if coverage < 0.70 else "high"
        alerts.append({
            "alert_type":    "low_coverage",
            "severity":      severity,
            "location_name": "pipeline",
            "market_type":   "ALL",
            "message": (
                f"Join coverage {coverage*100:.1f}% below threshold "
                f"{JOIN_COVERAGE_THRESHOLD*100:.0f}%: "
                f"{matched}/{total} predictions matched with weather actuals"
            ),
            "metric_value":    round(coverage, 4),
            "threshold_value": JOIN_COVERAGE_THRESHOLD,
            "deviation":       round(JOIN_COVERAGE_THRESHOLD - coverage, 4),
            "detected_at":     now.isoformat(),
        })
        log.warning(
            "⚠️  LOW COVERAGE: %.1f%% (%d/%d) [%s]",
            coverage * 100, matched, total, severity
        )

    return alerts


def detect_producer_timeouts(conn) -> list:
    """
    Checks pipeline_health table for producers that haven't reported
    in more than PRODUCER_TIMEOUT_MINUTES minutes.

    Returns list of alerts for each silent producer.
    """
    alerts = []
    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=PRODUCER_TIMEOUT_MINUTES)
    now_str = now.isoformat()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (component)
                    component, status, recorded_at, messages_processed, error_count
                FROM pipeline_health
                ORDER BY component, recorded_at DESC
            """)
            rows = cur.fetchall()
    except Exception as e:
        log.warning("Could not query pipeline_health: %s", e)
        return []

    seen_components = set()
    for component, status, recorded_at, msgs, errors in rows:
        seen_components.add(component)

        # Make recorded_at timezone-aware for comparison
        if recorded_at.tzinfo is None:
            from datetime import timezone as tz
            recorded_at = recorded_at.replace(tzinfo=tz.utc)

        minutes_silent = (now - recorded_at).total_seconds() / 60

        if recorded_at < cutoff.replace(tzinfo=None if recorded_at.tzinfo is None else cutoff.tzinfo):
            severity = "critical" if minutes_silent > 30 else "high"
            alerts.append({
                "alert_type":       "producer_timeout",
                "severity":         severity,
                "location_name":    component,
                "market_type":      "SYSTEM",
                "message":          (
                    f"Producer silent: {component} has not reported for "
                    f"{minutes_silent:.1f} min (threshold: {PRODUCER_TIMEOUT_MINUTES} min). "
                    f"Last seen: {recorded_at.strftime('%Y-%m-%d %H:%M:%S UTC')} | "
                    f"status={status} msgs={msgs} errors={errors}"
                ),
                "metric_value":     round(minutes_silent, 1),
                "threshold_value":  float(PRODUCER_TIMEOUT_MINUTES),
                "deviation":        round(minutes_silent - PRODUCER_TIMEOUT_MINUTES, 1),
                "price_sum":        None,
                "arbitrage_margin": None,
                "detected_at":      now_str,
            })
            log.warning(
                "PRODUCER TIMEOUT: %s | silent=%.1f min | severity=%s | last=%s",
                component, minutes_silent, severity,
                recorded_at.strftime("%H:%M:%S"),
            )
        else:
            log.info(
                "Producer %s OK | last=%.1f min ago | status=%s | msgs=%d",
                component, minutes_silent, status, msgs or 0,
            )

    # Alert for producers that never reported at all
    for component in KNOWN_PRODUCERS:
        if component not in seen_components:
            alerts.append({
                "alert_type":       "producer_timeout",
                "severity":         "critical",
                "location_name":    component,
                "market_type":      "SYSTEM",
                "message":          f"Producer never reported: {component} has no records in pipeline_health",
                "metric_value":     None,
                "threshold_value":  float(PRODUCER_TIMEOUT_MINUTES),
                "deviation":        None,
                "price_sum":        None,
                "arbitrage_margin": None,
                "detected_at":      now_str,
            })
            log.warning("PRODUCER NEVER REPORTED: %s", component)

    return alerts


# ── Detect arbitrage from raw predictions ─────────────────────────
def detect_arbitrage(predictions: list) -> list:
    """
    Scans raw prediction records for arbitrage opportunities.

    Two sources:
    1. IS_ARBITRAGE=True flag set by manifold_producer (mispricing)
    2. DEVIATION_FROM_BASE field for cross-market arbitrage
    """
    alerts = []
    now    = datetime.now(timezone.utc).isoformat()
    seen   = set()

    for r in predictions:
        condition_id = r.get("condition_id")
        if not condition_id or condition_id in seen:
            continue
        seen.add(condition_id)

        location = r.get("LOCATION_NAME", "unknown")
        mtype    = r.get("MARKET_TYPE", "WEATHER")
        price    = r.get("price", 0.5)

        # Type 1: Mispricing flagged by producer
        if r.get("IS_ARBITRAGE"):
            base_rate = r.get("BASE_RATE", 0.4)
            deviation = r.get("DEVIATION_FROM_BASE", 0)
            margin    = round(deviation * 100, 2)
            severity  = "critical" if deviation > 0.5 else "high"

            alerts.append({
                "alert_type":       "arbitrage_opportunity",
                "severity":         severity,
                "location_name":    location,
                "market_type":      mtype,
                "message":          (
                    f"Mispricing arbitrage: {location} {mtype} "
                    f"price={price:.3f} base={base_rate:.3f} "
                    f"deviation={margin:.2f}%"
                ),
                "metric_value":     price,
                "threshold_value":  base_rate,
                "deviation":        round(deviation, 4),
                "price_sum":        price,
                "arbitrage_margin": margin,
                "detected_at":      now,
            })
            log.warning(
                f"💰 MISPRICING: {location} | {mtype} | "
                f"price={price:.3f} base={base_rate:.3f} "
                f"margin={margin:.2f}% [{severity}]"
            )

    return alerts


# ── Main job ──────────────────────────────────────────────────────
def run():
    start_metrics_server(8004, "anomaly_job")
    log.info("Starting Anomaly Detection Job")
    log.info(f"Kafka:    {KAFKA_BROKER}")
    log.info(f"Input:    {TOPIC_IN}")
    log.info(f"Alerts:   {TOPIC_ALERTS}")
    log.info(
        f"Thresholds: accuracy<{ACCURACY_DROP_THRESHOLD*100:.0f}% | "
        f"|bias|>{BIAS_THRESHOLD}"
    )

    conn = get_db_connection()
    log.info("Connected to PostgreSQL ✅")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )

    # Read aggregates from PostgreSQL (most recent window per location+type)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (location_name, market_type)
                    location_name, market_type,
                    accuracy_rate, bias_score, total_predictions,
                    window_start, window_end
                FROM market_accuracy_aggregates
                ORDER BY location_name, market_type, window_start DESC
            """)
            rows = cur.fetchall()
        aggregates = [
            {
                "LOCATION_NAME":     r[0],
                "MARKET_TYPE":       r[1],
                "accuracy_rate":     float(r[2]) if r[2] is not None else 0.0,
                "bias_score":        float(r[3]) if r[3] is not None else 0.0,
                "total_predictions": int(r[4])   if r[4] is not None else 0,
            }
            for r in rows
        ]
    except Exception as e:
        log.error("Failed to load aggregates from PostgreSQL: %s", e)
        aggregates = []
    log.info(f"Loaded {len(aggregates)} aggregates from PostgreSQL")

    # Read raw predictions for arbitrage detection
    consumer_pred = KafkaConsumer(
        TOPIC_PREDICTIONS,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=_kafka_deserialize,
        group_id=None,
    )
    predictions = [msg.value for msg in consumer_pred if msg.value is not None]
    consumer_pred.close()
    log.info(f"Loaded {len(predictions)} predictions for arbitrage scan")

    # Detect anomalies
    all_alerts   = []
    accuracy_cnt = 0
    bias_cnt     = 0

    for aggregate in aggregates:
        location = aggregate.get("LOCATION_NAME", "unknown")
        mtype    = aggregate.get("MARKET_TYPE", "WEATHER")
        baseline = get_7day_baseline(conn, location, mtype)
        log.info(
            "Baseline for %s/%s: source=%s n=%d acc_threshold=%.3f bias_threshold=%.3f",
            location, mtype, baseline["source"], baseline["n"],
            baseline["accuracy_threshold"], baseline["bias_threshold"],
        )
        alerts = detect_anomalies(aggregate, baseline)
        all_alerts.extend(alerts)
        for a in alerts:
            if a["alert_type"] == "accuracy_drop":
                accuracy_cnt += 1
            elif a["alert_type"] == "bias_detected":
                bias_cnt += 1

    # Detect arbitrage from raw predictions
    arb_alerts = detect_arbitrage(predictions)
    all_alerts.extend(arb_alerts)

    # Auto-resolve producer_timeout alerts for producers that are now healthy
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE anomaly_alerts a
                SET is_resolved = TRUE, resolved_at = NOW()
                WHERE a.alert_type = 'producer_timeout'
                  AND a.is_resolved = FALSE
                  AND EXISTS (
                      SELECT 1 FROM (
                          SELECT DISTINCT ON (component) component, recorded_at
                          FROM pipeline_health ORDER BY component, recorded_at DESC
                      ) latest
                      WHERE latest.component = a.location_name
                        AND latest.recorded_at > NOW() - INTERVAL '10 minutes'
                  )
            """)
            resolved = cur.rowcount
        conn.commit()
        if resolved:
            log.info("Auto-resolved %d stale producer_timeout alert(s)", resolved)
    except Exception as e:
        conn.rollback()
        log.warning("Could not auto-resolve producer alerts: %s", e)

    # Detect producer health timeouts
    health_alerts = detect_producer_timeouts(conn)
    all_alerts.extend(health_alerts)

    # Detect join coverage drop
    coverage_alerts = detect_join_coverage(conn)
    all_alerts.extend(coverage_alerts)

    # Save and produce alerts
    for alert in all_alerts:
        save_alert(alert, conn)
        producer.send(TOPIC_ALERTS, value=alert)
        anomaly_alerts_fired_total.labels(
            alert_type=alert.get("alert_type", "unknown"),
            severity=alert.get("severity", "unknown"),
        ).inc()
        kafka_messages_produced_total.labels(topic=TOPIC_ALERTS).inc()

    anomaly_active_alerts.set(len(all_alerts))
    anomaly_arbitrage_opportunities.set(len(arb_alerts))

    producer.flush()
    producer.close()
    conn.close()

    log.info("=" * 55)
    log.info("ANOMALY DETECTION COMPLETE")
    log.info(f"Total alerts:     {len(all_alerts)}")
    log.info(f"Accuracy drops:   {accuracy_cnt}")
    log.info(f"Bias detections:  {bias_cnt}")
    log.info(f"Arbitrage alerts: {len(arb_alerts)}")
    log.info(f"Health timeouts:  {len(health_alerts)}")
    log.info(f"Coverage alerts:  {len(coverage_alerts)}")
    log.info("=" * 55)


if __name__ == "__main__":
    run()