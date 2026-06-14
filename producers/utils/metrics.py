"""
metrics.py
----------
Shared Prometheus metrics definitions and HTTP server starter.
Each process calls start_metrics_server(port) once on startup.

Uses get-or-create helpers so the module is safe to import multiple
times in the same process (e.g. during pytest collection).
"""

import threading
import logging
from prometheus_client import (
    Counter, Gauge, Histogram,
    start_http_server, REGISTRY
)

log = logging.getLogger(__name__)


def _counter(name: str, help_text: str, labelnames: list = None) -> Counter:
    try:
        return Counter(name, help_text, labelnames or [])
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)


def _gauge(name: str, help_text: str, labelnames: list = None) -> Gauge:
    try:
        return Gauge(name, help_text, labelnames or [])
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)


def _histogram(name: str, help_text: str, buckets: list = None) -> Histogram:
    kwargs = {"buckets": buckets} if buckets else {}
    try:
        return Histogram(name, help_text, **kwargs)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)


# ── Manifold producer ─────────────────────────────────────────────
manifold_polls_total = _counter(
    "manifold_polls_total",
    "Total number of Manifold Markets API poll cycles"
)
manifold_markets_fetched_total = _counter(
    "manifold_markets_fetched_total",
    "Total weather markets fetched from Manifold API"
)
manifold_markets_produced_total = _counter(
    "manifold_markets_produced_total",
    "Total prediction records successfully produced to Kafka"
)
manifold_arbitrage_detected_total = _counter(
    "manifold_arbitrage_detected_total",
    "Total markets flagged as IS_ARBITRAGE=true"
)
manifold_poll_duration_seconds = _gauge(
    "manifold_poll_duration_seconds",
    "Duration of last Manifold poll cycle in seconds"
)
manifold_cities_tracked = _gauge(
    "manifold_cities_tracked",
    "Number of unique cities currently tracked"
)
manifold_api_errors_total = _counter(
    "manifold_api_errors_total",
    "Total Manifold API errors (retried or failed)"
)

# ── Weather producer ──────────────────────────────────────────────
weather_polls_total = _counter(
    "weather_polls_total",
    "Total number of Open-Meteo poll cycles"
)
weather_records_produced_total = _counter(
    "weather_records_produced_total",
    "Total weather records produced to Kafka",
    ["type"]
)
weather_poll_duration_seconds = _gauge(
    "weather_poll_duration_seconds",
    "Duration of last weather poll cycle in seconds"
)
weather_cities_active = _gauge(
    "weather_cities_active",
    "Number of cities fetched in last poll"
)
weather_api_errors_total = _counter(
    "weather_api_errors_total",
    "Total Open-Meteo API errors"
)
weather_geocoding_cache_size = _gauge(
    "weather_geocoding_cache_size",
    "Number of cities in geocoding cache"
)

# ── Correlation job ───────────────────────────────────────────────
correlation_processed_total = _counter(
    "correlation_processed_total",
    "Total predictions processed by correlation job"
)
correlation_matched_total = _counter(
    "correlation_matched_total",
    "Total predictions successfully correlated with weather data",
    ["method"]
)
correlation_unmatched_total = _counter(
    "correlation_unmatched_total",
    "Total predictions with no weather match found"
)
correlation_join_coverage = _gauge(
    "correlation_join_coverage_ratio",
    "Current join coverage ratio (matched / total)"
)
correlation_latency_seconds = _histogram(
    "correlation_latency_seconds",
    "Distribution of correlation latency (POLL_TIMESTAMP to correlation)",
    buckets=[30, 60, 120, 300, 600, 1800, 3600]
)
correlation_prediction_error = _gauge(
    "correlation_prediction_error_avg",
    "Average prediction error across all correlated records"
)

# ── Aggregation job ───────────────────────────────────────────────
aggregation_windows_computed_total = _counter(
    "aggregation_windows_computed_total",
    "Total aggregation windows computed"
)
aggregation_accuracy_rate = _gauge(
    "aggregation_accuracy_rate",
    "Latest overall accuracy rate across all markets"
)
aggregation_bias_score = _gauge(
    "aggregation_bias_score",
    "Latest average bias score (positive = over-prediction)"
)
aggregation_records_processed_total = _counter(
    "aggregation_records_processed_total",
    "Total correlation records processed by aggregation job"
)

# ── Anomaly / alert job ───────────────────────────────────────────
anomaly_alerts_fired_total = _counter(
    "anomaly_alerts_fired_total",
    "Total anomaly alerts fired",
    ["alert_type", "severity"]
)
anomaly_active_alerts = _gauge(
    "anomaly_active_alerts",
    "Number of currently unresolved alerts"
)
anomaly_arbitrage_opportunities = _gauge(
    "anomaly_arbitrage_opportunities_active",
    "Number of active arbitrage opportunities detected"
)

# ── Kafka producer health ─────────────────────────────────────────
kafka_produce_errors_total = _counter(
    "kafka_produce_errors_total",
    "Total Kafka produce errors",
    ["topic"]
)
kafka_messages_produced_total = _counter(
    "kafka_messages_produced_total",
    "Total messages successfully produced to Kafka",
    ["topic"]
)


def start_metrics_server(port: int, service_name: str = "") -> None:
    """Start Prometheus HTTP metrics server on the given port (non-blocking)."""
    def _serve():
        try:
            start_http_server(port)
            log.info("Prometheus metrics server started on port %d (%s)", port, service_name)
        except OSError as e:
            log.warning("Could not start metrics server on port %d: %s", port, e)

    t = threading.Thread(target=_serve, daemon=True)
    t.start()
