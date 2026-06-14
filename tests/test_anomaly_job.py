"""
Tests for flink_jobs/anomaly_job.py — detect_anomalies, detect_arbitrage,
save_alert, _mean_std, get_7day_baseline.
No live Kafka or PostgreSQL required.
"""
import sys
import os
import math
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flink_jobs.anomaly_job import (
    detect_anomalies,
    detect_arbitrage,
    _mean_std,
    get_7day_baseline,
    ACCURACY_DROP_THRESHOLD,
    BIAS_THRESHOLD,
    BASELINE_MIN_POINTS,
)


# ── Helpers ───────────────────────────────────────────────────────

def _static_baseline(acc=ACCURACY_DROP_THRESHOLD, bias=BIAS_THRESHOLD):
    """Static fallback baseline dict (mimics get_7day_baseline with insufficient data)."""
    return {
        "accuracy_threshold": acc,
        "bias_threshold":     bias,
        "source":             "static_fallback",
        "n":                  0,
    }


def _statistical_baseline(acc_mean=0.90, acc_std=0.05,
                           bias_mean=0.10, bias_std=0.05, n=10):
    """Statistical baseline dict (mimics get_7day_baseline with enough data)."""
    acc_threshold  = max(0.0, acc_mean  - 2 * acc_std)
    bias_threshold = bias_mean + 2 * bias_std
    return {
        "accuracy_threshold": acc_threshold,
        "bias_threshold":     bias_threshold,
        "acc_mean":           round(acc_mean, 4),
        "acc_std":            round(acc_std, 4),
        "bias_mean":          round(bias_mean, 4),
        "bias_std":           round(bias_std, 4),
        "source":             "7day_statistical",
        "n":                  n,
    }


# ── _mean_std ─────────────────────────────────────────────────────

class TestMeanStd:

    def test_empty_list_returns_zeros(self):
        mean, std = _mean_std([])
        assert mean == 0.0
        assert std  == 0.0

    def test_single_value_zero_std(self):
        mean, std = _mean_std([0.8])
        assert mean == pytest.approx(0.8)
        assert std  == pytest.approx(0.0)

    def test_known_mean_std(self):
        # [0.6, 0.8, 1.0] → mean=0.8, population std = sqrt((0.04+0+0.04)/3) ≈ 0.1633
        mean, std = _mean_std([0.6, 0.8, 1.0])
        assert mean == pytest.approx(0.8, abs=1e-9)
        assert std  == pytest.approx(math.sqrt(0.08 / 3), abs=1e-6)

    def test_uniform_values_zero_std(self):
        mean, std = _mean_std([0.5, 0.5, 0.5])
        assert mean == pytest.approx(0.5)
        assert std  == pytest.approx(0.0)

    def test_two_values(self):
        mean, std = _mean_std([0.0, 1.0])
        assert mean == pytest.approx(0.5)
        assert std  == pytest.approx(0.5)


# ── get_7day_baseline ─────────────────────────────────────────────

class TestGet7DayBaseline:

    def _mock_conn(self, rows):
        conn   = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        return conn

    def test_fewer_than_min_points_returns_static_fallback(self):
        conn   = self._mock_conn([(0.8, 0.1, 20)])  # only 1 row < BASELINE_MIN_POINTS
        result = get_7day_baseline(conn, "London", "RAIN")
        assert result["source"] == "static_fallback"
        assert result["accuracy_threshold"] == ACCURACY_DROP_THRESHOLD
        assert result["bias_threshold"]     == BIAS_THRESHOLD

    def test_empty_rows_returns_static_fallback(self):
        conn   = self._mock_conn([])
        result = get_7day_baseline(conn, "London", "RAIN")
        assert result["source"] == "static_fallback"
        assert result["n"] == 0

    def test_sufficient_rows_returns_statistical_baseline(self):
        rows = [(0.9, 0.05, 30)] * 5  # 5 rows >= BASELINE_MIN_POINTS=3
        conn = self._mock_conn(rows)
        result = get_7day_baseline(conn, "London", "RAIN")
        assert result["source"] == "7day_statistical"
        assert result["n"] == 5

    def test_statistical_accuracy_threshold_is_mean_minus_2sigma(self):
        # acc_values all = 0.9 → mean=0.9, std=0
        rows = [(0.9, 0.1, 30)] * 5
        conn = self._mock_conn(rows)
        result = get_7day_baseline(conn, "London", "RAIN")
        assert result["accuracy_threshold"] == pytest.approx(0.9, abs=1e-6)

    def test_statistical_bias_threshold_is_mean_plus_2sigma(self):
        # bias_values all = 0.1 → mean=0.1, std=0
        rows = [(0.9, 0.1, 30)] * 5
        conn = self._mock_conn(rows)
        result = get_7day_baseline(conn, "London", "RAIN")
        assert result["bias_threshold"] == pytest.approx(0.1, abs=1e-6)

    def test_accuracy_threshold_never_below_zero(self):
        # Extreme case: huge spread could give negative threshold
        rows = [(0.1, 0.9, 5), (0.9, 0.1, 5), (0.1, 0.9, 5), (0.9, 0.1, 5), (0.5, 0.5, 5)]
        conn = self._mock_conn(rows)
        result = get_7day_baseline(conn, "London", "RAIN")
        assert result["accuracy_threshold"] >= 0.0

    def test_db_error_returns_static_fallback(self):
        conn   = MagicMock()
        conn.cursor.side_effect = Exception("DB down")
        result = get_7day_baseline(conn, "London", "RAIN")
        assert result["source"] == "static_fallback"

    def test_returns_acc_mean_and_std_keys(self):
        rows = [(0.8, 0.2, 20), (0.9, 0.1, 20), (0.85, 0.15, 20), (0.7, 0.3, 20)]
        conn = self._mock_conn(rows)
        result = get_7day_baseline(conn, "London", "RAIN")
        for key in ("acc_mean", "acc_std", "bias_mean", "bias_std"):
            assert key in result


# ── detect_anomalies ──────────────────────────────────────────────

class TestDetectAnomalies:

    def _agg(self, accuracy=0.85, bias=0.05, total=20):
        return {
            "LOCATION_NAME":     "London",
            "MARKET_TYPE":       "RAIN",
            "accuracy_rate":     accuracy,
            "bias_score":        bias,
            "total_predictions": total,
        }

    def test_healthy_aggregate_no_alerts(self):
        alerts = detect_anomalies(self._agg(), _static_baseline())
        assert alerts == []

    def test_accuracy_drop_generates_alert(self):
        alerts = detect_anomalies(self._agg(accuracy=0.60), _static_baseline())
        types  = [a["alert_type"] for a in alerts]
        assert "accuracy_drop" in types

    def test_accuracy_drop_critical_severity(self):
        # accuracy < threshold * 0.6 → critical
        bl     = _static_baseline(acc=0.80)
        alerts = detect_anomalies(self._agg(accuracy=0.45), bl)
        drop   = next(a for a in alerts if a["alert_type"] == "accuracy_drop")
        assert drop["severity"] == "critical"

    def test_accuracy_drop_high_severity(self):
        bl     = _static_baseline(acc=0.80)
        alerts = detect_anomalies(self._agg(accuracy=0.65), bl)
        drop   = next(a for a in alerts if a["alert_type"] == "accuracy_drop")
        assert drop["severity"] == "high"

    def test_high_positive_bias_generates_alert(self):
        alerts = detect_anomalies(self._agg(bias=0.45), _static_baseline())
        types  = [a["alert_type"] for a in alerts]
        assert "bias_detected" in types

    def test_high_negative_bias_generates_alert(self):
        alerts = detect_anomalies(self._agg(bias=-0.45), _static_baseline())
        types  = [a["alert_type"] for a in alerts]
        assert "bias_detected" in types

    def test_bias_severity_high_when_above_threshold_1_5x(self):
        # bias_threshold=0.30, 1.5x = 0.45 → bias=0.6 → high
        bl     = _static_baseline(bias=0.30)
        alerts = detect_anomalies(self._agg(bias=0.6), bl)
        bias_a = next(a for a in alerts if a["alert_type"] == "bias_detected")
        assert bias_a["severity"] == "high"

    def test_bias_severity_medium_between_threshold_and_1_5x(self):
        bl     = _static_baseline(bias=0.30)
        alerts = detect_anomalies(self._agg(bias=0.35), bl)
        bias_a = next(a for a in alerts if a["alert_type"] == "bias_detected")
        assert bias_a["severity"] == "medium"

    def test_skips_aggregate_below_min_predictions(self):
        alerts = detect_anomalies(self._agg(accuracy=0.1, bias=0.9, total=5), _static_baseline())
        assert alerts == []

    def test_both_accuracy_and_bias_alerts(self):
        alerts = detect_anomalies(self._agg(accuracy=0.4, bias=0.5), _static_baseline())
        types  = {a["alert_type"] for a in alerts}
        assert "accuracy_drop" in types
        assert "bias_detected" in types

    def test_alert_has_required_fields(self):
        alerts = detect_anomalies(self._agg(accuracy=0.5), _static_baseline())
        assert len(alerts) > 0
        for field in ("alert_type", "severity", "location_name", "market_type",
                      "message", "metric_value", "threshold_value", "detected_at"):
            assert field in alerts[0], f"Missing field: {field}"

    def test_accuracy_at_exact_threshold_not_flagged(self):
        bl     = _static_baseline(acc=0.80)
        alerts = detect_anomalies(self._agg(accuracy=0.80), bl)
        types  = [a["alert_type"] for a in alerts]
        assert "accuracy_drop" not in types

    def test_bias_at_exact_threshold_not_flagged(self):
        bl     = _static_baseline(bias=0.30)
        alerts = detect_anomalies(self._agg(bias=0.30), bl)
        types  = [a["alert_type"] for a in alerts]
        assert "bias_detected" not in types

    def test_deviation_field_is_positive(self):
        alerts = detect_anomalies(self._agg(accuracy=0.60), _static_baseline())
        drop   = next(a for a in alerts if a["alert_type"] == "accuracy_drop")
        assert drop["deviation"] > 0

    # ── Statistical baseline integration ──────────────────────────

    def test_statistical_baseline_tighter_threshold_no_alert(self):
        # Historical mean=0.95 → threshold = 0.85. accuracy=0.87 → no alert.
        bl     = _statistical_baseline(acc_mean=0.95, acc_std=0.05)  # threshold=0.85
        alerts = detect_anomalies(self._agg(accuracy=0.87), bl)
        types  = [a["alert_type"] for a in alerts]
        assert "accuracy_drop" not in types

    def test_statistical_baseline_tighter_threshold_triggers_alert(self):
        # Historical mean=0.95 → threshold = 0.85. accuracy=0.82 → alert.
        bl     = _statistical_baseline(acc_mean=0.95, acc_std=0.05)
        alerts = detect_anomalies(self._agg(accuracy=0.82), bl)
        types  = [a["alert_type"] for a in alerts]
        assert "accuracy_drop" in types

    def test_statistical_baseline_message_includes_mean_std(self):
        bl     = _statistical_baseline(acc_mean=0.95, acc_std=0.05, n=10)
        alerts = detect_anomalies(self._agg(accuracy=0.82), bl)
        drop   = next(a for a in alerts if a["alert_type"] == "accuracy_drop")
        assert "mean-2σ" in drop["message"]

    def test_static_fallback_message_includes_static_label(self):
        bl     = _static_baseline()
        alerts = detect_anomalies(self._agg(accuracy=0.60), bl)
        drop   = next(a for a in alerts if a["alert_type"] == "accuracy_drop")
        assert "static" in drop["message"]

    def test_threshold_value_reflects_baseline(self):
        # With statistical baseline, threshold_value should NOT be the static 0.80
        bl     = _statistical_baseline(acc_mean=0.95, acc_std=0.05)  # threshold=0.85
        alerts = detect_anomalies(self._agg(accuracy=0.82), bl)
        drop   = next(a for a in alerts if a["alert_type"] == "accuracy_drop")
        assert drop["threshold_value"] == pytest.approx(0.85, abs=1e-4)


# ── detect_arbitrage ─────────────────────────────────────────────

class TestDetectArbitrage:

    def _pred(self, is_arb=True, deviation=0.45, base_rate=0.35, price=0.8,
              condition_id="c1"):
        return {
            "condition_id":        condition_id,
            "LOCATION_NAME":       "London",
            "MARKET_TYPE":         "RAIN",
            "price":               price,
            "IS_ARBITRAGE":        is_arb,
            "DEVIATION_FROM_BASE": deviation,
            "BASE_RATE":           base_rate,
        }

    def test_no_arbitrage_flag_returns_no_alerts(self):
        assert detect_arbitrage([self._pred(is_arb=False)]) == []

    def test_arbitrage_flag_generates_alert(self):
        alerts = detect_arbitrage([self._pred(is_arb=True)])
        assert len(alerts) == 1
        assert alerts[0]["alert_type"] == "arbitrage_opportunity"

    def test_deduplicates_same_condition_id(self):
        preds  = [self._pred(condition_id="same"), self._pred(condition_id="same")]
        assert len(detect_arbitrage(preds)) == 1

    def test_different_condition_ids_both_flagged(self):
        preds  = [self._pred(condition_id="a"), self._pred(condition_id="b")]
        assert len(detect_arbitrage(preds)) == 2

    def test_critical_severity_for_large_deviation(self):
        alerts = detect_arbitrage([self._pred(deviation=0.6)])
        assert alerts[0]["severity"] == "critical"

    def test_high_severity_for_moderate_deviation(self):
        alerts = detect_arbitrage([self._pred(deviation=0.3)])
        assert alerts[0]["severity"] == "high"

    def test_alert_has_required_fields(self):
        alerts = detect_arbitrage([self._pred()])
        for field in ("alert_type", "severity", "location_name", "market_type",
                      "message", "metric_value", "arbitrage_margin", "detected_at"):
            assert field in alerts[0], f"Missing field: {field}"

    def test_empty_predictions_returns_no_alerts(self):
        assert detect_arbitrage([]) == []

    def test_skips_record_without_condition_id(self):
        pred = {"IS_ARBITRAGE": True, "LOCATION_NAME": "London"}
        assert detect_arbitrage([pred]) == []

    def test_arbitrage_margin_calculated(self):
        alerts = detect_arbitrage([self._pred(deviation=0.5)])
        assert alerts[0]["arbitrage_margin"] == pytest.approx(50.0, abs=0.1)


# ── save_alert (DB) ───────────────────────────────────────────────

class TestSaveAlert:

    def _alert(self):
        return {
            "alert_type": "accuracy_drop", "severity": "high",
            "location_name": "London", "market_type": "RAIN",
            "message": "test", "metric_value": 0.5, "threshold_value": 0.8,
            "deviation": 0.3, "price_sum": None, "arbitrage_margin": None,
            "detected_at": "2026-05-19T12:00:00Z",
        }

    def test_commits_on_success(self):
        from flink_jobs.anomaly_job import save_alert
        conn   = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        save_alert(self._alert(), conn)
        conn.commit.assert_called_once()

    def test_rollback_on_db_error(self):
        from flink_jobs.anomaly_job import save_alert
        conn   = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("DB error")
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        save_alert(self._alert(), conn)  # must not raise
        conn.rollback.assert_called_once()
