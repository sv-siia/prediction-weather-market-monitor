"""
Tests for flink_jobs/aggregation_job.py — compute_aggregates and save_aggregate.
No live Kafka or PostgreSQL required.
"""
import sys
import os
import pytest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flink_jobs.aggregation_job import compute_aggregates


# ── compute_aggregates ────────────────────────────────────────────

class TestComputeAggregates:

    def _record(self, price, outcome, volume=100):
        return {
            "price":          price,
            "ACTUAL_OUTCOME": outcome,
            "PREDICTION_ERROR": abs(price - outcome),
            "VOLUME":         volume,
        }

    def test_empty_returns_empty_dict(self):
        assert compute_aggregates([]) == {}

    def test_all_correct_high_price(self):
        records = [self._record(0.8, 1) for _ in range(10)]
        result  = compute_aggregates(records)
        assert result["accuracy_rate"] == 1.0

    def test_all_correct_low_price(self):
        records = [self._record(0.3, 0) for _ in range(10)]
        result  = compute_aggregates(records)
        assert result["accuracy_rate"] == 1.0

    def test_all_wrong(self):
        records = [self._record(0.8, 0) for _ in range(10)]
        result  = compute_aggregates(records)
        assert result["accuracy_rate"] == 0.0

    def test_mixed_accuracy(self):
        records = [
            self._record(0.8, 1),  # correct
            self._record(0.8, 1),  # correct
            self._record(0.8, 0),  # wrong
            self._record(0.2, 0),  # correct
        ]
        result = compute_aggregates(records)
        assert result["accuracy_rate"] == pytest.approx(0.75, abs=0.01)

    def test_total_predictions_count(self):
        records = [self._record(0.7, 1) for _ in range(7)]
        result  = compute_aggregates(records)
        assert result["total_predictions"] == 7

    def test_correct_predictions_count(self):
        records = [
            self._record(0.8, 1),
            self._record(0.8, 1),
            self._record(0.2, 0),
            self._record(0.9, 0),  # wrong
        ]
        result = compute_aggregates(records)
        assert result["correct_predictions"] == 3

    def test_avg_prediction_error(self):
        records = [
            {"price": 0.8, "ACTUAL_OUTCOME": 1, "PREDICTION_ERROR": 0.2, "VOLUME": 1},
            {"price": 0.6, "ACTUAL_OUTCOME": 1, "PREDICTION_ERROR": 0.4, "VOLUME": 1},
        ]
        result = compute_aggregates(records)
        assert result["avg_prediction_error"] == pytest.approx(0.3, abs=0.001)

    def test_min_max_prediction_error(self):
        records = [
            {"price": 0.9, "ACTUAL_OUTCOME": 1, "PREDICTION_ERROR": 0.1, "VOLUME": 1},
            {"price": 0.5, "ACTUAL_OUTCOME": 1, "PREDICTION_ERROR": 0.5, "VOLUME": 1},
        ]
        result = compute_aggregates(records)
        assert result["min_prediction_error"] == pytest.approx(0.1, abs=0.001)
        assert result["max_prediction_error"] == pytest.approx(0.5, abs=0.001)

    def test_positive_bias_when_over_predicting(self):
        # price=0.8 consistently, outcome=0 → over-predicting YES
        records = [self._record(0.8, 0) for _ in range(10)]
        result  = compute_aggregates(records)
        assert result["bias_score"] > 0

    def test_negative_bias_when_under_predicting(self):
        # price=0.2 consistently, outcome=1 → under-predicting YES
        records = [self._record(0.2, 1) for _ in range(10)]
        result  = compute_aggregates(records)
        assert result["bias_score"] < 0

    def test_zero_bias_when_calibrated(self):
        # price=0.5, half outcomes 1 and half 0 → near-zero bias
        records = [self._record(0.5, 1) for _ in range(5)]
        records += [self._record(0.5, 0) for _ in range(5)]
        result = compute_aggregates(records)
        assert abs(result["bias_score"]) < 0.1

    def test_over_prediction_count(self):
        records = [
            self._record(0.8, 0),  # over-predict
            self._record(0.8, 0),  # over-predict
            self._record(0.8, 1),  # correct
        ]
        result = compute_aggregates(records)
        assert result["over_prediction_count"] == 2

    def test_under_prediction_count(self):
        records = [
            self._record(0.3, 1),  # under-predict
            self._record(0.7, 1),  # correct (above 0.5)
        ]
        result = compute_aggregates(records)
        assert result["under_prediction_count"] == 1

    def test_volume_weighted_accuracy(self):
        records = [
            {"price": 0.8, "ACTUAL_OUTCOME": 1, "PREDICTION_ERROR": 0.2, "VOLUME": 900},  # correct
            {"price": 0.8, "ACTUAL_OUTCOME": 0, "PREDICTION_ERROR": 0.8, "VOLUME": 100},  # wrong
        ]
        result = compute_aggregates(records)
        # 900 correct out of 1000 total volume
        assert result["volume_weighted_accuracy"] == pytest.approx(0.9, abs=0.01)

    def test_missing_volume_defaults_to_one(self):
        records = [
            {"price": 0.8, "ACTUAL_OUTCOME": 1, "PREDICTION_ERROR": 0.2},
            {"price": 0.8, "ACTUAL_OUTCOME": 1, "PREDICTION_ERROR": 0.2},
        ]
        result = compute_aggregates(records)
        assert result["total_predictions"] == 2

    def test_none_outcome_skipped_in_accuracy(self):
        records = [
            {"price": 0.8, "ACTUAL_OUTCOME": None, "PREDICTION_ERROR": None, "VOLUME": 1},
            {"price": 0.7, "ACTUAL_OUTCOME": 1,    "PREDICTION_ERROR": 0.3,  "VOLUME": 1},
        ]
        # Only the second record has a valid outcome
        result = compute_aggregates(records)
        assert result["total_predictions"] == 2
        assert result["correct_predictions"] == 1

    def test_result_has_all_expected_keys(self):
        records = [self._record(0.7, 1)]
        result  = compute_aggregates(records)
        expected_keys = {
            "total_predictions", "correct_predictions", "accuracy_rate",
            "avg_prediction_error", "min_prediction_error", "max_prediction_error",
            "total_volume", "volume_weighted_accuracy", "bias_score",
            "over_prediction_count", "under_prediction_count",
        }
        assert expected_keys.issubset(result.keys())


# ── save_aggregate (DB integration) ──────────────────────────────

class TestSaveAggregate:
    def test_calls_execute_with_correct_params(self):
        from datetime import datetime, timezone
        from flink_jobs.aggregation_job import save_aggregate

        conn   = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__  = MagicMock(return_value=False)

        now     = datetime.now(timezone.utc)
        metrics = {
            "total_predictions": 10, "correct_predictions": 8,
            "accuracy_rate": 0.8, "avg_prediction_error": 0.2,
            "min_prediction_error": 0.05, "max_prediction_error": 0.45,
            "total_volume": 500.0, "volume_weighted_accuracy": 0.81,
            "bias_score": 0.05, "over_prediction_count": 1,
            "under_prediction_count": 1,
        }

        save_aggregate("London", "RAIN", now, now, metrics, conn)

        assert cursor.execute.called
        conn.commit.assert_called_once()

    def test_rollback_on_db_error(self):
        from datetime import datetime, timezone
        from flink_jobs.aggregation_job import save_aggregate

        conn   = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("DB error")
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__  = MagicMock(return_value=False)

        now     = datetime.now(timezone.utc)
        metrics = {
            "total_predictions": 1, "correct_predictions": 1,
            "accuracy_rate": 1.0, "avg_prediction_error": 0.0,
            "min_prediction_error": 0.0, "max_prediction_error": 0.0,
            "total_volume": 1.0, "volume_weighted_accuracy": 1.0,
            "bias_score": 0.0, "over_prediction_count": 0,
            "under_prediction_count": 0,
        }

        # Should not raise — error is logged and rolled back
        save_aggregate("London", "RAIN", now, now, metrics, conn)
        conn.rollback.assert_called_once()
