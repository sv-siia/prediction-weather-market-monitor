"""Tests for CircuitBreaker."""
import time
import pytest
from unittest.mock import MagicMock
from producers.utils.circuit_breaker import CircuitBreaker, CircuitBreakerError, CircuitState


def make_cb(**kwargs) -> CircuitBreaker:
    defaults = {"name": "test", "failure_threshold": 3, "recovery_timeout": 0.1}
    defaults.update(kwargs)
    return CircuitBreaker(**defaults)


# ── Initial state ─────────────────────────────────────────────────

class TestInitialState:
    def test_starts_closed(self):
        cb = make_cb()
        assert cb.state == CircuitState.CLOSED

    def test_repr_contains_name(self):
        cb = make_cb(name="myapi")
        assert "myapi" in repr(cb)

    def test_repr_contains_state(self):
        cb = make_cb()
        assert "closed" in repr(cb)


# ── Successful calls ──────────────────────────────────────────────

class TestSuccessfulCalls:
    def test_passes_through_return_value(self):
        cb  = make_cb()
        fn  = MagicMock(return_value=42)
        res = cb.call(fn)
        assert res == 42

    def test_calls_fn_with_args(self):
        cb = make_cb()
        fn = MagicMock(return_value="ok")
        cb.call(fn, 1, 2, key="val")
        fn.assert_called_once_with(1, 2, key="val")

    def test_stays_closed_after_success(self):
        cb = make_cb()
        cb.call(MagicMock(return_value=None))
        assert cb.state == CircuitState.CLOSED

    def test_resets_failure_count_on_success(self):
        cb = make_cb(failure_threshold=5)
        failing = MagicMock(side_effect=[ValueError, ValueError, None])
        for _ in range(2):
            try:
                cb.call(failing)
            except ValueError:
                pass
        cb.call(MagicMock(return_value=None))
        assert cb._failure_count == 0


# ── Failure handling ──────────────────────────────────────────────

class TestFailureHandling:
    def test_increments_failure_count(self):
        cb      = make_cb()
        failing = MagicMock(side_effect=RuntimeError("boom"))
        try:
            cb.call(failing)
        except RuntimeError:
            pass
        assert cb._failure_count == 1

    def test_re_raises_original_exception(self):
        cb = make_cb()
        with pytest.raises(ValueError, match="oops"):
            cb.call(MagicMock(side_effect=ValueError("oops")))

    def test_opens_after_threshold(self):
        cb      = make_cb(failure_threshold=3)
        failing = MagicMock(side_effect=IOError)
        for _ in range(3):
            try:
                cb.call(failing)
            except IOError:
                pass
        assert cb._state == CircuitState.OPEN

    def test_still_closed_below_threshold(self):
        cb      = make_cb(failure_threshold=3)
        failing = MagicMock(side_effect=IOError)
        for _ in range(2):
            try:
                cb.call(failing)
            except IOError:
                pass
        assert cb._state == CircuitState.CLOSED


# ── OPEN state ────────────────────────────────────────────────────

class TestOpenState:
    def _open_circuit(self, cb):
        failing = MagicMock(side_effect=IOError)
        for _ in range(cb.failure_threshold):
            try:
                cb.call(failing)
            except IOError:
                pass

    def test_rejects_calls_when_open(self):
        cb = make_cb()
        self._open_circuit(cb)
        with pytest.raises(CircuitBreakerError):
            cb.call(MagicMock(return_value=None))

    def test_fn_not_called_when_open(self):
        cb = make_cb()
        self._open_circuit(cb)
        fn = MagicMock(return_value=None)
        try:
            cb.call(fn)
        except CircuitBreakerError:
            pass
        fn.assert_not_called()

    def test_transitions_to_half_open_after_timeout(self):
        cb = make_cb(recovery_timeout=0.05)
        self._open_circuit(cb)
        time.sleep(0.1)
        _ = cb.state
        assert cb._state == CircuitState.HALF_OPEN

    def test_stays_open_before_timeout(self):
        cb = make_cb(recovery_timeout=9999)
        self._open_circuit(cb)
        assert cb.state == CircuitState.OPEN


# ── HALF_OPEN → recovery ──────────────────────────────────────────

class TestHalfOpenRecovery:
    def _open_and_wait(self, cb):
        failing = MagicMock(side_effect=IOError)
        for _ in range(cb.failure_threshold):
            try:
                cb.call(failing)
            except IOError:
                pass
        time.sleep(0.15)

    def test_probe_success_closes_circuit(self):
        cb = make_cb(recovery_timeout=0.05)
        self._open_and_wait(cb)
        _ = cb.state  # trigger HALF_OPEN transition
        cb.call(MagicMock(return_value="ok"))
        assert cb._state == CircuitState.CLOSED

    def test_probe_failure_reopens_circuit(self):
        cb = make_cb(recovery_timeout=0.05)
        self._open_and_wait(cb)
        _ = cb.state  # trigger HALF_OPEN transition
        try:
            cb.call(MagicMock(side_effect=IOError))
        except IOError:
            pass
        assert cb._state == CircuitState.OPEN


# ── Manual reset ──────────────────────────────────────────────────

class TestManualReset:
    def test_reset_closes_open_circuit(self):
        cb = make_cb()
        for _ in range(cb.failure_threshold):
            try:
                cb.call(MagicMock(side_effect=IOError))
            except IOError:
                pass
        cb.reset()
        assert cb._state == CircuitState.CLOSED

    def test_reset_zeroes_failure_count(self):
        cb = make_cb()
        try:
            cb.call(MagicMock(side_effect=IOError))
        except IOError:
            pass
        cb.reset()
        assert cb._failure_count == 0

    def test_reset_allows_calls_again(self):
        cb = make_cb()
        for _ in range(cb.failure_threshold):
            try:
                cb.call(MagicMock(side_effect=IOError))
            except IOError:
                pass
        cb.reset()
        result = cb.call(MagicMock(return_value="back"))
        assert result == "back"


# ── expected_exception filter ─────────────────────────────────────

class TestExpectedExceptionFilter:
    def test_only_expected_exception_counts(self):
        cb = CircuitBreaker(
            name="filtered",
            failure_threshold=3,
            recovery_timeout=0.1,
            expected_exception=ValueError,
        )
        # RuntimeError should propagate but NOT count as failure
        with pytest.raises(RuntimeError):
            cb.call(MagicMock(side_effect=RuntimeError("unexpected")))
        assert cb._failure_count == 0

    def test_expected_exception_counts(self):
        cb = CircuitBreaker(
            name="filtered",
            failure_threshold=3,
            recovery_timeout=0.1,
            expected_exception=ValueError,
        )
        for _ in range(3):
            try:
                cb.call(MagicMock(side_effect=ValueError))
            except ValueError:
                pass
        assert cb._state == CircuitState.OPEN
