"""
Circuit Breaker pattern for external API calls.

States:
  CLOSED   – normal operation, calls pass through
  OPEN     – failure threshold exceeded, calls are rejected immediately
  HALF_OPEN – after timeout, one probe call is allowed to test recovery
"""

import time
import logging
from enum import Enum
from typing import Callable, Any

log = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerError(Exception):
    """Raised when a call is rejected because the circuit is OPEN."""


class CircuitBreaker:
    """
    Thread-safe circuit breaker.

    Args:
        name:               Human-readable name (for logging).
        failure_threshold:  Consecutive failures before opening. Default 5.
        recovery_timeout:   Seconds to wait before probing. Default 60.
        expected_exception: Exception type(s) that count as failures.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception=Exception,
    ):
        self.name               = name
        self.failure_threshold  = failure_threshold
        self.recovery_timeout   = recovery_timeout
        self.expected_exception = expected_exception

        self._state            = CircuitState.CLOSED
        self._failure_count    = 0
        self._last_failure_time = None

    # ── Public API ────────────────────────────────────────────────

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if self._should_attempt_recovery():
                log.info("[%s] Circuit → HALF_OPEN (probing)", self.name)
                self._state = CircuitState.HALF_OPEN
        return self._state

    def call(self, fn: Callable, *args, **kwargs) -> Any:
        """
        Execute *fn* with circuit-breaker protection.

        Raises CircuitBreakerError immediately if the circuit is OPEN.
        Raises the original exception and trips the circuit if fn fails.
        """
        current = self.state

        if current == CircuitState.OPEN:
            raise CircuitBreakerError(
                f"Circuit '{self.name}' is OPEN — call rejected"
            )

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as exc:
            self._on_failure(exc)
            raise

    def reset(self) -> None:
        """Manually reset the circuit to CLOSED."""
        self._state         = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        log.info("[%s] Circuit manually reset → CLOSED", self.name)

    # ── Internal helpers ──────────────────────────────────────────

    def _on_success(self) -> None:
        if self._state != CircuitState.CLOSED:
            log.info("[%s] Circuit → CLOSED (recovered)", self.name)
        self._state         = CircuitState.CLOSED
        self._failure_count = 0

    def _on_failure(self, exc: Exception) -> None:
        self._failure_count    += 1
        self._last_failure_time = time.monotonic()
        log.warning(
            "[%s] Failure %d/%d: %s",
            self.name, self._failure_count, self.failure_threshold, exc,
        )

        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            log.error(
                "[%s] Circuit → OPEN (threshold %d reached)",
                self.name, self.failure_threshold,
            )

    def _should_attempt_recovery(self) -> bool:
        if self._last_failure_time is None:
            return True
        elapsed = time.monotonic() - self._last_failure_time
        return elapsed >= self.recovery_timeout

    # ── Repr ──────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"CircuitBreaker(name={self.name!r}, "
            f"state={self._state.value}, "
            f"failures={self._failure_count}/{self.failure_threshold})"
        )
