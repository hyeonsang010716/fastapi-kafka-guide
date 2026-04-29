"""
Circuit Breaker 패턴.

상태 3가지:
    CLOSED    — 정상. 호출이 통과한다. 연속 실패가 임계치를 넘으면 OPEN 으로.
    OPEN      — 차단. 호출이 즉시 CircuitBreakerOpenError 로 실패한다.
                일정 시간(recovery_timeout)이 지나면 HALF_OPEN 으로 자동 전이.
    HALF_OPEN — 회복 시도 중. 단 1건만 통과시켜 본다.
                  성공하면 CLOSED 로 돌아오고
                  실패하면 다시 OPEN 으로.

핵심 효과:
    "이미 죽었다고 알고 있는 서비스" 로의 호출을 *즉시 실패* 시켜서
    스레드/커넥션이 timeout 되는 동안 자원이 고갈되는 것을 막는다.
    이게 cascading failure 의 가장 흔한 차단 지점이다.
"""

import asyncio
import enum
import time

import structlog

log = structlog.get_logger()


class CircuitState(str, enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenError(Exception):
    """회로가 열려 있어 호출이 즉시 차단됨."""

    def __init__(self, name: str):
        super().__init__(f"circuit '{name}' is OPEN")
        self.name = name


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 10.0,
        expected_exception: type[Exception] = Exception,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()

    # --- 외부에서 들여다보는 용도 (메트릭/디버깅) ---------------------
    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "state": self._state.value,
            "failure_count": self._failure_count,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "opened_at": self._opened_at,
        }

    # --- 핵심 ----------------------------------------------------
    async def call(self, func, *args, **kwargs):
        """func 를 회로를 거쳐 호출한다. OPEN 이면 즉시 실패."""
        await self._before_call()
        try:
            result = await func(*args, **kwargs)
        except self.expected_exception:
            await self._on_failure()
            raise
        else:
            await self._on_success()
            return result

    async def _before_call(self) -> None:
        async with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_recovery():
                    # 시험 호출 한 건만 통과시킨다.
                    self._state = CircuitState.HALF_OPEN
                    log.info("circuit.half_open", name=self.name)
                else:
                    raise CircuitBreakerOpenError(self.name)

    def _should_attempt_recovery(self) -> bool:
        if self._opened_at is None:
            return True
        return (time.monotonic() - self._opened_at) >= self.recovery_timeout

    async def _on_success(self) -> None:
        async with self._lock:
            if self._state != CircuitState.CLOSED:
                log.info("circuit.closed", name=self.name)
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._opened_at = None

    async def _on_failure(self) -> None:
        async with self._lock:
            # HALF_OPEN 상태에서 실패는 곧장 OPEN 으로 되돌린다.
            if self._state == CircuitState.HALF_OPEN:
                self._open()
                return

            self._failure_count += 1
            if self._failure_count >= self.failure_threshold:
                self._open()

    def _open(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        log.warning(
            "circuit.open",
            name=self.name,
            failure_count=self._failure_count,
            recovery_in_sec=self.recovery_timeout,
        )
