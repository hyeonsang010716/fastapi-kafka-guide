"""
외부 결제 게이트웨이로의 호출 — resilience 패턴 3종을 합친 진입점.

호출 한 번에 적용되는 보호막 (안쪽부터 바깥쪽 순):

    1) Timeout            : httpx 타임아웃. "응답 없으면 빨리 끊기"
    2) Circuit Breaker    : 누적 실패가 임계치를 넘으면 즉시 차단
    3) Retry + Backoff    : 일시적 실패는 자동 재시도. 단, 회로가 열려 있으면 안 함

이 함수가 던질 수 있는 예외:
    - httpx.HTTPError       : 재시도 다 소진된 진짜 실패
    - CircuitBreakerOpenError: 회로 차단 중
"""

import httpx
import structlog

from app.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from app.config import (
    CB_FAILURE_THRESHOLD,
    CB_RECOVERY_TIMEOUT_SEC,
    GATEWAY_URL,
    REQUEST_TIMEOUT_SEC,
    RETRY_BASE_DELAY_MS,
    RETRY_MAX_ATTEMPTS,
)
from app.retry import retry_with_backoff

log = structlog.get_logger()


# 모듈 레벨에 단일 인스턴스. 회로 상태가 호출 사이에 유지되어야 한다.
gateway_breaker = CircuitBreaker(
    name="payment-gateway",
    failure_threshold=CB_FAILURE_THRESHOLD,
    recovery_timeout=CB_RECOVERY_TIMEOUT_SEC,
    expected_exception=httpx.HTTPError,
)

# httpx 클라이언트도 모듈 레벨 — 커넥션 풀 재사용을 위해.
http: httpx.AsyncClient | None = None


async def start_http_client() -> None:
    global http
    http = httpx.AsyncClient(
        base_url=GATEWAY_URL,
        timeout=httpx.Timeout(REQUEST_TIMEOUT_SEC),
    )


async def stop_http_client() -> None:
    global http
    if http is not None:
        await http.aclose()
        http = None


async def charge(order_id: str, amount: float) -> dict:
    """
    게이트웨이의 POST /charge 를 호출. 성공 시 응답 JSON 을 그대로 반환.
    """
    assert http is not None, "http client not started"

    async def _call_once() -> dict:
        # 진짜 호출은 회로를 거쳐서.
        async def _http_call() -> dict:
            response = await http.post("/charge", json={"order_id": order_id, "amount": amount})
            response.raise_for_status()
            return response.json()

        return await gateway_breaker.call(_http_call)

    return await retry_with_backoff(
        _call_once,
        max_attempts=RETRY_MAX_ATTEMPTS,
        base_delay_ms=RETRY_BASE_DELAY_MS,
        retriable=(httpx.HTTPError,),
        # 회로가 열려 있을 땐 재시도해도 의미가 없다 — 즉시 위로.
        do_not_retry=(CircuitBreakerOpenError,),
    )
