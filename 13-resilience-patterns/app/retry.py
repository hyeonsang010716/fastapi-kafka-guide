"""
Exponential backoff + jitter 재시도 데코레이터.

07 챕터에서 다룬 retry 와 같은 아이디어를 함수 데코레이터 형태로 정리해, 이번
챕터에서 timeout / circuit breaker 와 자연스럽게 합쳐 쓸 수 있게 한다.

쌓는 순서가 중요하다 (밖 → 안):

    @retry(...)              ← 가장 바깥
    @circuit_breaker.call    ← 그 다음
    @timeout(...)            ← 가장 안쪽

이 챕터에서는 코드를 보기 쉽게 만들기 위해 데코레이터 대신 명시적인 함수
호출 형태(`await retry(func)` 처럼)를 쓰지만, 의미는 같다.
"""

import asyncio
import logging
import random
from typing import Awaitable, Callable, TypeVar

import structlog

log = structlog.get_logger()
logging.basicConfig(level=logging.INFO)

T = TypeVar("T")


async def retry_with_backoff(
    func: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 3,
    base_delay_ms: int = 200,
    retriable: tuple[type[Exception], ...] = (Exception,),
    do_not_retry: tuple[type[Exception], ...] = (),
) -> T:
    """
    func() 를 최대 max_attempts 번까지 재시도.

    Backoff:    delay = base * 2^(attempt-1) + jitter(0~1)
    do_not_retry 에 든 예외는 즉시 위로 던진다 (예: CircuitBreakerOpenError).
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return await func()
        except do_not_retry:
            # 재시도 의미가 없는 예외 (예: 회로가 열려 있음) — 그대로 위로.
            raise
        except retriable as exc:
            if attempt >= max_attempts:
                log.warning(
                    "retry.exhausted",
                    attempts=attempt,
                    error=type(exc).__name__,
                )
                raise

            delay = (base_delay_ms / 1000) * (2 ** (attempt - 1)) + random.random() * 0.05
            log.info(
                "retry.scheduled",
                attempt=attempt,
                delay_sec=round(delay, 3),
                error=type(exc).__name__,
            )
            await asyncio.sleep(delay)
