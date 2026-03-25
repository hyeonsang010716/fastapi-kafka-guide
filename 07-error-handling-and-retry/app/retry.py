"""
재시도 유틸리티
- Exponential Backoff 전략을 사용한 비동기 재시도 함수
- 재시도 간 대기 시간이 지수적으로 증가하여 서버 부하를 방지
"""

import asyncio
import logging
import random

logger = logging.getLogger(__name__)


async def retry_with_backoff(
    func,
    max_retries: int = 3,
    base_delay: float = 1.0,
):
    """
    Exponential Backoff을 적용한 재시도 함수

    재시도 대기 시간 계산:
      delay = base_delay * (2 ** attempt) + jitter
      - attempt 0: ~1초
      - attempt 1: ~2초
      - attempt 2: ~4초

    Args:
        func: 실행할 비동기 함수 (인자 없이 호출 가능해야 함)
        max_retries: 최대 재시도 횟수 (기본값: 3)
        base_delay: 기본 대기 시간(초) (기본값: 1.0)

    Returns:
        func의 반환값

    Raises:
        Exception: 모든 재시도 실패 시 마지막 예외를 발생시킴
    """
    last_exception = None

    for attempt in range(max_retries):
        try:
            # 함수 실행 시도
            result = await func()
            if attempt > 0:
                logger.info(f"재시도 {attempt}회 만에 성공")
            return result
        except Exception as e:
            last_exception = e
            # 마지막 시도가 아니라면 대기 후 재시도
            if attempt < max_retries - 1:
                # Exponential Backoff + Jitter(무작위 지연)
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                logger.warning(
                    f"시도 {attempt + 1}/{max_retries} 실패: {e}. "
                    f"{delay:.2f}초 후 재시도..."
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"최대 재시도 횟수({max_retries}) 초과. "
                    f"마지막 오류: {e}"
                )

    # 모든 재시도 실패 — 마지막 예외를 그대로 발생
    raise last_exception
