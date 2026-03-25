"""
Redis 기반 멱등성(Idempotency) 저장소

- 메시지 처리 전: is_processed()로 이미 처리된 키인지 확인
- 메시지 처리 후: mark_processed()로 처리 완료 기록
- TTL을 설정하여 Redis 메모리를 효율적으로 관리
"""

import logging

import redis.asyncio as redis

from app.config import settings

logger = logging.getLogger(__name__)

# 멱등성 키에 사용할 Redis 키 접두사
KEY_PREFIX = "idempotency:"


class IdempotencyStore:
    """Redis 기반 멱등성 저장소 클래스"""

    def __init__(self) -> None:
        self.redis: redis.Redis | None = None

    async def connect(self) -> None:
        """Redis 연결 초기화"""
        self.redis = redis.from_url(
            settings.redis_url,
            decode_responses=True,  # 문자열로 자동 디코딩
        )
        # 연결 확인
        await self.redis.ping()
        logger.info("Redis 연결 성공: %s", settings.redis_url)

    async def close(self) -> None:
        """Redis 연결 종료"""
        if self.redis:
            await self.redis.close()
            logger.info("Redis 연결 종료")

    async def is_processed(self, idempotency_key: str) -> bool:
        """
        이미 처리된 멱등성 키인지 확인

        Args:
            idempotency_key: 확인할 멱등성 키

        Returns:
            True이면 이미 처리됨 (중복), False이면 신규
        """
        exists = await self.redis.exists(f"{KEY_PREFIX}{idempotency_key}")
        return exists > 0

    async def mark_processed(self, idempotency_key: str, ttl: int = 86400) -> None:
        """
        멱등성 키를 처리 완료로 표시

        Args:
            idempotency_key: 처리 완료로 기록할 키
            ttl: 키의 만료 시간 (초). 기본값 24시간(86400초)
        """
        await self.redis.set(
            f"{KEY_PREFIX}{idempotency_key}",
            "1",
            ex=ttl,
        )
        logger.info("멱등성 키 저장 완료: %s (TTL: %d초)", idempotency_key, ttl)

    async def get_all_keys(self) -> list[str]:
        """
        저장된 모든 멱등성 키 목록 조회 (모니터링용)

        Returns:
            처리된 멱등성 키 리스트 (접두사 제거된 형태)
        """
        keys = []
        # SCAN을 사용하여 대량 키도 안전하게 조회
        async for key in self.redis.scan_iter(match=f"{KEY_PREFIX}*"):
            # 접두사 제거하여 원래 키만 반환
            keys.append(key.removeprefix(KEY_PREFIX))
        return keys


# 싱글톤 인스턴스
idempotency_store = IdempotencyStore()
