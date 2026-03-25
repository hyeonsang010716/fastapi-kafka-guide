"""
멱등성 Consumer - Redis를 활용한 중복 처리 방지

처리 흐름:
1. 메시지 수신
2. idempotency_key로 Redis 조회 → 이미 처리된 키면 스킵
3. 신규 메시지면 비즈니스 로직 실행 (포인트 적립)
4. Redis에 처리 완료 기록
5. offset commit

이 순서를 통해 "최소 한 번 전달(at-least-once)" + "Consumer 멱등성"으로
실질적인 Exactly-once 효과를 달성합니다.
"""

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.idempotency import idempotency_store

logger = logging.getLogger(__name__)

# ── 인메모리 잔액 저장소 (실습용) ──
# 실제 서비스에서는 DB를 사용합니다.
balances: dict[str, int] = {}


async def start_consumer() -> None:
    """
    Consumer 시작 - 백그라운드 태스크로 실행됨
    auto_commit 비활성화하여 수동 커밋으로 정확한 오프셋 관리
    """
    consumer = AIOKafkaConsumer(
        settings.topic_name,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.consumer_group_id,
        # 수동 커밋: 처리 완료 후 직접 commit하여 메시지 유실/중복 최소화
        enable_auto_commit=False,
        # JSON 역직렬화
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # 가장 이른 오프셋부터 읽기 (첫 시작 시)
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info("포인트 Consumer 시작 (그룹: %s)", settings.consumer_group_id)

    try:
        async for msg in consumer:
            await _process_message(msg, consumer)
    except asyncio.CancelledError:
        logger.info("Consumer 종료 요청 수신")
    except Exception as e:
        logger.error("Consumer 오류 발생: %s", e, exc_info=True)
    finally:
        await consumer.stop()
        logger.info("Consumer 종료 완료")


async def _process_message(msg, consumer: AIOKafkaConsumer) -> None:
    """
    개별 메시지 처리 - 멱등성 보장 로직 포함

    Args:
        msg: Kafka 메시지
        consumer: offset commit을 위한 consumer 인스턴스
    """
    payload = msg.value
    idempotency_key = payload.get("idempotency_key")
    user_id = payload.get("user_id")
    points = payload.get("points", 0)

    logger.info(
        "메시지 수신: topic=%s, partition=%d, offset=%d, key=%s",
        msg.topic, msg.partition, msg.offset, idempotency_key,
    )

    # ── Step 1: 중복 확인 ──
    if await idempotency_store.is_processed(idempotency_key):
        logger.warning(
            "중복 메시지 스킵! idempotency_key=%s (user=%s, points=%d)",
            idempotency_key, user_id, points,
        )
        # 중복이어도 offset은 commit (다음에 다시 읽지 않도록)
        await consumer.commit()
        return

    # ── Step 2: 비즈니스 로직 실행 (포인트 적립) ──
    prev_balance = balances.get(user_id, 0)
    balances[user_id] = prev_balance + points
    logger.info(
        "포인트 적립 완료: user=%s, +%d점 (잔액: %d → %d)",
        user_id, points, prev_balance, balances[user_id],
    )

    # ── Step 3: 처리 완료 기록 (Redis) ──
    await idempotency_store.mark_processed(
        idempotency_key,
        ttl=settings.idempotency_ttl,
    )

    # ── Step 4: offset commit ──
    await consumer.commit()
    logger.info("offset commit 완료 (partition=%d, offset=%d)", msg.partition, msg.offset)
