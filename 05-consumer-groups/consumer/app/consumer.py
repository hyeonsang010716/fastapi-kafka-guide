"""
Kafka Consumer 모듈
- 수동 커밋(enable_auto_commit=False)을 사용합니다.
- 수신한 메시지를 메모리에 저장하고, 파티션 할당 정보를 추적합니다.
"""

import asyncio
import json
import logging
from datetime import datetime

from aiokafka import AIOKafkaConsumer

from app.config import settings

logger = logging.getLogger("consumer")


# ---------------------------------------------------------------------------
# 수신 메시지 저장소 (인메모리)
# ---------------------------------------------------------------------------
received_messages: list[dict] = []

# Consumer 상태 정보
consumer_status: dict = {
    "consumer_id": settings.CONSUMER_ID,
    "assigned_partitions": [],
    "message_count": 0,
    "started_at": None,
}

# 전역 Consumer 참조 (lifespan에서 설정)
_consumer: AIOKafkaConsumer | None = None


# ---------------------------------------------------------------------------
# Consumer 생성 및 실행
# ---------------------------------------------------------------------------
async def start_consumer() -> AIOKafkaConsumer:
    """
    AIOKafkaConsumer를 생성하고 시작합니다.

    핵심 설정:
    - group_id: 같은 그룹에 속한 컨슈머끼리 파티션을 분배받음
    - enable_auto_commit=False: 수동 오프셋 커밋 사용
    - auto_offset_reset="earliest": 처음부터 읽기 시작
    """
    global _consumer

    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_GROUP_ID,
        # ── 수동 커밋 설정 ──
        # 자동 커밋을 끄면 명시적으로 commit()을 호출해야 오프셋이 저장됨
        # 이를 통해 "처리 완료 후 커밋" 패턴을 구현할 수 있음
        enable_auto_commit=False,
        # 그룹에 저장된 오프셋이 없으면 가장 처음부터 읽기
        auto_offset_reset="earliest",
        # 메시지 값을 JSON 바이트 → Python dict로 역직렬화
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # 메시지 키를 바이트 → 문자열로 역직렬화
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    await consumer.start()
    _consumer = consumer
    consumer_status["started_at"] = datetime.now().isoformat()

    logger.info(
        f"[{settings.CONSUMER_ID}] Consumer 시작 완료 "
        f"(group_id={settings.KAFKA_GROUP_ID})"
    )

    return consumer


async def stop_consumer():
    """Consumer를 안전하게 종료합니다."""
    global _consumer
    if _consumer:
        await _consumer.stop()
        _consumer = None
        logger.info(f"[{settings.CONSUMER_ID}] Consumer 종료")


# ---------------------------------------------------------------------------
# 메시지 수신 루프 (백그라운드 태스크)
# ---------------------------------------------------------------------------
async def consume_loop(consumer: AIOKafkaConsumer):
    """
    Kafka 메시지를 지속적으로 수신하는 무한 루프입니다.
    lifespan에서 백그라운드 태스크로 실행됩니다.

    수동 커밋 흐름:
    1. 메시지 수신 (getmany 또는 async for)
    2. 메시지 처리 (비즈니스 로직)
    3. 처리 완료 후 commit() 호출 → 오프셋 저장
    """
    try:
        async for msg in consumer:
            # ── 1) 수신한 메시지 정보 로깅 ──
            logger.info(
                f"[{settings.CONSUMER_ID}] "
                f"파티션={msg.partition} | "
                f"오프셋={msg.offset} | "
                f"키={msg.key} | "
                f"값={msg.value}"
            )

            # ── 2) 메시지 처리 (여기서는 메모리에 저장) ──
            message_info = {
                "consumer_id": settings.CONSUMER_ID,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": msg.key,
                "value": msg.value,
                "timestamp": msg.timestamp,
                "received_at": datetime.now().isoformat(),
            }
            received_messages.append(message_info)

            # ── 3) 수동 커밋: 메시지 처리가 끝난 뒤 오프셋을 커밋 ──
            # 만약 커밋 전에 컨슈머가 죽으면, 해당 메시지는 다시 다른 컨슈머가 받게 됨
            # → "at-least-once" 보장
            await consumer.commit()

            # ── 4) 상태 업데이트 ──
            consumer_status["message_count"] = len(received_messages)

            # 현재 할당된 파티션 목록 갱신
            assigned = consumer.assignment()
            consumer_status["assigned_partitions"] = sorted(
                [tp.partition for tp in assigned]
            )

    except asyncio.CancelledError:
        # 태스크가 취소되면 (앱 종료 시) 정상적으로 빠져나옴
        logger.info(f"[{settings.CONSUMER_ID}] consume_loop 종료됨")
    except Exception as e:
        logger.error(f"[{settings.CONSUMER_ID}] consume_loop 에러: {e}")


def get_status() -> dict:
    """현재 Consumer 상태 정보를 반환합니다."""
    # 최신 파티션 할당 정보 반영
    if _consumer:
        assigned = _consumer.assignment()
        consumer_status["assigned_partitions"] = sorted(
            [tp.partition for tp in assigned]
        )
    consumer_status["message_count"] = len(received_messages)
    return consumer_status


def get_messages() -> list[dict]:
    """수신한 메시지 목록을 반환합니다."""
    return received_messages
