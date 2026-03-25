"""
Kafka Consumer 모듈
- AIOKafkaConsumer를 사용하여 백그라운드에서 메시지를 수신합니다.
- 수신된 메시지는 인메모리 리스트에 저장되어 API로 조회할 수 있습니다.
"""

import asyncio
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer

from app.config import settings

logger = logging.getLogger(__name__)

# 수신된 메시지를 저장하는 인메모리 리스트
received_messages: list[dict] = []

# Consumer 인스턴스를 전역으로 관리 (시작/종료에 사용)
_consumer: AIOKafkaConsumer | None = None

# 백그라운드 태스크 참조 (취소할 때 사용)
_consume_task: asyncio.Task | None = None


async def start_consumer() -> None:
    """
    Kafka Consumer를 시작하고, 백그라운드 태스크로 메시지 수신 루프를 실행합니다.
    asyncio.create_task()를 사용하여 메인 이벤트 루프를 블로킹하지 않습니다.
    """
    global _consumer, _consume_task

    _consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_GROUP_ID,
        auto_offset_reset=settings.AUTO_OFFSET_RESET,
        # enable_auto_commit=True (기본값) — 일정 간격으로 오프셋을 자동 커밋
        # auto_commit_interval_ms=5000 (기본값) — 5초마다 자동 커밋
        value_deserializer=lambda v: v.decode("utf-8"),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    await _consumer.start()
    logger.info("Kafka Consumer 시작됨 — 토픽: %s, 그룹: %s", settings.KAFKA_TOPIC, settings.KAFKA_GROUP_ID)

    # asyncio.create_task()로 consume 루프를 백그라운드 태스크로 등록
    # 이렇게 하면 FastAPI가 요청을 처리하면서 동시에 메시지를 수신할 수 있음
    _consume_task = asyncio.create_task(_consume_loop())


async def stop_consumer() -> None:
    """Consumer 백그라운드 태스크를 취소하고, Consumer를 안전하게 종료합니다."""
    global _consume_task, _consumer

    # 백그라운드 태스크 취소
    if _consume_task is not None:
        _consume_task.cancel()
        try:
            await _consume_task
        except asyncio.CancelledError:
            logger.info("Consumer 백그라운드 태스크가 취소되었습니다.")

    # Consumer 종료 — 오프셋 커밋 후 연결 닫기
    if _consumer is not None:
        await _consumer.stop()
        logger.info("Kafka Consumer 종료됨")


async def _consume_loop() -> None:
    """
    메시지 수신 무한 루프
    - async for 구문으로 새 메시지가 도착할 때마다 처리합니다.
    - 수신된 메시지를 인메모리 리스트에 저장합니다.
    """
    try:
        async for msg in _consumer:
            # 수신된 메시지 정보를 딕셔너리로 저장
            message_data = {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": msg.key,
                "value": msg.value,
                "timestamp": msg.timestamp,  # Kafka가 부여한 타임스탬프 (밀리초)
                "received_at": datetime.now(timezone.utc).isoformat(),  # 수신 시각
            }
            received_messages.append(message_data)
            logger.info(
                "메시지 수신 — 토픽: %s, 파티션: %d, 오프셋: %d, 값: %s",
                msg.topic,
                msg.partition,
                msg.offset,
                msg.value,
            )
    except asyncio.CancelledError:
        # 태스크 취소 시 정상 종료
        logger.info("consume 루프가 취소되었습니다.")
        raise


def get_received_messages() -> list[dict]:
    """저장된 메시지 리스트를 반환합니다."""
    return received_messages
