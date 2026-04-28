"""
샘플 다운스트림 컨슈머.

Outbox + Polling Relay 조합은 At-least-once다. 즉 같은 메시지가 두 번 도착할
수 있다. 컨슈머는 헤더의 event_id 를 보고 이미 처리한 이벤트는 무시해야 한다.

여기서는 학습용으로 PostgreSQL의 작은 테이블(processed_events)을 멱등성 키
저장소로 쓴다. 실무에서는 Redis 나 비즈니스 트랜잭션 안에서 같이 처리하는
방식을 더 많이 쓴다.
"""

import asyncio
import json
import logging
import signal

import structlog
from aiokafka import AIOKafkaConsumer
from sqlalchemy import text

from app.config import (
    CONSUMER_GROUP_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDER_EVENTS,
)
from app.database import SessionLocal, engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = structlog.get_logger()


async def _ensure_processed_table() -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id UUID PRIMARY KEY,
                    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )


async def _mark_processed(event_id: str) -> bool:
    """
    이미 처리된 event_id면 False, 새로 기록했으면 True.
    PRIMARY KEY 충돌을 활용한 atomic 한 'mark or skip'.
    """
    async with SessionLocal() as session:
        async with session.begin():
            result = await session.execute(
                text(
                    """
                    INSERT INTO processed_events (event_id)
                    VALUES (:eid)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING event_id
                    """
                ),
                {"eid": event_id},
            )
            return result.scalar_one_or_none() is not None


async def consume() -> None:
    await _ensure_processed_table()

    consumer = AIOKafkaConsumer(
        TOPIC_ORDER_EVENTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    log.info("consumer.started", topic=TOPIC_ORDER_EVENTS)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        while not stop.is_set():
            batch = await consumer.getmany(timeout_ms=500, max_records=50)
            for tp, messages in batch.items():
                for msg in messages:
                    headers = {k: v.decode("utf-8") for k, v in (msg.headers or [])}
                    event_id = headers.get("event_id")
                    event_type = headers.get("event_type", "unknown")

                    if event_id is None:
                        log.warning("event.missing_event_id", topic=tp.topic, offset=msg.offset)
                        continue

                    is_new = await _mark_processed(event_id)
                    if not is_new:
                        log.info("event.duplicate_skipped", event_id=event_id)
                        continue

                    # 실제로 비즈니스 로직을 처리하는 자리.
                    log.info(
                        "event.processed",
                        event_id=event_id,
                        event_type=event_type,
                        order_id=msg.value.get("order_id"),
                        partition=tp.partition,
                        offset=msg.offset,
                    )

                # 파티션별로 처리가 끝난 뒤 오프셋 커밋.
                if messages:
                    await consumer.commit({tp: messages[-1].offset + 1})
    finally:
        await consumer.stop()
        await engine.dispose()
        log.info("consumer.stopped")


if __name__ == "__main__":
    asyncio.run(consume())
