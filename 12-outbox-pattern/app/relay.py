"""
Outbox Relay (폴링 퍼블리셔).

DB의 outbox 테이블을 짧은 주기로 폴링하면서 미발행 이벤트를 Kafka로 옮긴다.

설계 포인트
- SELECT ... FOR UPDATE SKIP LOCKED
    여러 인스턴스가 동시에 떠 있어도 같은 행을 두 번 발행하지 않게 한다.
    잠긴 행은 다른 워커에게 양보(SKIP)한다.
- aggregate_id를 Kafka 메시지 key로 사용
    같은 주문의 이벤트가 같은 파티션으로 가서 순서가 보장된다.
- 발행 후 published_at 마킹
    "발행 완료"를 DB에 표시한다. 같은 트랜잭션 안에서 한다.
- At-least-once
    Kafka 발행 직후 DB 커밋 직전에 죽으면 재발행이 일어날 수 있다. 그래서
    이벤트마다 event_id(UUID)를 부여하고, 컨슈머가 멱등 처리하도록 한다.
"""

import asyncio
import json
from datetime import datetime, timezone

import structlog
from aiokafka import AIOKafkaProducer
from sqlalchemy import text

from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    RELAY_BATCH_SIZE,
    RELAY_IDLE_BACKOFF_MS,
    RELAY_POLL_INTERVAL_MS,
)
from app.database import SessionLocal

log = structlog.get_logger()


class OutboxRelay:
    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            # 멱등 프로듀서로 켜서 네트워크 재전송 시 브로커 레벨 중복을 막는다.
            enable_idempotence=True,
            acks="all",
            # 같은 키 메시지의 순서를 보장하기 위한 in-flight 제한.
            max_batch_size=16384,
            linger_ms=20,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        """폴링 루프. lifespan에서 create_task 로 띄운다."""
        assert self._producer is not None
        while not self._stop_event.is_set():
            try:
                published = await self._tick()
            except Exception:
                log.exception("outbox.relay.tick_failed")
                published = 0

            # 일이 있으면 바로 다음 폴, 일이 없으면 살짝 더 쉬어 DB 부하를 줄인다.
            delay_ms = RELAY_POLL_INTERVAL_MS if published else RELAY_IDLE_BACKOFF_MS
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay_ms / 1000)
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> int:
        """한 번의 폴링. 잠긴 행은 건너뛰고 잡힌 행만 발행한다."""
        async with SessionLocal() as session:
            async with session.begin():
                # 트랜잭션 안에서 행을 잠그고, 같은 트랜잭션에서 published_at을 업데이트한다.
                # 트랜잭션이 커밋될 때까지 다른 워커는 이 행을 보지 못한다.
                rows = (
                    await session.execute(
                        text(
                            """
                            SELECT id, event_id, aggregate_id, event_type, topic,
                                   payload, headers
                            FROM outbox
                            WHERE published_at IS NULL
                            ORDER BY id
                            FOR UPDATE SKIP LOCKED
                            LIMIT :batch
                            """
                        ),
                        {"batch": RELAY_BATCH_SIZE},
                    )
                ).mappings().all()

                if not rows:
                    return 0

                published_ids: list[int] = []
                for row in rows:
                    await self._publish(row)
                    published_ids.append(row["id"])

                # 한 트랜잭션에서 모든 행을 한 번에 마킹.
                await session.execute(
                    text(
                        """
                        UPDATE outbox
                        SET published_at = :now
                        WHERE id = ANY(:ids)
                        """
                    ),
                    {"now": datetime.now(timezone.utc), "ids": published_ids},
                )

                log.info("outbox.relay.published", count=len(published_ids))
                return len(published_ids)

    async def _publish(self, row) -> None:
        assert self._producer is not None

        # event_id, event_type을 Kafka 헤더에도 넣어서 컨슈머가 페이로드를
        # 파싱하지 않고도 멱등성/라우팅 결정을 내릴 수 있게 한다.
        headers = [
            ("event_id", str(row["event_id"]).encode("utf-8")),
            ("event_type", row["event_type"].encode("utf-8")),
        ]
        for k, v in (row["headers"] or {}).items():
            headers.append((k, str(v).encode("utf-8")))

        await self._producer.send_and_wait(
            topic=row["topic"],
            key=row["aggregate_id"].encode("utf-8"),
            value=json.dumps(row["payload"]).encode("utf-8"),
            headers=headers,
        )
