"""
Outbox 헬퍼.

도메인 로직은 이 함수만 호출하면 된다. Kafka를 직접 알 필요가 없다.
"비즈니스 데이터 변경 + 이벤트 기록"이 같은 트랜잭션 안에서 일어나도록
세션을 인자로 받는 점이 핵심이다.
"""

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent


async def enqueue_event(
    session: AsyncSession,
    *,
    aggregate_type: str,
    aggregate_id: str,
    event_type: str,
    topic: str,
    payload: dict[str, Any],
    headers: dict[str, Any] | None = None,
    event_id: UUID | None = None,
) -> OutboxEvent:
    """
    현재 트랜잭션에 outbox 이벤트를 추가한다.

    주의: 이 함수는 commit/flush를 호출하지 않는다. 호출 측이 트랜잭션 경계를
    소유하기 때문이다. 비즈니스 데이터 INSERT와 같은 트랜잭션에서 함께
    커밋되어야 outbox 패턴이 의미를 가진다.
    """
    event = OutboxEvent(
        event_id=event_id or uuid4(),
        aggregate_type=aggregate_type,
        aggregate_id=str(aggregate_id),
        event_type=event_type,
        topic=topic,
        payload=payload,
        headers=headers or {},
    )
    session.add(event)
    return event
