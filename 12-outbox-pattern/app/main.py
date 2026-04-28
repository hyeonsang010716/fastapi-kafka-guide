"""
주문 서비스 (Outbox 패턴 적용).

POST /orders 핸들러는 Kafka를 호출하지 않는다. 대신 같은 DB 트랜잭션 안에서
- orders 테이블에 주문을 INSERT 하고
- outbox 테이블에 OrderCreated 이벤트를 INSERT 한다.

Kafka 발행은 lifespan에서 띄우는 백그라운드 Relay 태스크가 담당한다.
이 분리가 dual-write 문제를 없앤다.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from decimal import Decimal
from uuid import UUID, uuid4

import structlog
from fastapi import FastAPI, HTTPException
from sqlalchemy import select

from app.config import RELAY_ENABLED, TOPIC_ORDER_EVENTS
from app.database import SessionLocal
from app.models import Order, OutboxEvent
from app.outbox import enqueue_event
from app.relay import OutboxRelay
from app.schemas import CreateOrderRequest, OrderResponse, OutboxRow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = structlog.get_logger()


def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"unsupported type: {type(value)}")


def _to_jsonable(obj):
    """JSONB 컬럼에 넣기 전에 Decimal/UUID를 직렬화 가능한 형태로 정규화."""
    return json.loads(json.dumps(obj, default=_json_default))


@asynccontextmanager
async def lifespan(app: FastAPI):
    relay: OutboxRelay | None = None
    relay_task: asyncio.Task | None = None

    if RELAY_ENABLED:
        relay = OutboxRelay()
        await relay.start()
        relay_task = asyncio.create_task(relay.run())
        log.info("outbox.relay.started")

    yield

    if relay and relay_task:
        relay.request_stop()
        try:
            await asyncio.wait_for(relay_task, timeout=5)
        except asyncio.TimeoutError:
            relay_task.cancel()
        await relay.stop()
        log.info("outbox.relay.stopped")


app = FastAPI(
    title="Order Service (Outbox Pattern)",
    description="DB 트랜잭션과 Kafka 발행 사이의 일관성을 outbox 패턴으로 해결하는 예제",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/orders", response_model=OrderResponse, status_code=201)
async def create_order(request: CreateOrderRequest):
    """
    주문 생성. 핵심은 아래 한 트랜잭션이다.

        async with session.begin():
            session.add(order)
            await enqueue_event(session, ...)

    이 트랜잭션이 커밋되면 주문과 이벤트는 동시에 커밋된다. 둘 다 들어가거나
    둘 다 안 들어간다. Kafka는 이 시점에서 호출되지 않는다.
    """
    order_id = uuid4()
    total_price = sum((item.price * item.quantity for item in request.items), Decimal("0"))
    items_payload = _to_jsonable([item.model_dump() for item in request.items])

    async with SessionLocal() as session:
        async with session.begin():
            order = Order(
                order_id=order_id,
                user_id=request.user_id,
                items=items_payload,
                total_price=total_price,
                status="CREATED",
            )
            session.add(order)

            await enqueue_event(
                session,
                aggregate_type="Order",
                aggregate_id=str(order_id),
                event_type="OrderCreated",
                topic=TOPIC_ORDER_EVENTS,
                payload=_to_jsonable({
                    "order_id": str(order_id),
                    "user_id": request.user_id,
                    "items": items_payload,
                    "total_price": total_price,
                }),
                headers={"schema_version": "1"},
            )
        # session.begin() 종료 시점에 commit. 여기서 처음으로 "주문 + 이벤트"가
        # 동시에 영속화된다. 이 줄을 넘기면 Relay가 곧 Kafka로 발행한다.

        await session.refresh(order)
        return OrderResponse.model_validate(order)


@app.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: UUID):
    async with SessionLocal() as session:
        order = await session.get(Order, order_id)
        if order is None:
            raise HTTPException(status_code=404, detail="order not found")
        return OrderResponse.model_validate(order)


@app.get("/outbox", response_model=list[OutboxRow])
async def list_outbox(unpublished_only: bool = False, limit: int = 50):
    """
    학습용 디버깅 엔드포인트. 실제 시스템에서는 일반적으로 노출하지 않는다.
    """
    async with SessionLocal() as session:
        stmt = select(OutboxEvent).order_by(OutboxEvent.id.desc()).limit(limit)
        if unpublished_only:
            stmt = stmt.where(OutboxEvent.published_at.is_(None))
        rows = (await session.execute(stmt)).scalars().all()
        return [OutboxRow.model_validate(r) for r in rows]
