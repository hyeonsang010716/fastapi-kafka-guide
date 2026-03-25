"""
Consumer Group 학습용 Producer
- POST /orders: 랜덤 order_id 키로 "orders" 토픽에 메시지를 전송합니다.
- POST /bulk-orders: N개의 주문 메시지를 한꺼번에 전송합니다.
- GET /health: Producer 연결 상태를 확인합니다.
"""

import json
import uuid
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException

from app.config import settings
from app.schemas import (
    BulkOrderRequest,
    BulkOrderResponse,
    HealthResponse,
    OrderResponse,
)


# ---------------------------------------------------------------------------
# Lifespan: Producer 생명주기 관리
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작 시 Producer를 생성하고, 종료 시 안전하게 닫습니다."""
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        # 키: UTF-8 문자열 → 바이트 변환
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # 값: Python dict → JSON 문자열 → 바이트 변환
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    app.state.producer = producer

    yield

    await producer.stop()


# ---------------------------------------------------------------------------
# FastAPI 앱 인스턴스
# ---------------------------------------------------------------------------
app = FastAPI(
    title="05-consumer-groups / Producer",
    description="Consumer Group 학습을 위한 주문 메시지 Producer",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# 헬퍼: 주문 메시지 하나를 Kafka로 전송
# ---------------------------------------------------------------------------
async def _send_order(producer: AIOKafkaProducer) -> OrderResponse:
    """
    랜덤 order_id를 키로 사용하여 주문 메시지를 전송합니다.
    - 키가 다르면 해시에 따라 다른 파티션으로 분배됩니다.
    - 이를 통해 Consumer Group의 파티션 분배를 관찰할 수 있습니다.
    """
    order_id = str(uuid.uuid4())[:8]  # 짧은 고유 ID 생성

    # 주문 데이터
    order_data = {
        "order_id": order_id,
        "item": f"item-{uuid.uuid4().hex[:4]}",
        "quantity": int(uuid.uuid4().int % 10) + 1,
    }

    # Kafka로 전송 (send_and_wait: 브로커 ACK를 기다림)
    record = await producer.send_and_wait(
        topic=settings.KAFKA_TOPIC,
        key=order_id,  # 키 기반 파티셔닝
        value=order_data,
    )

    return OrderResponse(
        order_id=order_id,
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
    )


# ---------------------------------------------------------------------------
# POST /orders — 단일 주문 전송
# ---------------------------------------------------------------------------
@app.post("/orders", response_model=OrderResponse)
async def send_order():
    """랜덤 order_id를 키로 사용하여 주문 메시지 1건을 전송합니다."""
    producer: AIOKafkaProducer = app.state.producer

    try:
        return await _send_order(producer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"주문 전송 실패: {e}")


# ---------------------------------------------------------------------------
# POST /bulk-orders — 대량 주문 전송
# ---------------------------------------------------------------------------
@app.post("/bulk-orders", response_model=BulkOrderResponse)
async def send_bulk_orders(body: BulkOrderRequest):
    """
    N개의 주문 메시지를 한꺼번에 전송합니다.
    각 메시지는 서로 다른 order_id 키를 가지므로,
    3개 파티션에 골고루 분배되는 것을 확인할 수 있습니다.
    """
    producer: AIOKafkaProducer = app.state.producer
    orders: list[OrderResponse] = []

    try:
        for _ in range(body.count):
            order = await _send_order(producer)
            orders.append(order)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"대량 주문 전송 실패: {e}")

    return BulkOrderResponse(
        total_sent=len(orders),
        orders=orders,
    )


# ---------------------------------------------------------------------------
# GET /health — Producer 연결 상태 확인
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Producer가 Kafka 클러스터에 정상 연결되어 있는지 확인합니다."""
    producer: AIOKafkaProducer = app.state.producer
    brokers = producer.client.cluster.brokers()
    connected = brokers is not None and len(brokers) > 0

    return HealthResponse(
        status="healthy" if connected else "unhealthy",
        kafka_connected=connected,
    )
