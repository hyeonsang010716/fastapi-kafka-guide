"""
FastAPI 메인 애플리케이션
- Kafka 프로듀서/컨슈머와 연동
- 직렬화/역직렬화 데모 엔드포인트 제공
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional
from uuid import uuid4

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query

from app.config import (
    APP_NAME,
    KAFKA_BOOTSTRAP_SERVERS,
    ORDER_EVENTS_TOPIC,
    USER_EVENTS_TOPIC,
)
from app.consumer import consumed_events, start_consumer
from app.schemas import (
    ConsumedEvent,
    CreateOrderRequest,
    CreateUserRequest,
    EventResponse,
    OrderItemRequest,
)
from app.serializers import json_serializer, key_serializer
from shared.events import (
    BaseEvent,
    OrderItem,
    OrderPlacedEvent,
    UserCreatedEvent,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 전역 프로듀서 인스턴스
producer: Optional[AIOKafkaProducer] = None

# 컨슈머 백그라운드 태스크
consumer_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    앱 수명주기 관리
    - 시작 시: 프로듀서 초기화 + 컨슈머 백그라운드 시작
    - 종료 시: 프로듀서/컨슈머 정리
    """
    global producer, consumer_task

    # ── 프로듀서 초기화 ──
    # value_serializer: Pydantic 모델/dict -> JSON bytes
    # key_serializer: str -> bytes
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
        key_serializer=key_serializer,
    )

    # 브로커 연결 재시도
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await producer.start()
            logger.info("Kafka 프로듀서 시작 완료")
            break
        except Exception as e:
            logger.warning(f"프로듀서 연결 시도 {attempt + 1}/{max_retries} 실패: {e}")
            await asyncio.sleep(2)
    else:
        logger.error("Kafka 프로듀서 연결 실패")

    # ── 컨슈머 백그라운드 태스크 시작 ──
    consumer_task = asyncio.create_task(start_consumer())

    yield

    # ── 정리 ──
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    if producer:
        await producer.stop()
        logger.info("Kafka 프로듀서 종료")


app = FastAPI(
    title="06 - 메시지 직렬화 (Message Serialization)",
    description="Kafka 메시지 직렬화/역직렬화 학습 - Pydantic 모델 기반",
    version="1.0.0",
    lifespan=lifespan,
)


# ──────────────────────────────────────────────
# 엔드포인트
# ──────────────────────────────────────────────


@app.get("/health", tags=["헬스체크"])
async def health_check():
    """앱 상태 확인"""
    return {
        "status": "healthy",
        "app": APP_NAME,
        "kafka_bootstrap": KAFKA_BOOTSTRAP_SERVERS,
        "producer_ready": producer is not None,
        "consumed_events_count": len(consumed_events),
    }


@app.post("/users", response_model=EventResponse, tags=["사용자"])
async def create_user(request: CreateUserRequest):
    """
    사용자 생성 이벤트를 Kafka로 전송

    처리 흐름:
    1. 요청 데이터로 UserCreatedEvent(Pydantic 모델) 생성
    2. json_serializer가 모델을 JSON bytes로 변환
    3. 메시지 헤더에 event_type, source 추가
    4. user-events 토픽으로 전송
    """
    if not producer:
        raise HTTPException(status_code=503, detail="프로듀서가 준비되지 않았습니다")

    # Pydantic 이벤트 모델 생성
    event = UserCreatedEvent(
        user_id=request.user_id,
        username=request.username,
        email=request.email,
    )

    # Kafka 메시지 헤더 설정 (메타데이터 전달용)
    # 헤더는 [(key, bytes)] 형태의 튜플 리스트
    headers = [
        ("event_type", b"user_created"),
        ("source", b"user-service"),
        ("content_type", b"application/json"),
    ]

    # 프로듀서를 통해 전송
    # value_serializer가 자동으로 Pydantic 모델 -> JSON bytes 변환
    await producer.send_and_wait(
        topic=USER_EVENTS_TOPIC,
        key=event.user_id,       # 같은 user_id는 같은 파티션으로
        value=event,             # Pydantic 모델 그대로 전달 (직렬화기가 변환)
        headers=headers,
    )

    logger.info(f"UserCreatedEvent 전송 완료: {event.event_id}")

    return EventResponse(
        status="success",
        event_id=event.event_id,
        topic=USER_EVENTS_TOPIC,
        message=f"사용자 '{request.username}' 생성 이벤트가 전송되었습니다",
    )


@app.post("/orders", response_model=EventResponse, tags=["주문"])
async def create_order(request: CreateOrderRequest):
    """
    주문 생성 이벤트를 Kafka로 전송

    처리 흐름:
    1. 요청 데이터로 OrderPlacedEvent(Pydantic 모델) 생성
    2. 총 금액 자동 계산
    3. json_serializer가 중첩된 Pydantic 모델까지 JSON bytes로 변환
    4. order-events 토픽으로 전송
    """
    if not producer:
        raise HTTPException(status_code=503, detail="프로듀서가 준비되지 않았습니다")

    # 주문 항목 변환 및 총 금액 계산
    items = [
        OrderItem(
            product_id=item.product_id,
            product_name=item.product_name,
            quantity=item.quantity,
            price=item.price,
        )
        for item in request.items
    ]
    total_price = sum(item.quantity * item.price for item in items)

    # Pydantic 이벤트 모델 생성
    event = OrderPlacedEvent(
        order_id=request.order_id,
        user_id=request.user_id,
        items=items,
        total_price=total_price,
    )

    # 메시지 헤더 설정
    headers = [
        ("event_type", b"order_placed"),
        ("source", b"order-service"),
        ("content_type", b"application/json"),
    ]

    # 프로듀서를 통해 전송
    await producer.send_and_wait(
        topic=ORDER_EVENTS_TOPIC,
        key=event.order_id,      # 같은 order_id는 같은 파티션으로
        value=event,             # 중첩 Pydantic 모델도 직렬화기가 처리
        headers=headers,
    )

    logger.info(f"OrderPlacedEvent 전송 완료: {event.event_id}")

    return EventResponse(
        status="success",
        event_id=event.event_id,
        topic=ORDER_EVENTS_TOPIC,
        message=f"주문 '{request.order_id}' 이벤트가 전송되었습니다 (총액: {total_price:,.0f}원)",
    )


@app.get("/events", tags=["이벤트 조회"])
async def get_events(
    topic: Optional[str] = Query(None, description="토픽 필터 (예: user-events)"),
    limit: int = Query(50, ge=1, le=500, description="최대 반환 개수"),
):
    """
    컨슈머가 수신한 이벤트 목록 조회

    역직렬화된 이벤트를 구조화된 형태로 반환.
    토픽 필터와 개수 제한 지원.
    """
    events = consumed_events

    # 토픽 필터링
    if topic:
        events = [e for e in events if e["topic"] == topic]

    # 최신 이벤트부터 반환 (최대 limit개)
    recent_events = events[-limit:][::-1]

    return {
        "total_count": len(events),
        "returned_count": len(recent_events),
        "filter_topic": topic,
        "events": recent_events,
    }
