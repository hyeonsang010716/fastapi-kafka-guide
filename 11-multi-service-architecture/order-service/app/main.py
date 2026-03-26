"""
주문 서비스 (Chapter 11 - 멀티 서비스 아키텍처)

주문 생성 및 관리를 담당하는 핵심 서비스
주문 생성 시 order.created 이벤트를 발행하고,
payment.result / inventory.result 이벤트를 소비하여 주문 상태 갱신

Chapter 09 대비 개선사항:
- 3-broker 클러스터 지원
- 배송 주소 필드 추가
- 이벤트 히스토리 추적
- 주문 완료 판단 로직 (결제 + 재고 모두 확인)
"""

import uuid
import asyncio
import logging
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

from .config import SERVICE_PORT
from .models import (
    CreateOrderRequest,
    Order,
    OrderStatus,
    orders_db,
)
from .producer import start_producer, stop_producer, publish_order_created
from .consumer import start_consumer

# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ORDER] %(levelname)s %(message)s",
)
logger = logging.getLogger("order-service")

# 백그라운드 컨슈머 태스크 참조
consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 수명 주기 - 프로듀서/컨슈머 시작 및 종료"""
    global consumer_task

    # 프로듀서 시작
    await start_producer()

    # 컨슈머를 백그라운드 태스크로 실행
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("주문 서비스 시작 완료 (포트: %d)", SERVICE_PORT)

    yield

    # 종료 처리
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    await stop_producer()
    logger.info("주문 서비스 종료")


app = FastAPI(
    title="주문 서비스",
    description="Chapter 11: 주문 생성 및 상태 관리. Kafka를 통해 이벤트 발행/소비",
    version="1.0.0",
    lifespan=lifespan,
)


# ──────────────────────────────────────────────
# API 엔드포인트
# ──────────────────────────────────────────────
@app.post("/orders", status_code=201)
async def create_order(request: CreateOrderRequest):
    """
    새로운 주문 생성
    1. UUID로 고유한 주문 ID 생성
    2. 총 가격 계산
    3. 인메모리 DB에 저장
    4. order.created 이벤트 발행
    """
    # 주문 ID 생성
    order_id = str(uuid.uuid4())

    # 총 가격 계산: 각 항목의 (가격 x 수량) 합산
    total_price = sum(item.price * item.quantity for item in request.items)

    # 주문 객체 생성 및 저장
    order = Order(
        order_id=order_id,
        user_id=request.user_id,
        items=request.items,
        total_price=total_price,
        shipping_address=request.shipping_address,
    )
    orders_db[order_id] = order

    # Kafka에 주문 생성 이벤트 발행
    event_data = {
        "event_type": "ORDER_CREATED",
        "order_id": order_id,
        "user_id": request.user_id,
        "items": [item.model_dump() for item in request.items],
        "total_price": total_price,
        "shipping_address": request.shipping_address,
        "timestamp": order.created_at,
    }
    await publish_order_created(event_data)

    logger.info("주문 생성: order_id=%s, total=%.2f", order_id, total_price)

    return {
        "order_id": order_id,
        "status": order.status,
        "total_price": total_price,
        "message": "주문이 생성되었습니다. 결제 및 재고 확인 중입니다.",
    }


@app.get("/orders")
async def list_orders():
    """모든 주문 목록 조회"""
    return {
        "total": len(orders_db),
        "orders": [
            {
                "order_id": o.order_id,
                "user_id": o.user_id,
                "total_price": o.total_price,
                "status": o.status,
                "created_at": o.created_at,
                "updated_at": o.updated_at,
            }
            for o in orders_db.values()
        ],
    }


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    """주문 상세 조회 (이벤트 히스토리 포함)"""
    if order_id not in orders_db:
        raise HTTPException(status_code=404, detail="주문을 찾을 수 없습니다")

    order = orders_db[order_id]
    return order.model_dump()


@app.get("/health")
async def health_check():
    """헬스 체크 엔드포인트"""
    return {
        "service": "order-service",
        "status": "healthy",
        "orders_count": len(orders_db),
    }


# ──────────────────────────────────────────────
# 메인 실행
# ──────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
