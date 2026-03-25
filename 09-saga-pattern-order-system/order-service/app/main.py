"""
주문 서비스 (Order Service)
- POST /orders: 주문 생성 → order.created 이벤트 발행
- GET /orders: 전체 주문 목록 조회
- GET /orders/{order_id}: 특정 주문 상태 조회

Kafka 컨슈머가 백그라운드에서 payment.result, inventory.result 토픽을 구독하여
주문 상태를 자동으로 업데이트합니다.
"""

import asyncio
import uuid
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

from app.schemas import CreateOrderRequest, OrderResponse
from app.models import create_order, get_order, get_all_orders, OrderStatus, update_order_status
from app.producer import start_producer, stop_producer, publish_order_created
from app.consumer import start_consumer

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# 컨슈머 백그라운드 태스크 참조
consumer_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    앱 시작/종료 시 Kafka 프로듀서/컨슈머 관리
    lifespan 컨텍스트 매니저로 리소스 생명주기 관리
    """
    global consumer_task

    # 시작: 프로듀서 초기화 + 컨슈머를 백그라운드 태스크로 실행
    await start_producer()
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("주문 서비스가 시작되었습니다 (포트: 8000)")

    yield

    # 종료: 컨슈머 태스크 취소 + 프로듀서 종료
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    await stop_producer()
    logger.info("주문 서비스가 종료되었습니다")


app = FastAPI(
    title="Order Service",
    description="주문 서비스 - 주문 생성 및 상태 추적",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    """헬스 체크 엔드포인트"""
    return {"service": "order-service", "status": "healthy"}


@app.post("/orders", response_model=OrderResponse, status_code=201)
async def create_new_order(request: CreateOrderRequest):
    """
    새 주문을 생성하고 order.created 이벤트를 발행

    1. 고유 order_id 생성
    2. 총 가격 계산
    3. 인메모리 저장소에 주문 저장
    4. Kafka order.created 토픽으로 이벤트 발행
    5. 주문 상태를 PAYMENT_PROCESSING으로 변경
    """
    # 고유한 주문 ID 생성
    order_id = str(uuid.uuid4())

    # 총 가격 계산 (각 항목의 가격 x 수량의 합)
    total_price = sum(item.price * item.quantity for item in request.items)

    # 주문 항목을 딕셔너리로 변환
    items = [item.model_dump() for item in request.items]

    # 인메모리 저장소에 주문 생성
    order = create_order(order_id, request.user_id, items, total_price)
    logger.info(f"[주문 생성] order_id={order_id}, user_id={request.user_id}, total={total_price}")

    # Kafka로 주문 생성 이벤트 발행
    event_data = {
        "event_type": "ORDER_CREATED",
        "order_id": order_id,
        "user_id": request.user_id,
        "items": items,
        "total_price": total_price,
    }
    await publish_order_created(event_data)

    # 상태를 결제 처리 중으로 변경
    update_order_status(order_id, OrderStatus.PAYMENT_PROCESSING)

    return get_order(order_id)


@app.get("/orders", response_model=list[OrderResponse])
async def list_orders():
    """모든 주문 목록을 반환"""
    return get_all_orders()


@app.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order_detail(order_id: str):
    """특정 주문의 상세 정보와 상태를 반환"""
    order = get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail=f"주문을 찾을 수 없습니다: {order_id}")
    return order
