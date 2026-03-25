"""
재고 서비스 (Inventory Service)
- payment.result 토픽을 구독하여 결제 성공 시 재고 확인
- 재고 결과를 inventory.result 토픽으로 발행
- GET /health: 헬스 체크
- GET /inventory: 재고 처리 기록 조회
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.producer import start_producer, stop_producer
from app.consumer import start_consumer, inventory_db

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

consumer_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작/종료 시 Kafka 리소스 관리"""
    global consumer_task

    await start_producer()
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("재고 서비스가 시작되었습니다 (포트: 8002)")

    yield

    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    await stop_producer()
    logger.info("재고 서비스가 종료되었습니다")


app = FastAPI(
    title="Inventory Service",
    description="재고 서비스 - 상품 재고 확인 및 예약",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    """헬스 체크 엔드포인트"""
    return {"service": "inventory-service", "status": "healthy"}


@app.get("/inventory")
async def list_inventory_operations():
    """모든 재고 처리 기록을 반환"""
    return {
        "total": len(inventory_db),
        "operations": inventory_db,
    }
