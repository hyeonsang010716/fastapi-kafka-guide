"""
재고 서비스 (Chapter 11 - 멀티 서비스 아키텍처)

order.created 이벤트를 소비하여 재고를 확인/예약하고
결과를 inventory.result 토픽으로 발행

인메모리 재고 데이터로 시뮬레이션
"""

import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

from .config import SERVICE_PORT, INVENTORY_FAILURE_RATE
from .producer import start_producer, stop_producer
from .consumer import start_consumer, inventory_stock, reservation_records

# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [INVENTORY] %(levelname)s %(message)s",
)
logger = logging.getLogger("inventory-service")

consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 수명 주기 관리"""
    global consumer_task
    await start_producer()
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("재고 서비스 시작 (실패율: %.0f%%)", INVENTORY_FAILURE_RATE * 100)
    yield
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    await stop_producer()
    logger.info("재고 서비스 종료")


app = FastAPI(
    title="재고 서비스",
    description="Chapter 11: 주문에 대한 재고 확인 및 예약 시뮬레이션",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/inventory")
async def list_inventory():
    """현재 재고 현황 조회"""
    return {
        "products": inventory_stock,
        "total_reservations": len(reservation_records),
    }


@app.get("/reservations")
async def list_reservations():
    """재고 예약 기록 조회"""
    return {
        "total": len(reservation_records),
        "reservations": reservation_records,
    }


@app.get("/health")
async def health_check():
    """헬스 체크"""
    return {
        "service": "inventory-service",
        "status": "healthy",
        "products_count": len(inventory_stock),
        "reservations_count": len(reservation_records),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
