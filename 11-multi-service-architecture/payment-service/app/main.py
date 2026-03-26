"""
결제 서비스 (Chapter 11 - 멀티 서비스 아키텍처)

order.created 이벤트를 소비하여 결제를 처리하고
결과를 payment.result 토픽으로 발행

결제 성공/실패를 랜덤으로 시뮬레이션하여
보상 트랜잭션(Saga) 패턴을 학습할 수 있도록 구성
"""

import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

from .config import SERVICE_PORT, PAYMENT_FAILURE_RATE
from .producer import start_producer, stop_producer
from .consumer import start_consumer, payment_records

# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PAYMENT] %(levelname)s %(message)s",
)
logger = logging.getLogger("payment-service")

consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 수명 주기 관리"""
    global consumer_task
    await start_producer()
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("결제 서비스 시작 (실패율: %.0f%%)", PAYMENT_FAILURE_RATE * 100)
    yield
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    await stop_producer()
    logger.info("결제 서비스 종료")


app = FastAPI(
    title="결제 서비스",
    description="Chapter 11: 주문에 대한 결제 처리 시뮬레이션",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/payments")
async def list_payments():
    """처리된 결제 목록 조회"""
    return {
        "total": len(payment_records),
        "payments": payment_records,
    }


@app.get("/health")
async def health_check():
    """헬스 체크"""
    return {
        "service": "payment-service",
        "status": "healthy",
        "payments_processed": len(payment_records),
        "failure_rate": PAYMENT_FAILURE_RATE,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
