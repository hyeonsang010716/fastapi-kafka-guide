"""
결제 서비스 (Payment Service)
- order.created 토픽을 구독하여 결제 처리
- 결제 결과를 payment.result 토픽으로 발행
- GET /health: 헬스 체크
- GET /payments: 처리된 결제 목록 조회
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.producer import start_producer, stop_producer
from app.consumer import start_consumer, payments_db, refunds_db

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
    logger.info("결제 서비스가 시작되었습니다 (포트: 8001)")

    yield

    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    await stop_producer()
    logger.info("결제 서비스가 종료되었습니다")


app = FastAPI(
    title="Payment Service",
    description="결제 서비스 - 주문에 대한 결제 처리",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    """헬스 체크 엔드포인트"""
    return {"service": "payment-service", "status": "healthy"}


@app.get("/payments")
async def list_payments():
    """처리된 모든 결제 기록을 반환"""
    return {
        "total": len(payments_db),
        "payments": payments_db,
    }


@app.get("/refunds")
async def list_refunds():
    """처리된 모든 환불 기록을 반환 (Saga 보상 트랜잭션 결과)"""
    return {
        "total": len(refunds_db),
        "refunds": refunds_db,
    }
