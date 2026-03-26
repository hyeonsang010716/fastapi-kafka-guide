"""
알림 서비스 (Chapter 11 - 멀티 서비스 아키텍처)

모든 주요 이벤트를 구독하여 사용자에게 알림을 발송
실제 이메일/SMS 대신 콘솔 로그로 시뮬레이션

이벤트 기반 아키텍처에서 알림 서비스의 역할:
- 다른 서비스의 비즈니스 로직에 영향을 주지 않음 (느슨한 결합)
- 알림 서비스가 다운되어도 주문 흐름은 계속 진행
- 서비스 복구 시 밀린 이벤트를 순차적으로 처리
"""

import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

from .config import SERVICE_PORT
from .consumer import start_consumer
from .notifier import sent_notifications

# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NOTIFICATION] %(levelname)s %(message)s",
)
logger = logging.getLogger("notification-service")

consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 수명 주기 관리"""
    global consumer_task
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("알림 서비스 시작 (포트: %d)", SERVICE_PORT)
    yield
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    logger.info("알림 서비스 종료")


app = FastAPI(
    title="알림 서비스",
    description="Chapter 11: 이벤트 기반 사용자 알림 발송 시뮬레이션",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/notifications")
async def list_notifications():
    """발송된 알림 목록 조회 (최신순)"""
    return {
        "total": len(sent_notifications),
        "notifications": list(reversed(sent_notifications)),
    }


@app.get("/notifications/by-order/{order_id}")
async def get_notifications_by_order(order_id: str):
    """특정 주문에 대한 알림 목록 조회"""
    order_notifications = [
        n for n in sent_notifications if n["order_id"] == order_id
    ]
    return {
        "order_id": order_id,
        "total": len(order_notifications),
        "notifications": order_notifications,
    }


@app.get("/health")
async def health_check():
    """헬스 체크"""
    return {
        "service": "notification-service",
        "status": "healthy",
        "notifications_sent": len(sent_notifications),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
