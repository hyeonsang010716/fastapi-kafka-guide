"""
FastAPI 메인 애플리케이션
- lifespan으로 Producer, Consumer, DLQ Consumer를 백그라운드 태스크로 관리
- 결제 전송, 처리 결과 조회, 실패 메시지 조회 엔드포인트 제공
"""

import uuid
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, HTTPException
from app.config import PAYMENTS_TOPIC, APP_NAME
from app.schemas import PaymentRequest, PaymentEvent
from app.producer import start_producer, stop_producer, send_message
from app.consumer import consume_payments, processed_payments
from app.dlq_consumer import consume_dlq, failed_payments

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    애플리케이션 수명 주기 관리
    - 시작: Producer 연결, Consumer/DLQ Consumer 백그라운드 태스크 실행
    - 종료: 모든 태스크 정리 및 연결 해제
    """
    logger.info(f"{APP_NAME} 시작 중...")

    # Producer 시작
    await start_producer()

    # Consumer와 DLQ Consumer를 백그라운드 태스크로 실행
    consumer_task = asyncio.create_task(consume_payments())
    dlq_consumer_task = asyncio.create_task(consume_dlq())
    logger.info("Consumer 및 DLQ Consumer 백그라운드 태스크 시작")

    yield

    # 종료: 백그라운드 태스크 취소
    logger.info("애플리케이션 종료 중...")
    consumer_task.cancel()
    dlq_consumer_task.cancel()
    try:
        await asyncio.gather(consumer_task, dlq_consumer_task, return_exceptions=True)
    except Exception:
        pass

    # Producer 종료
    await stop_producer()
    logger.info(f"{APP_NAME} 종료 완료")


app = FastAPI(
    title="07 - Error Handling & Retry",
    description="Kafka 에러 핸들링, 재시도, DLQ 패턴 실습",
    lifespan=lifespan,
)


@app.post("/payments", summary="결제 이벤트 전송")
async def create_payment(request: PaymentRequest):
    """
    결제 요청을 Kafka payments 토픽으로 전송
    - 고유 payment_id 생성
    - 결제 이벤트를 Kafka로 비동기 전송
    """
    # 결제 이벤트 생성
    payment_event = PaymentEvent(
        payment_id=str(uuid.uuid4()),
        user_id=request.user_id,
        amount=request.amount,
        currency=request.currency,
        description=request.description,
    )

    try:
        await send_message(
            topic=PAYMENTS_TOPIC,
            message=payment_event.model_dump(),
            key=payment_event.payment_id,
        )
        return {
            "status": "sent",
            "payment_id": payment_event.payment_id,
            "message": "결제 이벤트가 Kafka로 전송되었습니다",
        }
    except Exception as e:
        logger.error(f"결제 이벤트 전송 실패: {e}")
        raise HTTPException(status_code=500, detail=f"메시지 전송 실패: {e}")


@app.get("/processed", summary="처리 완료된 결제 조회")
async def get_processed_payments():
    """성공적으로 처리된 결제 목록 반환"""
    return {
        "count": len(processed_payments),
        "payments": processed_payments,
    }


@app.get("/failed", summary="실패한 결제 조회 (DLQ)")
async def get_failed_payments():
    """DLQ에 저장된 실패 결제 목록 반환"""
    return {
        "count": len(failed_payments),
        "payments": failed_payments,
    }


@app.get("/health", summary="헬스 체크")
async def health_check():
    """애플리케이션 상태 확인"""
    return {
        "status": "healthy",
        "app": APP_NAME,
        "processed_count": len(processed_payments),
        "failed_count": len(failed_payments),
    }
