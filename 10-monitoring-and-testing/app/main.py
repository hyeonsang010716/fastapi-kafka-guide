"""
FastAPI 메인 애플리케이션 모듈
- 구조화된 로깅이 적용된 모든 엔드포인트
- 헬스체크, 메트릭, 메시지 송수신 API 제공
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.config import settings
from app.consumer import consumer_service
from app.health import health_checker
from app.logging_config import get_logger, setup_logging
from app.producer import producer_service

# 구조화된 로깅 초기화
setup_logging(log_level=settings.log_level)
logger = get_logger(__name__)


# --- 요청/응답 모델 ---


class MessageRequest(BaseModel):
    """메시지 전송 요청 모델"""

    key: str | None = None
    value: dict
    topic: str | None = None


class MessageResponse(BaseModel):
    """메시지 전송 응답 모델"""

    status: str
    topic: str
    partition: int
    offset: int
    timestamp: str


# --- 애플리케이션 라이프사이클 ---


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    애플리케이션 시작/종료 시 Kafka 프로듀서와 컨슈머를 관리
    """
    logger.info("application_starting", app_name=settings.app_name)

    # 시작 시: 프로듀서와 컨슈머를 시작
    await producer_service.start()
    await consumer_service.start()
    logger.info("application_started")

    yield

    # 종료 시: 프로듀서와 컨슈머를 안전하게 정리
    await consumer_service.stop()
    await producer_service.stop()
    logger.info("application_stopped")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
)


# --- 엔드포인트 ---


@app.post("/messages", response_model=MessageResponse)
async def send_message(request: MessageRequest):
    """
    Kafka 토픽에 메시지를 전송하는 엔드포인트
    - 모든 전송 시도를 구조화된 로그로 기록
    """
    topic = request.topic or settings.kafka_topic

    logger.info("message_send_requested", topic=topic, key=request.key)

    try:
        result = await producer_service.send_message(
            topic=topic,
            message=request.value,
            key=request.key,
        )
        return MessageResponse(**result)
    except Exception as e:
        logger.error("message_send_failed", topic=topic, error=str(e))
        raise HTTPException(status_code=500, detail=f"메시지 전송 실패: {str(e)}")


@app.get("/messages")
async def get_messages(limit: int = 10):
    """
    컨슈머가 소비한 최근 메시지를 조회하는 엔드포인트

    Args:
        limit: 반환할 최대 메시지 수 (기본값: 10)
    """
    logger.info("messages_requested", limit=limit)
    messages = consumer_service.messages[-limit:]
    return {
        "count": len(messages),
        "total_consumed": consumer_service.consumed_count,
        "messages": messages,
    }


@app.get("/health")
async def health_check():
    """
    상세 헬스체크 엔드포인트
    - 브로커 연결 상태
    - 컨슈머 실행 상태
    - 프로듀서/컨슈머 메시지 카운트
    """
    logger.info("health_check_requested")

    # 브로커 연결 상태 확인
    broker_status = await health_checker.check_broker_connection()

    health = {
        "status": "healthy" if broker_status["connected"] else "unhealthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "checks": {
            "broker": broker_status,
            "consumer": {
                "running": consumer_service.is_running,
                "consumed_count": consumer_service.consumed_count,
            },
            "producer": {
                "sent_count": producer_service.sent_count,
            },
        },
    }

    logger.info("health_check_completed", status=health["status"])
    return health


@app.get("/health/kafka")
async def kafka_health():
    """
    Kafka 전용 상세 헬스체크 엔드포인트
    - 브로커 연결
    - 클러스터 정보 (브로커 목록, 토픽 목록)
    - 컨슈머 랙
    """
    logger.info("kafka_health_check_requested")

    # 브로커 연결, 클러스터 정보, 컨슈머 랙을 모두 조회
    broker_status = await health_checker.check_broker_connection()
    cluster_info = await health_checker.get_cluster_info()
    consumer_lag = await health_checker.get_consumer_lag()

    result = {
        "broker": broker_status,
        "cluster": cluster_info,
        "consumer_lag": consumer_lag,
    }

    logger.info("kafka_health_check_completed")
    return result


@app.get("/metrics")
async def get_metrics():
    """
    메트릭 엔드포인트
    - 프로듀서 전송 카운트
    - 컨슈머 소비 카운트
    - 컨슈머 랙 정보
    """
    logger.info("metrics_requested")

    consumer_lag = await health_checker.get_consumer_lag()

    metrics = {
        "producer": {
            "sent_count": producer_service.sent_count,
        },
        "consumer": {
            "consumed_count": consumer_service.consumed_count,
            "is_running": consumer_service.is_running,
            "stored_messages": len(consumer_service.messages),
        },
        "consumer_lag": consumer_lag,
    }

    logger.info(
        "metrics_collected",
        sent=producer_service.sent_count,
        consumed=consumer_service.consumed_count,
    )
    return metrics
