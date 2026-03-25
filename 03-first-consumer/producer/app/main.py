"""
Producer 메인 모듈
- FastAPI 앱을 생성하고, Kafka Producer의 생명주기를 관리합니다.
- POST /messages: Kafka로 메시지를 전송합니다.
- GET /health: 서버 및 Kafka 연결 상태를 확인합니다.
"""

from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException

from app.config import settings
from app.schemas import HealthResponse, MessageRequest, MessageResponse

# 전역 Producer 인스턴스 (lifespan에서 초기화/종료)
producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    앱 시작 시 Kafka Producer를 생성하고,
    앱 종료 시 Producer를 안전하게 닫습니다.
    """
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        # 메시지 값을 UTF-8 바이트로 직렬화
        value_serializer=lambda v: v.encode("utf-8"),
        # 메시지 키를 UTF-8 바이트로 직렬화 (키가 있을 경우)
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()
    yield
    await producer.stop()


app = FastAPI(
    title="Kafka Producer API",
    description="Kafka로 메시지를 전송하는 Producer API (Chapter 03)",
    lifespan=lifespan,
)


@app.post("/messages", response_model=MessageResponse)
async def send_message(request: MessageRequest):
    """Kafka 토픽으로 메시지를 전송합니다."""
    try:
        # send_and_wait: 메시지 전송 후 브로커 응답(RecordMetadata)을 기다림
        result = await producer.send_and_wait(
            topic=request.topic,
            value=request.value,
            key=request.key,
        )
        return MessageResponse(
            topic=result.topic,
            partition=result.partition,
            offset=result.offset,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """서버 및 Kafka 연결 상태를 확인합니다."""
    kafka_connected = producer is not None
    return HealthResponse(
        status="healthy" if kafka_connected else "unhealthy",
        kafka_connected=kafka_connected,
    )
