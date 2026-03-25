"""
FastAPI + AIOKafkaProducer 예제
- lifespan 컨텍스트 매니저를 사용하여 Producer의 생명주기를 관리합니다.
- POST /messages: Kafka로 메시지를 전송합니다.
- GET /health: Producer 연결 상태를 확인합니다.
"""

from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException

from app.config import settings
from app.schemas import HealthResponse, MessageRequest, MessageResponse


# ---------------------------------------------------------------------------
# Lifespan: 애플리케이션 시작/종료 시 Producer를 관리하는 컨텍스트 매니저
# ---------------------------------------------------------------------------
# FastAPI 0.93+ 에서 권장하는 방식입니다.
# `yield` 이전 코드는 애플리케이션 시작 시, 이후 코드는 종료 시 실행됩니다.
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    애플리케이션 시작 시 AIOKafkaProducer를 생성하고,
    종료 시 안전하게 닫습니다.
    """
    # ── 시작(startup) ──
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        # key와 value를 UTF-8 바이트로 직렬화
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: v.encode("utf-8"),
    )
    await producer.start()
    # app.state에 저장하여 라우터에서 접근 가능하게 함
    app.state.producer = producer

    yield  # ← 여기서 애플리케이션이 요청을 처리합니다

    # ── 종료(shutdown) ──
    await producer.stop()


# ---------------------------------------------------------------------------
# FastAPI 앱 인스턴스 생성
# ---------------------------------------------------------------------------
app = FastAPI(
    title="02-first-producer",
    description="Kafka Producer 기초 학습",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# POST /messages — Kafka로 메시지 전송
# ---------------------------------------------------------------------------
@app.post("/messages", response_model=MessageResponse)
async def send_message(body: MessageRequest):
    """
    Kafka 토픽으로 메시지를 전송합니다.

    - send_and_wait(): 브로커의 ACK를 기다립니다 (동기적, 안전함).
    - send(): ACK를 기다리지 않습니다 (비동기적, 빠르지만 유실 가능).

    이 예제에서는 send_and_wait()를 사용하여 전송 결과를 즉시 확인합니다.
    """
    producer: AIOKafkaProducer = app.state.producer

    try:
        # send_and_wait()는 브로커가 메시지를 저장한 뒤 결과를 반환합니다.
        # 반환값(RecordMetadata)에는 topic, partition, offset 정보가 들어있습니다.
        record = await producer.send_and_wait(
            topic=body.topic,
            key=body.key,
            value=body.value,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"메시지 전송 실패: {e}")

    return MessageResponse(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
    )


# ---------------------------------------------------------------------------
# GET /health — Producer 연결 상태 확인
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Producer가 Kafka 클러스터에 정상적으로 연결되어 있는지 확인합니다.
    producer.client.ready() 등 내부 상태를 점검합니다.
    """
    producer: AIOKafkaProducer = app.state.producer

    # Producer 내부 클라이언트의 bootstrap 연결 상태 확인
    connected = producer.client.cluster.brokers() is not None and len(
        producer.client.cluster.brokers()
    ) > 0

    return HealthResponse(
        status="healthy" if connected else "unhealthy",
        kafka_connected=connected,
    )
