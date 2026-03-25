"""
Chapter 04 — 토픽과 파티션
- AIOKafkaProducer로 메시지를 전송하고, 어떤 파티션에 도착했는지 확인합니다.
- AIOKafkaConsumer로 메시지를 소비하며 파티션 정보를 기록합니다.
- AIOKafkaClient를 통해 토픽/파티션 메타데이터를 조회합니다.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import FastAPI, HTTPException

from app.config import settings
from app.schemas import (
    ConsumedMessage,
    HealthResponse,
    OrderRequest,
    OrderResponse,
    PartitionInfo,
    TopicInfo,
)

logger = logging.getLogger("uvicorn.error")

# ──────────────────────────────────────────────
# 전역 상태: 프로듀서, 컨슈머, 소비된 메시지 저장소
# ──────────────────────────────────────────────

producer: AIOKafkaProducer | None = None
consumer: AIOKafkaConsumer | None = None
consumed_messages: list[ConsumedMessage] = []  # 소비된 메시지를 메모리에 보관


async def consume_loop():
    """
    백그라운드에서 Kafka 메시지를 계속 소비하는 루프.
    각 메시지의 파티션 정보도 함께 저장합니다.
    """
    global consumer
    try:
        async for msg in consumer:
            # 메시지 디코딩
            value = json.loads(msg.value.decode("utf-8"))
            key = msg.key.decode("utf-8") if msg.key else None

            consumed = ConsumedMessage(
                topic=msg.topic,
                partition=msg.partition,   # 어떤 파티션에서 왔는지
                offset=msg.offset,         # 파티션 내 오프셋
                key=key,
                value=value,
            )
            consumed_messages.append(consumed)

            logger.info(
                f"[소비] 토픽={msg.topic} 파티션={msg.partition} "
                f"오프셋={msg.offset} 키={key}"
            )
    except asyncio.CancelledError:
        logger.info("컨슈머 루프 종료")


# ──────────────────────────────────────────────
# Lifespan — 앱 시작/종료 시 Kafka 연결 관리
# ──────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    앱 시작 시 프로듀서/컨슈머를 초기화하고,
    종료 시 정리합니다.
    """
    global producer, consumer

    # ── 프로듀서 시작 ──
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        # 키 직렬화: 문자열 → 바이트
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # 값 직렬화: dict → JSON 바이트
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    await producer.start()
    logger.info("Kafka 프로듀서 시작 완료")

    # ── 컨슈머 시작 ──
    consumer = AIOKafkaConsumer(
        settings.ORDERS_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.CONSUMER_GROUP_ID,
        # 가장 오래된 메시지부터 읽기
        auto_offset_reset="earliest",
        # 값 역직렬화는 consume_loop에서 수동 처리
    )
    await consumer.start()
    logger.info("Kafka 컨슈머 시작 완료")

    # 백그라운드 컨슈머 태스크 시작
    consume_task = asyncio.create_task(consume_loop())

    yield  # ← 앱 실행 중

    # ── 정리 ──
    consume_task.cancel()
    await consumer.stop()
    await producer.stop()
    logger.info("Kafka 연결 종료")


# ──────────────────────────────────────────────
# FastAPI 앱 생성
# ──────────────────────────────────────────────

app = FastAPI(
    title="Chapter 04 — 토픽과 파티션",
    description="Kafka 토픽, 파티션, 메시지 키의 동작을 실습합니다.",
    version="0.4.0",
    lifespan=lifespan,
)


# ──────────────────────────────────────────────
# POST /orders — 주문 메시지 전송
# ──────────────────────────────────────────────

@app.post("/orders", response_model=OrderResponse)
async def create_order(order: OrderRequest):
    """
    주문을 Kafka 'orders' 토픽으로 전송합니다.
    order_id를 메시지 키로 사용하여 같은 주문은 항상 같은 파티션으로 전송됩니다.
    """
    if not producer:
        raise HTTPException(status_code=503, detail="프로듀서가 준비되지 않았습니다")

    # order_id를 키로 사용 → 같은 키는 항상 같은 파티션으로 라우팅됨
    value = order.model_dump()
    result = await producer.send_and_wait(
        topic=settings.ORDERS_TOPIC,
        key=order.order_id,
        value=value,
    )

    logger.info(
        f"[전송] order_id={order.order_id} → "
        f"파티션={result.partition}, 오프셋={result.offset}"
    )

    return OrderResponse(
        order_id=order.order_id,
        topic=result.topic,
        partition=result.partition,
        offset=result.offset,
    )


# ──────────────────────────────────────────────
# GET /topic-info — 토픽/파티션 메타데이터 조회
# ──────────────────────────────────────────────

@app.get("/topic-info", response_model=list[TopicInfo])
async def get_topic_info():
    """
    Kafka 클러스터의 토픽/파티션 메타데이터를 조회합니다.
    AIOKafkaAdminClient를 사용하여 각 토픽의 파티션 수, 리더, 레플리카 정보를 반환합니다.
    """
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    )
    try:
        await admin_client.start()

        # 클러스터 메타데이터에서 토픽 정보 추출
        metadata = await admin_client.describe_topics()

        topics = []
        for topic_meta in metadata:
            topic_name = topic_meta["topic"]

            # 내부 토픽(__consumer_offsets 등)은 제외
            if topic_name.startswith("_"):
                continue

            partitions = []
            for p in topic_meta["partitions"]:
                partitions.append(
                    PartitionInfo(
                        partition_id=p["partition"],
                        leader=p["leader"],
                        replicas=p["replicas"],
                        isr=p["isr"],
                    )
                )

            topics.append(
                TopicInfo(
                    topic=topic_name,
                    num_partitions=len(partitions),
                    partitions=partitions,
                )
            )

        return topics
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"토픽 정보 조회 실패: {str(e)}")
    finally:
        await admin_client.close()


# ──────────────────────────────────────────────
# GET /messages — 소비된 메시지 목록
# ──────────────────────────────────────────────

@app.get("/messages", response_model=list[ConsumedMessage])
async def get_messages():
    """
    컨슈머가 소비한 메시지 목록을 반환합니다.
    각 메시지가 어떤 파티션에서 왔는지 확인할 수 있습니다.
    """
    return consumed_messages


# ──────────────────────────────────────────────
# GET /health — 헬스체크
# ──────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """프로듀서와 컨슈머의 연결 상태를 확인합니다."""
    kafka_ok = producer is not None and consumer is not None
    return HealthResponse(
        status="healthy" if kafka_ok else "unhealthy",
        kafka_connected=kafka_ok,
    )
