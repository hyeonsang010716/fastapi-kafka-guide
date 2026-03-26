"""
재고 서비스 Kafka 프로듀서
재고 확인 결과를 inventory.result 토픽으로 발행
"""

import json
import logging
from aiokafka import AIOKafkaProducer

from .config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_INVENTORY_RESULT

logger = logging.getLogger("inventory-service.producer")

producer: AIOKafkaProducer | None = None


async def start_producer():
    """Kafka 프로듀서 시작"""
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        enable_idempotence=True,
    )
    await producer.start()
    logger.info("Kafka 프로듀서 시작 완료")


async def stop_producer():
    """Kafka 프로듀서 종료"""
    global producer
    if producer:
        await producer.stop()
        logger.info("Kafka 프로듀서 종료")


async def publish_inventory_result(event_data: dict):
    """재고 확인 결과 이벤트 발행"""
    if not producer:
        raise RuntimeError("프로듀서가 초기화되지 않았습니다")

    order_id = event_data.get("order_id", "")
    await producer.send_and_wait(
        topic=TOPIC_INVENTORY_RESULT,
        key=order_id,
        value=event_data,
    )
    logger.info("재고 결과 이벤트 발행: order_id=%s, type=%s",
                order_id, event_data.get("event_type"))
