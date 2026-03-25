"""
주문 서비스 Kafka 프로듀서
주문 생성 시 order.created 토픽으로 이벤트 발행
"""

import json
import logging
from aiokafka import AIOKafkaProducer
from app.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ORDER_CREATED, TOPIC_REFUND_REQUEST

logger = logging.getLogger(__name__)

# 전역 프로듀서 인스턴스
producer: AIOKafkaProducer | None = None


async def start_producer():
    """Kafka 프로듀서 시작"""
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # 메시지 직렬화: dict → JSON 바이트
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()
    logger.info("Kafka 프로듀서가 시작되었습니다")


async def stop_producer():
    """Kafka 프로듀서 종료"""
    global producer
    if producer:
        await producer.stop()
        logger.info("Kafka 프로듀서가 종료되었습니다")


async def publish_order_created(event_data: dict):
    """
    주문 생성 이벤트를 order.created 토픽으로 발행
    order_id를 키로 사용하여 같은 주문의 이벤트가 같은 파티션으로 전송됨
    """
    if not producer:
        raise RuntimeError("프로듀서가 초기화되지 않았습니다")

    order_id = event_data["order_id"]
    await producer.send_and_wait(
        topic=TOPIC_ORDER_CREATED,
        key=order_id,
        value=event_data,
    )
    logger.info(f"[발행] order.created 토픽 → order_id={order_id}")


async def publish_refund_request(event_data: dict):
    """
    환불 요청 이벤트를 payment.refund-request 토픽으로 발행
    재고 부족 시 보상 트랜잭션으로 결제 환불을 트리거
    """
    if not producer:
        raise RuntimeError("프로듀서가 초기화되지 않았습니다")

    order_id = event_data["order_id"]
    await producer.send_and_wait(
        topic=TOPIC_REFUND_REQUEST,
        key=order_id,
        value=event_data,
    )
    logger.info(f"[발행] payment.refund-request 토픽 → order_id={order_id}")
