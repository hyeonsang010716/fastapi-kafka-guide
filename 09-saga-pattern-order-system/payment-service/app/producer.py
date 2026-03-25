"""
결제 서비스 Kafka 프로듀서
결제 결과를 payment.result 토픽으로 발행
"""

import json
import logging
from aiokafka import AIOKafkaProducer
from app.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_PAYMENT_RESULT, TOPIC_REFUND_RESULT

logger = logging.getLogger(__name__)

# 전역 프로듀서 인스턴스
producer: AIOKafkaProducer | None = None


async def start_producer():
    """Kafka 프로듀서 시작"""
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()
    logger.info("결제 서비스 프로듀서가 시작되었습니다")


async def stop_producer():
    """Kafka 프로듀서 종료"""
    global producer
    if producer:
        await producer.stop()
        logger.info("결제 서비스 프로듀서가 종료되었습니다")


async def publish_payment_result(event_data: dict):
    """
    결제 결과 이벤트를 payment.result 토픽으로 발행
    성공(PAYMENT_COMPLETED) 또는 실패(PAYMENT_FAILED) 이벤트
    """
    if not producer:
        raise RuntimeError("프로듀서가 초기화되지 않았습니다")

    order_id = event_data["order_id"]
    event_type = event_data["event_type"]

    await producer.send_and_wait(
        topic=TOPIC_PAYMENT_RESULT,
        key=order_id,
        value=event_data,
    )
    logger.info(f"[발행] payment.result 토픽 → order_id={order_id}, type={event_type}")


async def publish_refund_result(event_data: dict):
    """
    환불 결과 이벤트를 payment.refund-result 토픽으로 발행
    성공(REFUND_COMPLETED) 또는 실패(REFUND_FAILED) 이벤트
    """
    if not producer:
        raise RuntimeError("프로듀서가 초기화되지 않았습니다")

    order_id = event_data["order_id"]
    event_type = event_data["event_type"]

    await producer.send_and_wait(
        topic=TOPIC_REFUND_RESULT,
        key=order_id,
        value=event_data,
    )
    logger.info(f"[발행] payment.refund-result 토픽 → order_id={order_id}, type={event_type}")
