"""
주문 서비스 Kafka 프로듀서
주문 생성 시 order.created 토픽으로 이벤트를 발행
"""

import json
import logging
from aiokafka import AIOKafkaProducer

from .config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ORDER_CREATED

logger = logging.getLogger("order-service.producer")

# 전역 프로듀서 인스턴스
producer: AIOKafkaProducer | None = None


async def start_producer():
    """Kafka 프로듀서 시작 - 애플리케이션 시작 시 호출"""
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # JSON 직렬화: 메시지를 UTF-8 인코딩된 JSON으로 변환
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # 메시지 전달 보장 설정
        acks="all",            # 모든 레플리카 확인 후 응답 (가장 안전)
        enable_idempotence=True,  # 중복 전송 방지 (멱등성)
    )
    await producer.start()
    logger.info("Kafka 프로듀서 시작 완료 (브로커: %s)", KAFKA_BOOTSTRAP_SERVERS)


async def stop_producer():
    """Kafka 프로듀서 종료 - 애플리케이션 종료 시 호출"""
    global producer
    if producer:
        await producer.stop()
        logger.info("Kafka 프로듀서 종료")


async def publish_order_created(event_data: dict):
    """
    주문 생성 이벤트를 Kafka에 발행
    - key: order_id (동일 주문의 이벤트가 같은 파티션으로 전달되도록)
    - value: 이벤트 전체 데이터 (JSON)
    """
    if not producer:
        raise RuntimeError("프로듀서가 초기화되지 않았습니다")

    order_id = event_data.get("order_id", "")
    await producer.send_and_wait(
        topic=TOPIC_ORDER_CREATED,
        key=order_id,
        value=event_data,
    )
    logger.info("주문 생성 이벤트 발행 완료: order_id=%s", order_id)
