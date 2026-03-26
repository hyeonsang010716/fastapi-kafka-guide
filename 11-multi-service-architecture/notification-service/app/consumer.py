"""
알림 서비스 Kafka 컨슈머
order.created, payment.result, inventory.result 토픽을 모두 구독하여
각 이벤트에 대해 적절한 알림을 생성/발송
"""

import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDER_CREATED,
    TOPIC_PAYMENT_RESULT,
    TOPIC_INVENTORY_RESULT,
    CONSUMER_GROUP_ID,
)
from .notifier import send_notification, build_message

logger = logging.getLogger("notification-service.consumer")


async def start_consumer():
    """
    Kafka 컨슈머 시작
    3개의 이벤트 토픽을 구독하여 사용자 알림 발송
    - order.created → "주문 접수" 알림
    - payment.result → "결제 완료/실패" 알림
    - inventory.result → "재고 확인 완료/부족" 알림
    """
    consumer = AIOKafkaConsumer(
        TOPIC_ORDER_CREATED,
        TOPIC_PAYMENT_RESULT,
        TOPIC_INVENTORY_RESULT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    # Kafka 브로커 연결 대기
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await consumer.start()
            logger.info(
                "Kafka 컨슈머 시작 완료 - 구독 토픽: %s, %s, %s",
                TOPIC_ORDER_CREATED, TOPIC_PAYMENT_RESULT, TOPIC_INVENTORY_RESULT,
            )
            break
        except Exception as e:
            logger.warning("Kafka 연결 실패 (시도 %d/%d): %s", attempt + 1, max_retries, e)
            await asyncio.sleep(2)
    else:
        logger.error("Kafka 연결 최대 재시도 횟수 초과")
        return

    try:
        async for msg in consumer:
            try:
                event = msg.value
                event_type = event.get("event_type", "")
                order_id = event.get("order_id", "")
                user_id = event.get("user_id", None)  # ORDER_CREATED에만 포함

                logger.info(
                    "이벤트 수신: topic=%s, event_type=%s, order_id=%s",
                    msg.topic, event_type, order_id,
                )

                # 이벤트 타입에 맞는 알림 메시지 생성
                message = build_message(event_type, event)

                # 알림 발송 (콘솔 시뮬레이션)
                send_notification(
                    order_id=order_id,
                    event_type=event_type,
                    message=message,
                    user_id=user_id,
                    channel="console",
                )

            except Exception as e:
                logger.error("알림 처리 오류: %s", e, exc_info=True)

    except asyncio.CancelledError:
        logger.info("컨슈머 태스크 취소됨")
    finally:
        await consumer.stop()
        logger.info("Kafka 컨슈머 종료")
