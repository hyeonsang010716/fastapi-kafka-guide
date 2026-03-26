"""
주문 서비스 Kafka 컨슈머
결제 결과(payment.result)와 재고 결과(inventory.result) 이벤트를 소비하여
주문 상태를 업데이트
"""

import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from datetime import datetime

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_PAYMENT_RESULT,
    TOPIC_INVENTORY_RESULT,
    CONSUMER_GROUP_ID,
)
from .models import orders_db, OrderStatus

logger = logging.getLogger("order-service.consumer")


async def start_consumer():
    """
    Kafka 컨슈머 시작 - 백그라운드 태스크로 실행
    payment.result와 inventory.result 토픽을 구독하여 주문 상태 갱신
    """
    consumer = AIOKafkaConsumer(
        TOPIC_PAYMENT_RESULT,
        TOPIC_INVENTORY_RESULT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        # JSON 역직렬화
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        # 컨슈머 그룹 최초 시작 시 가장 오래된 메시지부터 읽기
        auto_offset_reset="earliest",
    )

    # Kafka 브로커 연결 대기 (재시도 로직)
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await consumer.start()
            logger.info("Kafka 컨슈머 시작 완료 - 구독 토픽: %s, %s",
                        TOPIC_PAYMENT_RESULT, TOPIC_INVENTORY_RESULT)
            break
        except Exception as e:
            logger.warning("Kafka 연결 실패 (시도 %d/%d): %s", attempt + 1, max_retries, e)
            await asyncio.sleep(2)
    else:
        logger.error("Kafka 연결 최대 재시도 횟수 초과")
        return

    try:
        # 메시지 무한 루프: 이벤트를 지속적으로 소비
        async for msg in consumer:
            try:
                event = msg.value
                event_type = event.get("event_type", "")
                order_id = event.get("order_id", "")
                topic = msg.topic

                logger.info(
                    "이벤트 수신: topic=%s, event_type=%s, order_id=%s",
                    topic, event_type, order_id,
                )

                # 주문이 존재하는 경우에만 상태 업데이트
                if order_id in orders_db:
                    order = orders_db[order_id]
                    now = datetime.utcnow().isoformat()

                    if event_type == "PAYMENT_COMPLETED":
                        order.status = OrderStatus.PAYMENT_COMPLETED
                        order.events.append("PAYMENT_COMPLETED")
                        logger.info("주문 상태 업데이트: %s → PAYMENT_COMPLETED", order_id)

                    elif event_type == "PAYMENT_FAILED":
                        order.status = OrderStatus.CANCELLED
                        order.events.append("PAYMENT_FAILED")
                        logger.warning("결제 실패로 주문 취소: %s, 사유: %s",
                                       order_id, event.get("reason"))

                    elif event_type == "INVENTORY_RESERVED":
                        order.status = OrderStatus.INVENTORY_RESERVED
                        order.events.append("INVENTORY_RESERVED")
                        # 결제와 재고 모두 완료되면 주문 완료
                        if "PAYMENT_COMPLETED" in order.events:
                            order.status = OrderStatus.COMPLETED
                            logger.info("주문 완료: %s (결제 + 재고 확인 완료)", order_id)

                    elif event_type == "INVENTORY_FAILED":
                        order.status = OrderStatus.CANCELLED
                        order.events.append("INVENTORY_FAILED")
                        logger.warning("재고 부족으로 주문 취소: %s, 사유: %s",
                                       order_id, event.get("reason"))

                    object.__setattr__(order, "updated_at", now)
                else:
                    logger.warning("알 수 없는 주문 ID: %s (아직 저장되지 않았을 수 있음)", order_id)

            except Exception as e:
                logger.error("이벤트 처리 오류: %s", e, exc_info=True)

    except asyncio.CancelledError:
        logger.info("컨슈머 태스크 취소됨")
    finally:
        await consumer.stop()
        logger.info("Kafka 컨슈머 종료")
