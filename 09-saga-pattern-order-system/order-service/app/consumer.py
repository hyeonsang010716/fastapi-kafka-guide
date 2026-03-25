"""
주문 서비스 Kafka 컨슈머
payment.result, inventory.result, payment.refund-result 토픽을 구독하여 주문 상태를 업데이트

Saga 보상 트랜잭션 흐름:
- INVENTORY_FAILED 수신 → COMPENSATING 상태로 전환 → 환불 요청 발행
- REFUND_COMPLETED 수신 → COMPENSATED 상태 (보상 완료)
- REFUND_FAILED 수신 → COMPENSATION_FAILED 상태 (수동 개입 필요)
"""

import json
import logging
import asyncio
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_PAYMENT_RESULT,
    TOPIC_INVENTORY_RESULT,
    TOPIC_REFUND_RESULT,
    CONSUMER_GROUP_ID,
)
from app.models import update_order_status, get_order, OrderStatus
from app.producer import publish_refund_request

logger = logging.getLogger(__name__)

# 전역 컨슈머 인스턴스
consumer: AIOKafkaConsumer | None = None


async def start_consumer():
    """
    Kafka 컨슈머를 시작하고 메시지 처리 루프를 실행
    payment.result, inventory.result, payment.refund-result 세 토픽을 동시에 구독
    """
    global consumer
    consumer = AIOKafkaConsumer(
        TOPIC_PAYMENT_RESULT,
        TOPIC_INVENTORY_RESULT,
        TOPIC_REFUND_RESULT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        # JSON 역직렬화
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    # Kafka 연결 재시도 (Kafka가 준비될 때까지 대기)
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await consumer.start()
            logger.info(
                "Kafka 컨슈머가 시작되었습니다 "
                "(payment.result, inventory.result, payment.refund-result 구독)"
            )
            break
        except Exception as e:
            logger.warning(f"Kafka 연결 재시도 중... ({attempt + 1}/{max_retries}): {e}")
            await asyncio.sleep(2)
    else:
        logger.error("Kafka 연결에 실패했습니다")
        return

    # 메시지 처리 루프
    try:
        async for msg in consumer:
            await process_message(msg)
    except asyncio.CancelledError:
        logger.info("컨슈머 태스크가 취소되었습니다")
    except Exception as e:
        logger.error(f"컨슈머 오류: {e}")
    finally:
        await consumer.stop()
        logger.info("Kafka 컨슈머가 종료되었습니다")


async def process_message(msg):
    """
    수신된 메시지를 이벤트 타입에 따라 처리
    - PAYMENT_COMPLETED → 재고 처리 단계로 전환
    - PAYMENT_FAILED → 주문 실패로 마킹
    - INVENTORY_RESERVED → 주문 완료
    - INVENTORY_FAILED → 보상 트랜잭션 시작 (환불 요청 발행)
    - REFUND_COMPLETED → 보상 완료
    - REFUND_FAILED → 보상 실패 (수동 개입 필요)
    """
    topic = msg.topic
    event = msg.value
    event_type = event.get("event_type", "UNKNOWN")
    order_id = event.get("order_id", "UNKNOWN")

    logger.info(f"[수신] 토픽={topic}, event_type={event_type}, order_id={order_id}")

    if topic == TOPIC_PAYMENT_RESULT:
        if event_type == "PAYMENT_COMPLETED":
            # 결제 성공 → 재고 처리 단계로 전환
            update_order_status(order_id, OrderStatus.INVENTORY_PROCESSING)
            logger.info(f"[상태 변경] order_id={order_id} → INVENTORY_PROCESSING")

        elif event_type == "PAYMENT_FAILED":
            # 결제 실패 → 주문 실패 (보상 불필요: 아직 아무것도 예약되지 않음)
            reason = event.get("reason", "결제 실패")
            update_order_status(order_id, OrderStatus.PAYMENT_FAILED, reason=reason)
            logger.info(f"[상태 변경] order_id={order_id} → PAYMENT_FAILED (사유: {reason})")

    elif topic == TOPIC_INVENTORY_RESULT:
        if event_type == "INVENTORY_RESERVED":
            # 재고 확보 성공 → 주문 완료
            update_order_status(order_id, OrderStatus.COMPLETED)
            logger.info(f"[상태 변경] order_id={order_id} → COMPLETED")

        elif event_type == "INVENTORY_FAILED":
            # 재고 부족 → 보상 트랜잭션 시작 (결제 환불 요청)
            reason = event.get("reason", "재고 부족")
            update_order_status(order_id, OrderStatus.COMPENSATING, reason=reason)
            logger.info(f"[상태 변경] order_id={order_id} → COMPENSATING (사유: {reason})")

            # 주문 정보에서 결제 금액을 가져와 환불 요청 이벤트 발행
            order = get_order(order_id)
            if order:
                refund_event = {
                    "event_type": "REFUND_REQUESTED",
                    "order_id": order_id,
                    "amount": order["total_price"],
                    "reason": f"재고 부족으로 인한 환불: {reason}",
                    "timestamp": datetime.utcnow().isoformat(),
                }
                await publish_refund_request(refund_event)
                logger.info(f"[보상 트랜잭션] order_id={order_id} 환불 요청 발행 완료")
            else:
                update_order_status(order_id, OrderStatus.COMPENSATION_FAILED, reason="주문 정보 조회 실패")
                logger.error(f"[보상 실패] order_id={order_id} 주문 정보를 찾을 수 없습니다")

    elif topic == TOPIC_REFUND_RESULT:
        if event_type == "REFUND_COMPLETED":
            # 환불 성공 → 보상 완료
            amount = event.get("amount", 0)
            update_order_status(order_id, OrderStatus.COMPENSATED, reason=f"환불 완료 (금액: {amount})")
            logger.info(f"[상태 변경] order_id={order_id} → COMPENSATED (환불 성공, 금액: {amount})")

        elif event_type == "REFUND_FAILED":
            # 환불 실패 → 수동 개입 필요
            reason = event.get("reason", "환불 처리 실패")
            update_order_status(order_id, OrderStatus.COMPENSATION_FAILED, reason=reason)
            logger.error(f"[상태 변경] order_id={order_id} → COMPENSATION_FAILED (사유: {reason})")
            logger.error(f"[긴급] order_id={order_id} 수동 환불 처리가 필요합니다!")
