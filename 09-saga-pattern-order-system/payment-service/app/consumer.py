"""
결제 서비스 Kafka 컨슈머
order.created 토픽을 구독하여 결제를 처리하고 결과를 발행
payment.refund-request 토픽을 구독하여 환불을 처리하고 결과를 발행

결제 시뮬레이션:
- 80% 확률로 결제 성공 (PAYMENT_COMPLETED)
- 20% 확률로 결제 실패 (PAYMENT_FAILED)

환불 시뮬레이션 (Saga 보상 트랜잭션):
- 95% 확률로 환불 성공 (REFUND_COMPLETED)
- 5% 확률로 환불 실패 (REFUND_FAILED)
"""

import json
import random
import logging
import asyncio
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDER_CREATED,
    TOPIC_REFUND_REQUEST,
    CONSUMER_GROUP_ID,
    PAYMENT_SUCCESS_RATE,
    REFUND_SUCCESS_RATE,
)
from app.producer import publish_payment_result, publish_refund_result

logger = logging.getLogger(__name__)

# 처리된 결제 기록 저장소
payments_db: list[dict] = []

# 처리된 환불 기록 저장소
refunds_db: list[dict] = []


async def start_consumer():
    """
    Kafka 컨슈머를 시작하고 order.created, payment.refund-request 토픽 메시지를 처리
    """
    consumer = AIOKafkaConsumer(
        TOPIC_ORDER_CREATED,
        TOPIC_REFUND_REQUEST,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    # Kafka 연결 재시도
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await consumer.start()
            logger.info("결제 서비스 컨슈머가 시작되었습니다 (order.created, payment.refund-request 구독)")
            break
        except Exception as e:
            logger.warning(f"Kafka 연결 재시도 중... ({attempt + 1}/{max_retries}): {e}")
            await asyncio.sleep(2)
    else:
        logger.error("Kafka 연결에 실패했습니다")
        return

    # 메시지 처리 루프 - 토픽에 따라 분기
    try:
        async for msg in consumer:
            if msg.topic == TOPIC_ORDER_CREATED:
                await process_order_created(msg.value)
            elif msg.topic == TOPIC_REFUND_REQUEST:
                await process_refund_request(msg.value)
    except asyncio.CancelledError:
        logger.info("컨슈머 태스크가 취소되었습니다")
    except Exception as e:
        logger.error(f"컨슈머 오류: {e}")
    finally:
        await consumer.stop()


async def process_order_created(event: dict):
    """
    주문 생성 이벤트를 수신하여 결제를 시뮬레이션

    - 랜덤으로 성공/실패를 결정 (80% 성공)
    - 결과를 payment.result 토픽으로 발행
    """
    order_id = event.get("order_id")
    total_price = event.get("total_price", 0)
    user_id = event.get("user_id", "unknown")

    logger.info(f"[수신] 주문 생성 이벤트 → order_id={order_id}, amount={total_price}")

    # 결제 처리 시뮬레이션 (약간의 지연)
    await asyncio.sleep(1)

    # 80% 확률로 결제 성공
    is_success = random.random() < PAYMENT_SUCCESS_RATE

    if is_success:
        # 결제 성공 이벤트 발행
        result_event = {
            "event_type": "PAYMENT_COMPLETED",
            "order_id": order_id,
            "amount": total_price,
            "timestamp": datetime.utcnow().isoformat(),
        }
        payment_record = {
            "order_id": order_id,
            "user_id": user_id,
            "amount": total_price,
            "status": "COMPLETED",
            "processed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"[결제 성공] order_id={order_id}, amount={total_price}")
    else:
        # 결제 실패 이벤트 발행
        reason = "잔액 부족으로 결제가 거부되었습니다"
        result_event = {
            "event_type": "PAYMENT_FAILED",
            "order_id": order_id,
            "reason": reason,
            "timestamp": datetime.utcnow().isoformat(),
        }
        payment_record = {
            "order_id": order_id,
            "user_id": user_id,
            "amount": total_price,
            "status": "FAILED",
            "reason": reason,
            "processed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"[결제 실패] order_id={order_id}, reason={reason}")

    # 결제 기록 저장
    payments_db.append(payment_record)

    # 결제 결과를 payment.result 토픽으로 발행
    await publish_payment_result(result_event)


async def process_refund_request(event: dict):
    """
    환불 요청 이벤트를 수신하여 환불을 시뮬레이션 (Saga 보상 트랜잭션)

    - 95% 확률로 환불 성공 (REFUND_COMPLETED)
    - 5% 확률로 환불 실패 (REFUND_FAILED)
    """
    order_id = event.get("order_id")
    amount = event.get("amount", 0)
    reason = event.get("reason", "")

    logger.info(f"[수신] 환불 요청 이벤트 → order_id={order_id}, amount={amount}")

    # 환불 처리 시뮬레이션 (약간의 지연)
    await asyncio.sleep(1)

    # 95% 확률로 환불 성공
    is_success = random.random() < REFUND_SUCCESS_RATE

    if is_success:
        result_event = {
            "event_type": "REFUND_COMPLETED",
            "order_id": order_id,
            "amount": amount,
            "timestamp": datetime.utcnow().isoformat(),
        }
        refund_record = {
            "order_id": order_id,
            "amount": amount,
            "status": "COMPLETED",
            "reason": reason,
            "processed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"[환불 성공] order_id={order_id}, amount={amount}")
    else:
        fail_reason = "환불 처리 중 오류가 발생했습니다"
        result_event = {
            "event_type": "REFUND_FAILED",
            "order_id": order_id,
            "reason": fail_reason,
            "timestamp": datetime.utcnow().isoformat(),
        }
        refund_record = {
            "order_id": order_id,
            "amount": amount,
            "status": "FAILED",
            "reason": fail_reason,
            "processed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"[환불 실패] order_id={order_id}, reason={fail_reason}")

    # 환불 기록 저장
    refunds_db.append(refund_record)

    # 환불 결과를 payment.refund-result 토픽으로 발행
    await publish_refund_result(result_event)
