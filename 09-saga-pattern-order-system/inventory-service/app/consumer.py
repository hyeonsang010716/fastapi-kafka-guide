"""
재고 서비스 Kafka 컨슈머
payment.result 토픽을 구독하여 결제 성공 시 재고를 확인

재고 시뮬레이션:
- 90% 확률로 재고 확보 성공 (INVENTORY_RESERVED)
- 10% 확률로 재고 부족 실패 (INVENTORY_FAILED)
"""

import json
import random
import logging
import asyncio
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_PAYMENT_RESULT,
    CONSUMER_GROUP_ID,
    INVENTORY_SUCCESS_RATE,
)
from app.producer import publish_inventory_result

logger = logging.getLogger(__name__)

# 재고 처리 기록 저장소
inventory_db: list[dict] = []


async def start_consumer():
    """
    Kafka 컨슈머를 시작하고 payment.result 토픽 메시지를 처리
    결제 성공(PAYMENT_COMPLETED) 이벤트만 처리
    """
    consumer = AIOKafkaConsumer(
        TOPIC_PAYMENT_RESULT,
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
            logger.info("재고 서비스 컨슈머가 시작되었습니다 (payment.result 구독)")
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
            await process_payment_result(msg.value)
    except asyncio.CancelledError:
        logger.info("컨슈머 태스크가 취소되었습니다")
    except Exception as e:
        logger.error(f"컨슈머 오류: {e}")
    finally:
        await consumer.stop()


async def process_payment_result(event: dict):
    """
    결제 결과 이벤트를 수신하여 재고를 확인

    - PAYMENT_COMPLETED 이벤트만 처리 (PAYMENT_FAILED는 무시)
    - 90% 확률로 재고 확보 성공
    - 결과를 inventory.result 토픽으로 발행
    """
    event_type = event.get("event_type")
    order_id = event.get("order_id")

    # 결제 성공 이벤트만 처리 (실패 이벤트는 재고 서비스가 처리할 필요 없음)
    if event_type != "PAYMENT_COMPLETED":
        logger.info(f"[무시] 결제 실패 이벤트 건너뜀 → order_id={order_id}")
        return

    logger.info(f"[수신] 결제 성공 이벤트 → order_id={order_id}, 재고 확인 시작")

    # 재고 확인 시뮬레이션 (약간의 지연)
    await asyncio.sleep(1)

    # 90% 확률로 재고 확보 성공
    is_success = random.random() < INVENTORY_SUCCESS_RATE

    if is_success:
        # 재고 확보 성공
        result_event = {
            "event_type": "INVENTORY_RESERVED",
            "order_id": order_id,
            "items": event.get("items", []),
            "timestamp": datetime.utcnow().isoformat(),
        }
        inventory_record = {
            "order_id": order_id,
            "status": "RESERVED",
            "processed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"[재고 확보 성공] order_id={order_id}")
    else:
        # 재고 부족 실패
        reason = "요청한 상품의 재고가 부족합니다"
        result_event = {
            "event_type": "INVENTORY_FAILED",
            "order_id": order_id,
            "reason": reason,
            "timestamp": datetime.utcnow().isoformat(),
        }
        inventory_record = {
            "order_id": order_id,
            "status": "FAILED",
            "reason": reason,
            "processed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"[재고 부족] order_id={order_id}, reason={reason}")

    # 재고 처리 기록 저장
    inventory_db.append(inventory_record)

    # 재고 결과를 inventory.result 토픽으로 발행
    await publish_inventory_result(result_event)
