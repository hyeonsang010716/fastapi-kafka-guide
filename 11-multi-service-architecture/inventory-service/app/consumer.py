"""
재고 서비스 Kafka 컨슈머
order.created 이벤트를 소비하여 재고를 확인/예약하고
결과를 inventory.result 토픽으로 발행
"""

import json
import random
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from datetime import datetime

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDER_CREATED,
    CONSUMER_GROUP_ID,
    INVENTORY_FAILURE_RATE,
)
from .producer import publish_inventory_result

logger = logging.getLogger("inventory-service.consumer")

# ──────────────────────────────────────────────
# 인메모리 재고 데이터 (시뮬레이션)
# ──────────────────────────────────────────────
inventory_stock: dict = {
    "PROD-001": {"name": "노트북", "stock": 100},
    "PROD-002": {"name": "키보드", "stock": 200},
    "PROD-003": {"name": "마우스", "stock": 500},
    "PROD-004": {"name": "모니터", "stock": 50},
    "PROD-005": {"name": "헤드셋", "stock": 150},
}

# 재고 예약 기록
reservation_records: dict = {}


async def start_consumer():
    """
    Kafka 컨슈머 시작
    order.created 이벤트를 소비하여 재고 확인 및 예약
    - 재고 충분: INVENTORY_RESERVED 이벤트 발행
    - 재고 부족: INVENTORY_FAILED 이벤트 발행
    """
    consumer = AIOKafkaConsumer(
        TOPIC_ORDER_CREATED,
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
            logger.info("Kafka 컨슈머 시작 완료 - 구독 토픽: %s", TOPIC_ORDER_CREATED)
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
                order_id = event.get("order_id", "")
                items = event.get("items", [])

                logger.info("주문 수신 - 재고 확인 시작: order_id=%s, 항목수=%d",
                            order_id, len(items))

                # 재고 확인 시뮬레이션 (0.3~1.0초 지연)
                await asyncio.sleep(random.uniform(0.3, 1.0))

                # 랜덤 확률로 재고 충분/부족 결정
                if random.random() > INVENTORY_FAILURE_RATE:
                    # 재고 예약 성공
                    result_event = {
                        "event_type": "INVENTORY_RESERVED",
                        "order_id": order_id,
                        "items": items,
                        "warehouse_id": f"WH-{random.randint(1, 5):03d}",
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                    reservation_records[order_id] = {
                        "status": "reserved",
                        "items": items,
                    }
                    logger.info("재고 예약 성공: order_id=%s", order_id)
                else:
                    # 재고 부족 시뮬레이션
                    failed_item = random.choice(items) if items else {}
                    reason = f"상품 '{failed_item.get('product_name', '알 수 없음')}' 재고 부족"
                    result_event = {
                        "event_type": "INVENTORY_FAILED",
                        "order_id": order_id,
                        "reason": reason,
                        "failed_items": [failed_item.get("product_id", "")],
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                    reservation_records[order_id] = {
                        "status": "failed",
                        "reason": reason,
                    }
                    logger.warning("재고 부족: order_id=%s, 사유=%s", order_id, reason)

                # 재고 결과 이벤트 발행
                await publish_inventory_result(result_event)

            except Exception as e:
                logger.error("재고 처리 오류: %s", e, exc_info=True)

    except asyncio.CancelledError:
        logger.info("컨슈머 태스크 취소됨")
    finally:
        await consumer.stop()
        logger.info("Kafka 컨슈머 종료")
