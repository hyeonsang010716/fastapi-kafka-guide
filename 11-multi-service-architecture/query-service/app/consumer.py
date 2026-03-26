"""
쿼리 서비스 Kafka 컨슈머 (CQRS 읽기 모델)

모든 이벤트 토픽을 구독하여 인메모리 읽기 모델(Read Model)을 구축
이벤트 소싱의 핵심 개념:
- 모든 이벤트를 시간순으로 기록
- 이벤트를 리플레이하여 현재 상태를 재구성 가능
- 다양한 읽기 뷰를 독립적으로 생성 가능

CQRS (Command Query Responsibility Segregation):
- 쓰기(Command): order-service가 담당 → Kafka로 이벤트 발행
- 읽기(Query): query-service가 담당 → 이벤트를 소비하여 읽기 전용 뷰 구축
- 쓰기와 읽기를 분리하여 각각 독립적으로 최적화 가능
"""

import json
import asyncio
import logging
from typing import Dict, List, Any
from aiokafka import AIOKafkaConsumer
from datetime import datetime

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    ALL_TOPICS,
    CONSUMER_GROUP_ID,
)

logger = logging.getLogger("query-service.consumer")

# ──────────────────────────────────────────────
# 인메모리 읽기 모델 (Read Model)
# ──────────────────────────────────────────────

# 주문별 집계된 뷰 (order_id → 주문 상세 정보)
orders_read_model: Dict[str, Dict[str, Any]] = {}

# 전체 이벤트 로그 (이벤트 소싱 - 시간순 기록)
event_log: List[Dict[str, Any]] = []


def process_event(event: dict, topic: str):
    """
    수신된 이벤트를 읽기 모델에 반영
    이벤트 타입에 따라 주문 뷰를 생성하거나 업데이트

    이벤트 소싱 패턴: 이벤트를 적용(apply)하여 현재 상태 도출
    """
    event_type = event.get("event_type", "")
    order_id = event.get("order_id", "")
    timestamp = event.get("timestamp", datetime.utcnow().isoformat())

    # 이벤트 로그에 기록 (모든 이벤트를 시간순으로 보관)
    event_log.append({
        "event_type": event_type,
        "order_id": order_id,
        "topic": topic,
        "timestamp": timestamp,
        "data": event,
    })

    if not order_id:
        return

    # ── ORDER_CREATED: 새로운 주문 뷰 생성 ──
    if event_type == "ORDER_CREATED":
        orders_read_model[order_id] = {
            "order_id": order_id,
            "user_id": event.get("user_id", ""),
            "items": event.get("items", []),
            "total_price": event.get("total_price", 0),
            "shipping_address": event.get("shipping_address"),
            "status": "CREATED",
            "payment_status": "PENDING",
            "inventory_status": "PENDING",
            "created_at": timestamp,
            "updated_at": timestamp,
            "events": [
                {"type": event_type, "timestamp": timestamp},
            ],
        }
        logger.info("읽기 모델 생성: order_id=%s", order_id)

    # ── PAYMENT_COMPLETED: 결제 완료 반영 ──
    elif event_type == "PAYMENT_COMPLETED":
        if order_id in orders_read_model:
            view = orders_read_model[order_id]
            view["payment_status"] = "COMPLETED"
            view["payment_amount"] = event.get("amount", 0)
            view["payment_method"] = event.get("payment_method", "")
            view["transaction_id"] = event.get("transaction_id", "")
            view["updated_at"] = timestamp
            view["events"].append({"type": event_type, "timestamp": timestamp})
            # 결제 + 재고 모두 완료 시 주문 완료
            if view["inventory_status"] == "RESERVED":
                view["status"] = "COMPLETED"
            else:
                view["status"] = "PAYMENT_COMPLETED"
            logger.info("읽기 모델 업데이트 (결제 완료): order_id=%s", order_id)

    # ── PAYMENT_FAILED: 결제 실패 반영 ──
    elif event_type == "PAYMENT_FAILED":
        if order_id in orders_read_model:
            view = orders_read_model[order_id]
            view["payment_status"] = "FAILED"
            view["payment_failure_reason"] = event.get("reason", "")
            view["status"] = "CANCELLED"
            view["updated_at"] = timestamp
            view["events"].append({"type": event_type, "timestamp": timestamp})
            logger.info("읽기 모델 업데이트 (결제 실패): order_id=%s", order_id)

    # ── INVENTORY_RESERVED: 재고 예약 반영 ──
    elif event_type == "INVENTORY_RESERVED":
        if order_id in orders_read_model:
            view = orders_read_model[order_id]
            view["inventory_status"] = "RESERVED"
            view["warehouse_id"] = event.get("warehouse_id", "")
            view["updated_at"] = timestamp
            view["events"].append({"type": event_type, "timestamp": timestamp})
            # 결제 + 재고 모두 완료 시 주문 완료
            if view["payment_status"] == "COMPLETED":
                view["status"] = "COMPLETED"
            else:
                view["status"] = "INVENTORY_RESERVED"
            logger.info("읽기 모델 업데이트 (재고 예약): order_id=%s", order_id)

    # ── INVENTORY_FAILED: 재고 부족 반영 ──
    elif event_type == "INVENTORY_FAILED":
        if order_id in orders_read_model:
            view = orders_read_model[order_id]
            view["inventory_status"] = "FAILED"
            view["inventory_failure_reason"] = event.get("reason", "")
            view["status"] = "CANCELLED"
            view["updated_at"] = timestamp
            view["events"].append({"type": event_type, "timestamp": timestamp})
            logger.info("읽기 모델 업데이트 (재고 부족): order_id=%s", order_id)

    # ── NOTIFICATION_SENT: 알림 발송 기록 반영 ──
    elif event_type == "NOTIFICATION_SENT":
        if order_id in orders_read_model:
            view = orders_read_model[order_id]
            view["updated_at"] = timestamp
            view["events"].append({"type": event_type, "timestamp": timestamp})


async def start_consumer():
    """
    Kafka 컨슈머 시작
    모든 이벤트 토픽을 구독하여 읽기 모델 구축
    """
    consumer = AIOKafkaConsumer(
        *ALL_TOPICS,
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
            logger.info("Kafka 컨슈머 시작 완료 - 구독 토픽: %s", ALL_TOPICS)
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
                logger.info(
                    "이벤트 수신: topic=%s, event_type=%s, order_id=%s",
                    msg.topic,
                    event.get("event_type", ""),
                    event.get("order_id", ""),
                )

                # 읽기 모델에 이벤트 반영
                process_event(event, msg.topic)

            except Exception as e:
                logger.error("이벤트 처리 오류: %s", e, exc_info=True)

    except asyncio.CancelledError:
        logger.info("컨슈머 태스크 취소됨")
    finally:
        await consumer.stop()
        logger.info("Kafka 컨슈머 종료")
