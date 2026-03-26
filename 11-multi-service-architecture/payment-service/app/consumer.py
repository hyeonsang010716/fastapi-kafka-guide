"""
결제 서비스 Kafka 컨슈머
order.created 이벤트를 소비하여 결제 처리를 시뮬레이션하고
결과를 payment.result 토픽으로 발행
"""

import json
import uuid
import random
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from datetime import datetime

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDER_CREATED,
    CONSUMER_GROUP_ID,
    PAYMENT_FAILURE_RATE,
)
from .producer import publish_payment_result

logger = logging.getLogger("payment-service.consumer")

# 처리된 결제 기록 (인메모리)
payment_records: dict = {}


async def start_consumer():
    """
    Kafka 컨슈머 시작
    order.created 이벤트를 소비하여 결제 처리
    - 성공 시: PAYMENT_COMPLETED 이벤트 발행
    - 실패 시: PAYMENT_FAILED 이벤트 발행 (보상 트랜잭션 트리거)
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
                total_price = event.get("total_price", 0)

                logger.info("주문 수신 - 결제 처리 시작: order_id=%s, amount=%.2f",
                            order_id, total_price)

                # 결제 처리 시뮬레이션 (0.5~1.5초 랜덤 지연)
                await asyncio.sleep(random.uniform(0.5, 1.5))

                # 랜덤 확률로 결제 성공/실패 결정
                if random.random() > PAYMENT_FAILURE_RATE:
                    # 결제 성공
                    transaction_id = str(uuid.uuid4())[:8]
                    result_event = {
                        "event_type": "PAYMENT_COMPLETED",
                        "order_id": order_id,
                        "amount": total_price,
                        "payment_method": "credit_card",
                        "transaction_id": transaction_id,
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                    payment_records[order_id] = {
                        "status": "completed",
                        "amount": total_price,
                        "transaction_id": transaction_id,
                    }
                    logger.info("결제 성공: order_id=%s, tx=%s", order_id, transaction_id)
                else:
                    # 결제 실패 시뮬레이션
                    reasons = [
                        "잔액 부족",
                        "카드 한도 초과",
                        "결제 게이트웨이 오류",
                        "카드 만료",
                    ]
                    reason = random.choice(reasons)
                    result_event = {
                        "event_type": "PAYMENT_FAILED",
                        "order_id": order_id,
                        "amount": total_price,
                        "reason": reason,
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                    payment_records[order_id] = {
                        "status": "failed",
                        "reason": reason,
                    }
                    logger.warning("결제 실패: order_id=%s, 사유=%s", order_id, reason)

                # 결제 결과 이벤트 발행
                await publish_payment_result(result_event)

            except Exception as e:
                logger.error("결제 처리 오류: %s", e, exc_info=True)

    except asyncio.CancelledError:
        logger.info("컨슈머 태스크 취소됨")
    finally:
        await consumer.stop()
        logger.info("Kafka 컨슈머 종료")
