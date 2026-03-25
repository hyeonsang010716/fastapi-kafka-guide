"""
DLQ(Dead Letter Queue) Consumer
- payments-dlq 토픽에서 실패한 결제 메시지를 소비
- 실패 메시지를 인메모리 리스트에 저장하여 모니터링 제공
- 실제 환경에서는 DB 저장, 알림 발송, 대시보드 연동 등으로 확장
"""

import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    PAYMENTS_DLQ_TOPIC,
    DLQ_CONSUMER_GROUP_ID,
)

logger = logging.getLogger(__name__)

# DLQ 메시지 저장 (인메모리)
failed_payments: list[dict] = []


async def consume_dlq():
    """
    DLQ 컨슈머 메인 루프
    - 실패한 결제 메시지를 소비하여 로깅 및 저장
    - 운영 환경에서는 알림 시스템과 연동하여 즉시 대응
    """
    consumer = AIOKafkaConsumer(
        PAYMENTS_DLQ_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=DLQ_CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    # Kafka 연결 재시도
    while True:
        try:
            await consumer.start()
            logger.info(f"DLQ Consumer 시작 - 토픽: {PAYMENTS_DLQ_TOPIC}")
            break
        except Exception as e:
            logger.warning(f"DLQ Kafka 연결 실패, 5초 후 재시도: {e}")
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            failed_payment = msg.value
            # 실패 메시지를 인메모리 리스트에 저장
            failed_payments.append(failed_payment)

            logger.error(
                f"[DLQ] 실패 결제 수신 - "
                f"payment_id: {failed_payment.get('payment_id')}, "
                f"user_id: {failed_payment.get('user_id')}, "
                f"amount: {failed_payment.get('amount')}, "
                f"error: {failed_payment.get('error')}, "
                f"retry_count: {failed_payment.get('retry_count')}"
            )
    except asyncio.CancelledError:
        logger.info("DLQ Consumer 태스크 취소됨")
    finally:
        await consumer.stop()
        logger.info("DLQ Consumer 종료 완료")
