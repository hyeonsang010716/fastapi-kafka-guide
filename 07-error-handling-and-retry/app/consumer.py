"""
Kafka Consumer (결제 처리)
- enable_auto_commit=False: 수동 오프셋 커밋
  -> 메시지 처리 완료 후에만 커밋하여 메시지 유실 방지
- 결제 처리 시뮬레이션: ~30% 확률로 실패
- 실패 시 Exponential Backoff으로 재시도
- 최대 재시도 초과 시 DLQ(Dead Letter Queue)로 전송
"""

import json
import asyncio
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    PAYMENTS_TOPIC,
    PAYMENTS_DLQ_TOPIC,
    CONSUMER_GROUP_ID,
    MAX_RETRIES,
    BASE_DELAY,
)
from app.retry import retry_with_backoff
from app.producer import send_message

logger = logging.getLogger(__name__)

# 성공적으로 처리된 결제 저장 (인메모리)
processed_payments: list[dict] = []


async def process_payment(payment: dict) -> dict:
    """
    결제 처리 시뮬레이션
    - amount=10이면 무조건 실패하여 에러 핸들링/DLQ 테스트에 활용
    - 실제 환경에서는 PG사 API 호출 등으로 대체
    """
    # amount가 10이면 무조건 실패 (에러 핸들링 테스트용)
    if payment.get("amount") == 10:
        raise Exception(
            f"결제 처리 실패 (payment_id={payment.get('payment_id')}): "
            f"금액 검증 오류 - amount=10은 처리할 수 없습니다"
        )

    # 처리 성공
    return {
        "payment_id": payment["payment_id"],
        "user_id": payment["user_id"],
        "amount": payment["amount"],
        "currency": payment["currency"],
        "status": "completed",
        "processed_at": datetime.now().isoformat(),
    }


async def send_to_dlq(payment: dict, error: str, retry_count: int):
    """
    실패한 메시지를 DLQ(Dead Letter Queue) 토픽으로 전송
    - 원본 메시지에 오류 정보와 재시도 횟수를 추가
    """
    dlq_message = {
        "payment_id": payment.get("payment_id", "unknown"),
        "user_id": payment.get("user_id", "unknown"),
        "amount": payment.get("amount", 0),
        "currency": payment.get("currency", "KRW"),
        "error": str(error),
        "retry_count": retry_count,
        "failed_at": datetime.now().isoformat(),
    }
    try:
        await send_message(
            topic=PAYMENTS_DLQ_TOPIC,
            message=dlq_message,
            key=payment.get("payment_id"),
        )
        logger.warning(
            f"DLQ로 전송 완료 - payment_id: {payment.get('payment_id')}"
        )
    except Exception as e:
        logger.error(f"DLQ 전송마저 실패: {e}")


async def consume_payments():
    """
    결제 토픽 컨슈머 메인 루프
    - 수동 커밋: 처리 성공 후에만 오프셋 커밋
    - 재시도: Exponential Backoff 적용
    - DLQ: 최대 재시도 초과 시 실패 메시지를 별도 토픽으로 이동
    """
    consumer = AIOKafkaConsumer(
        PAYMENTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        # 수동 오프셋 커밋 — 처리 완료 전 커밋 방지
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    # Kafka 연결 재시도 (브로커 준비 대기)
    while True:
        try:
            await consumer.start()
            logger.info(f"Consumer 시작 - 토픽: {PAYMENTS_TOPIC}")
            break
        except Exception as e:
            logger.warning(f"Kafka 연결 실패, 5초 후 재시도: {e}")
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            payment = msg.value
            logger.info(
                f"메시지 수신 - payment_id: {payment.get('payment_id')}, "
                f"파티션: {msg.partition}, 오프셋: {msg.offset}"
            )

            try:
                # Exponential Backoff 재시도로 결제 처리 시도
                result = await retry_with_backoff(
                    func=lambda p=payment: process_payment(p),
                    max_retries=MAX_RETRIES,
                    base_delay=BASE_DELAY,
                )
                # 처리 성공 — 결과 저장
                processed_payments.append(result)
                logger.info(
                    f"결제 처리 성공 - payment_id: {result['payment_id']}"
                )
            except Exception as e:
                # 모든 재시도 실패 — DLQ로 전송
                logger.error(
                    f"결제 최종 실패, DLQ 전송 - payment_id: {payment.get('payment_id')}"
                )
                await send_to_dlq(payment, str(e), MAX_RETRIES)

            # 성공이든 실패(DLQ 전송)든 오프셋 커밋
            # -> 같은 메시지를 무한 반복 처리하지 않도록 함
            await consumer.commit()
            logger.debug(
                f"오프셋 커밋 완료 - 파티션: {msg.partition}, 오프셋: {msg.offset}"
            )
    except asyncio.CancelledError:
        logger.info("Consumer 태스크 취소됨")
    finally:
        await consumer.stop()
        logger.info("Consumer 종료 완료")
