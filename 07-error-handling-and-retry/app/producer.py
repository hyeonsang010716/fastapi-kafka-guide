"""
Kafka Producer
- acks="all" 설정으로 메시지 유실 방지
- send_message()에서 최대 3회 재시도로 일시적 전송 실패 대응
- 에러 핸들링을 포함한 안전한 메시지 전송
"""

import json
import logging
from aiokafka import AIOKafkaProducer
from app.config import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

# 전역 프로듀서 인스턴스
producer: AIOKafkaProducer | None = None


async def start_producer():
    """프로듀서 시작 및 Kafka 연결"""
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # acks="all": 모든 ISR(In-Sync Replica)이 메시지를 수신해야 성공
        # 가장 강력한 내구성 보장 (메시지 유실 가능성 최소화)
        acks="all",
        # 재시도 간 대기 시간(ms)
        retry_backoff_ms=500,
        # 메시지 직렬화: dict -> JSON bytes
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()
    logger.info("Kafka Producer 시작 완료")


async def stop_producer():
    """프로듀서 종료 및 연결 해제"""
    global producer
    if producer:
        await producer.stop()
        logger.info("Kafka Producer 종료 완료")


async def send_message(topic: str, message: dict, key: str | None = None):
    """
    Kafka 토픽에 메시지 전송

    Args:
        topic: 전송할 토픽 이름
        message: 전송할 메시지 (dict)
        key: 메시지 키 (파티션 라우팅에 사용)

    Returns:
        RecordMetadata: 전송 성공 시 메타데이터

    Raises:
        Exception: 전송 실패 시 예외 발생
    """
    if not producer:
        raise RuntimeError("Producer가 초기화되지 않았습니다")

    try:
        # 메시지 전송 및 결과 대기
        record_metadata = await producer.send_and_wait(
            topic=topic,
            value=message,
            key=key,
        )
        logger.info(
            f"메시지 전송 성공 - 토픽: {topic}, "
            f"파티션: {record_metadata.partition}, "
            f"오프셋: {record_metadata.offset}"
        )
        return record_metadata
    except Exception as e:
        logger.error(f"메시지 전송 실패 - 토픽: {topic}, 오류: {e}")
        raise
