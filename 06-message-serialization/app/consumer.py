"""
Kafka 컨슈머 모듈
- 백그라운드에서 메시지를 소비하고 인메모리 저장소에 보관
- 역직렬화기를 통해 bytes -> dict 변환
"""

import asyncio
import logging
from typing import List

from aiokafka import AIOKafkaConsumer

from app.config import (
    CONSUMER_GROUP_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    ORDER_EVENTS_TOPIC,
    USER_EVENTS_TOPIC,
)
from app.serializers import json_deserializer, key_deserializer
from shared.events import OrderPlacedEvent, UserCreatedEvent

logger = logging.getLogger(__name__)

# 헤더 event_type → Pydantic 모델 매핑
EVENT_MODEL_MAP: dict[str, type] = {
    "user_created": UserCreatedEvent,
    "order_placed": OrderPlacedEvent,
}

# 수신한 이벤트를 인메모리에 저장 (데모용)
consumed_events: List[dict] = []

# 최대 저장 이벤트 수 (메모리 보호)
MAX_EVENTS = 1000


async def start_consumer():
    """
    Kafka 컨슈머를 시작하고 메시지를 소비하는 비동기 루프

    컨슈머 설정:
    - value_deserializer: bytes -> dict (JSON 역직렬화)
    - key_deserializer: bytes -> str
    - auto_offset_reset: earliest (처음부터 읽기)
    """
    consumer = AIOKafkaConsumer(
        USER_EVENTS_TOPIC,
        ORDER_EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        # 값 역직렬화기 설정 - bytes를 자동으로 dict로 변환
        value_deserializer=json_deserializer,
        # 키 역직렬화기 설정 - bytes를 자동으로 str로 변환
        key_deserializer=key_deserializer,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    # 브로커 연결 재시도 (Kafka가 준비될 때까지 대기)
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await consumer.start()
            logger.info("Kafka 컨슈머 시작 완료")
            break
        except Exception as e:
            logger.warning(f"컨슈머 연결 시도 {attempt + 1}/{max_retries} 실패: {e}")
            await asyncio.sleep(2)
    else:
        logger.error("Kafka 컨슈머 연결 실패 - 최대 재시도 초과")
        return

    try:
        # 메시지 소비 루프
        async for msg in consumer:
            # 헤더 파싱: [(key, bytes)] -> {key: str}
            headers = {}
            if msg.headers:
                for header_key, header_value in msg.headers:
                    headers[header_key] = header_value.decode("utf-8") if header_value else None

            # 수신한 이벤트를 구조화하여 저장
            event_record = {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": msg.key,  # key_deserializer가 이미 str로 변환
                "headers": headers,
                "value": msg.value,  # value_deserializer가 이미 dict로 변환
            }

            consumed_events.append(event_record)

            # 메모리 보호: 최대 저장 수 초과 시 오래된 이벤트 제거
            if len(consumed_events) > MAX_EVENTS:
                consumed_events.pop(0)

            logger.info(
                f"이벤트 수신 - 토픽: {msg.topic}, "
                f"파티션: {msg.partition}, "
                f"오프셋: {msg.offset}, "
                f"키: {msg.key}, "
                f"헤더: {headers}, "
                f"역직렬화된 값: {msg.value}"
            )

            # dict → Pydantic 모델 복원 시도
            event_type = headers.get("event_type")
            model_class = EVENT_MODEL_MAP.get(event_type) if event_type else None

            if model_class:
                try:
                    validated = model_class.model_validate(msg.value)
                    logger.info(
                        f"Pydantic 변환 성공 - 모델: {model_class.__name__}, "
                        f"event_id: {validated.event_id}, "
                        f"created_at: {validated.created_at} (type: {type(validated.created_at).__name__})"
                    )
                except Exception as e:
                    logger.warning(f"Pydantic 변환 실패 - 모델: {model_class.__name__}, 오류: {e}")
    except asyncio.CancelledError:
        logger.info("컨슈머 태스크 취소됨")
    except Exception as e:
        logger.error(f"컨슈머 오류: {e}")
    finally:
        await consumer.stop()
        logger.info("Kafka 컨슈머 종료")
