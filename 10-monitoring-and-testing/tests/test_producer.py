"""
프로듀서 통합 테스트
- testcontainers로 실행된 실제 Kafka에 메시지를 전송
- 메시지가 올바른 토픽에 도착하는지 검증
"""

import json

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# 테스트 토픽 이름
TEST_TOPIC = "test-producer-topic"


@pytest.mark.asyncio
async def test_send_message(kafka_bootstrap_servers: str):
    """
    프로듀서가 메시지를 Kafka에 성공적으로 전송하는지 테스트

    검증 항목:
    - 전송 결과에 토픽, 파티션, 오프셋 정보가 포함되는가
    - 에러 없이 전송이 완료되는가
    """
    # 프로듀서 생성 및 시작
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    try:
        # 메시지 전송
        message = {"content": "테스트 메시지", "test": True}
        result = await producer.send_and_wait(TEST_TOPIC, value=message)

        # 전송 결과 검증
        assert result.topic == TEST_TOPIC
        assert result.partition >= 0
        assert result.offset >= 0
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_message_arrives_in_correct_topic(kafka_bootstrap_servers: str):
    """
    전송한 메시지가 올바른 토픽에 도착하는지 검증하는 테스트

    흐름:
    1. 프로듀서로 특정 토픽에 메시지 전송
    2. 컨슈머로 해당 토픽에서 메시지를 소비
    3. 전송한 메시지와 소비한 메시지가 일치하는지 확인
    """
    topic = "test-correct-topic"
    test_message = {"id": 1, "text": "토픽 검증 테스트"}

    # 프로듀서로 메시지 전송
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    await producer.send_and_wait(topic, value=test_message)
    await producer.stop()

    # 컨슈머로 메시지 확인
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        group_id="test-group-producer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,  # 최대 10초 대기
    )
    await consumer.start()

    try:
        # 메시지 소비 (최대 10초 대기)
        msg = await asyncio.wait_for(consumer.__anext__(), timeout=10.0)

        # 메시지 내용 검증
        assert msg.topic == topic
        assert msg.value["id"] == test_message["id"]
        assert msg.value["text"] == test_message["text"]
    finally:
        await consumer.stop()


# asyncio import (test_message_arrives_in_correct_topic에서 사용)
import asyncio
