"""
컨슈머 통합 테스트
- testcontainers로 실행된 실제 Kafka에서 메시지를 소비
- 프로듀서가 보낸 메시지를 컨슈머가 올바르게 수신하는지 검증
"""

import asyncio
import json

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


@pytest.mark.asyncio
async def test_consume_messages(kafka_bootstrap_servers: str):
    """
    컨슈머가 메시지를 정상적으로 소비하는지 테스트

    검증 항목:
    - 컨슈머가 토픽에서 메시지를 가져올 수 있는가
    - 소비된 메시지의 값이 올바른가
    """
    topic = "test-consume-topic"
    test_message = {"action": "consume_test", "data": "소비 테스트 데이터"}

    # 먼저 프로듀서로 메시지를 전송
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    await producer.send_and_wait(topic, value=test_message)
    await producer.stop()

    # 컨슈머로 메시지 소비
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        group_id="test-group-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()

    try:
        # 메시지 수신 대기 (최대 10초)
        msg = await asyncio.wait_for(consumer.__anext__(), timeout=10.0)

        # 소비된 메시지 검증
        assert msg.value["action"] == "consume_test"
        assert msg.value["data"] == "소비 테스트 데이터"
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_consumer_receives_what_producer_sent(kafka_bootstrap_servers: str):
    """
    프로듀서가 전송한 메시지를 컨슈머가 정확히 수신하는지 검증하는 E2E 테스트

    흐름:
    1. 여러 개의 메시지를 프로듀서로 전송
    2. 컨슈머로 모든 메시지를 소비
    3. 전송한 메시지와 소비한 메시지가 순서와 내용 모두 일치하는지 확인
    """
    topic = "test-e2e-topic"
    # 테스트용 메시지 3개 생성
    messages_to_send = [
        {"id": i, "content": f"E2E 테스트 메시지 {i}"}
        for i in range(3)
    ]

    # 프로듀서로 메시지 전송
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    for msg in messages_to_send:
        await producer.send_and_wait(topic, value=msg)
    await producer.stop()

    # 컨슈머로 메시지 소비
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        group_id="test-group-e2e",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()

    received_messages = []
    try:
        # 3개의 메시지를 모두 수신할 때까지 대기 (최대 15초)
        for _ in range(3):
            msg = await asyncio.wait_for(consumer.__anext__(), timeout=15.0)
            received_messages.append(msg.value)
    finally:
        await consumer.stop()

    # 수신된 메시지 수 검증
    assert len(received_messages) == 3

    # 각 메시지의 내용이 전송한 것과 일치하는지 검증
    for i, received in enumerate(received_messages):
        assert received["id"] == messages_to_send[i]["id"]
        assert received["content"] == messages_to_send[i]["content"]
