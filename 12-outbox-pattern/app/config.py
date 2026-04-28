"""
환경 변수 기반 설정.
"""

import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://outbox:outbox@localhost:5432/outbox",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# 도메인 이벤트가 발행되는 토픽
TOPIC_ORDER_EVENTS = "order.events"

# 컨슈머 그룹 (샘플 다운스트림 컨슈머용)
CONSUMER_GROUP_ID = "order-events-consumer"

# Relay 설정
RELAY_ENABLED = os.getenv("RELAY_ENABLED", "true").lower() == "true"
RELAY_BATCH_SIZE = int(os.getenv("RELAY_BATCH_SIZE", "100"))
RELAY_POLL_INTERVAL_MS = int(os.getenv("RELAY_POLL_INTERVAL_MS", "200"))
RELAY_IDLE_BACKOFF_MS = int(os.getenv("RELAY_IDLE_BACKOFF_MS", "1000"))
