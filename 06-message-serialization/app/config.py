"""
애플리케이션 설정
- 환경 변수에서 Kafka 및 앱 설정을 로드
"""

import os


# Kafka 브로커 주소
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Kafka 토픽 이름
USER_EVENTS_TOPIC = os.getenv("USER_EVENTS_TOPIC", "user-events")
ORDER_EVENTS_TOPIC = os.getenv("ORDER_EVENTS_TOPIC", "order-events")

# 컨슈머 그룹 ID
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "serialization-demo-group")

# 앱 설정
APP_NAME = "06-message-serialization"
APP_PORT = int(os.getenv("APP_PORT", "8000"))
