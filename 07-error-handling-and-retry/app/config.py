"""
애플리케이션 설정
- 환경 변수에서 Kafka 및 앱 설정을 로드
"""

import os

# Kafka 브로커 주소
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Kafka 토픽 이름
PAYMENTS_TOPIC = os.getenv("PAYMENTS_TOPIC", "payments")
PAYMENTS_DLQ_TOPIC = os.getenv("PAYMENTS_DLQ_TOPIC", "payments-dlq")

# 컨슈머 그룹 ID
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "payments-group")
DLQ_CONSUMER_GROUP_ID = os.getenv("DLQ_CONSUMER_GROUP_ID", "payments-dlq-group")

# 재시도 설정
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_DELAY = float(os.getenv("BASE_DELAY", "1.0"))

# 앱 설정
APP_NAME = "07-error-handling-and-retry"
APP_PORT = int(os.getenv("APP_PORT", "8000"))
