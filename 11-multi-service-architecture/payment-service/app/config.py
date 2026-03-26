"""
결제 서비스 설정
Kafka 브로커 및 토픽 설정
"""

import os

# Kafka 설정 (3-broker 클러스터)
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-0:9092,kafka-1:9092,kafka-2:9092",
)

# 토픽 이름
TOPIC_ORDER_CREATED = "order.created"
TOPIC_PAYMENT_RESULT = "payment.result"

# 컨슈머 그룹 ID
CONSUMER_GROUP_ID = "payment-service-group"

# 서비스 포트
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8002"))

# 결제 실패 시뮬레이션 확률 (0.0 ~ 1.0, 기본 20% 확률로 실패)
PAYMENT_FAILURE_RATE = float(os.getenv("PAYMENT_FAILURE_RATE", "0.2"))
