"""
주문 서비스 설정
환경 변수에서 Kafka 브로커 주소 등 설정을 읽어옴
3-broker 클러스터에 맞게 복수 브로커 주소 사용
"""

import os

# ──────────────────────────────────────────────
# Kafka 설정 (3-broker 클러스터)
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-0:9092,kafka-1:9092,kafka-2:9092",
)

# ──────────────────────────────────────────────
# 토픽 이름 정의
# ──────────────────────────────────────────────
TOPIC_ORDER_CREATED = "order.created"
TOPIC_PAYMENT_RESULT = "payment.result"
TOPIC_INVENTORY_RESULT = "inventory.result"

# ──────────────────────────────────────────────
# 컨슈머 그룹 ID
# ──────────────────────────────────────────────
CONSUMER_GROUP_ID = "order-service-group"

# ──────────────────────────────────────────────
# 서비스 포트
# ──────────────────────────────────────────────
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8001"))
