"""
쿼리 서비스 설정 (CQRS 읽기 모델)
모든 이벤트 토픽을 구독하여 통합 읽기 뷰 구축
"""

import os

# Kafka 설정 (3-broker 클러스터)
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-0:9092,kafka-1:9092,kafka-2:9092",
)

# 구독할 모든 이벤트 토픽
TOPIC_ORDER_CREATED = "order.created"
TOPIC_PAYMENT_RESULT = "payment.result"
TOPIC_INVENTORY_RESULT = "inventory.result"
TOPIC_NOTIFICATION = "notification.sent"

ALL_TOPICS = [
    TOPIC_ORDER_CREATED,
    TOPIC_PAYMENT_RESULT,
    TOPIC_INVENTORY_RESULT,
    TOPIC_NOTIFICATION,
]

# 컨슈머 그룹 ID (쿼리 서비스 전용 - 다른 서비스와 독립적으로 소비)
CONSUMER_GROUP_ID = "query-service-group"

# 서비스 포트
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8005"))
