"""
알림 서비스 설정
모든 이벤트 토픽을 구독하여 사용자에게 알림 전송
"""

import os

# Kafka 설정 (3-broker 클러스터)
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-0:9092,kafka-1:9092,kafka-2:9092",
)

# 구독할 토픽 목록 (주문 생성, 결제 결과, 재고 결과)
TOPIC_ORDER_CREATED = "order.created"
TOPIC_PAYMENT_RESULT = "payment.result"
TOPIC_INVENTORY_RESULT = "inventory.result"
TOPIC_NOTIFICATION = "notification.sent"

# 컨슈머 그룹 ID
CONSUMER_GROUP_ID = "notification-service-group"

# 서비스 포트
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8004"))
