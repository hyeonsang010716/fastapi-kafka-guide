"""
주문 서비스 설정
환경 변수에서 Kafka 브로커 주소 등 설정을 읽어옴
"""

import os

# Kafka 브로커 주소 (docker-compose 환경에서는 서비스 이름 사용)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# 토픽 이름 정의
TOPIC_ORDER_CREATED = "order.created"
TOPIC_PAYMENT_RESULT = "payment.result"
TOPIC_INVENTORY_RESULT = "inventory.result"

# 보상 트랜잭션 토픽
TOPIC_REFUND_REQUEST = "payment.refund-request"
TOPIC_REFUND_RESULT = "payment.refund-result"

# 컨슈머 그룹 ID
CONSUMER_GROUP_ID = "order-service-group"

# 서비스 포트
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8000"))
