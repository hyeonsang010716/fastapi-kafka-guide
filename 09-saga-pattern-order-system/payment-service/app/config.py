"""
결제 서비스 설정
"""

import os

# Kafka 브로커 주소
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# 토픽 이름 정의
TOPIC_ORDER_CREATED = "order.created"
TOPIC_PAYMENT_RESULT = "payment.result"

# 보상 트랜잭션 토픽
TOPIC_REFUND_REQUEST = "payment.refund-request"
TOPIC_REFUND_RESULT = "payment.refund-result"

# 컨슈머 그룹 ID
CONSUMER_GROUP_ID = "payment-service-group"

# 결제 성공 확률 (80%)
PAYMENT_SUCCESS_RATE = 0.8

# 환불 성공 확률 (95% - 대부분의 환불은 성공)
REFUND_SUCCESS_RATE = 0.95

# 서비스 포트
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8001"))
