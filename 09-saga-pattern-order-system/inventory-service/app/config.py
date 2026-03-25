"""
재고 서비스 설정
"""

import os

# Kafka 브로커 주소
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# 토픽 이름 정의
TOPIC_PAYMENT_RESULT = "payment.result"
TOPIC_INVENTORY_RESULT = "inventory.result"

# 컨슈머 그룹 ID
CONSUMER_GROUP_ID = "inventory-service-group"

# 재고 확보 성공 확률 (90%)
INVENTORY_SUCCESS_RATE = 0.9

# 서비스 포트
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8002"))
