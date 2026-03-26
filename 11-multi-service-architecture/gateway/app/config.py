"""
API Gateway 설정
각 내부 서비스의 URL을 환경 변수에서 읽어옴
"""

import os

# ──────────────────────────────────────────────
# 내부 서비스 URL (Docker Compose 네트워크 내부 통신)
# ──────────────────────────────────────────────
ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:8001")
PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://payment-service:8002")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL", "http://inventory-service:8003")
NOTIFICATION_SERVICE_URL = os.getenv("NOTIFICATION_SERVICE_URL", "http://notification-service:8004")
QUERY_SERVICE_URL = os.getenv("QUERY_SERVICE_URL", "http://query-service:8005")

# Gateway 서비스 포트
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8000"))

# 내부 서비스 요청 타임아웃 (초)
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10.0"))

# 모든 서비스 목록 (헬스 체크 용도)
INTERNAL_SERVICES = {
    "order-service": ORDER_SERVICE_URL,
    "payment-service": PAYMENT_SERVICE_URL,
    "inventory-service": INVENTORY_SERVICE_URL,
    "notification-service": NOTIFICATION_SERVICE_URL,
    "query-service": QUERY_SERVICE_URL,
}
