"""
환경 변수 기반 설정.
"""

import os

# 클라이언트 측
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:9000")
REQUEST_TIMEOUT_SEC = float(os.getenv("REQUEST_TIMEOUT_SEC", "2.0"))

# Retry
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
RETRY_BASE_DELAY_MS = int(os.getenv("RETRY_BASE_DELAY_MS", "200"))

# Circuit breaker
CB_FAILURE_THRESHOLD = int(os.getenv("CB_FAILURE_THRESHOLD", "5"))
CB_RECOVERY_TIMEOUT_SEC = float(os.getenv("CB_RECOVERY_TIMEOUT_SEC", "10"))

# 게이트웨이 측
GATEWAY_MODE = os.getenv("GATEWAY_MODE", "HEALTHY")     # HEALTHY | SLOW | DEAD | FLAKY
GATEWAY_LATENCY_MS = int(os.getenv("GATEWAY_LATENCY_MS", "50"))
