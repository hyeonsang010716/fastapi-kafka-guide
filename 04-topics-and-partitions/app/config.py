"""
설정 모듈
- pydantic-settings를 사용하여 환경 변수에서 설정을 로드합니다.
- Kafka 브로커 주소, 토픽 이름, 컨슈머 그룹 등을 관리합니다.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """애플리케이션 설정"""

    # Kafka 브로커 주소 (docker-compose 내부 네트워크에서는 "kafka:9092")
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"

    # 주문 토픽 이름 (파티션 3개로 생성)
    ORDERS_TOPIC: str = "orders"

    # 컨슈머 그룹 ID
    CONSUMER_GROUP_ID: str = "orders-consumer-group"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


# 싱글턴 인스턴스 — 앱 전체에서 공유
settings = Settings()
