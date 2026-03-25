"""
설정 모듈
- Consumer 식별을 위한 CONSUMER_ID를 환경 변수에서 로드합니다.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Consumer 애플리케이션 설정"""

    # Kafka 브로커 주소
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"

    # 구독할 토픽 이름
    KAFKA_TOPIC: str = "orders"

    # Consumer Group ID — 같은 그룹의 컨슈머끼리 파티션을 나눠 가짐
    KAFKA_GROUP_ID: str = "order-group"

    # 컨슈머 식별자 (docker-compose에서 환경 변수로 주입)
    CONSUMER_ID: str = "consumer-0"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


# 싱글턴 인스턴스
settings = Settings()
