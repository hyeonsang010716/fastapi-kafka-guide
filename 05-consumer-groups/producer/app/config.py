"""
설정 모듈
- pydantic-settings를 사용하여 환경 변수에서 설정을 로드합니다.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Producer 애플리케이션 설정"""

    # Kafka 브로커 주소
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"

    # 주문 메시지를 보낼 토픽 이름
    KAFKA_TOPIC: str = "orders"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


# 싱글턴 인스턴스
settings = Settings()
