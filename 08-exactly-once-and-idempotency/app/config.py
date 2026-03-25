"""
애플리케이션 설정 - 환경변수에서 값을 읽어옴
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Kafka 설정
    kafka_bootstrap_servers: str = "localhost:9092"
    topic_name: str = "point-events"
    consumer_group_id: str = "point-consumer-group"

    # Redis 설정
    redis_url: str = "redis://localhost:6379/0"

    # 멱등성 키 TTL (초) - 기본 24시간
    idempotency_ttl: int = 86400

    model_config = {"env_file": ".env"}


settings = Settings()
