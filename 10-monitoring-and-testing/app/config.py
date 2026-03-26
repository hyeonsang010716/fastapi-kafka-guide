"""
설정 모듈 - 환경 변수 기반 애플리케이션 설정
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """애플리케이션 설정 클래스"""

    # Kafka 설정
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "monitoring-topic"
    kafka_consumer_group: str = "monitoring-group"

    # 애플리케이션 설정
    app_name: str = "FastAPI Kafka Monitoring"
    app_version: str = "1.0.0"
    log_level: str = "INFO"

    model_config = {"env_prefix": "", "case_sensitive": False}


# 싱글톤 설정 인스턴스
settings = Settings()
