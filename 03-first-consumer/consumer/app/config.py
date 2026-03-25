"""
Consumer 설정 모듈
- Kafka Consumer에 필요한 환경 변수를 관리합니다.
- KAFKA_TOPIC: 구독할 토픽 이름
- KAFKA_GROUP_ID: Consumer Group ID (같은 그룹의 Consumer끼리 파티션을 나눠 가짐)
- AUTO_OFFSET_RESET: Consumer가 처음 시작할 때 어디서부터 읽을지 결정
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Consumer 애플리케이션 설정"""

    # Kafka 브로커 주소
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"

    # 구독할 Kafka 토픽 이름
    KAFKA_TOPIC: str = "test-topic"

    # Consumer Group ID — 같은 그룹 내 Consumer끼리 파티션을 분배받음
    KAFKA_GROUP_ID: str = "test-group"

    # 오프셋 초기화 정책
    # "earliest": 토픽의 처음부터 읽기 (과거 메시지 포함)
    # "latest": 지금부터 새로 들어오는 메시지만 읽기
    AUTO_OFFSET_RESET: str = "earliest"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


# 싱글턴 인스턴스
settings = Settings()
