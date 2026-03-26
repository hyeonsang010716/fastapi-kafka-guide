"""
테스트 설정 모듈
- testcontainers를 사용하여 실제 Kafka 컨테이너를 실행하는 통합 테스트 환경
- 테스트마다 격리된 Kafka 인스턴스를 제공
"""

import pytest
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="session")
def kafka_container():
    """
    세션 범위의 Kafka 컨테이너 픽스처
    - testcontainers가 Docker에서 실제 Kafka를 실행
    - 테스트 세션 동안 유지되며 종료 시 자동 정리
    - 실제 Kafka와 동일한 환경에서 통합 테스트 가능
    """
    # KafkaContainer는 confluentinc/cp-kafka 이미지 기반으로 동작
    # (apache/kafka 이미지는 내부 스크립트 구조가 달라 호환되지 않음)
    container = KafkaContainer("confluentinc/cp-kafka:7.6.0")
    container.start(timeout=120)

    # 부트스트랩 서버 주소를 반환
    yield container

    # 테스트 종료 후 컨테이너 정리
    container.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap_servers(kafka_container: KafkaContainer) -> str:
    """
    Kafka 컨테이너의 부트스트랩 서버 주소를 반환하는 픽스처
    """
    return kafka_container.get_bootstrap_server()
