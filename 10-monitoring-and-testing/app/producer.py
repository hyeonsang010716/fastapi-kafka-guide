"""
Kafka 프로듀서 모듈
- 메시지 전송 및 전송 결과 로깅
- 전송 카운터를 통한 메트릭 수집
"""

import json
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

from app.config import settings
from app.logging_config import get_logger

logger = get_logger(__name__)


class KafkaProducerService:
    """Kafka 프로듀서 서비스 클래스"""

    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None
        # 전송된 메시지 수 카운터 (메트릭용)
        self.sent_count: int = 0

    async def start(self) -> None:
        """프로듀서를 시작하고 Kafka 브로커에 연결"""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await self._producer.start()
        logger.info("kafka_producer_started", bootstrap_servers=settings.kafka_bootstrap_servers)

    async def stop(self) -> None:
        """프로듀서를 안전하게 종료"""
        if self._producer:
            await self._producer.stop()
            logger.info("kafka_producer_stopped", total_sent=self.sent_count)

    async def send_message(
        self, topic: str, message: dict, key: str | None = None
    ) -> dict:
        """
        Kafka 토픽에 메시지를 전송

        Args:
            topic: 대상 토픽 이름
            message: 전송할 메시지 딕셔너리
            key: 메시지 키 (파티션 결정에 사용)

        Returns:
            전송 결과 정보 딕셔너리
        """
        if not self._producer:
            raise RuntimeError("프로듀서가 시작되지 않았습니다")

        # 메시지에 메타데이터 추가
        enriched_message = {
            **message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": settings.app_name,
        }

        # 메시지 전송
        result = await self._producer.send_and_wait(
            topic, value=enriched_message, key=key
        )

        self.sent_count += 1

        # 전송 성공 로깅 (구조화된 로그)
        logger.info(
            "message_sent",
            topic=topic,
            partition=result.partition,
            offset=result.offset,
            key=key,
            sent_count=self.sent_count,
        )

        return {
            "status": "sent",
            "topic": result.topic,
            "partition": result.partition,
            "offset": result.offset,
            "timestamp": enriched_message["timestamp"],
        }


# 싱글톤 프로듀서 인스턴스
producer_service = KafkaProducerService()
