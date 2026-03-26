"""
Kafka 컨슈머 모듈
- 백그라운드에서 메시지를 소비하고 저장
- 소비된 메시지 수 카운터를 통한 메트릭 수집
"""

import asyncio
import json

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.logging_config import get_logger

logger = get_logger(__name__)


class KafkaConsumerService:
    """Kafka 컨슈머 서비스 클래스"""

    def __init__(self) -> None:
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None
        # 소비된 메시지 저장소 (최근 100개)
        self.messages: list[dict] = []
        # 소비된 메시지 수 카운터 (메트릭용)
        self.consumed_count: int = 0
        # 컨슈머 실행 상태
        self.is_running: bool = False

    async def start(self) -> None:
        """컨슈머를 시작하고 백그라운드 소비 태스크 생성"""
        self._consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await self._consumer.start()
        self.is_running = True

        # 백그라운드 소비 태스크 시작
        self._task = asyncio.create_task(self._consume_loop())
        logger.info(
            "kafka_consumer_started",
            topic=settings.kafka_topic,
            group_id=settings.kafka_consumer_group,
        )

    async def stop(self) -> None:
        """컨슈머를 안전하게 종료"""
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()
            logger.info("kafka_consumer_stopped", total_consumed=self.consumed_count)

    async def _consume_loop(self) -> None:
        """
        백그라운드에서 메시지를 지속적으로 소비하는 루프
        - 최근 100개 메시지만 메모리에 유지
        - 각 메시지 소비 시 구조화된 로그 기록
        """
        try:
            async for msg in self._consumer:
                # 메시지 파싱 및 메타데이터 추가
                consumed = {
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": msg.key.decode("utf-8") if msg.key else None,
                    "value": msg.value,
                }

                # 최근 100개만 유지 (메모리 보호)
                self.messages.append(consumed)
                if len(self.messages) > 100:
                    self.messages = self.messages[-100:]

                self.consumed_count += 1

                # 소비 성공 로깅
                logger.info(
                    "message_consumed",
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    consumed_count=self.consumed_count,
                )
        except asyncio.CancelledError:
            # 정상적인 종료 시 발생
            logger.info("consumer_loop_cancelled")
        except Exception as e:
            # 예상치 못한 에러 로깅
            logger.error("consumer_loop_error", error=str(e), error_type=type(e).__name__)
            self.is_running = False


# 싱글톤 컨슈머 인스턴스
consumer_service = KafkaConsumerService()
