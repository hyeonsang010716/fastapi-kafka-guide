"""
멱등성 Kafka 프로듀서

- enable_idempotence=True: 브로커 레벨 중복 방지 (PID + 시퀀스 번호)
- acks="all": 모든 ISR 복제본이 확인해야 전송 성공으로 처리
- 각 메시지에 idempotency_key를 포함하여 Consumer 측 중복 방지에도 활용
"""

import json
import logging

from aiokafka import AIOKafkaProducer

from app.config import settings

logger = logging.getLogger(__name__)


class PointProducer:
    """포인트 이벤트 프로듀서"""

    def __init__(self) -> None:
        self.producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """프로듀서 시작 - 멱등성 옵션 활성화"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            # ── 멱등성 프로듀서 핵심 설정 ──
            enable_idempotence=True,  # PID + 시퀀스 번호로 중복 전송 방지
            acks="all",               # 모든 ISR 복제본 확인 (데이터 유실 방지)
            # JSON 직렬화
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await self.producer.start()
        logger.info("멱등성 프로듀서 시작 (enable_idempotence=True, acks=all)")

    async def stop(self) -> None:
        """프로듀서 종료"""
        if self.producer:
            await self.producer.stop()
            logger.info("프로듀서 종료")

    async def send_point_event(
        self,
        user_id: str,
        points: int,
        idempotency_key: str,
    ) -> None:
        """
        포인트 적립 이벤트를 Kafka로 전송

        Args:
            user_id: 사용자 ID
            points: 적립 포인트
            idempotency_key: 멱등성 키 (Consumer 측 중복 방지용)
        """
        # 메시지 페이로드에 멱등성 키 포함
        payload = {
            "user_id": user_id,
            "points": points,
            "idempotency_key": idempotency_key,
        }

        # user_id를 Kafka 메시지 키로 사용 → 같은 유저의 이벤트는 같은 파티션으로
        await self.producer.send_and_wait(
            topic=settings.topic_name,
            key=user_id,
            value=payload,
        )
        logger.info(
            "포인트 이벤트 전송 완료: user=%s, points=%d, key=%s",
            user_id, points, idempotency_key,
        )


# 싱글톤 인스턴스
point_producer = PointProducer()
