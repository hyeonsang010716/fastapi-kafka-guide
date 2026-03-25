"""
FastAPI 메인 애플리케이션

Exactly-once & Idempotency 실습:
- POST /points: 포인트 적립 이벤트 전송
- POST /points/duplicate-test: 동일 이벤트 2회 전송으로 멱등성 확인
- GET /balances: 사용자별 포인트 잔액 조회
- GET /processed-keys: 처리된 멱등성 키 목록 조회
- GET /health: 헬스체크
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.consumer import balances, start_consumer
from app.idempotency import idempotency_store
from app.producer import point_producer
from app.schemas import (
    BalancesResponse,
    DuplicateTestResponse,
    PointEvent,
    PointEventResponse,
    ProcessedKeysResponse,
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 수명 주기 관리"""
    # ── 시작 ──
    await idempotency_store.connect()
    logger.info("Redis 멱등성 저장소 연결 완료")

    await point_producer.start()
    logger.info("Kafka 프로듀서 시작 완료")

    consumer_task = asyncio.create_task(start_consumer())
    logger.info("Kafka Consumer 백그라운드 태스크 시작")

    yield

    # ── 종료 ──
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    logger.info("Consumer 태스크 종료")

    await point_producer.stop()
    await idempotency_store.close()
    logger.info("모든 리소스 정리 완료")


app = FastAPI(
    title="08 - Exactly-Once & Idempotency",
    description="Kafka 멱등성 프로듀서 + Redis 기반 Consumer 멱등성 실습",
    version="1.0.0",
    lifespan=lifespan,
)


# ──────────────────────────────────────────────
# API 엔드포인트
# ──────────────────────────────────────────────


@app.post("/points", response_model=PointEventResponse)
async def send_point_event(event: PointEvent):
    """
    포인트 적립 이벤트를 Kafka로 전송

    - 멱등성 프로듀서가 브로커 레벨 중복을 방지
    - idempotency_key를 페이로드에 포함하여 Consumer 측 중복도 방지
    """
    await point_producer.send_point_event(
        user_id=event.user_id,
        points=event.points,
        idempotency_key=event.idempotency_key,
    )

    return PointEventResponse(
        status="sent",
        idempotency_key=event.idempotency_key,
        message=f"포인트 이벤트 전송 완료: {event.user_id}에게 {event.points}점",
    )


@app.post("/points/duplicate-test", response_model=DuplicateTestResponse)
async def duplicate_test(event: PointEvent):
    """
    동일한 이벤트를 2번 연속 전송하여 멱등성을 테스트

    - 같은 idempotency_key로 2번 전송
    - Consumer는 첫 번째만 처리하고 두 번째는 스킵
    - /balances에서 포인트가 1번만 적립되었는지 확인 가능
    """
    # 첫 번째 전송
    await point_producer.send_point_event(
        user_id=event.user_id,
        points=event.points,
        idempotency_key=event.idempotency_key,
    )
    logger.info("중복 테스트 - 1차 전송 완료: %s", event.idempotency_key)

    # 약간의 지연 후 동일 메시지 재전송
    await asyncio.sleep(0.5)

    # 두 번째 전송 (동일한 idempotency_key)
    await point_producer.send_point_event(
        user_id=event.user_id,
        points=event.points,
        idempotency_key=event.idempotency_key,
    )
    logger.info("중복 테스트 - 2차 전송 완료 (중복): %s", event.idempotency_key)

    return DuplicateTestResponse(
        status="duplicate_test_sent",
        idempotency_key=event.idempotency_key,
        sent_count=2,
        message=(
            f"동일 이벤트 2회 전송 완료. "
            f"/balances에서 {event.user_id}의 잔액이 {event.points}점인지 확인하세요. "
            f"(멱등성이 작동하면 {event.points}점, 실패하면 {event.points * 2}점)"
        ),
    )


@app.get("/balances", response_model=BalancesResponse)
async def get_balances():
    """
    사용자별 포인트 잔액 조회

    Consumer가 처리한 결과를 인메모리에서 조회합니다.
    중복 테스트 후 이 엔드포인트로 멱등성 작동 여부를 확인할 수 있습니다.
    """
    return BalancesResponse(balances=balances)


@app.get("/processed-keys", response_model=ProcessedKeysResponse)
async def get_processed_keys():
    """
    Redis에 저장된 처리 완료 멱등성 키 목록 조회

    어떤 이벤트가 처리되었는지 모니터링할 수 있습니다.
    """
    keys = await idempotency_store.get_all_keys()
    return ProcessedKeysResponse(keys=keys, count=len(keys))


@app.get("/health")
async def health():
    """헬스체크 엔드포인트"""
    return {
        "status": "healthy",
        "service": "08-exactly-once-and-idempotency",
    }
