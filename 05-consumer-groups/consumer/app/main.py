"""
Consumer Group 학습용 Consumer 앱
- 백그라운드에서 Kafka 메시지를 수신합니다.
- GET /status: Consumer ID, 할당된 파티션, 수신 메시지 수 확인
- GET /messages: 수신한 메시지 목록 (파티션 정보 포함)
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.consumer import (
    consume_loop,
    get_messages,
    get_status,
    start_consumer,
    stop_consumer,
)

# 로깅 설정 — 어떤 컨슈머가 어떤 메시지를 받았는지 확인하기 위함
logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{settings.CONSUMER_ID}] %(message)s",
)
logger = logging.getLogger("consumer")


# ---------------------------------------------------------------------------
# Lifespan: Consumer 시작/종료 및 백그라운드 수신 루프 관리
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    1. Consumer를 시작합니다.
    2. 백그라운드 태스크로 메시지 수신 루프를 실행합니다.
    3. 앱 종료 시 태스크를 취소하고 Consumer를 닫습니다.
    """
    # ── 시작 ──
    consumer = await start_consumer()

    # 백그라운드에서 메시지 수신 루프 실행
    task = asyncio.create_task(consume_loop(consumer))

    yield

    # ── 종료 ──
    task.cancel()  # 수신 루프 중단
    try:
        await task
    except asyncio.CancelledError:
        pass
    await stop_consumer()


# ---------------------------------------------------------------------------
# FastAPI 앱 인스턴스
# ---------------------------------------------------------------------------
app = FastAPI(
    title=f"05-consumer-groups / {settings.CONSUMER_ID}",
    description="Consumer Group 학습용 Consumer",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# GET /status — Consumer 상태 확인
# ---------------------------------------------------------------------------
@app.get("/status")
async def status():
    """
    현재 Consumer의 상태를 반환합니다.
    - consumer_id: 이 컨슈머의 식별자
    - assigned_partitions: 현재 할당받은 파티션 번호 목록
    - message_count: 지금까지 수신한 메시지 수
    - started_at: Consumer 시작 시각
    """
    return get_status()


# ---------------------------------------------------------------------------
# GET /messages — 수신한 메시지 목록
# ---------------------------------------------------------------------------
@app.get("/messages")
async def messages():
    """
    이 Consumer가 수신한 모든 메시지를 반환합니다.
    각 메시지에는 어떤 파티션에서 왔는지, 오프셋 등의 정보가 포함됩니다.
    """
    return get_messages()
