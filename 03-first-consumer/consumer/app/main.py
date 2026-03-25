"""
Consumer 메인 모듈
- FastAPI 앱을 생성하고, lifespan을 통해 Kafka Consumer를 관리합니다.
- asyncio.create_task()로 백그라운드에서 메시지를 수신합니다.
- GET /messages: 수신된 메시지 목록을 반환합니다.
- GET /health: 서버 상태를 확인합니다.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.consumer import get_received_messages, start_consumer, stop_consumer


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    앱 시작 시 Kafka Consumer를 백그라운드로 실행하고,
    앱 종료 시 Consumer를 안전하게 정리합니다.
    """
    # 시작: Consumer를 백그라운드 태스크로 등록
    await start_consumer()
    yield
    # 종료: Consumer 태스크 취소 및 연결 닫기
    await stop_consumer()


app = FastAPI(
    title="Kafka Consumer API",
    description="Kafka에서 메시지를 수신하는 Consumer API (Chapter 03)",
    lifespan=lifespan,
)


@app.get("/messages")
async def list_messages():
    """수신된 메시지 목록을 반환합니다."""
    messages = get_received_messages()
    return {
        "count": len(messages),
        "messages": messages,
    }


@app.get("/health")
async def health_check():
    """서버 상태를 확인합니다."""
    return {"status": "healthy"}
