"""
흉내낸 외부 결제 게이트웨이.

GATEWAY_MODE 환경변수에 따라 다르게 동작한다:

    HEALTHY  — 항상 200 (지연만 GATEWAY_LATENCY_MS)
    SLOW     — 5초 지연 후 200 (클라이언트 timeout 을 시연)
    DEAD     — 항상 500 응답 (외부 서비스가 죽은 상황)
    FLAKY    — 50% 확률로 500 (간헐 장애)

런타임에 모드를 바꾸려면 POST /admin/mode 엔드포인트를 사용하거나
컨테이너의 GATEWAY_MODE 환경변수를 바꾸고 재시작.
"""

import asyncio
import logging
import random
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.config import GATEWAY_LATENCY_MS, GATEWAY_MODE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = structlog.get_logger()


# 학습용으로 런타임에 모드를 바꿀 수 있게 모듈 변수에 둔다.
state = {"mode": GATEWAY_MODE, "latency_ms": GATEWAY_LATENCY_MS}


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("gateway.start", mode=state["mode"])
    yield


app = FastAPI(
    title="Mock Payment Gateway",
    description="HEALTHY / SLOW / DEAD / FLAKY 모드를 흉내낸 외부 결제 게이트웨이",
    version="1.0.0",
    lifespan=lifespan,
)


class ChargeRequest(BaseModel):
    order_id: str
    amount: float


class ModeUpdate(BaseModel):
    mode: str  # HEALTHY | SLOW | DEAD | FLAKY
    latency_ms: int | None = None


@app.get("/health")
async def health():
    return {"status": "healthy", "mode": state["mode"]}


@app.get("/admin/mode")
async def get_mode():
    return state


@app.post("/admin/mode")
async def set_mode(update: ModeUpdate):
    if update.mode not in ("HEALTHY", "SLOW", "DEAD", "FLAKY"):
        raise HTTPException(status_code=400, detail="invalid mode")
    state["mode"] = update.mode
    if update.latency_ms is not None:
        state["latency_ms"] = update.latency_ms
    log.info("gateway.mode_changed", **state)
    return state


@app.post("/charge")
async def charge(req: ChargeRequest):
    mode = state["mode"]

    if mode == "DEAD":
        # 외부 서비스가 명백히 죽은 상황을 흉내냄.
        raise HTTPException(status_code=500, detail="gateway down")

    if mode == "SLOW":
        # 클라이언트의 timeout 을 시연하기 위해 일부러 길게 잠.
        await asyncio.sleep(5)

    if mode == "FLAKY" and random.random() < 0.5:
        raise HTTPException(status_code=500, detail="flaky failure")

    # 정상 경로 — 약간의 지연 후 성공 응답.
    await asyncio.sleep(state["latency_ms"] / 1000)
    return {
        "order_id": req.order_id,
        "amount": req.amount,
        "status": "APPROVED",
        "transaction_id": f"tx-{random.randint(100000, 999999)}",
    }
