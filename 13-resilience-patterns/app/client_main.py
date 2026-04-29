"""
결제 클라이언트 서비스 (FastAPI).

POST /payments  — 게이트웨이를 호출해 결제를 시도. 보호막 3종이 자동 적용됨.
GET  /breaker   — circuit breaker 상태 들여다보기 (학습용)
GET  /health    — 헬스체크
"""

import logging
from contextlib import asynccontextmanager

import httpx
import structlog
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.circuit_breaker import CircuitBreakerOpenError
from app.gateway_client import (
    charge,
    gateway_breaker,
    start_http_client,
    stop_http_client,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_http_client()
    yield
    await stop_http_client()


app = FastAPI(
    title="Resilience Patterns — Payment Client",
    description="외부 결제 게이트웨이 호출에 timeout / retry / circuit breaker 적용",
    version="1.0.0",
    lifespan=lifespan,
)


class PaymentRequest(BaseModel):
    order_id: str = Field(min_length=1)
    amount: float = Field(gt=0)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/breaker")
async def breaker_state():
    """학습용. 운영에서는 Prometheus 메트릭으로 노출하는 게 일반적."""
    return gateway_breaker.snapshot()


@app.post("/payments")
async def create_payment(req: PaymentRequest):
    try:
        result = await charge(req.order_id, req.amount)
    except CircuitBreakerOpenError:
        # 회로가 열려 있으면 사용자에게 빠르게 503 으로 거절.
        # "지금 게이트웨이가 죽어 있다는 걸 우리가 이미 알고 있다" 라는 뜻.
        log.warning("payment.short_circuited", order_id=req.order_id)
        raise HTTPException(status_code=503, detail="payment gateway temporarily unavailable")
    except httpx.HTTPError as exc:
        log.error("payment.failed", order_id=req.order_id, error=str(exc))
        raise HTTPException(status_code=502, detail="payment gateway error")

    return {"order_id": req.order_id, "result": result}
