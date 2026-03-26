"""
쿼리 서비스 (Chapter 11 - CQRS 읽기 모델)

모든 이벤트 토픽을 구독하여 통합 읽기 뷰(Read View)를 구축하는 서비스

CQRS 패턴의 핵심:
- 명령(Command)과 조회(Query)를 완전히 분리
- 쓰기 모델: order-service (정규화된 데이터)
- 읽기 모델: query-service (비정규화된, 조회에 최적화된 뷰)
- 이벤트를 통해 읽기 모델을 비동기적으로 동기화 (최종 일관성)

이벤트 소싱 개념:
- 모든 상태 변경을 이벤트로 기록
- 이벤트를 리플레이하면 언제든 현재 상태를 재구성 가능
- 시간 여행 쿼리 (특정 시점의 상태 조회) 가능
"""

import asyncio
import logging
from fastapi import FastAPI, HTTPException, Query
from contextlib import asynccontextmanager
from typing import Optional

from .config import SERVICE_PORT
from .consumer import start_consumer, orders_read_model, event_log

# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [QUERY] %(levelname)s %(message)s",
)
logger = logging.getLogger("query-service")

consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 수명 주기 관리"""
    global consumer_task
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("쿼리 서비스 시작 (CQRS 읽기 모델, 포트: %d)", SERVICE_PORT)
    yield
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    logger.info("쿼리 서비스 종료")


app = FastAPI(
    title="쿼리 서비스 (CQRS 읽기 모델)",
    description="Chapter 11: 모든 이벤트를 집계하여 통합 읽기 뷰 제공",
    version="1.0.0",
    lifespan=lifespan,
)


# ──────────────────────────────────────────────
# 주문 조회 API (읽기 전용)
# ──────────────────────────────────────────────
@app.get("/orders")
async def query_orders(
    status: Optional[str] = Query(None, description="주문 상태 필터 (CREATED, COMPLETED, CANCELLED 등)"),
    user_id: Optional[str] = Query(None, description="사용자 ID 필터"),
):
    """
    주문 목록 조회 (필터링 지원)
    CQRS 읽기 모델에서 비정규화된 데이터를 직접 반환 (JOIN 불필요)
    """
    orders = list(orders_read_model.values())

    # 상태 필터 적용
    if status:
        orders = [o for o in orders if o.get("status", "").upper() == status.upper()]

    # 사용자 ID 필터 적용
    if user_id:
        orders = [o for o in orders if o.get("user_id") == user_id]

    # 최신순 정렬
    orders.sort(key=lambda o: o.get("created_at", ""), reverse=True)

    return {
        "total": len(orders),
        "filters": {"status": status, "user_id": user_id},
        "orders": [
            {
                "order_id": o["order_id"],
                "user_id": o.get("user_id", ""),
                "total_price": o.get("total_price", 0),
                "status": o.get("status", ""),
                "payment_status": o.get("payment_status", ""),
                "inventory_status": o.get("inventory_status", ""),
                "created_at": o.get("created_at", ""),
                "updated_at": o.get("updated_at", ""),
            }
            for o in orders
        ],
    }


@app.get("/orders/{order_id}")
async def get_order_detail(order_id: str):
    """
    주문 상세 조회 (이벤트 히스토리 포함)
    모든 서비스의 이벤트를 집계한 통합 뷰 제공
    - 주문 정보 + 결제 정보 + 재고 정보 + 이벤트 타임라인
    """
    if order_id not in orders_read_model:
        raise HTTPException(status_code=404, detail="주문을 찾을 수 없습니다")

    order_view = orders_read_model[order_id]

    # 해당 주문의 전체 이벤트 로그 추출
    order_events = [
        e for e in event_log if e.get("order_id") == order_id
    ]

    return {
        "order": order_view,
        "event_timeline": order_events,
        "total_events": len(order_events),
    }


# ──────────────────────────────────────────────
# 이벤트 로그 조회 (이벤트 소싱 디버깅용)
# ──────────────────────────────────────────────
@app.get("/events")
async def list_events(limit: int = Query(50, description="최근 이벤트 개수")):
    """전체 이벤트 로그 조회 (최신순, 디버깅용)"""
    recent_events = event_log[-limit:] if limit < len(event_log) else event_log
    return {
        "total_events": len(event_log),
        "showing": len(recent_events),
        "events": list(reversed(recent_events)),
    }


# ──────────────────────────────────────────────
# 통계 API
# ──────────────────────────────────────────────
@app.get("/stats")
async def get_stats():
    """주문 통계 - 읽기 모델 기반 실시간 집계"""
    orders = list(orders_read_model.values())
    total = len(orders)

    # 상태별 카운트
    status_counts = {}
    for o in orders:
        status = o.get("status", "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1

    # 총 매출 (COMPLETED 주문만)
    total_revenue = sum(
        o.get("total_price", 0) for o in orders
        if o.get("status") == "COMPLETED"
    )

    return {
        "total_orders": total,
        "status_breakdown": status_counts,
        "total_revenue": total_revenue,
        "total_events_processed": len(event_log),
    }


@app.get("/health")
async def health_check():
    """헬스 체크"""
    return {
        "service": "query-service",
        "status": "healthy",
        "orders_in_read_model": len(orders_read_model),
        "total_events_processed": len(event_log),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
