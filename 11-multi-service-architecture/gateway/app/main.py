"""
API Gateway 서비스 (Chapter 11)

모든 클라이언트 요청의 단일 진입점 (Single Entry Point)
httpx를 사용하여 내부 마이크로서비스로 요청을 라우팅

주요 역할:
- 요청 라우팅: 경로 기반으로 적절한 서비스로 전달
- 헬스 체크 집계: 모든 서비스의 상태를 한번에 확인
- 에러 핸들링: 내부 서비스 장애 시 적절한 응답 반환
"""

import httpx
import asyncio
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

from .config import (
    ORDER_SERVICE_URL,
    NOTIFICATION_SERVICE_URL,
    QUERY_SERVICE_URL,
    INTERNAL_SERVICES,
    REQUEST_TIMEOUT,
    SERVICE_PORT,
)

# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GATEWAY] %(levelname)s %(message)s",
)
logger = logging.getLogger("gateway")

# ──────────────────────────────────────────────
# 비동기 HTTP 클라이언트 (전역 재사용)
# ──────────────────────────────────────────────
http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 수명 주기 관리 - httpx 클라이언트 생성/해제"""
    global http_client
    # 시작 시 HTTP 클라이언트 생성 (커넥션 풀 재사용으로 성능 향상)
    http_client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)
    logger.info("API Gateway 시작 - 포트 %d", SERVICE_PORT)
    logger.info("내부 서비스 목록: %s", list(INTERNAL_SERVICES.keys()))
    yield
    # 종료 시 HTTP 클라이언트 정리
    await http_client.aclose()
    logger.info("API Gateway 종료")


app = FastAPI(
    title="API Gateway - 멀티 서비스 아키텍처",
    description="Chapter 11: 모든 요청의 단일 진입점. 내부 마이크로서비스로 라우팅합니다.",
    version="1.0.0",
    lifespan=lifespan,
)


# ──────────────────────────────────────────────
# 내부 서비스로 요청 프록시하는 헬퍼 함수
# ──────────────────────────────────────────────
async def proxy_request(
    method: str,
    service_url: str,
    path: str,
    body: dict | None = None,
    params: dict | None = None,
) -> JSONResponse:
    """
    내부 서비스로 요청을 프록시
    - 서비스 장애 시 503 반환
    - 타임아웃 시 504 반환
    """
    url = f"{service_url}{path}"
    logger.info("프록시 요청: %s %s → %s", method, path, url)

    try:
        response = await http_client.request(
            method=method,
            url=url,
            json=body,
            params=params,
        )
        # 내부 서비스의 응답을 그대로 클라이언트에게 전달
        return JSONResponse(
            status_code=response.status_code,
            content=response.json(),
        )
    except httpx.ConnectError:
        logger.error("서비스 연결 실패: %s", url)
        raise HTTPException(status_code=503, detail=f"서비스 연결 불가: {url}")
    except httpx.TimeoutException:
        logger.error("서비스 응답 타임아웃: %s", url)
        raise HTTPException(status_code=504, detail=f"서비스 응답 타임아웃: {url}")
    except Exception as e:
        logger.error("프록시 에러: %s - %s", url, str(e))
        raise HTTPException(status_code=502, detail=f"프록시 에러: {str(e)}")


# ──────────────────────────────────────────────
# 주문 관련 라우트 → order-service
# ──────────────────────────────────────────────
@app.post("/api/orders")
async def create_order(request: Request):
    """주문 생성 - order-service로 프록시"""
    body = await request.json()
    return await proxy_request("POST", ORDER_SERVICE_URL, "/orders", body=body)


@app.get("/api/orders")
async def list_orders():
    """주문 목록 조회 - order-service로 프록시"""
    return await proxy_request("GET", ORDER_SERVICE_URL, "/orders")


@app.get("/api/orders/{order_id}")
async def get_order(order_id: str):
    """주문 상세 조회 - order-service로 프록시"""
    return await proxy_request("GET", ORDER_SERVICE_URL, f"/orders/{order_id}")


# ──────────────────────────────────────────────
# 알림 관련 라우트 → notification-service
# ──────────────────────────────────────────────
@app.get("/api/notifications")
async def list_notifications():
    """알림 목록 조회 - notification-service로 프록시"""
    return await proxy_request("GET", NOTIFICATION_SERVICE_URL, "/notifications")


# ──────────────────────────────────────────────
# 쿼리 관련 라우트 → query-service (CQRS 읽기 모델)
# ──────────────────────────────────────────────
@app.get("/api/query/orders")
async def query_orders(status: str | None = None, user_id: str | None = None):
    """주문 쿼리 (CQRS 읽기 모델) - query-service로 프록시"""
    params = {}
    if status:
        params["status"] = status
    if user_id:
        params["user_id"] = user_id
    return await proxy_request("GET", QUERY_SERVICE_URL, "/orders", params=params)


@app.get("/api/query/orders/{order_id}")
async def query_order_detail(order_id: str):
    """주문 상세 쿼리 (이벤트 집계) - query-service로 프록시"""
    return await proxy_request("GET", QUERY_SERVICE_URL, f"/orders/{order_id}")


# ──────────────────────────────────────────────
# 헬스 체크 - 모든 서비스 상태 집계
# ──────────────────────────────────────────────
@app.get("/health")
async def aggregated_health():
    """
    모든 내부 서비스의 헬스 체크를 동시에 수행하여 집계된 결과 반환
    하나라도 unhealthy이면 전체 상태도 degraded로 표시
    """
    results = {}

    async def check_service(name: str, url: str):
        """개별 서비스 헬스 체크"""
        try:
            response = await http_client.get(f"{url}/health", timeout=3.0)
            if response.status_code == 200:
                results[name] = {"status": "healthy", "details": response.json()}
            else:
                results[name] = {"status": "unhealthy", "status_code": response.status_code}
        except Exception as e:
            results[name] = {"status": "unhealthy", "error": str(e)}

    # 모든 서비스에 동시에 헬스 체크 요청 (asyncio.gather로 병렬 처리)
    tasks = [check_service(name, url) for name, url in INTERNAL_SERVICES.items()]
    await asyncio.gather(*tasks)

    # 전체 상태 판단: 모든 서비스가 healthy여야 overall도 healthy
    all_healthy = all(s["status"] == "healthy" for s in results.values())
    overall_status = "healthy" if all_healthy else "degraded"

    return {
        "service": "gateway",
        "status": overall_status,
        "services": results,
    }


# ──────────────────────────────────────────────
# 메인 실행
# ──────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
