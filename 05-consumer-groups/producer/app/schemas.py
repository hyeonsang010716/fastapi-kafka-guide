"""
Pydantic 스키마 모듈
- 주문(Order) 관련 요청/응답 스키마를 정의합니다.
"""

from pydantic import BaseModel, Field


class OrderResponse(BaseModel):
    """단일 주문 전송 응답"""

    order_id: str
    topic: str
    partition: int
    offset: int


class BulkOrderRequest(BaseModel):
    """대량 주문 전송 요청"""

    # 한 번에 보낼 주문 수
    count: int = Field(
        default=10,
        ge=1,
        le=1000,
        description="전송할 주문 메시지 수",
        examples=[10],
    )


class BulkOrderResponse(BaseModel):
    """대량 주문 전송 응답"""

    total_sent: int
    orders: list[OrderResponse]


class HealthResponse(BaseModel):
    """헬스체크 응답"""

    status: str
    kafka_connected: bool
