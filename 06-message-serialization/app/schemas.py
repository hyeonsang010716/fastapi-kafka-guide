"""
API 요청/응답 스키마 정의
- FastAPI 엔드포인트에서 사용하는 Pydantic 모델
"""

from typing import List, Optional

from pydantic import BaseModel, Field


class CreateUserRequest(BaseModel):
    """사용자 생성 요청"""

    user_id: str = Field(..., description="사용자 고유 ID", examples=["user-001"])
    username: str = Field(..., description="사용자 이름", examples=["홍길동"])
    email: str = Field(..., description="이메일 주소", examples=["hong@example.com"])


class OrderItemRequest(BaseModel):
    """주문 항목 요청"""

    product_id: str = Field(..., description="상품 ID", examples=["prod-001"])
    product_name: str = Field(..., description="상품명", examples=["키보드"])
    quantity: int = Field(..., ge=1, description="수량", examples=[2])
    price: float = Field(..., ge=0, description="단가", examples=[55000])


class CreateOrderRequest(BaseModel):
    """주문 생성 요청"""

    order_id: str = Field(..., description="주문 고유 ID", examples=["order-001"])
    user_id: str = Field(..., description="주문자 ID", examples=["user-001"])
    items: List[OrderItemRequest] = Field(..., description="주문 항목 목록")


class EventResponse(BaseModel):
    """이벤트 응답"""

    status: str
    event_id: str
    topic: str
    message: str


class ConsumedEvent(BaseModel):
    """컨슈머가 수신한 이벤트"""

    topic: str
    partition: int
    offset: int
    key: Optional[str] = None
    headers: Optional[dict] = None
    value: dict
