"""
공유 이벤트 모델 정의
- Kafka 메시지의 스키마를 Pydantic 모델로 정의
- 프로듀서/컨슈머 모두 동일한 모델을 사용하여 일관성 보장
"""

from datetime import datetime
from typing import Any, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class UserCreatedEvent(BaseModel):
    """사용자 생성 이벤트"""

    event_id: str = Field(default_factory=lambda: str(uuid4()), description="이벤트 고유 ID")
    user_id: str = Field(..., description="사용자 고유 ID")
    username: str = Field(..., description="사용자 이름")
    email: str = Field(..., description="이메일 주소")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="생성 시각")


class OrderItem(BaseModel):
    """주문 항목"""

    product_id: str = Field(..., description="상품 ID")
    product_name: str = Field(..., description="상품명")
    quantity: int = Field(..., ge=1, description="수량")
    price: float = Field(..., ge=0, description="단가")


class OrderPlacedEvent(BaseModel):
    """주문 생성 이벤트"""

    event_id: str = Field(default_factory=lambda: str(uuid4()), description="이벤트 고유 ID")
    order_id: str = Field(..., description="주문 고유 ID")
    user_id: str = Field(..., description="주문자 ID")
    items: List[OrderItem] = Field(..., description="주문 항목 목록")
    total_price: float = Field(..., ge=0, description="총 주문 금액")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="생성 시각")


class BaseEvent(BaseModel):
    """
    범용 이벤트 래퍼
    - 모든 이벤트를 감싸는 공통 구조
    - event_type 필드로 이벤트 종류를 구분
    """

    event_type: str = Field(..., description="이벤트 타입 (예: user_created, order_placed)")
    event_id: str = Field(default_factory=lambda: str(uuid4()), description="이벤트 고유 ID")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="이벤트 발생 시각")
    payload: dict = Field(default_factory=dict, description="이벤트 본문 데이터")
