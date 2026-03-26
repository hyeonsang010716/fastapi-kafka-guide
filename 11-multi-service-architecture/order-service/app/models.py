"""
주문 서비스 데이터 모델
API 요청/응답에 사용되는 Pydantic 모델과 인메모리 저장소
"""

from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime
from enum import Enum


# ──────────────────────────────────────────────
# 주문 상태 열거형
# ──────────────────────────────────────────────
class OrderStatus(str, Enum):
    """주문의 라이프사이클 상태"""
    CREATED = "CREATED"             # 주문 생성됨
    PAYMENT_COMPLETED = "PAYMENT_COMPLETED"   # 결제 완료
    PAYMENT_FAILED = "PAYMENT_FAILED"         # 결제 실패
    INVENTORY_RESERVED = "INVENTORY_RESERVED" # 재고 예약 완료
    INVENTORY_FAILED = "INVENTORY_FAILED"     # 재고 부족
    COMPLETED = "COMPLETED"         # 모든 처리 완료
    CANCELLED = "CANCELLED"         # 주문 취소됨


# ──────────────────────────────────────────────
# API 요청 모델
# ──────────────────────────────────────────────
class OrderItemRequest(BaseModel):
    """주문 항목 요청"""
    product_id: str
    product_name: str
    quantity: int
    price: float


class CreateOrderRequest(BaseModel):
    """주문 생성 요청"""
    user_id: str
    items: List[OrderItemRequest]
    shipping_address: Optional[str] = None


# ──────────────────────────────────────────────
# 내부 주문 저장 모델
# ──────────────────────────────────────────────
class Order(BaseModel):
    """인메모리 주문 저장 모델"""
    order_id: str
    user_id: str
    items: List[OrderItemRequest]
    total_price: float
    status: OrderStatus = OrderStatus.CREATED
    shipping_address: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""
    events: List[str] = []  # 이 주문에 대해 발생한 이벤트 타입 목록

    def __init__(self, **data):
        super().__init__(**data)
        now = datetime.utcnow().isoformat()
        if not self.created_at:
            object.__setattr__(self, "created_at", now)
        if not self.updated_at:
            object.__setattr__(self, "updated_at", now)


# ──────────────────────────────────────────────
# 인메모리 주문 저장소
# ──────────────────────────────────────────────
orders_db: Dict[str, Order] = {}
