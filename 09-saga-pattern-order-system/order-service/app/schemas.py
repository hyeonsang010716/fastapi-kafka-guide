"""
주문 서비스 API 요청/응답 스키마
"""

from pydantic import BaseModel
from typing import List


class OrderItemRequest(BaseModel):
    """주문 항목 요청 스키마"""
    product_id: str
    product_name: str
    quantity: int
    price: float


class CreateOrderRequest(BaseModel):
    """주문 생성 요청 스키마"""
    user_id: str
    items: List[OrderItemRequest]


class OrderResponse(BaseModel):
    """주문 응답 스키마"""
    order_id: str
    user_id: str
    items: list
    total_price: float
    status: str
    created_at: str
    updated_at: str
    status_history: list
