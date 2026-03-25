"""
공유 이벤트 모델 정의
모든 서비스가 동일한 이벤트 스키마를 사용하도록 Pydantic 모델로 정의
"""

from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class OrderItem(BaseModel):
    """주문 항목"""
    product_id: str
    product_name: str
    quantity: int
    price: float


class OrderCreated(BaseModel):
    """주문 생성 이벤트 - order.created 토픽으로 발행"""
    event_type: str = "ORDER_CREATED"
    order_id: str
    user_id: str
    items: List[OrderItem]
    total_price: float
    timestamp: str = ""

    def __init__(self, **data):
        super().__init__(**data)
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()


class PaymentCompleted(BaseModel):
    """결제 성공 이벤트 - payment.result 토픽으로 발행"""
    event_type: str = "PAYMENT_COMPLETED"
    order_id: str
    amount: float
    timestamp: str = ""

    def __init__(self, **data):
        super().__init__(**data)
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()


class PaymentFailed(BaseModel):
    """결제 실패 이벤트 - payment.result 토픽으로 발행"""
    event_type: str = "PAYMENT_FAILED"
    order_id: str
    reason: str
    timestamp: str = ""

    def __init__(self, **data):
        super().__init__(**data)
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()


class InventoryReserved(BaseModel):
    """재고 예약 성공 이벤트 - inventory.result 토픽으로 발행"""
    event_type: str = "INVENTORY_RESERVED"
    order_id: str
    items: List[OrderItem]
    timestamp: str = ""

    def __init__(self, **data):
        super().__init__(**data)
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()


class InventoryFailed(BaseModel):
    """재고 부족 이벤트 - inventory.result 토픽으로 발행 (보상 트랜잭션 트리거)"""
    event_type: str = "INVENTORY_FAILED"
    order_id: str
    reason: str
    timestamp: str = ""

    def __init__(self, **data):
        super().__init__(**data)
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()


# ===== 보상 트랜잭션 (Saga Compensation) 이벤트 =====

class RefundRequested(BaseModel):
    """환불 요청 이벤트 - payment.refund-request 토픽으로 발행"""
    event_type: str = "REFUND_REQUESTED"
    order_id: str
    amount: float
    reason: str
    timestamp: str = ""

    def __init__(self, **data):
        super().__init__(**data)
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()


class RefundCompleted(BaseModel):
    """환불 완료 이벤트 - payment.refund-result 토픽으로 발행"""
    event_type: str = "REFUND_COMPLETED"
    order_id: str
    amount: float
    timestamp: str = ""

    def __init__(self, **data):
        super().__init__(**data)
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()


class RefundFailed(BaseModel):
    """환불 실패 이벤트 - payment.refund-result 토픽으로 발행"""
    event_type: str = "REFUND_FAILED"
    order_id: str
    reason: str
    timestamp: str = ""

    def __init__(self, **data):
        super().__init__(**data)
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()
