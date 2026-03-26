"""
공유 이벤트 모델 정의 (Chapter 11 - 멀티 서비스 아키텍처)
모든 서비스가 동일한 이벤트 스키마를 사용하도록 Pydantic 모델로 정의
Chapter 09의 이벤트를 확장하여 NotificationSent 이벤트 추가
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


# ──────────────────────────────────────────────
# 이벤트 타입 열거형
# ──────────────────────────────────────────────
class EventType(str, Enum):
    """시스템 전체에서 사용되는 이벤트 타입"""
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    INVENTORY_RESERVED = "INVENTORY_RESERVED"
    INVENTORY_FAILED = "INVENTORY_FAILED"
    NOTIFICATION_SENT = "NOTIFICATION_SENT"


# ──────────────────────────────────────────────
# 토픽 이름 상수
# ──────────────────────────────────────────────
TOPIC_ORDER_CREATED = "order.created"
TOPIC_PAYMENT_RESULT = "payment.result"
TOPIC_INVENTORY_RESULT = "inventory.result"
TOPIC_NOTIFICATION = "notification.sent"


# ──────────────────────────────────────────────
# 기본 이벤트 모델
# ──────────────────────────────────────────────
class BaseEvent(BaseModel):
    """모든 이벤트의 공통 기반 모델"""
    event_type: str
    timestamp: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data):
        super().__init__(**data)
        if not self.timestamp:
            # UTC 타임스탬프 자동 생성
            object.__setattr__(self, "timestamp", datetime.utcnow().isoformat())


# ──────────────────────────────────────────────
# 주문 관련 이벤트
# ──────────────────────────────────────────────
class OrderItem(BaseModel):
    """주문 항목 - 상품 정보와 수량, 가격을 담는 모델"""
    product_id: str
    product_name: str
    quantity: int
    price: float


class OrderCreated(BaseEvent):
    """
    주문 생성 이벤트 - order.created 토픽으로 발행
    주문 서비스에서 새로운 주문이 생성될 때 발행됨
    결제 서비스, 재고 서비스, 알림 서비스, 쿼리 서비스가 소비
    """
    event_type: str = EventType.ORDER_CREATED
    order_id: str
    user_id: str
    items: List[OrderItem]
    total_price: float
    shipping_address: Optional[str] = None


class OrderCancelled(BaseEvent):
    """
    주문 취소 이벤트 - 보상 트랜잭션 시 발행
    결제 실패 또는 재고 부족 시 주문을 취소
    """
    event_type: str = EventType.ORDER_CANCELLED
    order_id: str
    reason: str


# ──────────────────────────────────────────────
# 결제 관련 이벤트
# ──────────────────────────────────────────────
class PaymentCompleted(BaseEvent):
    """
    결제 성공 이벤트 - payment.result 토픽으로 발행
    결제 서비스에서 결제가 성공적으로 완료될 때 발행
    """
    event_type: str = EventType.PAYMENT_COMPLETED
    order_id: str
    amount: float
    payment_method: str = "credit_card"
    transaction_id: Optional[str] = None


class PaymentFailed(BaseEvent):
    """
    결제 실패 이벤트 - payment.result 토픽으로 발행
    결제 서비스에서 결제가 실패할 때 발행 (보상 트랜잭션 트리거)
    """
    event_type: str = EventType.PAYMENT_FAILED
    order_id: str
    reason: str
    amount: float = 0.0


# ──────────────────────────────────────────────
# 재고 관련 이벤트
# ──────────────────────────────────────────────
class InventoryReserved(BaseEvent):
    """
    재고 예약 성공 이벤트 - inventory.result 토픽으로 발행
    재고 서비스에서 재고 예약이 성공할 때 발행
    """
    event_type: str = EventType.INVENTORY_RESERVED
    order_id: str
    items: List[OrderItem]
    warehouse_id: Optional[str] = None


class InventoryFailed(BaseEvent):
    """
    재고 부족 이벤트 - inventory.result 토픽으로 발행
    재고 서비스에서 재고가 부족할 때 발행 (보상 트랜잭션 트리거)
    """
    event_type: str = EventType.INVENTORY_FAILED
    order_id: str
    reason: str
    failed_items: List[str] = Field(default_factory=list)


# ──────────────────────────────────────────────
# 알림 관련 이벤트
# ──────────────────────────────────────────────
class NotificationSent(BaseEvent):
    """
    알림 발송 이벤트 - notification.sent 토픽으로 발행
    알림 서비스에서 사용자에게 알림을 보낸 후 발행
    """
    event_type: str = EventType.NOTIFICATION_SENT
    notification_id: str
    order_id: str
    user_id: Optional[str] = None
    channel: str = "console"          # console, email, sms, push 등
    message: str = ""
    status: str = "sent"              # sent, failed
