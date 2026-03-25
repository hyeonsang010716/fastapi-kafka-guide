"""
인메모리 주문 저장소
실제 프로덕션에서는 데이터베이스를 사용하지만, 학습 목적으로 딕셔너리 사용

주문 상태 흐름:
CREATED → PAYMENT_PROCESSING → PAYMENT_FAILED (실패 시)
                              → INVENTORY_PROCESSING (성공 시)
                                → COMPLETED (재고 확보 성공)
                                → COMPENSATING (재고 부족 → 환불 요청)
                                  → COMPENSATED (환불 성공)
                                  → COMPENSATION_FAILED (환불 실패 → 수동 개입 필요)
"""

from enum import Enum
from typing import Dict, Any
from datetime import datetime


class OrderStatus(str, Enum):
    """주문 상태 열거형"""
    CREATED = "CREATED"                         # 주문 생성됨
    PAYMENT_PROCESSING = "PAYMENT_PROCESSING"   # 결제 처리 중
    PAYMENT_FAILED = "PAYMENT_FAILED"           # 결제 실패
    INVENTORY_PROCESSING = "INVENTORY_PROCESSING"  # 재고 확인 중
    COMPLETED = "COMPLETED"                     # 주문 완료
    FAILED = "FAILED"                           # 주문 실패 (재고 부족 등)
    COMPENSATING = "COMPENSATING"               # 보상 트랜잭션 진행 중 (환불 요청됨)
    COMPENSATED = "COMPENSATED"                 # 보상 완료 (환불 성공)
    COMPENSATION_FAILED = "COMPENSATION_FAILED" # 보상 실패 (환불 실패 → 수동 개입 필요)


# 인메모리 주문 저장소 (order_id -> 주문 정보)
orders_db: Dict[str, Dict[str, Any]] = {}


def create_order(order_id: str, user_id: str, items: list, total_price: float) -> dict:
    """새 주문을 생성하고 저장소에 추가"""
    order = {
        "order_id": order_id,
        "user_id": user_id,
        "items": items,
        "total_price": total_price,
        "status": OrderStatus.CREATED,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "status_history": [
            {"status": OrderStatus.CREATED, "timestamp": datetime.utcnow().isoformat()}
        ],
    }
    orders_db[order_id] = order
    return order


def update_order_status(order_id: str, status: OrderStatus, reason: str = "") -> dict | None:
    """주문 상태를 업데이트하고 이력을 기록"""
    if order_id not in orders_db:
        return None
    orders_db[order_id]["status"] = status
    orders_db[order_id]["updated_at"] = datetime.utcnow().isoformat()
    history_entry = {"status": status, "timestamp": datetime.utcnow().isoformat()}
    if reason:
        history_entry["reason"] = reason
    orders_db[order_id]["status_history"].append(history_entry)
    return orders_db[order_id]


def get_order(order_id: str) -> dict | None:
    """주문 ID로 주문 조회"""
    return orders_db.get(order_id)


def get_all_orders() -> list:
    """모든 주문 목록 반환"""
    return list(orders_db.values())
