"""
Pydantic 요청/응답 스키마.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class OrderItem(BaseModel):
    product_id: str
    quantity: int = Field(gt=0)
    price: Decimal = Field(gt=0)


class CreateOrderRequest(BaseModel):
    user_id: str
    items: list[OrderItem] = Field(min_length=1)


class OrderResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    order_id: UUID
    user_id: str
    items: list[dict[str, Any]]
    total_price: Decimal
    status: str
    created_at: datetime
    updated_at: datetime


class OutboxRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: UUID
    aggregate_type: str
    aggregate_id: str
    event_type: str
    topic: str
    created_at: datetime
    published_at: datetime | None
