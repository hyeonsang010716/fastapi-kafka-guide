"""
Pydantic 스키마 정의
- 결제 이벤트 요청/응답 모델
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class PaymentRequest(BaseModel):
    """결제 요청 스키마"""
    user_id: str = Field(..., description="사용자 ID")
    amount: float = Field(..., gt=0, description="결제 금액")
    currency: str = Field(default="KRW", description="통화 단위")
    description: str = Field(default="", description="결제 설명")


class PaymentEvent(BaseModel):
    """Kafka로 전송되는 결제 이벤트"""
    payment_id: str = Field(..., description="결제 고유 ID")
    user_id: str
    amount: float
    currency: str
    description: str
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())


class ProcessedPayment(BaseModel):
    """처리 완료된 결제 정보"""
    payment_id: str
    user_id: str
    amount: float
    currency: str
    status: str = "completed"
    processed_at: str = Field(default_factory=lambda: datetime.now().isoformat())


class FailedPayment(BaseModel):
    """DLQ에 저장된 실패 결제 정보"""
    payment_id: str
    user_id: str
    amount: float
    currency: str
    error: str
    retry_count: int
    failed_at: str = Field(default_factory=lambda: datetime.now().isoformat())
