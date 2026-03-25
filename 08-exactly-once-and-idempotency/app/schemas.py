"""
요청/응답 스키마 정의
"""

from pydantic import BaseModel, Field


class PointEvent(BaseModel):
    """포인트 적립 이벤트"""
    user_id: str = Field(..., description="사용자 ID")
    points: int = Field(..., gt=0, description="적립 포인트 (양수)")
    idempotency_key: str = Field(..., description="멱등성 키 (중복 방지용 고유 값)")


class PointEventResponse(BaseModel):
    """포인트 이벤트 전송 응답"""
    status: str
    idempotency_key: str
    message: str


class DuplicateTestResponse(BaseModel):
    """중복 테스트 응답"""
    status: str
    idempotency_key: str
    sent_count: int
    message: str


class BalancesResponse(BaseModel):
    """잔액 조회 응답"""
    balances: dict[str, int]


class ProcessedKeysResponse(BaseModel):
    """처리된 키 목록 응답"""
    keys: list[str]
    count: int
