"""
Pydantic 스키마 모듈
- 주문 요청/응답 및 토픽 정보 스키마를 정의합니다.
- 파티션 정보를 포함한 응답 구조를 제공합니다.
"""

from pydantic import BaseModel, Field


# ──────────────────────────────────────────────
# 주문 관련 스키마
# ──────────────────────────────────────────────

class OrderRequest(BaseModel):
    """주문 생성 요청 스키마"""

    # 주문 ID — 같은 order_id는 항상 같은 파티션으로 전송됨
    order_id: str = Field(
        ...,
        examples=["order-001"],
        description="주문 ID (메시지 키로 사용되어 파티션을 결정)",
    )

    # 상품명
    product: str = Field(
        ...,
        examples=["MacBook Pro"],
        description="상품명",
    )

    # 수량
    quantity: int = Field(
        default=1,
        ge=1,
        examples=[2],
        description="주문 수량",
    )

    # 가격
    price: float = Field(
        ...,
        gt=0,
        examples=[2500000],
        description="상품 가격",
    )


class OrderResponse(BaseModel):
    """주문 전송 응답 스키마 — 어떤 파티션에 저장되었는지 확인 가능"""

    status: str = "success"
    order_id: str
    topic: str
    partition: int       # 메시지가 저장된 파티션 번호
    offset: int          # 파티션 내 오프셋 (순서 번호)


# ──────────────────────────────────────────────
# 토픽/파티션 정보 스키마
# ──────────────────────────────────────────────

class PartitionInfo(BaseModel):
    """개별 파티션 정보"""

    partition_id: int
    leader: int          # 리더 브로커 ID
    replicas: list[int]  # 레플리카가 있는 브로커 목록
    isr: list[int]       # In-Sync Replica 목록


class TopicInfo(BaseModel):
    """토픽 상세 정보"""

    topic: str
    num_partitions: int
    partitions: list[PartitionInfo]


# ──────────────────────────────────────────────
# 컨슈머 메시지 스키마
# ──────────────────────────────────────────────

class ConsumedMessage(BaseModel):
    """소비된 메시지 정보 — 파티션/오프셋 포함"""

    topic: str
    partition: int
    offset: int
    key: str | None
    value: dict


# ──────────────────────────────────────────────
# 헬스체크 스키마
# ──────────────────────────────────────────────

class HealthResponse(BaseModel):
    """헬스체크 응답 스키마"""

    status: str
    kafka_connected: bool
