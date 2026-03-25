"""
Pydantic 스키마 모듈
- 요청/응답 데이터의 유효성을 검사하고, 직렬화/역직렬화를 처리합니다.
"""

from pydantic import BaseModel, Field


class MessageRequest(BaseModel):
    """메시지 전송 요청 스키마"""

    # 메시지를 보낼 Kafka 토픽 이름
    topic: str = Field(..., examples=["test-topic"], description="Kafka 토픽 이름")

    # 메시지 키 (같은 키를 가진 메시지는 같은 파티션으로 전송됨)
    key: str | None = Field(
        default=None,
        examples=["user-1"],
        description="메시지 키 (파티션 결정에 사용)",
    )

    # 메시지 본문
    value: str = Field(..., examples=["Hello, Kafka!"], description="메시지 내용")


class MessageResponse(BaseModel):
    """메시지 전송 응답 스키마"""

    # 메시지가 저장된 토픽
    topic: str

    # 메시지가 저장된 파티션 번호
    partition: int

    # 파티션 내 메시지 오프셋 (순서 번호)
    offset: int


class HealthResponse(BaseModel):
    """헬스체크 응답 스키마"""

    status: str
    kafka_connected: bool
