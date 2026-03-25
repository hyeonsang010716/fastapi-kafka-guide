"""
Kafka 메시지 직렬화/역직렬화 모듈

Kafka는 내부적으로 bytes만 전송할 수 있다.
따라서 Python 객체를 bytes로 변환(직렬화)하고,
bytes를 다시 Python 객체로 복원(역직렬화)하는 과정이 필요하다.

직렬화 흐름:
  Pydantic 모델 -> dict -> JSON 문자열 -> bytes

역직렬화 흐름:
  bytes -> JSON 문자열 -> dict -> (선택적) Pydantic 모델
"""

import json
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


def _default_serializer(obj: Any) -> Any:
    """
    JSON 기본 직렬화가 지원하지 않는 타입 처리
    - datetime -> ISO 8601 문자열
    - Pydantic 모델 -> dict
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    raise TypeError(f"직렬화할 수 없는 타입: {type(obj)}")


def json_serializer(value: Any) -> bytes:
    """
    값 직렬화기: Python 객체 -> JSON bytes

    Pydantic 모델이 들어오면 model_dump()로 dict 변환 후 JSON 인코딩.
    일반 dict도 처리 가능.
    """
    if isinstance(value, BaseModel):
        # Pydantic 모델은 model_dump()로 dict 변환
        data = value.model_dump()
    elif isinstance(value, dict):
        data = value
    else:
        data = value

    # dict -> JSON 문자열 -> UTF-8 bytes
    return json.dumps(data, default=_default_serializer, ensure_ascii=False).encode("utf-8")


def json_deserializer(data: bytes) -> dict:
    """
    값 역직렬화기: JSON bytes -> Python dict

    Kafka에서 수신한 bytes를 JSON 파싱하여 dict로 변환.
    """
    if data is None:
        return {}
    return json.loads(data.decode("utf-8"))


def key_serializer(key: Any) -> Optional[bytes]:
    """
    키 직렬화기: 문자열 키 -> bytes

    Kafka 메시지 키는 파티셔닝에 사용됨.
    같은 키를 가진 메시지는 같은 파티션으로 전송됨.
    """
    if key is None:
        return None
    if isinstance(key, str):
        return key.encode("utf-8")
    return str(key).encode("utf-8")


def key_deserializer(data: bytes) -> Optional[str]:
    """
    키 역직렬화기: bytes -> 문자열 키
    """
    if data is None:
        return None
    return data.decode("utf-8")
