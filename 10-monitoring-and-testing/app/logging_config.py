"""
구조화된 로깅 설정 모듈
- structlog을 사용하여 JSON 형식의 구조화된 로그를 생성
- 모든 로그에 타임스탬프, 로그 레벨, 이벤트 정보 포함
"""

import logging
import sys

import structlog


def setup_logging(log_level: str = "INFO") -> None:
    """
    structlog 기반 구조화된 로깅을 설정하는 함수

    Args:
        log_level: 로그 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """

    # structlog 프로세서 체인 설정
    # 각 프로세서는 로그 이벤트를 순차적으로 처리한다
    shared_processors: list[structlog.types.Processor] = [
        # 로그 레벨 필터링
        structlog.stdlib.filter_by_level,
        # 로그 레벨 이름 추가 (예: "info", "error")
        structlog.stdlib.add_log_level,
        # 로거 이름 추가
        structlog.stdlib.add_logger_name,
        # ISO 형식 타임스탬프 추가
        structlog.processors.TimeStamper(fmt="iso"),
        # 스택 정보 포맷팅 (예외 발생 시)
        structlog.processors.StackInfoRenderer(),
        # 예외 정보를 보기 좋게 포맷팅
        structlog.processors.format_exc_info,
        # 유니코드 디코딩
        structlog.processors.UnicodeDecoder(),
    ]

    # structlog 전역 설정
    structlog.configure(
        processors=[
            *shared_processors,
            # 마지막 프로세서: JSON 형식으로 렌더링
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
        # 표준 라이브러리 로거 팩토리 사용
        logger_factory=structlog.stdlib.LoggerFactory(),
        # 바인딩된 로거 래퍼 클래스
        wrapper_class=structlog.stdlib.BoundLogger,
        # 캐싱 활성화 (성능 최적화)
        cache_logger_on_first_use=True,
    )

    # 표준 라이브러리 로깅 설정
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )

    # 외부 라이브러리의 과도한 로깅 억제
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    이름이 지정된 구조화된 로거를 반환하는 헬퍼 함수

    Args:
        name: 로거 이름 (보통 모듈명)

    Returns:
        구조화된 로거 인스턴스
    """
    return structlog.get_logger(name)
