# Chapter 10: 모니터링과 테스트

## 1. 이 챕터에서 배우는 것

이 챕터에서는 Kafka 기반 애플리케이션의 **운영 안정성**을 위한 핵심 기술을 학습합니다:

- **Consumer Lag 모니터링**: 메시지 처리 지연을 감지하는 방법
- **헬스체크 패턴**: 브로커 연결 상태와 클러스터 정보를 실시간으로 확인
- **구조화된 로깅 (structlog)**: JSON 형식의 검색 가능한 로그 생성
- **통합 테스트 (testcontainers)**: 실제 Kafka 환경에서의 자동화된 테스트

---

## 2. Consumer Lag 모니터링

### Consumer Lag이란?

Consumer Lag은 **토픽의 최신 메시지 오프셋**과 **컨슈머 그룹이 마지막으로 커밋한 오프셋**의 차이입니다.

```
Consumer Lag = End Offset - Committed Offset
```

| 상태 | Lag 값 | 의미 |
|------|--------|------|
| 정상 | 0 ~ 소량 | 컨슈머가 메시지를 잘 따라가고 있음 |
| 경고 | 지속 증가 | 컨슈머 처리 속도가 생산 속도보다 느림 |
| 위험 | 급격히 증가 | 컨슈머 장애 또는 심각한 성능 문제 |

### 모니터링이 중요한 이유

- **메시지 유실 방지**: 랙이 너무 커지면 오래된 메시지가 retention 정책에 의해 삭제될 수 있음
- **실시간성 보장**: 랙이 크면 사용자에게 보여지는 데이터가 오래된 것
- **스케일링 판단**: 랙이 지속적으로 증가하면 컨슈머 인스턴스를 늘려야 함

---

## 3. 헬스체크 패턴

### 헬스체크란?

애플리케이션이 정상적으로 동작하는지 외부에서 확인할 수 있는 엔드포인트입니다. 쿠버네티스의 `livenessProbe`와 `readinessProbe`에서 활용됩니다.

### 이 프로젝트의 헬스체크 구조

```
GET /health          → 전체 상태 요약 (브로커 연결 + 컨슈머 상태)
GET /health/kafka    → Kafka 전용 상세 정보 (클러스터 + 랙)
GET /metrics         → 메트릭 데이터 (카운터 + 랙)
```

### KafkaHealthChecker 클래스

| 메서드 | 역할 |
|--------|------|
| `check_broker_connection()` | 브로커에 연결을 시도하여 가용성 확인 |
| `get_consumer_lag()` | 파티션별 컨슈머 랙 조회 |
| `get_cluster_info()` | AdminClient로 클러스터 메타데이터 조회 |

---

## 4. structlog 구조화된 로깅

### 왜 구조화된 로깅인가?

일반 텍스트 로그:
```
2024-01-01 12:00:00 INFO Message sent to topic orders, partition 0, offset 42
```

구조화된 JSON 로그:
```json
{
  "event": "message_sent",
  "topic": "orders",
  "partition": 0,
  "offset": 42,
  "timestamp": "2024-01-01T12:00:00Z",
  "level": "info"
}
```

### 구조화된 로깅의 장점

- **검색 용이**: Elasticsearch, CloudWatch 등에서 필드 기반 검색 가능
- **필터링**: `topic == "orders" AND partition == 0` 같은 조건 필터링
- **대시보드**: JSON 필드를 기반으로 Grafana 등에서 시각화 가능
- **알림**: 특정 조건 (예: error 레벨) 발생 시 자동 알림 설정 가능

### structlog 프로세서 체인

```python
[
    filter_by_level,      # 로그 레벨 필터링
    add_log_level,        # 레벨 이름 추가
    add_logger_name,      # 로거 이름 추가
    TimeStamper,          # ISO 타임스탬프 추가
    StackInfoRenderer,    # 스택 정보 (예외 시)
    format_exc_info,      # 예외 포맷팅
    UnicodeDecoder,       # 유니코드 처리
    JSONRenderer,         # 최종 JSON 변환
]
```

---

## 5. testcontainers 통합 테스트

### testcontainers란?

**testcontainers-python**은 Docker 컨테이너를 테스트 픽스처로 사용할 수 있게 해주는 라이브러리입니다. 실제 Kafka 브로커를 테스트 코드에서 자동으로 실행하고 정리합니다.

### 왜 Mock 대신 실제 Kafka를 사용하는가?

| 접근 방식 | 장점 | 단점 |
|-----------|------|------|
| Mock | 빠르고 가벼움 | 실제 동작과 차이 가능 |
| **testcontainers** | **실제 환경과 동일** | Docker 필요, 느림 |

### 테스트 구조

```python
@pytest.fixture(scope="session")
def kafka_container():
    """세션 동안 Kafka 컨테이너를 유지"""
    container = KafkaContainer("apache/kafka:3.8.0")
    container.start()
    yield container
    container.stop()
```

테스트 파일별 역할:

| 파일 | 테스트 내용 |
|------|------------|
| `test_producer.py` | 메시지 전송 성공, 올바른 토픽 도착 확인 |
| `test_consumer.py` | 메시지 소비, 프로듀서-컨슈머 E2E 검증 |

---

## 6. 실행 방법

### Docker Compose로 전체 실행

```bash
cd 10-monitoring-and-testing
docker compose up --build -d
```

실행되는 서비스:

| 서비스 | 포트 | 설명 |
|--------|------|------|
| Kafka | 9092 | KRaft 모드 브로커 |
| Kafka UI | 8080 | 웹 기반 모니터링 |
| FastAPI App | 8000 | 애플리케이션 |

### API 테스트

```bash
# 메시지 전송
curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"key": "user-1", "value": {"action": "login", "user": "홍길동"}}'

# 소비된 메시지 조회
curl http://localhost:8000/messages

# 헬스체크
curl http://localhost:8000/health

# Kafka 상세 헬스체크
curl http://localhost:8000/health/kafka

# 메트릭 조회
curl http://localhost:8000/metrics
```

### 종료

```bash
docker compose down -v
```

---

## 7. 테스트 실행 방법

### 사전 요구사항

- Docker가 실행 중이어야 합니다 (testcontainers가 Docker를 사용)
- Python 가상환경에 의존성 설치 필요

```bash
# 가상환경 생성 및 활성화
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt

# 테스트 실행
pytest tests/ -v

# 특정 테스트만 실행
pytest tests/test_producer.py -v
pytest tests/test_consumer.py -v
```

### 테스트 케이스 목록

| 테스트 | 파일 | 검증 내용 |
|--------|------|----------|
| `test_send_message` | test_producer.py | 메시지 전송 성공 및 결과(토픽, 파티션, 오프셋) 검증 |
| `test_message_arrives_in_correct_topic` | test_producer.py | 특정 토픽에 보낸 메시지가 해당 토픽에서 소비되는지 확인 |
| `test_consume_messages` | test_consumer.py | 컨슈머가 메시지를 정상적으로 소비하고 값이 올바른지 확인 |
| `test_consumer_receives_what_producer_sent` | test_consumer.py | 3개 메시지를 전송 후 순서와 내용이 일치하는지 E2E 검증 |

### 테스트 실행 예시

```bash
# 전체 테스트 실행
pytest tests/ -v

# 실행 결과 예시:
# tests/test_producer.py::test_send_message PASSED
# tests/test_producer.py::test_message_arrives_in_correct_topic PASSED
# tests/test_consumer.py::test_consume_messages PASSED
# tests/test_consumer.py::test_consumer_receives_what_producer_sent PASSED
```

### 테스트 실행 시 동작

1. testcontainers가 Docker에서 Kafka 컨테이너를 자동 실행
2. 각 테스트가 실제 Kafka에 메시지를 전송/소비
3. 테스트 완료 후 컨테이너 자동 정리

> **참고**: 첫 실행 시 Kafka Docker 이미지 다운로드로 시간이 걸릴 수 있습니다.

---

## 8. 핵심 코드 해설

### health.py - 브로커 연결 확인

```python
async def check_broker_connection(self) -> dict:
    """AdminClient를 사용하여 브로커 연결을 테스트"""
    try:
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
        )
        await admin_client.start()
        await admin_client.close()
        return {"connected": True, "broker": self.bootstrap_servers}
    except Exception as e:
        return {"connected": False, "broker": self.bootstrap_servers, "error": str(e)}
```

`AIOKafkaAdminClient`를 사용하여 브로커 연결을 테스트합니다. `start()`가 성공하면 브로커가 정상이라고 판단하고, 실패하면 에러 메시지와 함께 `connected: False`를 반환합니다. Consumer 대신 AdminClient를 사용하는 이유는, 임시 Consumer를 생성하면 내부 group coordinator 초기화 중에 `stop()`이 호출되어 `CancelledError`가 발생할 수 있기 때문입니다.

### health.py - Consumer Lag 계산

```python
# 각 파티션의 최신 오프셋 (토픽에 쌓인 마지막 메시지 위치)
end_offsets = await consumer.end_offsets([tp])

# 컨슈머 그룹이 처리 완료를 보고한 오프셋
committed = await consumer.committed(tp)

# 랙 = 아직 처리하지 못한 메시지 수
lag = end_offset - committed_offset
```

### logging_config.py - structlog 프로세서 체인

```python
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,       # 레벨 미만 로그 제거
        structlog.stdlib.add_log_level,          # "level": "info" 추가
        structlog.processors.TimeStamper(fmt="iso"),  # ISO 타임스탬프
        structlog.processors.JSONRenderer(),     # 최종 JSON 출력
    ],
)
```

프로세서는 파이프라인처럼 동작하며, 각 프로세서가 로그 이벤트 딕셔너리에 필드를 추가하거나 변환합니다.

### conftest.py - testcontainers 픽스처

```python
@pytest.fixture(scope="session")
def kafka_container():
    # KafkaContainer는 confluentinc/cp-kafka 이미지 기반으로 동작
    # (apache/kafka 이미지는 내부 스크립트 구조가 달라 호환되지 않음)
    container = KafkaContainer("confluentinc/cp-kafka:7.6.0")
    container.start(timeout=120)    # Docker에서 Kafka 컨테이너 실행
    yield container
    container.stop()                # 테스트 종료 후 자동 정리
```

`scope="session"`으로 설정하여 전체 테스트 세션 동안 하나의 Kafka 컨테이너를 재사용합니다. 테스트마다 새로 실행하면 너무 느려지기 때문입니다.

> **주의**: `testcontainers`의 `KafkaContainer`는 `confluentinc/cp-kafka` 이미지 전용입니다. `apache/kafka` 이미지는 내부 스크립트(`/etc/confluent/docker/configure` 등)와 로그 형식이 달라 호환되지 않습니다.
