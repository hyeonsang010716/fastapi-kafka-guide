# 06 - 메시지 직렬화 (Message Serialization)

## 1. 이 챕터에서 배우는 것

- Kafka가 bytes만 전송하는 이유와 직렬화의 필요성
- JSON 직렬화/역직렬화 구현 방법
- Pydantic 모델을 활용한 메시지 스키마 검증
- Kafka 메시지 헤더(Headers) 활용법
- Avro / Schema Registry 개념 소개

---

## 2. Kafka는 bytes만 전송한다

Kafka의 프로듀서와 컨슈머는 내부적으로 **bytes(바이트 배열)** 만 주고받습니다.

```
프로듀서 측: Python 객체 → bytes (직렬화)
브로커:      bytes 저장 및 전달
컨슈머 측: bytes → Python 객체 (역직렬화)
```

따라서 우리가 보내고 싶은 데이터(dict, Pydantic 모델 등)를 bytes로 변환하는 **직렬화(Serialization)** 과정이 반드시 필요합니다.

### 왜 bytes인가?

- **언어 중립적**: Java, Python, Go 등 어떤 언어든 bytes를 읽고 쓸 수 있음
- **효율적 저장**: 브로커는 메시지 내용을 해석하지 않고 그대로 저장
- **유연한 포맷**: JSON, Avro, Protobuf 등 원하는 포맷을 자유롭게 선택 가능

---

## 3. JSON 직렬화/역직렬화 과정

이 프로젝트에서 사용하는 직렬화 흐름:

### 직렬화 (Serializer) - 프로듀서 측

```
Pydantic 모델
    ↓ model_dump()
Python dict
    ↓ json.dumps()
JSON 문자열 (str)
    ↓ .encode("utf-8")
bytes
    ↓ Kafka 전송
```

### 역직렬화 (Deserializer) - 컨슈머 측

```
bytes (Kafka 수신)
    ↓ .decode("utf-8")
JSON 문자열 (str)
    ↓ json.loads()
Python dict
    ↓ (선택) Model.model_validate()
Pydantic 모델
```

### 핵심 코드 (`serializers.py`)

```python
def json_serializer(value):
    """Pydantic 모델 또는 dict → JSON bytes"""
    if isinstance(value, BaseModel):
        data = value.model_dump()
    else:
        data = value
    return json.dumps(data, default=_default_serializer, ensure_ascii=False).encode("utf-8")

def json_deserializer(data):
    """JSON bytes → dict"""
    return json.loads(data.decode("utf-8"))
```

---

## 4. Pydantic 모델 기반 스키마 검증

Pydantic을 사용하면 메시지의 구조를 명확하게 정의하고 검증할 수 있습니다.

### 이벤트 모델 정의 (`shared/events.py`)

```python
class UserCreatedEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id: str
    username: str
    email: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

class OrderPlacedEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    order_id: str
    user_id: str
    items: List[OrderItem]
    total_price: float
    created_at: datetime = Field(default_factory=datetime.utcnow)
```

### 장점

- **타입 안전성**: 잘못된 타입의 데이터가 Kafka로 전송되는 것을 방지
- **자동 검증**: 필수 필드 누락, 타입 불일치 등을 자동으로 감지
- **문서화**: 모델 정의 자체가 스키마 문서 역할
- **IDE 지원**: 자동완성, 타입 힌트 등 개발 편의성

---

## 5. 메시지 헤더(Headers) 활용법

Kafka 메시지에는 key, value 외에 **headers**를 추가할 수 있습니다.

### 헤더란?

- 메시지 본문(value)을 수정하지 않고 메타데이터를 전달하는 방법
- HTTP 헤더와 유사한 개념
- `List[Tuple[str, bytes]]` 형태

### 활용 예시

```python
headers = [
    ("event_type", b"user_created"),    # 이벤트 타입
    ("source", b"user-service"),        # 발신 서비스
    ("content_type", b"application/json"),  # 직렬화 포맷
]

await producer.send_and_wait(
    topic="user-events",
    key=user_id,
    value=event,
    headers=headers,
)
```

### 헤더의 장점

- **라우팅**: 컨슈머가 메시지 본문을 파싱하지 않고도 이벤트 타입 확인 가능
- **추적**: 메시지 출처, 처리 이력 등을 추적
- **호환성**: 본문 스키마를 변경하지 않고 메타데이터 추가 가능

---

## 6. Avro / Schema Registry 소개 (개념)

JSON 직렬화는 간편하지만, 프로덕션 환경에서는 한계가 있습니다.

### JSON의 한계

| 항목 | JSON | Avro |
|------|------|------|
| 크기 | 큼 (필드명 포함) | 작음 (스키마 별도 저장) |
| 스키마 검증 | 런타임에만 가능 | Schema Registry에서 강제 |
| 스키마 진화 | 관리 어려움 | 호환성 규칙 자동 적용 |
| 속도 | 느림 | 빠름 (바이너리 인코딩) |

### Schema Registry란?

```
프로듀서 → Schema Registry에 스키마 등록
         → 스키마 ID + 데이터를 Kafka로 전송

컨슈머 → Schema Registry에서 스키마 ID로 스키마 조회
       → 스키마에 따라 데이터 역직렬화
```

- **Confluent Schema Registry**: 가장 널리 사용되는 구현체
- **스키마 호환성 모드**: BACKWARD, FORWARD, FULL 등
- 이 프로젝트에서는 JSON 직렬화로 충분하지만, 대규모 시스템에서는 Avro + Schema Registry 조합을 권장

---

## 7. 실행 방법 및 실습

### Docker Compose로 실행

```bash
cd 06-message-serialization
docker-compose up --build
```

### 서비스 확인

| 서비스 | URL | 설명 |
|--------|-----|------|
| FastAPI | http://localhost:8000/docs | API 문서 (Swagger UI) |
| Kafka UI | http://localhost:8080 | 토픽/메시지 모니터링 |

### 실습 1: 사용자 생성 이벤트

```bash
curl -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-001",
    "username": "홍길동",
    "email": "hong@example.com"
  }'
```

### 실습 2: 주문 생성 이벤트

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "order-001",
    "user_id": "user-001",
    "items": [
      {"product_id": "prod-001", "product_name": "기계식 키보드", "quantity": 1, "price": 89000},
      {"product_id": "prod-002", "product_name": "마우스패드", "quantity": 2, "price": 15000}
    ]
  }'
```

### 실습 3: 수신된 이벤트 조회

```bash
# 전체 이벤트 조회
curl http://localhost:8000/events

# 특정 토픽 필터링
curl "http://localhost:8000/events?topic=user-events"

# 헬스체크
curl http://localhost:8000/health
```

### 실습 4: Kafka UI에서 메시지 확인

1. http://localhost:8080 접속
2. Topics → `user-events` 또는 `order-events` 클릭
3. Messages 탭에서 직렬화된 JSON 메시지 확인
4. Headers 영역에서 `event_type`, `source` 헤더 확인

---

## 8. 핵심 코드 해설

### 프로듀서 설정

```python
producer = AIOKafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=json_serializer,  # 값 직렬화기 지정
    key_serializer=key_serializer,     # 키 직렬화기 지정
)
```

- `value_serializer`: `send()` 호출 시 value를 자동으로 bytes로 변환
- `key_serializer`: key도 마찬가지로 자동 변환
- Pydantic 모델을 그대로 `value`에 전달해도 직렬화기가 처리

### 컨슈머 설정

```python
consumer = AIOKafkaConsumer(
    "user-events", "order-events",
    value_deserializer=json_deserializer,  # 값 역직렬화기 지정
    key_deserializer=key_deserializer,     # 키 역직렬화기 지정
)
```

- `value_deserializer`: 수신한 bytes를 자동으로 dict로 변환
- `msg.value`가 이미 dict 타입으로 사용 가능

### 메시지 흐름 전체 요약

```
[클라이언트] POST /users
    ↓
[FastAPI] CreateUserRequest 검증
    ↓
[이벤트 생성] UserCreatedEvent (Pydantic 모델)
    ↓
[직렬화] json_serializer → bytes
    ↓
[Kafka 브로커] bytes 저장
    ↓
[역직렬화] json_deserializer → dict
    ↓
[컨슈머] consumed_events 리스트에 저장
    ↓
[클라이언트] GET /events → 구조화된 이벤트 반환
```

### 프로젝트 구조

```
06-message-serialization/
├── README.md              # 이 문서
├── docker-compose.yml     # Kafka + UI + 앱 실행 환경
├── requirements.txt       # Python 의존성
├── Dockerfile             # 앱 컨테이너 이미지
├── app/
│   ├── __init__.py
│   ├── main.py            # FastAPI 앱 + 엔드포인트
│   ├── config.py          # 환경 설정
│   ├── schemas.py         # API 요청/응답 스키마
│   ├── serializers.py     # 직렬화/역직렬화 함수
│   └── consumer.py        # Kafka 컨슈머 (백그라운드)
└── shared/
    └── events.py          # 공유 이벤트 Pydantic 모델
```
