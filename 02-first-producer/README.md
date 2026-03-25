# 02. 첫 번째 Kafka Producer

## 1. 이 챕터에서 배우는 것

- FastAPI 애플리케이션에서 **AIOKafkaProducer**를 사용하여 Kafka로 메시지를 전송하는 방법
- FastAPI의 **lifespan** 컨텍스트 매니저를 활용한 리소스 관리
- 메시지의 **key**와 **value** 개념
- `send_and_wait()`와 `send()`의 차이

---

## 2. AIOKafkaProducer 개념 설명

`AIOKafkaProducer`는 Python 비동기 환경에서 Kafka로 메시지를 보내는 클라이언트입니다.

```
[ FastAPI App ] ---> [ AIOKafkaProducer ] ---> [ Kafka Broker ] ---> [ Topic/Partition ]
```

**주요 특징:**
- `asyncio` 기반이라 FastAPI와 자연스럽게 통합됩니다.
- 내부적으로 배치(batch) 전송을 지원하여 높은 처리량을 제공합니다.
- `key_serializer`와 `value_serializer`를 지정하여 데이터를 바이트로 변환합니다.

```python
producer = AIOKafkaProducer(
    bootstrap_servers="kafka:9092",
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: v.encode("utf-8"),
)
```

---

## 3. FastAPI lifespan 설명

`lifespan`은 FastAPI 0.93+에서 도입된 애플리케이션 생명주기 관리 방식입니다.
기존의 `@app.on_event("startup")` / `@app.on_event("shutdown")`을 대체합니다.

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # === startup ===
    # DB 연결, Kafka Producer 생성 등 초기화 작업
    producer = AIOKafkaProducer(...)
    await producer.start()
    app.state.producer = producer

    yield  # 이 시점에서 앱이 요청을 처리합니다

    # === shutdown ===
    # 리소스 정리
    await producer.stop()
```

**장점:**
- `yield` 하나로 시작/종료 로직을 한 곳에 모을 수 있어 가독성이 좋습니다.
- 예외 발생 시에도 `yield` 이후 코드가 실행되어 안전한 정리가 보장됩니다.

---

## 4. send_and_wait() vs send() 차이

| 메서드 | 동작 방식 | 반환값 | 사용 시나리오 |
|---|---|---|---|
| `send_and_wait()` | 브로커 ACK를 **기다림** | `RecordMetadata` | 메시지 전송 성공을 보장해야 할 때 |
| `send()` | ACK를 **기다리지 않음** | `asyncio.Future` | 높은 처리량이 필요하고 일부 유실을 허용할 때 |

```python
# send_and_wait() — 결과를 즉시 확인 (안전)
record = await producer.send_and_wait("my-topic", value="hello")
print(f"partition={record.partition}, offset={record.offset}")

# send() — Future를 반환 (빠름, 나중에 확인)
future = await producer.send("my-topic", value="hello")
record = await future  # 필요할 때 기다림
```

---

## 5. Message key와 value 설명

Kafka 메시지는 크게 **key**와 **value**로 구성됩니다.

### Value (메시지 본문)
- 실제 전달하려는 데이터입니다.
- 문자열, JSON, Avro 등 어떤 형식이든 바이트로 직렬화하여 전송합니다.

### Key (메시지 키)
- **같은 key를 가진 메시지는 항상 같은 파티션**으로 전송됩니다.
- 이를 통해 특정 데이터의 순서를 보장할 수 있습니다.

```
key=null  → 라운드 로빈으로 파티션에 분배
key="user-1" → hash("user-1") % 파티션 수 → 항상 같은 파티션
key="user-2" → hash("user-2") % 파티션 수 → 항상 같은 파티션
```

**예시:** 주문 시스템에서 `user_id`를 key로 사용하면, 같은 사용자의 주문은 항상 같은 파티션에 저장되어 순서가 보장됩니다.

---

## 6. 실행 방법

```bash
# 프로젝트 디렉토리로 이동
cd 02-first-producer

# 컨테이너 빌드 및 실행
docker compose up --build -d

# 로그 확인
docker compose logs -f app
```

서비스가 준비되면:
- **FastAPI**: http://localhost:8000
- **Swagger UI**: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8080

---

## 7. 실습 가이드

### 메시지 전송

```bash
# 기본 메시지 전송 (key 없이)
curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"topic": "test-topic", "value": "Hello, Kafka!"}'
```

응답 예시:
```json
{
  "topic": "test-topic",
  "partition": 0,
  "offset": 0
}
```

```bash
# key를 지정하여 전송 (같은 key → 같은 파티션)
curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"topic": "test-topic", "key": "user-1", "value": "주문 생성"}'

curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"topic": "test-topic", "key": "user-1", "value": "결제 완료"}'
```

### 헬스체크

```bash
curl http://localhost:8000/health
```

응답 예시:
```json
{
  "status": "healthy",
  "kafka_connected": true
}
```

---

## 8. kafka-ui에서 메시지 확인

1. 브라우저에서 http://localhost:8080 접속
2. 왼쪽 메뉴에서 **Topics** 클릭
3. `test-topic` 선택
4. **Messages** 탭에서 전송한 메시지 확인

확인할 수 있는 정보:
- **Offset**: 파티션 내 메시지 순번
- **Partition**: 메시지가 저장된 파티션 번호
- **Key**: 메시지 키
- **Value**: 메시지 본문
- **Timestamp**: 전송 시각

---

## 9. 핵심 코드 해설

### config.py — 설정 관리

```python
class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
```

- `pydantic-settings`의 `BaseSettings`를 상속하면 환경 변수를 자동으로 읽습니다.
- `KAFKA_BOOTSTRAP_SERVERS` 환경 변수가 있으면 그 값을, 없으면 기본값 `"kafka:9092"`를 사용합니다.

### main.py — Producer 생명주기

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(...)
    await producer.start()         # 시작 시 Kafka에 연결
    app.state.producer = producer  # 앱 상태에 저장
    yield
    await producer.stop()          # 종료 시 안전하게 닫기
```

- `app.state`에 저장하면 어느 라우터에서든 `app.state.producer`로 접근할 수 있습니다.

### main.py — 메시지 전송

```python
record = await producer.send_and_wait(
    topic=body.topic,
    key=body.key,
    value=body.value,
)
```

- `send_and_wait()`는 브로커의 ACK를 받을 때까지 대기합니다.
- 반환된 `RecordMetadata`에서 partition과 offset을 확인할 수 있습니다.

---

## 10. 다음 챕터 미리보기

**03장: 첫 번째 Consumer**에서는 Kafka에서 메시지를 읽어오는 Consumer를 구현합니다.

- `AIOKafkaConsumer` 사용법
- Consumer Group의 개념
- 오프셋 관리와 자동/수동 커밋
- WebSocket을 활용한 실시간 메시지 수신
