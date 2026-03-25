# Chapter 03 - 첫 번째 Kafka Consumer

## 1. 이 챕터에서 배우는 것

이번 챕터에서는 **Kafka Consumer**를 구현합니다.

- AIOKafkaConsumer를 사용하여 비동기로 메시지를 수신하는 방법
- `asyncio.create_task()`로 FastAPI 안에서 백그라운드 태스크를 실행하는 방법
- Consumer Group, Offset, Auto Commit의 개념
- Producer가 보낸 메시지를 Consumer가 받아서 API로 조회하는 전체 흐름

### 아키텍처

```
[Client] → POST /messages → [Producer :8000] → [Kafka] → [Consumer :8001] → GET /messages
```

---

## 2. AIOKafkaConsumer 개념

`AIOKafkaConsumer`는 `aiokafka` 라이브러리가 제공하는 **비동기 Kafka Consumer**입니다.

```python
from aiokafka import AIOKafkaConsumer

consumer = AIOKafkaConsumer(
    "test-topic",                          # 구독할 토픽
    bootstrap_servers="kafka:9092",        # 브로커 주소
    group_id="test-group",                 # Consumer Group ID
    auto_offset_reset="earliest",          # 오프셋 초기화 정책
)

await consumer.start()

async for msg in consumer:
    print(f"수신: {msg.value}")

await consumer.stop()
```

**핵심 파라미터:**

| 파라미터 | 설명 |
|---------|------|
| `bootstrap_servers` | Kafka 브로커 주소 |
| `group_id` | Consumer Group ID — 같은 그룹의 Consumer끼리 파티션을 나눠 가짐 |
| `auto_offset_reset` | 저장된 오프셋이 없을 때 어디서부터 읽을지 결정 |
| `enable_auto_commit` | 오프셋 자동 커밋 여부 (기본값: True) |
| `value_deserializer` | 수신한 바이트를 원하는 타입으로 변환 |

---

## 3. asyncio.create_task()로 백그라운드 실행

FastAPI는 **하나의 이벤트 루프**에서 동작합니다. Consumer의 `async for` 루프는 무한히 실행되기 때문에, 이것을 메인 흐름에서 직접 호출하면 FastAPI가 요청을 처리할 수 없게 됩니다.

이 문제를 `asyncio.create_task()`로 해결합니다:

```python
import asyncio

async def _consume_loop():
    """무한 루프로 메시지를 수신"""
    async for msg in consumer:
        received_messages.append(msg)

# 백그라운드 태스크로 등록 — 메인 이벤트 루프를 블로킹하지 않음
task = asyncio.create_task(_consume_loop())
```

**동작 원리:**
1. `create_task()`는 코루틴을 이벤트 루프에 **스케줄링**만 하고, 즉시 반환합니다.
2. 이벤트 루프는 HTTP 요청 처리와 메시지 수신을 **번갈아가며** 실행합니다.
3. 앱 종료 시 `task.cancel()`로 태스크를 안전하게 취소합니다.

```
이벤트 루프
├── HTTP 요청 처리 (FastAPI)
└── 백그라운드 태스크 (Consumer consume 루프)
```

---

## 4. auto_offset_reset 설명

Consumer가 토픽을 처음 구독할 때(저장된 오프셋이 없을 때), **어디서부터 메시지를 읽을지** 결정하는 설정입니다.

### earliest vs latest

```
토픽의 메시지: [A] [B] [C] [D] [E]  ← 새 메시지
                ↑                     ↑
           earliest                latest
```

| 옵션 | 동작 | 사용 시나리오 |
|------|------|-------------|
| `earliest` | 토픽의 **처음부터** 모든 메시지를 읽음 | 과거 데이터도 처리해야 할 때, 개발/테스트 시 |
| `latest` | **지금부터** 새로 들어오는 메시지만 읽음 | 실시간 스트리밍, 과거 데이터가 불필요할 때 |

> **주의:** 이 설정은 **저장된 오프셋이 없을 때만** 적용됩니다. 이미 오프셋이 커밋되어 있으면 마지막 커밋 위치부터 이어서 읽습니다.

---

## 5. Offset과 자동 커밋

### Offset이란?

Kafka 파티션 내 각 메시지의 **고유 순서 번호**입니다. Consumer는 이 오프셋을 기준으로 "어디까지 읽었는지"를 추적합니다.

```
파티션 0:  [offset 0] [offset 1] [offset 2] [offset 3] [offset 4]
                                      ↑
                              현재 커밋된 오프셋
                              (여기까지 읽었음)
```

### 자동 커밋 (Auto Commit)

`AIOKafkaConsumer`는 기본적으로 **자동 커밋**이 활성화되어 있습니다:

- `enable_auto_commit=True` (기본값)
- `auto_commit_interval_ms=5000` (기본값: 5초마다 커밋)

**자동 커밋의 동작:**
1. Consumer가 메시지를 읽음
2. 5초마다 현재까지 읽은 오프셋을 Kafka에 커밋
3. Consumer가 재시작되면 마지막 커밋 위치부터 이어서 읽음

**장점:** 구현이 간단함
**단점:** 메시지 처리 중 장애가 발생하면, 커밋은 되었지만 처리되지 않은 메시지가 유실될 수 있음

> 수동 커밋은 이후 챕터에서 다룹니다.

---

## 6. 실행 방법

```bash
# 03-first-consumer 디렉토리로 이동
cd 03-first-consumer

# 모든 서비스 빌드 및 실행
docker compose up --build

# 백그라운드 실행
docker compose up --build -d

# 로그 확인
docker compose logs -f consumer

# 종료
docker compose down -v
```

실행 후 접속 가능한 서비스:
- **Producer API:** http://localhost:8000/docs
- **Consumer API:** http://localhost:8001/docs
- **Kafka UI:** http://localhost:8080

---

## 7. 실습: Producer로 메시지 보내고 → Consumer API에서 확인

### Step 1: 메시지 전송 (Producer)

```bash
# 메시지 전송
curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"topic": "test-topic", "key": "user-1", "value": "안녕하세요!"}'
```

응답:
```json
{
  "topic": "test-topic",
  "partition": 0,
  "offset": 0
}
```

### Step 2: 여러 메시지 전송

```bash
curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"topic": "test-topic", "key": "user-2", "value": "두 번째 메시지"}'

curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"topic": "test-topic", "value": "키 없는 메시지"}'
```

### Step 3: Consumer에서 수신된 메시지 확인

```bash
curl http://localhost:8001/messages
```

응답:
```json
{
  "count": 3,
  "messages": [
    {
      "topic": "test-topic",
      "partition": 0,
      "offset": 0,
      "key": "user-1",
      "value": "안녕하세요!",
      "timestamp": 1711234567890,
      "received_at": "2024-03-24T12:00:00+00:00"
    },
    ...
  ]
}
```

### Step 4: 헬스체크

```bash
# Producer 헬스체크
curl http://localhost:8000/health

# Consumer 헬스체크
curl http://localhost:8001/health
```

### Step 5: Kafka UI에서 확인

http://localhost:8080 에서 토픽, 메시지, Consumer Group 상태를 시각적으로 확인할 수 있습니다.

---

## 8. 핵심 코드 해설

### consumer.py — 메시지 수신 루프

```python
# 인메모리 리스트에 메시지 저장
received_messages: list[dict] = []

async def _consume_loop() -> None:
    """async for 구문으로 메시지를 하나씩 수신"""
    async for msg in _consumer:
        message_data = {
            "topic": msg.topic,
            "partition": msg.partition,
            "offset": msg.offset,
            "key": msg.key,
            "value": msg.value,
            "timestamp": msg.timestamp,
        }
        received_messages.append(message_data)
```

- `async for msg in _consumer`: 새 메시지가 올 때까지 대기하다가, 도착하면 하나씩 처리
- 인메모리 리스트이므로 앱 재시작 시 데이터가 사라짐 (학습용으로 충분)

### main.py — lifespan으로 Consumer 관리

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_consumer()   # 앱 시작 시 Consumer 백그라운드 실행
    yield
    await stop_consumer()    # 앱 종료 시 Consumer 정리
```

- `lifespan`은 FastAPI의 생명주기 관리 패턴
- `yield` 이전: 앱 시작 시 실행 (startup)
- `yield` 이후: 앱 종료 시 실행 (shutdown)

---

## 9. 다음 챕터 미리보기

**Chapter 04**에서는 다음 내용을 다룹니다:

- **JSON 직렬화/역직렬화** — 단순 문자열이 아닌 구조화된 데이터를 Kafka로 주고받기
- **Pydantic 모델을 Kafka 메시지로 변환**하는 패턴
- **여러 토픽 구독** 및 메시지 라우팅
