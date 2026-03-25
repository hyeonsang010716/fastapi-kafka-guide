# 05 - Consumer Groups

## 1. 이 챕터에서 배우는 것

- **Consumer Group**이 무엇이고 왜 필요한지
- 파티션과 컨슈머 사이의 할당(Assignment) 관계
- 컨슈머가 추가/제거될 때 일어나는 **리밸런싱(Rebalancing)**
- **수동 오프셋 커밋** vs 자동 커밋의 차이
- 실제로 3개의 컨슈머가 3개 파티션을 나눠 가지는 모습 관찰

---

## 2. Consumer Group 개념

```
                         ┌──────────────────────────────┐
                         │       Kafka Broker           │
                         │                              │
                         │   Topic: "orders"            │
                         │   ┌──────────┐               │
    Producer ──────────▶ │   │Partition 0│──────▶ consumer-1 (port 8001)
         │               │   └──────────┘               │
         │               │   ┌──────────┐               │
         ├──────────────▶│   │Partition 1│──────▶ consumer-2 (port 8002)
         │               │   └──────────┘               │
         │               │   ┌──────────┐               │
         └──────────────▶│   │Partition 2│──────▶ consumer-3 (port 8003)
                         │   └──────────┘               │
                         └──────────────────────────────┘
                                    │
                                    ▼
                         group_id = "order-group"
                         (3 consumers, 3 partitions)
```

**핵심 규칙:**
- 같은 Consumer Group 내에서, **하나의 파티션은 오직 하나의 컨슈머만** 읽을 수 있다.
- 컨슈머 수가 파티션 수보다 많으면 → 일부 컨슈머는 **유휴(idle)** 상태가 된다.
- 컨슈머 수가 파티션 수보다 적으면 → 하나의 컨슈머가 **여러 파티션**을 담당한다.

---

## 3. 파티션-컨슈머 할당 관계

| 컨슈머 수 | 파티션 수 | 할당 결과 |
|-----------|----------|-----------|
| 3         | 3        | 각 컨슈머가 파티션 1개씩 담당 |
| 2         | 3        | 한 컨슈머가 2개, 다른 컨슈머가 1개 |
| 1         | 3        | 혼자서 3개 파티션 모두 담당 |
| 4         | 3        | 3개만 활성, 1개는 유휴 상태 |

```
컨슈머 3개일 때:                  컨슈머 2개일 때 (하나 중지):
┌──────────┐                  ┌──────────┐ 
│consumer-1│ ← P0             │consumer-1│ ← P0, P1
└──────────┘                  └──────────┘
┌──────────┐                  ┌──────────┐ 
│consumer-2│ ← P1             │consumer-2│ ← P2
└──────────┘                  └──────────┘
┌──────────┐ 
│consumer-3│ ← P2
└──────────┘
```

---

## 4. 리밸런싱 (Rebalancing)

Consumer Group에서 **컨슈머가 추가되거나 제거**되면 리밸런싱이 발생합니다.

**리밸런싱이 발생하는 경우:**
1. 새로운 컨슈머가 그룹에 참가 (join)
2. 기존 컨슈머가 종료 또는 장애로 이탈
3. 구독 토픽의 파티션 수가 변경됨

**리밸런싱 과정:**
```
1. 컨슈머 이탈 감지 (heartbeat 실패)
        ↓
2. Group Coordinator가 리밸런싱 트리거
        ↓
3. 모든 컨슈머가 파티션 소유권을 반납
        ↓
4. 파티션을 남아있는 컨슈머에게 재분배
        ↓
5. 각 컨슈머가 새로 할당받은 파티션에서 이어서 읽기
```

> **주의:** 리밸런싱 중에는 해당 그룹의 모든 컨슈머가 일시적으로 메시지를 읽을 수 없습니다.

---

## 5. 수동 오프셋 커밋 vs 자동 커밋

### 자동 커밋 (Auto Commit)
```python
# enable_auto_commit=True (기본값)
# auto_commit_interval_ms=5000 (5초마다 자동 커밋)
consumer = AIOKafkaConsumer(
    "orders",
    enable_auto_commit=True,
)
```
- 일정 간격으로 자동으로 오프셋을 저장
- 편리하지만, 메시지 처리 중 장애 시 **메시지 유실** 가능

### 수동 커밋 (Manual Commit) — 이 예제에서 사용
```python
# enable_auto_commit=False → 직접 commit() 호출 필요
consumer = AIOKafkaConsumer(
    "orders",
    enable_auto_commit=False,
)

async for msg in consumer:
    process(msg)             # 1. 메시지 처리
    await consumer.commit()  # 2. 처리 완료 후 커밋
```
- 메시지를 **확실히 처리한 후**에만 오프셋을 저장
- 장애 시 커밋 안 된 메시지는 재전달됨 → **at-least-once** 보장
- 대신 중복 처리 가능성이 있으므로 멱등성(idempotency) 고려 필요

---

## 6. 실행 방법

```bash
# 프로젝트 디렉토리로 이동
cd 05-consumer-groups

# 전체 서비스 시작 (Kafka + Producer + Consumer 3개)
docker compose up --build -d

# 로그 확인 (모든 컨슈머의 로그를 한눈에)
docker compose logs -f consumer-1 consumer-2 consumer-3
```

**서비스 포트 정보:**

| 서비스 | 포트 | 용도 |
|--------|------|------|
| Kafka | 9092 / 9094 | 브로커 |
| Kafka UI | 8080 | 웹 UI |
| Producer | 8000 | 주문 메시지 전송 |
| Consumer 1 | 8001 | 메시지 수신 |
| Consumer 2 | 8002 | 메시지 수신 |
| Consumer 3 | 8003 | 메시지 수신 |

---

## 7. 실습: 메시지 분배 확인

### Step 1: 대량 주문 전송
```bash
# 30개의 주문 메시지를 한꺼번에 전송
curl -X POST http://localhost:8000/bulk-orders \
  -H "Content-Type: application/json" \
  -d '{"count": 30}'
```

### Step 2: 각 Consumer의 상태 확인
```bash
# consumer-1 상태 확인
curl http://localhost:8001/status | python3 -m json.tool

# consumer-2 상태 확인
curl http://localhost:8002/status | python3 -m json.tool

# consumer-3 상태 확인
curl http://localhost:8003/status | python3 -m json.tool
```

**기대 결과:**
- 각 컨슈머가 서로 다른 파티션을 담당
- 메시지가 3개 컨슈머에 분배되어 `message_count`의 합이 30

### Step 3: 수신 메시지 상세 확인
```bash
# consumer-1이 받은 메시지들 (파티션 정보 포함)
curl http://localhost:8001/messages | python3 -m json.tool
```

---

## 8. 실습: 리밸런싱 관찰

### Step 1: Consumer 하나 중지
```bash
# consumer-3를 중지
docker compose stop consumer-3
```

### Step 2: 로그에서 리밸런싱 관찰
```bash
# 남은 컨슈머들의 로그 확인
docker compose logs -f consumer-1 consumer-2
```
> 리밸런싱이 발생하면서 consumer-3가 담당하던 파티션이 재분배됩니다.

### Step 3: 추가 메시지 전송 후 분배 확인
```bash
# 메시지 20개 추가 전송
curl -X POST http://localhost:8000/bulk-orders \
  -H "Content-Type: application/json" \
  -d '{"count": 20}'

# 상태 확인 — consumer-1 또는 consumer-2가 2개 파티션을 담당하는 것을 확인
curl http://localhost:8001/status | python3 -m json.tool
curl http://localhost:8002/status | python3 -m json.tool
```

### Step 4: Consumer 재시작
```bash
# consumer-3 다시 시작
docker compose start consumer-3

# 다시 리밸런싱이 일어나고 파티션이 재분배됨
curl http://localhost:8003/status | python3 -m json.tool
```

---

## 9. Kafka UI에서 Consumer Group 확인

1. 브라우저에서 http://localhost:8080 접속
2. 좌측 메뉴에서 **"Consumers"** 탭 클릭
3. **"order-group"** 클릭
4. 확인할 수 있는 정보:
   - 각 파티션별 할당된 컨슈머 ID
   - 현재 오프셋 (Current Offset)
   - 마지막 오프셋 (End Offset)
   - 지연(Lag) — 아직 처리하지 않은 메시지 수

---

## 10. 핵심 코드 해설

### Consumer 생성 (수동 커밋)
```python
# consumer/app/consumer.py

consumer = AIOKafkaConsumer(
    "orders",
    bootstrap_servers="kafka:9092",
    group_id="order-group",              # 같은 그룹 → 파티션 분배
    enable_auto_commit=False,            # 수동 커밋 활성화
    auto_offset_reset="earliest",        # 오프셋 없으면 처음부터
    value_deserializer=...,
    key_deserializer=...,
)
```

### 메시지 수신 및 수동 커밋
```python
async for msg in consumer:
    # 1. 어떤 컨슈머가 어떤 파티션에서 메시지를 받았는지 로깅
    logger.info(f"파티션={msg.partition} | 오프셋={msg.offset} | 키={msg.key}")

    # 2. 메시지 처리 (비즈니스 로직)
    received_messages.append(message_info)

    # 3. 처리 완료 후 수동 커밋
    await consumer.commit()
```

### Producer에서 키 기반 파티셔닝
```python
# producer/app/main.py

# order_id를 키로 사용 → 같은 키는 항상 같은 파티션으로
record = await producer.send_and_wait(
    topic="orders",
    key=order_id,          # 해시(key) % 파티션 수 → 파티션 결정
    value=order_data,
)
```

> **키 기반 파티셔닝:** `hash(key) % num_partitions` 공식으로 파티션이 결정됩니다.
> 따라서 같은 order_id를 가진 메시지는 항상 같은 파티션에 저장되고,
> 같은 컨슈머가 처리하게 됩니다.
