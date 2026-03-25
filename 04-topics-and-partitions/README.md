# Chapter 04 — 토픽과 파티션

## 1. 이 챕터에서 배우는 것

- **토픽(Topic)**: 메시지가 저장되는 논리적 채널
- **파티션(Partition)**: 토픽을 물리적으로 분할하는 단위
- **메시지 키(Message Key)**: 특정 파티션으로 메시지를 라우팅하는 기준
- **Replication Factor**: 데이터 안정성을 위한 복제 전략

이 챕터에서는 같은 `order_id`로 메시지를 보내면 항상 같은 파티션에 도착하는 것을 직접 확인합니다.

---

## 2. 파티션 = 순서 보장의 단위

Kafka에서 **순서가 보장되는 범위는 파티션 단위**입니다.

```
Topic: orders (파티션 3개)
┌─────────────────────────────────────────────────┐
│                                                 │
│  Partition 0:  [msg-A] → [msg-D] → [msg-G]      │  ← 이 안에서만 순서 보장
│                                                 │
│  Partition 1:  [msg-B] → [msg-E] → [msg-H]      │  ← 이 안에서만 순서 보장
│                                                 │
│  Partition 2:  [msg-C] → [msg-F] → [msg-I]      │  ← 이 안에서만 순서 보장
│                                                 │
└─────────────────────────────────────────────────┘

    ※ Partition 0의 msg-A와 Partition 1의 msg-B 사이에는
      순서 보장이 없습니다!
```

> 토픽 전체에서 완벽한 순서 보장이 필요하면 **파티션을 1개**로 설정해야 합니다.
> 그러나 이 경우 병렬 처리가 불가능합니다.

---

## 3. Message Key 기반 파티션 배정

프로듀서가 메시지를 보낼 때 **키(Key)**를 지정하면, Kafka는 키의 해시값을 기반으로 파티션을 결정합니다.

```
Producer가 메시지 전송:
  key = "order-001"

  hash("order-001") % 3(파티션 수) = 2
  → Partition 2로 전송

다시 전송:
  key = "order-001"

  hash("order-001") % 3 = 2   (해시값은 동일)
  → 다시 Partition 2로 전송!
```

**같은 키 → 같은 파티션 → 해당 키의 메시지 순서 보장**

이것이 Kafka에서 "특정 엔티티의 이벤트 순서를 보장"하는 핵심 패턴입니다.

| 키 설정 | 동작 |
|---------|------|
| 키 있음 | `hash(key) % partition_count`로 파티션 결정 |
| 키 없음 (null) | 라운드 로빈 또는 스티키 파티셔닝으로 분배 |

---

## 4. 파티션 수와 병렬 처리 관계

```
파티션 수 = 최대 병렬 컨슈머 수

Topic: orders (파티션 3개)          Consumer Group
┌──────────────┐
│ Partition 0  │  ─────────────→    Consumer A
│ Partition 1  │  ─────────────→    Consumer B
│ Partition 2  │  ─────────────→    Consumer C
└──────────────┘

※ 컨슈머가 4개면? → 1개는 놀게 됩니다 (파티션보다 많은 컨슈머는 의미 없음)
※ 컨슈머가 2개면? → 1개가 파티션 2개를 담당합니다
```

| 파티션 수 | 장점 | 단점 |
|-----------|------|------|
| 적음 (1~2) | 순서 보장 쉬움, 관리 간단 | 처리량 제한 |
| 많음 (6+) | 높은 병렬 처리, 높은 처리량 | 리밸런싱 오버헤드, 메모리 사용 증가 |

---

## 5. Replication Factor 개념 소개

```
Replication Factor = 2 (데이터를 2개 브로커에 복제)

Broker 1                    Broker 2
┌─────────────────┐        ┌─────────────────┐
│ Partition 0     │        │ Partition 0     │
│ (Leader)        │───────→│ (Follower)      │
│                 │        │                 │
│ Partition 1     │        │ Partition 1     │
│ (Follower)      │←───────│ (Leader)        │
└─────────────────┘        └─────────────────┘

※ Leader가 죽으면 Follower가 자동으로 Leader로 승격
```

- **Replication Factor**: 각 파티션이 몇 개의 브로커에 복제되는지
- **ISR (In-Sync Replica)**: Leader와 동기화된 복제본 목록
- 이 챕터에서는 브로커가 1개이므로 `replication-factor=1`로 설정합니다

> 프로덕션 환경에서는 최소 **replication-factor=3**을 권장합니다.

---

## 6. 실행 방법

```bash
# 1. 전체 서비스 시작 (Kafka + 토픽 생성 + 앱)
docker compose up --build -d

# 2. 토픽이 정상 생성되었는지 확인
docker logs kafka-init

# 3. 앱 헬스체크
curl http://localhost:8000/health

# 4. 토픽/파티션 정보 조회
curl http://localhost:8000/topic-info | python3 -m json.tool

# 5. 주문 메시지 전송 (파티션 번호가 응답에 포함됨)
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"order_id": "order-001", "product": "MacBook Pro", "quantity": 1, "price": 2500000}'

# 6. 소비된 메시지 확인 (파티션 정보 포함)
curl http://localhost:8000/messages | python3 -m json.tool

# 7. 종료
docker compose down -v
```

---

## 7. 실습: 같은 order_id로 여러 번 보내서 같은 파티션 확인

같은 `order_id`로 여러 번 전송하면 항상 **같은 파티션 번호**가 반환됩니다.

```bash
# 같은 order_id로 3번 전송
curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"order_id": "order-001", "product": "MacBook", "quantity": 1, "price": 2500000}' | python3 -m json.tool

curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"order_id": "order-001", "product": "AirPods", "quantity": 2, "price": 350000}' | python3 -m json.tool

curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"order_id": "order-001", "product": "iPad", "quantity": 1, "price": 1200000}' | python3 -m json.tool

# → 세 번 모두 같은 partition 번호가 나옵니다!

# 다른 order_id로 전송하면 다른 파티션에 갈 수 있습니다
curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"order_id": "order-999", "product": "iPhone", "quantity": 1, "price": 1500000}' | python3 -m json.tool

# → order-001과 다른 partition 번호가 나올 수 있습니다 (해시값에 따라)
```

---

## 8. kafka-ui에서 파티션별 메시지 분포 확인

1. 브라우저에서 [http://localhost:8080](http://localhost:8080) 접속
2. **Topics** → **orders** 클릭
3. **Messages** 탭에서 각 메시지의 파티션 번호 확인
4. **Partitions** 탭에서 파티션별 오프셋(메시지 수) 분포 확인

```
확인 포인트:
✓ 같은 key를 가진 메시지는 같은 파티션에 있는지
✓ 서로 다른 key의 메시지가 여러 파티션에 분산되는지
✓ orders(3개), logs(1개), events(6개) 파티션 수가 다른지
```

---

## 9. 핵심 코드 해설

### 프로듀서 — 키 기반 파티션 라우팅

```python
# order_id를 키로 사용 → 같은 키는 항상 같은 파티션으로 라우팅됨
result = await producer.send_and_wait(
    topic="orders",
    key=order.order_id,      # ← 이 키의 해시값으로 파티션 결정
    value=value,
)
# result.partition → 실제로 저장된 파티션 번호
```

### 컨슈머 — 파티션 정보와 함께 메시지 소비

```python
async for msg in consumer:
    print(f"파티션={msg.partition}, 오프셋={msg.offset}, 키={msg.key}")
    # 같은 키의 메시지는 항상 같은 파티션에서 순서대로 도착
```

### 토픽 메타데이터 조회

```python
admin_client = AIOKafkaAdminClient(bootstrap_servers="kafka:9092")
await admin_client.start()
metadata = await admin_client.describe_topics()
# 각 토픽의 파티션 수, 리더, 레플리카, ISR 정보 확인 가능
```

### 토픽 생성 (create-topics.sh)

```bash
# 파티션 수를 다르게 설정하여 생성
kafka-topics.sh --create --topic orders --partitions 3   # 주문: 적당한 병렬성
kafka-topics.sh --create --topic logs   --partitions 1   # 로그: 순서 보장
kafka-topics.sh --create --topic events --partitions 6   # 이벤트: 높은 처리량
```

---

## 10. 다음 챕터 미리보기

**Chapter 05 — 컨슈머 그룹과 오프셋 관리**

- 여러 컨슈머가 하나의 토픽을 나눠서 처리하는 **컨슈머 그룹**
- 파티션 리밸런싱이 일어나는 시점과 동작
- 오프셋 커밋 전략 (자동 vs 수동)
- `earliest` vs `latest` 오프셋 리셋 정책
