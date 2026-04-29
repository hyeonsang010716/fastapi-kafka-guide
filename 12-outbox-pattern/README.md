# 12 — Outbox Pattern: "DB는 됐는데 Kafka는 안 갔어요" 문제 해결하기

> 이 글은 *분산 시스템을 처음 만나는 백엔드 개발자* 를 대상으로 합니다. Kafka 기본기 정도만 있으면 따라올 수 있게 천천히 풀어 썼습니다.

---

## 0. 이 글을 다 읽고 나면

- "DB와 Kafka 둘 다에 써야 하는데, 한쪽만 성공하면 어떡하지?" 라는 질문에 정확히 답할 수 있게 됩니다.
- 위 문제를 푸는 **Outbox 패턴** 을 직접 구현하고, 코드의 모든 줄이 왜 거기 있는지 설명할 수 있게 됩니다.
- Kafka 컨테이너를 *일부러* 꺼도 주문 API가 멀쩡히 동작하는 걸 두 눈으로 확인합니다.

---

## 1. 어느 평범한 월요일 아침에 일어난 일

쇼핑몰 백엔드 팀에 CS 티켓이 들어왔습니다.

> "고객 user-001 님이 결제는 했는데, 주문이 진행이 안 되고 있어요. 주문 화면에는 '결제 대기' 상태로 멈춰 있다고 합니다."

DB를 열어보니 주문은 분명히 잘 들어가 있습니다.

```
orders 테이블
+-----------+----------+--------+---------+
| order_id  | user_id  | total  | status  |
+-----------+----------+--------+---------+
| abc-123   | user-001 | 4900   | CREATED |
+-----------+----------+--------+---------+
```

그런데 결제 서비스의 컨슈머 그룹 어디에도 이 주문에 해당하는 메시지가 없습니다. Kafka UI에서 `order.created` 토픽을 뒤져봐도 `abc-123`은 없습니다.

**왜 이런 일이 생겼을까요?** 코드를 보면 답이 보입니다.

```python
@app.post("/orders")
async def create_order(req: CreateOrderRequest):
    order = save_order_to_db(req)           # ① DB에 INSERT
    await publish_order_created(order)       # ② Kafka에 send
    return order
```

이 코드는 우리 책에 나오는 거의 모든 예제(특히 `09-saga-pattern-order-system`)에서 자연스럽게 보이는 모양입니다. **그런데 잘 보면, 우리는 두 군데에 따로따로 쓰고 있습니다.** ①은 PostgreSQL에, ②는 Kafka에. 이 두 시스템은 **서로의 성공/실패를 모릅니다.**

이게 분산 시스템에서 가장 유명한 함정 — **dual-write 문제(이중 쓰기 문제)** 입니다.

---

## 2. 슬로우 모션으로 다시 보기

위 코드가 어떻게 깨지는지, 시간 축을 그려가며 천천히 봅시다.

### 시나리오 A: 정상 흐름 (우리가 원하는 그림)

```
시간 ─────────────────────────────────────────►
T1  사용자 → POST /orders
T2  서버 → DB INSERT (성공) ✅
T3  서버 → Kafka send (성공) ✅
T4  서버 → 200 응답
```

### 시나리오 B: Kafka가 잠깐 흔들렸다

```
시간 ─────────────────────────────────────────►
T1  사용자 → POST /orders
T2  서버 → DB INSERT (성공) ✅
T3  서버 → Kafka send  ❌ (네트워크 끊김)
T4  서버 → 500 응답
```

이때 **DB에는 주문이 남아 있습니다.** 사용자는 500을 받고 "결제가 안 됐나?" 하면서 다시 시도하거나 CS에 문의합니다. 그 동안 우리 시스템은:
- DB에는 주문이 있는데
- 결제 서비스/재고 서비스/알림 서비스는 이 주문이 존재하는지조차 모릅니다.

### 시나리오 C: send는 됐는데 직후에 서버가 죽었다

```
시간 ─────────────────────────────────────────►
T1  사용자 → POST /orders
T2  서버 → DB INSERT (성공) ✅
T3  서버 → Kafka send (성공) ✅
T4  서버 OOM kill 💀  (응답 못함)
```

겉보기에는 사용자가 timeout을 봅니다. 사용자는 다시 시도합니다. 또 주문이 들어갑니다. **이번에는** 정상으로 끝나서 200을 받습니다. 결과:
- DB에는 주문이 2개
- Kafka에도 메시지가 2개
- 결제도 2번 (!)

### 시나리오 D: send는 timeout 났는데 실제로는 성공했다

가장 사악한 시나리오입니다.

```
시간 ─────────────────────────────────────────►
T1  사용자 → POST /orders
T2  서버 → DB INSERT (성공) ✅
T3  서버 → Kafka send (10초 타임아웃)
T4  서버는 "실패했네" 하고 DB 롤백을 시도 ❌
T5  하지만 Kafka 입장에서는 메시지가 잘 들어갔음 ✅
```

DB 롤백 자체도 실패하면 어떻게 될까요? 또는 롤백은 성공했는데 메시지는 이미 살아 있다면? **존재하지 않는 주문이 결제까지 가는** 끔찍한 일이 일어납니다.

---

## 3. 일단 떠오르는 대로 고쳐 보자 (그리고 다 부숴 보자)

자연스럽게 떠오르는 해법 4가지를 차례로 검토해 봅시다. 이걸 건너뛰면 "왜 outbox가 정답인지" 가 와닿지 않습니다.

### 시도 1. "Kafka 먼저 보내고 DB에 저장하면 되지 않나?"

```python
await publish_order_created(...)
save_order_to_db(...)
```

이러면 시나리오 B가 거꾸로 됩니다.
- Kafka에는 메시지가 갔는데 DB INSERT 실패.
- 결제 서비스는 이 메시지를 받아서 **존재하지도 않는 주문에 대한 결제** 를 진행합니다.

훨씬 나쁩니다.

### 시도 2. "실패하면 보상(compensating action)으로 DB 롤백"

```python
try:
    save_order_to_db(...)
    await publish_order_created(...)
except KafkaError:
    delete_order_from_db(...)   # 보상
```

- `delete_order_from_db` 호출 *직전* 에 프로세스가 죽으면? → 시나리오 B와 똑같음.
- 시나리오 D — `publish` 가 timeout 났지만 실제로 메시지는 살아남았다면? → 보상으로 DB 행을 지우는 순간, **DB에는 없는데 Kafka에는 있는** 최악의 상태.

### 시도 3. "그러면 분산 트랜잭션(2PC, XA)을 쓰자"

- Kafka는 XA 트랜잭션의 참여자(participant)가 아닙니다. 애초에 안 됩니다.
- 가능하다 한들, 한쪽이 죽으면 다른 쪽 락이 영원히 잡혀 있는 식의 운영 악몽이 시작됩니다. 큰 회사들도 거의 안 씁니다.

### 시도 4. "Kafka Transactions 쓰면 되잖아?"

이건 헷갈리기 쉬운 부분이라 정리하고 갑니다.

> Kafka 자체에도 `producer.beginTransaction()` ... `commitTransaction()` 이라는 트랜잭션 기능이 있습니다. **하지만 이건 "Kafka에서 읽고 → Kafka에 쓰는" 시나리오 안에서만 의미가 있습니다.** "DB에 쓰는 행위" 와 한 트랜잭션으로 묶을 수 없습니다. 우리 문제는 그대로입니다.

### 결론

위 4가지 시도가 다 실패하는 **공통 원인** 은 하나입니다.

> **두 개의 외부 시스템(DB, Kafka)에 동시에 쓰려고 하기 때문이다.**

그러면 답도 자연스럽게 나옵니다.

> **두 시스템에 동시에 쓰지 말자. 한 시스템(DB)에만 쓰고, 다른 한 곳(Kafka)으로는 누군가가 천천히 옮겨 가게 하자.**

이게 Outbox 패턴의 전부입니다. 진짜로 이게 다입니다. 나머지는 디테일.

---

## 4. 비유 하나: 우체통

코드 들어가기 전에 한 번만 비유로 정리합시다.

당신이 친구에게 편지를 부친다고 칩시다. 두 가지 방법이 있어요.

**방법 A — 직접 우체국까지 가져가기.** 우체국이 문 닫혀 있으면? 우체국 가는 길에 비가 오면? 당신은 편지도 못 부치고 시간만 날립니다.

**방법 B — 집 앞 우체통에 넣기.** 당신은 그냥 우체통에 넣고 일상으로 돌아갑니다. 우체부가 다음 날 와서 그걸 수거해 우체국까지 가져갑니다. 우체부가 늦어도, 우체국이 잠깐 닫혀 있어도, **편지는 우체통에 안전하게 있습니다.**

| 비유 | 우리 시스템 |
|------|-------------|
| 편지 쓰기 | 주문 데이터 만들기 |
| 우체통(outbox)에 넣기 | `outbox` 테이블에 INSERT |
| 우체부 | Outbox **Relay** (백그라운드 폴러) |
| 우체국 | Kafka |
| 친구 | 다운스트림 컨슈머 (결제, 재고, 알림 ...) |

이 비유가 머릿속에 자리 잡으면 코드는 그냥 디테일을 채우는 일이 됩니다.

---

## 5. 전체 그림

```
            [POST /orders]
                  │
                  ▼
    ┌──────────────────────────────────────────┐
    │     FastAPI: 단 한 번의 DB 트랜잭션          │
    │   ┌────────────────────────────────────┐ │
    │   │  INSERT INTO orders (...)          │ │ ← ① 주문
    │   │  INSERT INTO outbox (event ...)    │ │ ← ② "Kafka로 보내야 할 일" 기록
    │   │  COMMIT                            │ │
    │   └────────────────────────────────────┘ │
    └──────────────────────────────────────────┘
                  │
                  ▼
        ┌────────────────────┐
        │     PostgreSQL     │
        │  ┌──────────────┐  │
        │  │   orders     │  │
        │  ├──────────────┤  │
        │  │   outbox     │ ◀──── 폴링 (몇 백 ms마다 한 번)
        │  └──────────────┘  │           ▲
        └────────────────────┘           │
                                         │ "미발행 이벤트 좀 줘"
                                         │
                              ┌──────────────────────┐
                              │   Outbox Relay       │
                              │   (백그라운드 태스크)    │
                              │   1) 폴링             │
                              │   2) Kafka로 보냄      │
                              │   3) "발행 완료" 도장    │
                              └─────────┬────────────┘
                                        ▼
                                   ┌─────────┐
                                   │  Kafka  │
                                   └────┬────┘
                                        ▼
                              다운스트림 컨슈머
                              (event_id로 중복 거름)
```

**원칙 3가지** 만 지키면 됩니다.

1. **주문 INSERT 와 outbox INSERT 는 같은 트랜잭션 안에서.**
   둘이 같이 커밋되거나, 둘 다 안 커밋되거나. 어중간한 상태가 없습니다.
2. **Kafka 발행은 별도 백그라운드 프로세스(Relay)가 outbox 테이블을 읽어서 비동기로 한다.**
   API 요청은 Kafka 가용성에 묶이지 않습니다. Kafka 가 죽어도 주문은 받습니다.
3. **컨슈머는 같은 메시지를 두 번 받아도 한 번만 처리하도록 만든다(멱등성).**
   Relay는 "발행은 했는데 도장 찍기 직전 죽음" 같은 사고가 나면 다음에 또 발행합니다. 그게 컨슈머 책임으로 흡수됩니다.

위 3가지가 이 챕터의 전부입니다. 아래는 그걸 실제 코드로 어떻게 옮기는지에 대한 디테일입니다.

---

## 6. 데이터 모델

`init.sql` 에 들어 있는 두 테이블을 천천히 봅시다.

```sql
CREATE TABLE orders (
    order_id     UUID         PRIMARY KEY,
    user_id      VARCHAR(255) NOT NULL,
    items        JSONB        NOT NULL,
    total_price  NUMERIC(12, 2) NOT NULL,
    status       VARCHAR(50)  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

평범한 주문 테이블입니다. 여기까지는 새로운 게 없어요.

```sql
CREATE TABLE outbox (
    id              BIGSERIAL    PRIMARY KEY,
    event_id        UUID         NOT NULL UNIQUE,
    aggregate_type  VARCHAR(255) NOT NULL,
    aggregate_id    VARCHAR(255) NOT NULL,
    event_type      VARCHAR(255) NOT NULL,
    topic           VARCHAR(255) NOT NULL,
    payload         JSONB        NOT NULL,
    headers         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ  NULL
);
```

이게 **우체통** 입니다. 컬럼 하나하나 왜 있는지 봅시다.

| 컬럼 | 무엇? | 왜 필요? |
|------|-------|----------|
| `id BIGSERIAL` | 자동 증가 정수 | "넣은 순서대로" 발행하기 위해. `ORDER BY id` 하면 시간순. |
| `event_id UUID UNIQUE` | 이벤트 고유 ID | **컨슈머가 중복을 거를 때 쓰는 키.** 같은 이벤트가 두 번 발행되어도 이 값은 똑같습니다. |
| `aggregate_id` | 이 이벤트가 누구 얘긴지 (예: order_id) | **Kafka 메시지 key 로 사용** → 같은 주문의 이벤트가 같은 파티션 → 순서 보장. |
| `aggregate_type` | "Order", "Payment" 등 | 한 outbox 테이블을 여러 도메인이 공유할 때 라우팅용. |
| `event_type` | "OrderCreated", "OrderCancelled" 등 | 컨슈머가 페이로드를 까기 전에 결정 내릴 수 있게. |
| `topic` | 이 이벤트가 어떤 Kafka 토픽으로 가야 하는지 | Relay 코드가 단순해집니다. "그냥 이 토픽으로 보내라" |
| `payload` | 진짜 데이터 (JSONB) | 컨슈머가 받는 메시지 본문. |
| `headers` | 부가 정보 (예: schema_version) | Kafka 메시지 헤더로 옮겨갑니다. |
| `published_at` | 발행 완료 타임스탬프, 또는 NULL | **NULL = 아직 안 보냄.** Relay 가 이 컬럼을 보고 일감을 찾습니다. |

마지막에 한 줄 있는 인덱스가 핵심입니다.

```sql
CREATE INDEX idx_outbox_unpublished
    ON outbox (id)
    WHERE published_at IS NULL;
```

이건 **부분 인덱스(partial index)** 입니다. "발행이 안 된 행만 인덱싱한다" 라는 뜻이에요. 시간이 지날수록 발행된 행은 늘어나지만, 미발행 행은 늘 적게 유지됩니다(Relay 가 빨리 비우니까). 그래서 폴링 쿼리는 **테이블 크기가 아무리 커져도 일정한 속도** 로 돕니다.

---

## 7. 가장 중요한 한 트랜잭션 (`app/main.py`)

지금부터가 진짜 핵심입니다. `POST /orders` 핸들러를 천천히 봅시다.

```python
async with SessionLocal() as session:
    async with session.begin():                # ← 트랜잭션 시작
        order = Order(
            order_id=order_id,
            user_id=request.user_id,
            items=items_payload,
            total_price=total_price,
            status="CREATED",
        )
        session.add(order)                     # ① orders INSERT 예약

        await enqueue_event(                   # ② outbox INSERT 예약
            session,
            aggregate_type="Order",
            aggregate_id=str(order_id),
            event_type="OrderCreated",
            topic=TOPIC_ORDER_EVENTS,
            payload={...},
        )
    # ↑ 이 줄을 빠져나오는 순간 COMMIT 이 일어납니다.
    #   ① 과 ② 가 한 번에 영속화됩니다.
```

이 코드를 한 줄씩 곱씹어 봅시다.

**`async with session.begin():`** 이 한 줄이 SQLAlchemy 의 트랜잭션 컨텍스트입니다. 이 블록 안에서 일어나는 모든 SQL 은 한 트랜잭션입니다. 블록을 정상 빠져나오면 COMMIT, 예외가 나면 ROLLBACK.

**`session.add(order)` (①)** 와 **`enqueue_event(...)` (②)** — 둘 다 *예약* 만 합니다. 실제 INSERT 는 트랜잭션 끝에서 한꺼번에 flush 됩니다.

**그래서 이 두 INSERT 는 같이 커밋되거나 같이 롤백됩니다.** 어중간한 상태가 물리적으로 불가능합니다. 이게 dual-write 문제의 절반을 그냥 없애 버립니다.

> 잠깐, **Kafka 호출이 어디에도 없다는 점** 을 보세요. 이 함수는 `aiokafka` 를 import 하지도 않습니다. 비즈니스 코드는 메시지 인프라의 존재를 모릅니다. 인프라가 흔들려도 비즈니스 로직은 안 흔들립니다.

### 보조 헬퍼 (`app/outbox.py`)

```python
async def enqueue_event(session, *, aggregate_type, aggregate_id,
                       event_type, topic, payload, headers=None, event_id=None):
    event = OutboxEvent(
        event_id=event_id or uuid4(),
        aggregate_type=aggregate_type,
        aggregate_id=str(aggregate_id),
        event_type=event_type,
        topic=topic,
        payload=payload,
        headers=headers or {},
    )
    session.add(event)
    return event
```

**이 함수가 commit 을 안 한다는 점** 이 중요합니다. 왜냐고요?

> 만약 이 함수가 `await session.commit()` 을 호출해 버리면, "주문 INSERT" 와 "outbox INSERT" 가 *서로 다른* 트랜잭션이 됩니다. 그러면 outbox INSERT 만 커밋되고 주문이 롤백되는 상황이 가능해집니다. 패턴이 바로 깨집니다.

**규칙: outbox 헬퍼는 트랜잭션 경계를 절대 자기가 결정하지 않는다.** 코드 리뷰에서 가장 많이 깨지는 지점이라 댓글로 못 박을 가치가 있습니다.

---

## 8. 우체부 (`app/relay.py`)

이제 outbox 테이블에 차곡차곡 쌓인 이벤트를 누가 Kafka 로 옮길까요? `OutboxRelay` 라는 백그라운드 태스크입니다.

기본 골격은 **무한 루프** 입니다.

```python
async def run(self):
    while not self._stop_event.is_set():
        published = await self._tick()    # 한 번 폴링
        delay_ms = RELAY_POLL_INTERVAL_MS if published else RELAY_IDLE_BACKOFF_MS
        await asyncio.sleep(delay_ms / 1000)
```

요점만 보면 *"미발행 이벤트 한 무리를 가져와 → Kafka 로 발행 → '발행 완료' 도장 찍기"* 를 반복합니다. 그런데 그 한 사이클(`_tick`) 안에 디테일이 잔뜩 들어 있어요.

### 8.1 동시에 두 개의 Relay 가 떠 있어도 안전한가?

네. 이것 때문입니다.

```sql
SELECT id, event_id, aggregate_id, event_type, topic, payload, headers
FROM outbox
WHERE published_at IS NULL
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT :batch
```

`FOR UPDATE SKIP LOCKED` — 이름은 어렵지만 동작은 직관적입니다.

- `FOR UPDATE`: "이 행 잠가, 트랜잭션 끝날 때까지 다른 사람이 못 건드리게."
- `SKIP LOCKED`: "근데 누가 이미 잠가 놨으면 **기다리지 말고** 다음 행으로 넘어가."

상상해 봅시다. Relay-A 와 Relay-B 가 동시에 떠 있어요.

```
Relay-A : SELECT ... LIMIT 100  →  id 1~100 행을 잠금 (FOR UPDATE)
Relay-B : SELECT ... LIMIT 100  →  id 1~100 은 이미 잠겨 있으니 SKIP
                                  →  id 101~200 을 잠금
```

둘이 부딪히지 않고 **자기 몫만 가져갑니다.** 그래서 K8s 에서 Relay 를 스케일 아웃해도 코드 한 줄 바꿀 필요가 없습니다.

### 8.2 발행 → 도장 찍기, 같은 트랜잭션 안에서

```python
async with session.begin():
    rows = SELECT ... FOR UPDATE SKIP LOCKED LIMIT N
    for row in rows:
        await self._publish(row)            # Kafka send_and_wait
    UPDATE outbox SET published_at = NOW() WHERE id = ANY(:ids)
```

이 한 트랜잭션 안에서 두 일이 일어납니다.

1. Kafka 로 보냄
2. DB 에 "발행 완료" 도장 찍음

이걸 같이 묶는 이유? **Kafka 발행이 실패하면, 도장도 찍히지 않게 하려고요.**

타임라인으로 봅시다.

**케이스 1 — Kafka send 실패**

```
T1: SELECT (행 잠금)
T2: send_and_wait → 예외 ❌
T3: 트랜잭션 롤백 (도장 안 찍힘, 잠금도 풀림)
T4: 다음 사이클에서 같은 행이 다시 잡혀 발행 재시도 ✅
```

좋습니다. 메시지 손실 없음.

**케이스 2 — Kafka 는 받았는데 도장 직전 프로세스가 죽음**

```
T1: SELECT (행 잠금)
T2: send_and_wait → 성공 ✅ (Kafka 에 메시지 들어감)
T3: 프로세스 OOM 💀 (UPDATE 못함)
T4: DB 트랜잭션 롤백, 행 잠금 해제, published_at 그대로 NULL
T5: 다음 사이클에서 또 발행 → Kafka 에 같은 메시지가 두 번 들어감 ⚠️
```

이게 outbox 패턴의 **본질적 한계** 입니다. **"At-least-once" — 적어도 한 번 (즉 두 번 갈 수도 있음).** 이걸 받아들이는 대신 우리는 "절대 누락되지 않는다" 라는 강한 보장을 얻습니다.

그리고 두 번 가는 게 컨슈머에서 한 번 처리되도록 만드는 게 다음 섹션의 일입니다.

### 8.3 Producer 설정

```python
AIOKafkaProducer(
    bootstrap_servers=...,
    enable_idempotence=True,
    acks="all",
    linger_ms=20,
)
```

- `enable_idempotence=True` — Kafka 0.11 부터 있는 기능. **네트워크 재시도로 같은 send 가 두 번 가는 일을 브로커 측에서 막아줍니다.** 단, 이건 같은 producer 인스턴스의 같은 send 호출 한정. **트랜잭션 롤백으로 다음 사이클에 다시 send 하는 케이스 2 는 이걸로 막을 수 없습니다.** 헷갈리기 쉬운 지점이라 한 번 더 강조.
- `acks="all"` — 모든 ISR 복제본이 메시지를 받았다고 확인해야 send 성공으로 간주. 가장 안전.
- `linger_ms=20` — 20ms 동안 메시지를 모았다가 배치 전송. throughput 증가.

### 8.4 일이 없을 땐 살짝 쉬기

```python
delay_ms = RELAY_POLL_INTERVAL_MS if published else RELAY_IDLE_BACKOFF_MS
```

방금 일감을 한 무리 처리했으면 200ms 만에 다시 폴링합니다 (다음 일감이 더 있을 가능성이 높으니까). 한 번 폴링했는데 아무것도 없었으면 1초쯤 쉬고 다시 봅니다. **트래픽이 한가한 새벽에 DB CPU 가 1% 넘게 안 올라가는** 작은 디테일.

---

## 9. 컨슈머 — 같은 편지 두 번 받아도 한 번만 처리 (`app/consumer.py`)

위에서 본 케이스 2 때문에 컨슈머는 **멱등** 해야 합니다. "같은 메시지를 두 번 받아도 결과가 한 번 받은 것과 같다" 라는 뜻입니다.

방법: **`event_id` 를 키로, 처리한 적이 있는지 기록.**

```python
async def _mark_processed(event_id: str) -> bool:
    INSERT INTO processed_events (event_id) VALUES (:eid)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id
```

이 한 줄 SQL 이 **"표시 또는 건너뛰기(mark or skip)"** 를 원자적으로 합니다.

- 처음 보는 `event_id` → INSERT 성공 → `RETURNING` 이 행을 돌려줌 → 함수 결과 `True` (새 메시지)
- 이미 처리한 `event_id` → `ON CONFLICT DO NOTHING` 으로 INSERT 무시 → `RETURNING` 비어 있음 → 함수 결과 `False` (중복)

컨슈머 메인 루프는 그래서 이렇게 됩니다.

```python
event_id = headers.get("event_id")

is_new = await _mark_processed(event_id)
if not is_new:
    log.info("event.duplicate_skipped", event_id=event_id)
    continue

# 처음 보는 메시지일 때만 비즈니스 로직 실행
process_business_logic(msg.value)
```

**여기가 outbox 패턴의 마지막 퍼즐 조각** 입니다. Producer 측이 at-least-once 라는 약점을 가지고 있지만, Consumer 측이 멱등이면 결과적으로 **"정확히 한 번 처리됨(effectively exactly-once)"** 가 됩니다. 챕터 08 에서 이미 봤던 패턴이죠.

> 실무 팁: 멱등성 키 저장소를 어디에 둘지가 종종 논의됩니다. 가장 강력한 건 **비즈니스 결과를 적는 DB 와 같은 트랜잭션** 에 dedup 행을 INSERT 하는 방식입니다. 이 글의 컨슈머는 학습용으로 그 방향을 보여주고 있어요.

---

## 9-bis. 사실 우리는 방금 **Inbox 패턴** 을 구현했습니다

위의 `processed_events` 테이블 — 이게 정식 명칭이 있어요. **Inbox 패턴(받은 편지함 패턴)** 입니다. Outbox 의 짝꿍이에요.

대칭을 보면 한눈에 들어옵니다.

```
        Producer 측                                  Consumer 측
   ┌──────────────────────┐                    ┌──────────────────────┐
   │  Outbox 테이블         │                    │  Inbox 테이블         │
   │  (보낼 편지함)          │                    │  (받은 편지함)         │
   │                      │                    │                     │
   │  "이 이벤트를           │                    │  "이 event_id 는     │
   │   발행해야 한다"        │                    │   이미 처리했다"        │
   └──────────────────────┘                    └──────────────────────┘
       비즈니스 INSERT 와                          비즈니스 UPDATE 와
       같은 트랜잭션                              같은 트랜잭션
```

### 왜 이름이 따로 있는가 — 사실 *컨슈머 측의 dual-write 문제* 를 푼다

Producer 쪽 dual-write 만 본 분이 많지만, 컨슈머에도 똑같은 문제가 있어요. 실무 컨슈머는 보통 메시지를 받으면 두 가지를 합니다.

```
[메시지 수신]
   │
   ├── ① 자기 DB 의 비즈니스 데이터 변경  (예: 결제 INSERT)
   └── ② "이 메시지 처리 완료" 표시       (예: processed_events INSERT)
```

①과 ② 가 따로따로 일어나면 — 익숙하죠? **컨슈머 측 dual-write 문제** 입니다. ①까지 했는데 ② 직전에 죽으면, 재시작 후 같은 메시지를 또 처리해서 결제가 두 번 일어납니다.

**해결책도 똑같습니다 — 같은 트랜잭션으로 묶기.**

```python
async with session.begin():
    await mark_processed(session, event_id)     # ② Inbox INSERT
    await process_business(session, msg.value)  # ① 비즈니스 변경
```

이 두 줄이 한 트랜잭션이라서 *어중간한 상태가 절대 없게* 만들어 줘요. Outbox 와 정확히 같은 원리, 반대 방향.

### Outbox + Inbox 가 한 쌍

마이크로서비스 환경에서 한 서비스는 보통 *받기도 하고 보내기도 합니다.* 그래서 표준적인 서비스의 모양은 이렇게 됩니다.

```
┌────────────────────────────────────────────────────────┐
│                  서비스 X (예: Payment)                 │
│                                                         │
│   ┌─ Kafka 에서 받음 ─┐                                  │
│                                                         │
│       ▼                                                 │
│   ┌────────────────────────────────────────┐            │
│   │  begin transaction                     │            │
│   │     INSERT inbox      (← event_id)     │            │
│   │     UPDATE/INSERT 비즈니스 테이블          │            │
│   │     INSERT outbox     (다음 이벤트)      │            │
│   │  commit                                │            │
│   └────────────────────────────────────────┘            │
│                                                         │
│       │                                                 │
│   ┌─ Relay 가 Kafka 로 발행 ─┐                            │
└────────────────────────────────────────────────────────┘
```

**한 트랜잭션에서 inbox + 비즈니스 + outbox 가 같이 커밋** 됩니다. 이게 마이크로서비스에서 이벤트를 신뢰성 있게 다루는 *표준 골격* 이에요. 이름은 종종 **"Transactional Inbox & Outbox"** 라고 한 묶음으로 부릅니다.

### 우리 코드에서의 위치

이 챕터의 컨슈머(`app/consumer.py`)는 학습 단순화를 위해 비즈니스 로직 자리를 *로그 출력* 으로만 두었습니다. 실무에 옮기면 그 자리에 *진짜 비즈니스 변경* 이 들어가고, **그 변경과 `mark_processed` 가 같은 트랜잭션** 이어야 한다는 규칙을 지키면 됩니다.

> **요약**: 우리는 이 챕터에서 Outbox(보낼 편지함) 와 Inbox(받은 편지함) 를 *둘 다* 만든 셈이에요. Outbox 만큼 자주 입에 오르진 않지만, 실무 코드 리뷰에서는 *"이 컨슈머에는 inbox 가 있나?"* 가 outbox 만큼이나 자주 등장하는 질문입니다.

---

## 10. 직접 실행해 보기

말로만 보면 안 와닿으니 직접 띄웁시다.

### 10.1 띄우기

```bash
cd 12-outbox-pattern
docker compose up --build -d
```

올라오는 컨테이너:

| 컨테이너 | 무엇 | 어디 |
|----------|------|------|
| `outbox-postgres` | orders + outbox + processed_events 보관 | `localhost:5432` |
| `outbox-kafka` | 메시지 브로커 | `localhost:29092` |
| `outbox-kafka-ui` | 토픽/메시지 UI | http://localhost:8080 |
| `outbox-order-service` | FastAPI + Outbox Relay (한 컨테이너) | http://localhost:8000 |
| `outbox-consumer` | 멱등 컨슈머 | (로그로 확인) |

### 10.2 주문 생성

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-001",
    "items": [
      {"product_id": "sku-a", "quantity": 2, "price": 1500},
      {"product_id": "sku-b", "quantity": 1, "price": 4900}
    ]
  }'
```

응답 예:

```json
{
  "order_id": "abc-123-...",
  "user_id": "user-001",
  "total_price": "7900.00",
  "status": "CREATED",
  ...
}
```

### 10.3 outbox 들여다보기

```bash
# 미발행만
curl 'http://localhost:8000/outbox?unpublished_only=true'

# 최근 50건
curl 'http://localhost:8000/outbox'
```

평소엔 미발행 목록이 거의 비어 있을 겁니다. Relay 가 거의 실시간으로 비우니까요.

DB 에 직접 들어가서 보고 싶다면:

```bash
docker exec -it outbox-postgres psql -U outbox -d outbox -c \
  "SELECT id, event_type, published_at FROM outbox ORDER BY id DESC LIMIT 10;"
```

`published_at` 이 시간 값으로 채워져 있으면 발행 완료입니다.

### 10.4 Kafka 에 정말 갔는지 확인

http://localhost:8080 → **outbox** 클러스터 → **order.events** 토픽 → Messages.

각 메시지의 헤더를 펼치면 `event_id`, `event_type`, `schema_version=1` 이 보일 겁니다.

### 10.5 컨슈머 로그

```bash
docker logs -f outbox-consumer
```

```
event.processed event_id=... event_type=OrderCreated order_id=...
```

---

## 11. 진짜 짜릿한 데모: Kafka 를 일부러 죽여 보기

여기까지가 outbox 패턴의 *진짜 자랑할 만한* 부분입니다.

```bash
# 1) Kafka 만 내린다.
docker compose stop kafka

# 2) 그래도 주문은 200 OK 가 떨어진다.
curl -X POST http://localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"user_id": "u1", "items":[{"product_id":"sku-a","quantity":1,"price":100}]}'
# → 정상 응답!

# 3) outbox 에는 미발행 이벤트가 쌓인다.
curl 'http://localhost:8000/outbox?unpublished_only=true'
# → 우리 이벤트가 published_at: null 로 보인다.

# 4) Kafka 를 다시 켠다.
docker compose start kafka

# 5) 잠깐 기다린 뒤 다시 본다 — 알아서 발행됐다.
curl 'http://localhost:8000/outbox?unpublished_only=true'
# → 비어 있다. 컨슈머 로그에도 처리 흔적이 찍혀 있다.
```

**이게 outbox 의 사명** 입니다. 메시지 인프라가 흔들려도 사용자 트랜잭션은 흔들리지 않습니다.

이전 챕터 코드(`09-saga-pattern-order-system`)로 같은 실험을 하면 어떻게 될까요? `publish_order_created` 가 Kafka 가 죽었으니 timeout 또는 즉시 실패합니다. 사용자에게 500 이 떨어지고, DB 상태에 따라서는 시나리오 B/D 로 들어갑니다. 그게 우리가 12 챕터에서 고치는 일이었어요.

### 종료

```bash
docker compose down -v
```

---

## 12. 폴링 vs Debezium(CDC) — 언제 무엇을 골라야 하나

지금 우리가 만든 Relay 는 "polling 방식" 입니다. DB 를 200ms 마다 두드립니다. 더 큰 회사들이 자주 쓰는 대안이 있어요. **Debezium** 입니다. PostgreSQL 의 logical replication(WAL) 스트림을 직접 읽어서 outbox 행이 INSERT 되는 순간 Kafka 로 흘려보냅니다.

| 비교 | 폴링 (이 챕터) | Debezium / CDC |
|------|--------------|---------------|
| **지연** | 폴링 주기에 비례 (수십~수백 ms) | 거의 실시간 (수 ms) |
| **DB 부담** | 미미하지만 상시 폴링 | 거의 없음 (WAL 읽기) |
| **추가 인프라** | 없음 | Kafka Connect 클러스터 |
| **순서 보장** | `ORDER BY id` + Kafka key | WAL 순서 그대로 |
| **운영 난이도** | 낮음 (DB 만 알면 됨) | 중상 (replication slot, connector 관리) |
| **언제 적합?** | 작은~중간 트래픽, 운영 인력 적을 때 | 수십만 TPS 이상, 이미 Kafka Connect 쓰는 조직 |

**실무 가이드**

- 시작할 때는 **폴링** 이 거의 항상 정답입니다. 단순하고 디버깅 쉽고, 작동합니다.
- 트래픽이 본격적으로 커지면(수십만 TPS) 그때 Debezium 으로 옮깁니다.
- **outbox 테이블 스키마는 안 바꿔도 됩니다.** 발행 메커니즘만 갈아끼우면 돼요. 우리가 outbox 테이블을 "인터페이스" 로 두고, Relay 는 "구현 디테일" 로 둔 이유.

---

## 13. 운영 체크리스트

이 글의 코드는 학습용으로 정직합니다. 실제 프로덕션에 가져갈 때 추가로 챙기는 것들:

- **outbox 테이블 정리 잡** — `published_at < now() - interval '7 days'` 행을 archive 또는 delete. 보관 기간은 "감사/리플레이가 필요한 최대 기간" 으로 정합니다.
- **모니터링 메트릭 (이 둘이면 충분)**
  - `COUNT(*) WHERE published_at IS NULL` — 백로그 크기
  - 가장 오래된 미발행 행의 나이 — Relay 지연
- **알람**
  - 미발행 행 > 10,000
  - 가장 오래된 미발행 행 > 60s
  → 이러면 Relay 가 죽어 있거나 Kafka 가 막혀 있는 거예요.
- **이벤트 스키마 진화** — `headers.schema_version` 을 항상 채우고, 컨슈머는 모르는 버전을 만나면 무시 말고 DLQ 로 보냅니다.
- **PII** — outbox payload 는 평문 JSON 입니다. 카드번호 같은 민감정보는 토큰화하거나 ID 만 담고 컨슈머가 별도 조회합니다.
- **트랜잭션 길이** — 비즈니스 트랜잭션 안에서 outbox INSERT 는 추가 비용이 거의 없지만, payload 만들기 위한 **무거운 계산은 트랜잭션 바깥에서** 끝내고 들어갑니다.

---

## 14. 자주 묻는 질문

**Q1. outbox 테이블이 너무 커지지 않을까요?**
부분 인덱스(`WHERE published_at IS NULL`) 덕분에 *발행된* 행이 아무리 쌓여도 폴링 쿼리 비용은 거의 일정합니다. 그래도 디스크는 차니까 N일 정리 잡이 필요해요.

**Q2. 같은 주문에 대한 여러 이벤트의 순서가 보장되나요?**
예. 같은 `aggregate_id` 를 Kafka key 로 쓰면 같은 파티션으로 가서 순서가 보장됩니다. outbox 안에서는 `id` 단조 증가 시퀀스가 발행 순서를 보장합니다.

**Q3. Relay 를 별도 프로세스로 분리해야 하나요?**
이 챕터는 학습 편의로 FastAPI 와 한 컨테이너에 두었어요. 실무에서는 보통 트래픽이 커지면 분리합니다 — 책임 분리, 독립 스케일, 재시작 영향 격리. **`SKIP LOCKED` 덕분에 어느 쪽이든 코드는 그대로** 입니다.

**Q4. 컨슈머의 dedup 테이블이 무한히 커지지 않을까요?**
TTL(예: 24시간) 또는 주기 청소가 필요합니다. "메시지가 세상에 살아 있을 수 있는 최대 시간" 으로 잡으면 충분해요.

**Q5. payload 가 너무 큰 메시지는 어떻게 하나요?**
JSONB 도 큰 데이터는 부담입니다. 큰 본문은 S3 같은 외부 스토리지에 두고 outbox 에는 참조(URL 또는 ID)만 담습니다. 이걸 **Claim Check 패턴** 이라고 부릅니다.

**Q6. 한 트랜잭션에서 이벤트 여러 개를 발행해도 되나요?**
됩니다. `enqueue_event` 를 여러 번 호출하면 outbox 에 여러 행이 같이 INSERT 되고 같은 트랜잭션에서 커밋됩니다.

**Q7. saga 패턴(09 챕터)과 어떻게 결합되나요?**
saga 의 모든 이벤트를 outbox 로 발행하면 saga 의 가장 큰 약점인 *"이벤트 유실로 인한 reconciliation 지옥"* 이 사라집니다. 다음 단계로 권장합니다.

---

## 마무리 — 한 줄로 정리

> **분산 시스템에서는 두 시스템에 동시에 쓰지 말고, 한 시스템(DB)에 쓴 뒤 비동기로 옮긴다. 옮기는 쪽은 at-least-once 라서 받는 쪽이 멱등해야 한다.**

이 한 줄이 코드로는 **`outbox` 테이블 + `SELECT FOR UPDATE SKIP LOCKED` + `event_id` 기반 dedup** 이 됩니다. 우리가 이 챕터에서 만든 게 정확히 그것이에요.

수고하셨어요. 다음 챕터에서는 이 위에 saga(09)를 다시 얹어서 *"이벤트 손실 없는 분산 트랜잭션"* 까지 마무리해 봅시다.

---

## 부록 A. 디렉토리 구조

```
12-outbox-pattern/
├── README.md                ← 이 글
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── init.sql                 ← orders / outbox 스키마
└── app/
    ├── __init__.py
    ├── config.py            ← 환경 변수 / 토픽 / 폴링 주기
    ├── database.py          ← SQLAlchemy 비동기 엔진
    ├── models.py            ← Order / OutboxEvent ORM
    ├── schemas.py           ← Pydantic 요청/응답
    ├── outbox.py            ← enqueue_event 헬퍼 (★)
    ├── main.py              ← FastAPI: POST /orders 의 단일 트랜잭션 (★)
    ├── relay.py             ← 폴링 릴레이 (★ FOR UPDATE SKIP LOCKED)
    └── consumer.py          ← 멱등 컨슈머 (event_id dedup)
```

(★) 표시된 파일이 패턴의 본체입니다. 나머지는 인프라 보일러플레이트에요.

## 부록 B. 한눈에 보는 흐름 (요약 카드)

```
[POST /orders]
     │
     ├── 한 트랜잭션 ──► orders INSERT
     │                  outbox INSERT (event_id, payload, topic, ...)
     │                  COMMIT
     │
     ▼
[Outbox Relay] ── 200ms 주기 폴링 ──► SELECT ... FOR UPDATE SKIP LOCKED
     │                                  └─► Kafka send_and_wait (idempotent)
     │                                  └─► UPDATE published_at = NOW()
     ▼
[Kafka topic: order.events]
     │                                   message key = order_id
     │                                   headers   = event_id, event_type, schema_version
     ▼
[Consumer] ── ON CONFLICT DO NOTHING (event_id) ──► 새 메시지면 처리, 중복이면 skip
```

이 카드 한 장이 머릿속에 들어오면, 이 패턴을 이해한 거예요.
