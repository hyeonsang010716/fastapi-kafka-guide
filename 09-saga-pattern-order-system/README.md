# Chapter 09: 실전 주문 시스템 (Real-World Order System)

## 1. 이 챕터에서 배우는 것

- **이벤트 드리븐 아키텍처(EDA)** 를 활용한 마이크로서비스 설계
- **Saga 패턴 (Choreography 방식)** 으로 분산 트랜잭션 관리
- **보상 트랜잭션** 개념과 실패 처리
- FastAPI + Kafka를 사용한 3개 서비스 간의 비동기 이벤트 통신
- Docker Compose를 이용한 멀티 서비스 오케스트레이션

---

## 2. 이벤트 드리븐 아키텍처 (Event-Driven Architecture)

전통적인 동기식(REST API 호출) 방식과 달리, 이벤트 드리븐 아키텍처에서는 서비스 간 통신이 **이벤트(메시지)** 를 통해 이루어집니다.

### 동기식 vs 비동기식(이벤트 드리븐)

```
[동기식 - 강한 결합]
주문 서비스 --HTTP POST--> 결제 서비스 --HTTP POST--> 재고 서비스
     <---응답 대기---         <---응답 대기---

[비동기식 - 느슨한 결합]
주문 서비스 --이벤트 발행--> Kafka <--이벤트 구독-- 결제 서비스
                             <--이벤트 구독-- 재고 서비스
```

### 장점
- **느슨한 결합**: 서비스가 서로를 직접 호출하지 않음
- **확장성**: 새로운 서비스를 추가해도 기존 서비스 변경 불필요
- **탄력성**: 한 서비스가 다운되어도 이벤트가 Kafka에 보관됨
- **비동기 처리**: 응답을 기다리지 않아 처리 성능 향상

---

## 3. Saga 패턴 (Choreography)

분산 시스템에서는 여러 서비스에 걸친 트랜잭션을 하나의 DB 트랜잭션으로 처리할 수 없습니다. **Saga 패턴**은 각 서비스가 로컬 트랜잭션을 실행하고, 실패 시 보상 트랜잭션을 통해 일관성을 유지합니다.

### Choreography 방식

중앙 오케스트레이터 없이, 각 서비스가 이벤트를 발행하고 구독하여 자율적으로 동작합니다.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Saga 이벤트 흐름 (성공 케이스)                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [Client]                                                       │
│     │                                                           │
│     │ POST /orders                                              │
│     ▼                                                           │
│  ┌──────────────┐   order.created    ┌──────────────────┐       │
│  │   Order      │ =================> │     Payment      │       │
│  │  Service     │                    │     Service      │       │
│  │ (port 8000)  │   payment.result   │    (port 8001)   │       │
│  │              │ <================= │                  │       │
│  │              │   (COMPLETED)      └──────────────────┘       │
│  │              │                                               │
│  │              │                    ┌──────────────────┐       │
│  │              │   inventory.result │ Inventory        │       │
│  │              │ <================= │ Service          │       │
│  │              │   (RESERVED)       │ (port 8002)      │       │
│  └──────────────┘                    └──────────────────┘       │
│                                            ▲                    │
│                          payment.result    │                    │
│                          (COMPLETED) ======┘                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 상태 전이 다이어그램

```
CREATED
   │
   ▼
PAYMENT_PROCESSING
   │
   ├── 결제 성공 ──▶ INVENTORY_PROCESSING
   │                      │
   │                      ├── 재고 확보 ──▶ COMPLETED  ✅
   │                      │
   │                      └── 재고 부족 ──▶ FAILED     ❌
   │                                     (환불 필요)
   │
   └── 결제 실패 ──▶ PAYMENT_FAILED        ❌
```

---

## 4. 보상 트랜잭션 (Compensating Transaction)

Saga 패턴에서 중간 단계가 실패하면, 이미 완료된 이전 단계를 **되돌리는 작업**이 필요합니다.

### 이 시스템의 보상 트랜잭션 시나리오

| 실패 지점 | 보상 트랜잭션 |
|-----------|--------------|
| 결제 실패 | 보상 불필요 (아직 아무것도 변경되지 않음) |
| 재고 부족 | 결제 환불 처리 필요 (이미 결제 완료 상태) |

```
[재고 부족 시 보상 트랜잭션 흐름]

결제 성공 → 재고 확인 → 재고 부족 발생!
                           │
                           ▼
                  INVENTORY_FAILED 이벤트 발행
                           │
                           ▼
              주문 서비스: 상태를 FAILED로 변경
              + 로그: "결제 환불이 필요합니다" (실제 환불 로직 추가 가능)
```

> **참고**: 이 학습 프로젝트에서는 보상 트랜잭션을 로그로만 기록합니다.
> 실제 프로덕션에서는 환불 API 호출 등의 구체적인 보상 로직이 필요합니다.

---

## 5. 이벤트 흐름 다이어그램

```
                        Kafka Topics
                   ┌────────────────────┐
                   │                    │
  POST /orders     │   order.created    │
  ───────────┐     │                    │
             ▼     │                    │
  ┌─────────────┐  │                    │  ┌─────────────────┐
  │   Order     │──┼───발행──────────────┼─▶│   Payment       │
  │   Service   │  │                    │  │   Service       │
  │             │  │  payment.result    │  │                 │
  │             │◀─┼────────────────────┼──│   (80% 성공)     │
  │             │  │                    │  └─────────────────┘
  │             │  │                    │           │
  │             │  │  payment.result    │           │
  │             │  │  (COMPLETED만)      │           ▼
  │             │  │                    │  ┌─────────────────┐
  │             │  │  inventory.result  │  │   Inventory     │
  │             │◀─┼────────────────────┼──│   Service       │
  │             │  │                    │  │                 │
  └─────────────┘  │                    │  │   (90% 성공)     │
                   │                    │  └─────────────────┘
                   └────────────────────┘
```

### 토픽별 이벤트 타입

| 토픽 | 이벤트 타입 | 발행 서비스 | 구독 서비스 |
|------|------------|-----------|-----------|
| `order.created` | ORDER_CREATED | Order Service | Payment Service |
| `payment.result` | PAYMENT_COMPLETED | Payment Service | Order Service, Inventory Service |
| `payment.result` | PAYMENT_FAILED | Payment Service | Order Service |
| `inventory.result` | INVENTORY_RESERVED | Inventory Service | Order Service |
| `inventory.result` | INVENTORY_FAILED | Inventory Service | Order Service |

---

## 6. 실행 방법

### 전체 서비스 시작

```bash
cd 09-real-world-order-system
docker-compose up --build
```

### 서비스 접속 URL

| 서비스 | URL |
|--------|-----|
| 주문 서비스 API | http://localhost:8000/docs |
| 결제 서비스 API | http://localhost:8001/docs |
| 재고 서비스 API | http://localhost:8002/docs |
| Kafka UI | http://localhost:8080 |

### 서비스 종료

```bash
docker-compose down -v
```

---

## 7. 실습: 주문 생성부터 최종 상태 확인까지

### Step 1: 주문 생성

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-001",
    "items": [
      {
        "product_id": "prod-001",
        "product_name": "맥북 프로 16인치",
        "quantity": 1,
        "price": 3990000
      },
      {
        "product_id": "prod-002",
        "product_name": "에어팟 프로",
        "quantity": 2,
        "price": 359000
      }
    ]
  }'
```

### Step 2: 각 서비스 로그 확인

```bash
# 전체 로그 확인
docker-compose logs -f

# 서비스별 로그 확인
docker-compose logs -f order-service
docker-compose logs -f payment-service
docker-compose logs -f inventory-service
```

예상 로그 흐름 (성공 케이스):
```
order-service      | [주문 생성] order_id=abc-123, total=4708000
order-service      | [발행] order.created 토픽 → order_id=abc-123
payment-service    | [수신] 주문 생성 이벤트 → order_id=abc-123
payment-service    | [결제 성공] order_id=abc-123, amount=4708000
payment-service    | [발행] payment.result 토픽 → order_id=abc-123
order-service      | [상태 변경] order_id=abc-123 → INVENTORY_PROCESSING
inventory-service  | [수신] 결제 성공 이벤트 → order_id=abc-123
inventory-service  | [재고 확보 성공] order_id=abc-123
inventory-service  | [발행] inventory.result 토픽 → order_id=abc-123
order-service      | [상태 변경] order_id=abc-123 → COMPLETED
```

### Step 3: 최종 주문 상태 확인

```bash
# 특정 주문 상태 조회 (order_id를 Step 1의 응답에서 복사)
curl http://localhost:8000/orders/{order_id} | python3 -m json.tool

# 전체 주문 목록 조회
curl http://localhost:8000/orders | python3 -m json.tool

# 결제 기록 확인
curl http://localhost:8001/payments | python3 -m json.tool

# 재고 처리 기록 확인
curl http://localhost:8002/inventory | python3 -m json.tool
```

### Step 4: 여러 번 주문하여 실패 케이스 확인

결제 성공률이 80%, 재고 확보 성공률이 90%이므로 여러 번 주문하면 다양한 상태를 확인할 수 있습니다.

```bash
# 반복 주문 테스트 (5번)
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8000/orders \
    -H "Content-Type: application/json" \
    -d "{
      \"user_id\": \"user-00$i\",
      \"items\": [{
        \"product_id\": \"prod-001\",
        \"product_name\": \"테스트 상품\",
        \"quantity\": 1,
        \"price\": 10000
      }]
    }"
  echo ""
done

# 잠시 후 전체 주문 상태 확인
sleep 5
curl http://localhost:8000/orders | python3 -m json.tool
```

### Step 5: Kafka UI에서 토픽 확인

브라우저에서 http://localhost:8080 접속 후:
1. **Topics** 메뉴에서 `order.created`, `payment.result`, `inventory.result` 토픽 확인
2. 각 토픽의 **Messages** 탭에서 실제 전송된 이벤트 내용 확인

---

## 8. 핵심 코드 해설

### 8.1 이벤트 모델 (shared/events.py)

모든 서비스가 동일한 이벤트 스키마를 공유합니다. Pydantic 모델로 정의하여 타입 안전성을 보장합니다.

```python
class OrderCreated(BaseModel):
    """주문 생성 이벤트"""
    event_type: str = "ORDER_CREATED"
    order_id: str
    user_id: str
    items: List[OrderItem]
    total_price: float
    timestamp: str
```

### 8.2 Saga 상태 관리 (order-service/app/models.py)

주문의 상태를 열거형으로 정의하고, 상태 전이 이력을 기록합니다.

```python
class OrderStatus(str, Enum):
    CREATED = "CREATED"
    PAYMENT_PROCESSING = "PAYMENT_PROCESSING"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    INVENTORY_PROCESSING = "INVENTORY_PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
```

### 8.3 이벤트 발행 (Producer 패턴)

`aiokafka`의 `AIOKafkaProducer`를 사용하여 비동기로 이벤트를 발행합니다. `order_id`를 키로 사용하면 동일 주문의 이벤트가 같은 파티션으로 전송되어 순서가 보장됩니다.

```python
await producer.send_and_wait(
    topic=TOPIC_ORDER_CREATED,
    key=order_id,        # 같은 주문 = 같은 파티션
    value=event_data,    # JSON 직렬화된 이벤트 데이터
)
```

### 8.4 이벤트 구독 (Consumer 패턴)

`AIOKafkaConsumer`를 백그라운드 태스크로 실행하여 FastAPI와 동시에 동작합니다.

```python
# 여러 토픽을 동시에 구독
consumer = AIOKafkaConsumer(
    TOPIC_PAYMENT_RESULT,
    TOPIC_INVENTORY_RESULT,
    group_id=CONSUMER_GROUP_ID,
)
```

### 8.5 Lifespan으로 리소스 관리

FastAPI의 `lifespan` 컨텍스트 매니저로 프로듀서/컨슈머의 시작과 종료를 관리합니다.

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 시작: 프로듀서 + 컨슈머 백그라운드 태스크
    await start_producer()
    consumer_task = asyncio.create_task(start_consumer())
    yield
    # 종료: 정리 작업
    consumer_task.cancel()
    await stop_producer()
```
