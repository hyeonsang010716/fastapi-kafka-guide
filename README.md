# FastAPI + Kafka 스터디

> Kafka를 모르는 백엔드 개발자를 위한, FastAPI 기반 단계별 Kafka 학습 가이드 (내가 공부하기 위한)

## 소개

이 스터디는 **각 디렉토리가 독립적으로 실행 가능한 프로젝트**로 구성되어 있습니다.
`docker-compose up` 한 번이면 Kafka 클러스터부터 FastAPI 앱까지 모두 실행됩니다.

## 사전 준비

| 항목 | 버전 | 비고 |
|------|------|------|
| Docker Desktop | 최신 | [설치 가이드](https://docs.docker.com/desktop/) |
| Python | 3.12+ | `python3 --version`으로 확인 |
| Docker Compose | v2+ | Docker Desktop에 포함 |

## 기술 스택

| 기술 | 역할 |
|------|------|
| **FastAPI** | 웹 프레임워크 |
| **aiokafka** | 비동기 Kafka 클라이언트 (FastAPI의 async/await와 자연스럽게 통합) |
| **Apache Kafka (KRaft)** | ZooKeeper 없이 실행되는 Kafka 브로커 |
| **kafbat/kafka-ui** | 토픽, 파티션, 컨슈머 그룹을 시각적으로 확인하는 웹 UI |
| **Docker Compose** | 인프라 + 앱을 한 번에 실행 |

## 커리큘럼

### Phase 1: 기초 다지기

| 챕터 | 주제 | 핵심 내용 |
|------|------|-----------|
| [00-environment-setup](./00-environment-setup/) | 환경 설정 | Docker, Python 설치 및 확인 |
| [01-kafka-basics](./01-kafka-basics/) | Kafka 개념 이해 | Broker, Topic, Partition, Offset 개념 + 첫 클러스터 실행 |
| [02-first-producer](./02-first-producer/) | 첫 번째 Producer | FastAPI에서 Kafka로 메시지 보내기 |
| [03-first-consumer](./03-first-consumer/) | 첫 번째 Consumer | FastAPI에서 백그라운드로 메시지 수신하기 |

### Phase 2: 핵심 메커니즘

| 챕터 | 주제 | 핵심 내용 |
|------|------|-----------|
| [04-topics-and-partitions](./04-topics-and-partitions/) | 토픽과 파티션 | 파티션 분배, Message Key, 순서 보장 |
| [05-consumer-groups](./05-consumer-groups/) | 컨슈머 그룹 | 수평 확장, 리밸런싱, 수동 오프셋 커밋 |
| [06-message-serialization](./06-message-serialization/) | 메시지 직렬화 | JSON 직렬화, Pydantic 스키마 검증, 메시지 헤더 |

### Phase 3: 안정성 확보

| 챕터 | 주제 | 핵심 내용 |
|------|------|-----------|
| [07-error-handling-and-retry](./07-error-handling-and-retry/) | 에러 처리와 재시도 | 재시도 전략, Dead Letter Queue(DLQ), acks 설정 |
| [08-exactly-once-and-idempotency](./08-exactly-once-and-idempotency/) | 멱등성과 정확히 한 번 전달 | 멱등성 프로듀서, Consumer 측 중복 방지 (Redis) |

### Phase 4: 실전 프로젝트

| 챕터 | 주제 | 핵심 내용 |
|------|------|-----------|
| [09-saga-pattern-order-system](./09-saga-pattern-order-system/) | 주문 처리 시스템 | 이벤트 드리븐 아키텍처, Saga 패턴, 보상 트랜잭션 |
| [10-monitoring-and-testing](./10-monitoring-and-testing/) | 모니터링과 테스트 | Consumer Lag, 헬스체크, testcontainers 통합 테스트 |
| [11-multi-service-architecture](./11-multi-service-architecture/) | 마이크로서비스 아키텍처 | API Gateway, 알림 서비스, CQRS 패턴, 3-broker 클러스터 |
| [12-outbox-pattern](./12-outbox-pattern/) | Transactional Outbox | DB ↔ Kafka 일관성, 폴링 릴레이(`FOR UPDATE SKIP LOCKED`), `event_id` 기반 멱등 컨슈머 |
| [13-resilience-patterns](./13-resilience-patterns/) | Resilience 패턴 | Timeout, Retry+Backoff, **Circuit Breaker**, Bulkhead — cascading failure 차단 |

## 학습 로드맵

```
   Phase 1: 기초             Phase 2: 핵심            Phase 3: 안정성          Phase 4: 실전
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│ 00 환경 설정       │    │ 04 토픽과 파티션     │    │ 07 에러 처리/DLQ   │    │ 09 주문 시스템      │
│        ↓         │    │        ↓         │    │        ↓         │    │        ↓         │
│ 01 Kafka 개념     │ →  │ 05 컨슈머 그룹      │ →  │ 08 멱등성/중복방지   │ →  │ 10 모니터링/테스트   │
│        ↓         │    │        ↓         │    │                  │    │        ↓         │
│ 02 Producer      │    │ 06 메시지 직렬화    │    │                  │    │  11 마이크로서비스   │
│        ↓         │    │                  │    │                  │    │        ↓         │
│ 03 Consumer      │    │                  │    │                  │    │  12 Outbox 패턴    │
│                  │    │                  │    │                  │    │        ↓         │
│                  │    │                  │    │                  │    │ 13 Resilience    │
└──────────────────┘    └──────────────────┘    └──────────────────┘    └──────────────────┘
```

## 각 챕터의 공통 구조

모든 챕터의 README는 다음 섹션으로 구성됩니다:

1. **이 챕터에서 배우는 것** - 학습 목표
2. **핵심 개념 설명** - 다이어그램 포함
3. **사전 준비** - 이전 챕터 완료 여부
4. **실행 방법** - `docker-compose up` 한 줄로 실행
5. **실습 가이드** - 단계별 따라하기
6. **확인 방법** - kafka-ui에서 결과 확인
7. **핵심 코드 해설** - 중요한 코드 설명
8. **자주 묻는 질문**
9. **다음 챕터 미리보기**

## 챕터별 상세 설명

---

### 00 - 환경 설정

개발 환경을 준비합니다. 코드 없이 순수 가이드 문서입니다.

- Docker Desktop 설치 및 확인
- Python 3.12+ 설치 및 가상환경(venv) 사용법
- `docker-compose` 기본 명령어

---

### 01 - Kafka 기본 개념

**Kafka가 무엇인지**, 왜 필요한지를 이해하고 로컬에서 첫 Kafka 클러스터를 실행합니다.

```
HTTP 요청 방식 (동기)          Kafka 메시지 방식 (비동기)
┌────────┐    ┌────────┐     ┌────────┐    ┌───────┐    ┌────────┐
│ 서비스A  │───→│ 서비스B │     │ 서비스A  │───→│ Kafka │───→│ 서비스B  │
└────────┘    └────────┘     └────────┘    └───────┘    └────────┘
  서비스B가 죽으면?              서비스B가 죽어도
  → 요청 실패!                  → 메시지는 Kafka에 보관!
```

**핵심 개념:**
- Broker, Topic, Partition, Offset
- KRaft 모드 (ZooKeeper 없이 실행)
- kafka CLI로 토픽 생성/조회
- kafka-ui로 클러스터 상태 확인

---

### 02 - 첫 번째 Producer

FastAPI 엔드포인트에서 Kafka로 메시지를 보내는 Producer를 구현합니다.

```
┌──────────────────────┐         ┌───────┐
│  FastAPI             │         │       │
│  POST /messages ─────│────────→│ Kafka │
│                      │         │       │
│  GET /health         │         └───────┘
└──────────────────────┘
```

**배우는 것:**
- `aiokafka.AIOKafkaProducer` 사용법
- FastAPI `lifespan`으로 Producer 생명주기 관리
- `send_and_wait()` vs `send()`
- Message Key와 Value

---

### 03 - 첫 번째 Consumer

Kafka에서 메시지를 수신하는 Consumer를 FastAPI 앱 내 백그라운드 태스크로 실행합니다.

```
┌──────────┐         ┌───────┐         ┌──────────┐
│ Producer │────────→│ Kafka │────────→│ Consumer │
│ (FastAPI)│         │       │         │ (FastAPI)│
│ :8000    │         └───────┘         │ :8001    │
└──────────┘                           └──────────┘
```

**배우는 것:**
- `aiokafka.AIOKafkaConsumer` 사용법
- `asyncio.create_task()`로 백그라운드 Consumer 실행
- `auto_offset_reset`: `earliest` vs `latest`
- Offset과 자동 커밋

---

### 04 - 토픽과 파티션

파티션이 무엇인지, 메시지가 어떤 파티션으로 가는지, 파티션이 처리량에 미치는 영향을 이해합니다.

```
                    Topic: orders (3 partitions)
                    ┌──────────────────────────┐
 key: order-1 ────→ │ Partition 0: [msg1, msg4]│
 key: order-2 ────→ │ Partition 1: [msg2, msg5]│
 key: order-3 ────→ │ Partition 2: [msg3, msg6]│
                    └──────────────────────────┘
                    같은 key → 항상 같은 partition → 순서 보장!
```

**배우는 것:**
- 파티션 = 순서 보장의 단위
- Message Key 기반 파티션 배정
- AdminClient로 토픽 정보 조회

---

### 05 - 컨슈머 그룹

컨슈머 그룹을 이용한 **수평 확장**과 리밸런싱을 이해합니다.

```
                    Topic (3 partitions)
                    ┌────────────┐
                    │ Partition 0│──→ Consumer 1 ┐
                    │ Partition 1│──→ Consumer 2 ├─ Group: "order-group"
                    │ Partition 2│──→ Consumer 3 ┘
                    └────────────┘
                    1 파티션 = 최대 1 컨슈머 (같은 그룹 내)
```

**배우는 것:**
- Consumer Group의 개념
- 파티션-컨슈머 할당 관계
- 리밸런싱 (Consumer 추가/제거 시)
- 수동 오프셋 커밋

---

### 06 - 메시지 직렬화

메시지를 구조화된 형태(JSON)로 직렬화/역직렬화하는 방법을 익힙니다.

**배우는 것:**
- Kafka는 bytes만 전송한다
- JSON 직렬화/역직렬화
- Pydantic 모델 기반 스키마 검증
- 메시지 헤더(headers) 활용
- (소개) Avro / Schema Registry

**시나리오:** `UserCreatedEvent` - 사용자 가입 이벤트를 구조화된 JSON으로 전송/수신

---

### 07 - 에러 처리와 재시도

메시지 처리 실패 시 **재시도**와 **Dead Letter Queue(DLQ)** 패턴을 구현합니다.

```
                    처리 성공 ──→ 커밋
                   /
메시지 수신 → 처리 시도
                   \
                    처리 실패 → 재시도 (최대 3회)
                                  \
                                   여전히 실패 → DLQ 토픽으로 이동
```

**배우는 것:**
- Producer `acks` 설정 (`all` vs `1` vs `0`)
- Exponential Backoff 재시도 전략
- Dead Letter Queue 패턴
- 수동 오프셋 커밋으로 "처리 완료" 보장

**시나리오:** 결제 처리 시뮬레이션 - 일정 확률로 실패, 3회 재시도 후 DLQ로 이동

---

### 08 - 멱등성과 정확히 한 번 전달

메시지 **중복 처리를 방지**하는 패턴을 이해합니다.

```
전달 보장 수준:

At-most-once    → 유실 가능, 중복 없음     → 로그 수집
At-least-once   → 유실 없음, 중복 가능     → 대부분의 시스템
Exactly-once    → 유실 없음, 중복 없음     → 결제, 포인트 (구현 어려움)
```

**배우는 것:**
- 멱등성 프로듀서 (`enable_idempotence=True`)
- Consumer 측 멱등성 구현 (Redis 기반 중복 체크)
- 왜 Exactly-once가 어려운가

**시나리오:** 포인트 적립 시스템 - 같은 메시지가 두 번 와도 포인트는 한 번만 적립

---

### 09 - 실전: 주문 처리 시스템

3개의 마이크로서비스로 구성된 **이벤트 드리븐 주문 처리 시스템**을 구현합니다.

```
POST /orders
     │
     ▼
┌─────────────┐  order.created   ┌─────────────────┐
│   Order     │ ───────────────→ │     Payment     │
│   Service   │                  │     Service     │
│   :8000     │ ←─────────────── │     :8001       │
└─────────────┘  payment.result  └─────────────────┘
     │                                   │
     │                          payment.completed
     │                                   │
     │                                   ▼
     │                           ┌─────────────────┐
     │  inventory.result         │    Inventory    │
     │ ←──────────────────────── │     Service     │
     │                           │     :8002       │
     ▼                           └─────────────────┘
 주문 상태 업데이트
 (완료 / 실패)
```

**배우는 것:**
- 이벤트 드리븐 아키텍처 설계
- Saga 패턴 (Choreography 방식)
- 보상 트랜잭션 (실패 시 롤백)

---

### 10 - 모니터링과 테스트

Kafka 시스템의 **모니터링, 헬스체크, 통합 테스트**를 구현합니다.

**배우는 것:**
- Consumer Lag 모니터링
- FastAPI 헬스체크 엔드포인트
- `pytest` + `testcontainers`로 통합 테스트
- `structlog`로 구조화된 로깅

---

### 11 - 마이크로서비스 아키텍처 (최종 프로젝트)

여러 마이크로서비스가 Kafka를 통해 통신하는 **완전한 시스템**을 구축합니다.

```
┌──────────────────────────────────────────────────────┐
│                    API Gateway                       │
│                     :8000                            │
└───────────┬──────────┬───────────────┬───────────────┘
            │          │               │
            ▼          ▼               ▼
     ┌──────────┐ ┌──────────┐ ┌──────────────┐
     │ Order    │ │ Payment  │ │   Inventory  │
     │ Service  │ │ Service  │ │   Service    │
     └────┬─────┘ └────┬─────┘ └──────┬───────┘
          │            │               │
          ▼            ▼               ▼
     ┌─────────────────────────────────────────┐
     │         Kafka Cluster (3 Brokers)       │
     └─────────────────────────────────────────┘
          │                        │
          ▼                        ▼
     ┌──────────────┐      ┌──────────────┐
     │ Notification │      │ Query        │
     │ Service      │      │ Service      │
     │ (알림)        │      │ (CQRS 조회)   │
     └──────────────┘      └──────────────┘
```

**배우는 것:**
- API Gateway 패턴
- CQRS (Command/Query 분리) 패턴 소개
- 이벤트 소싱 개념 소개
- 3-broker 클러스터 구성
- 프로덕션 체크리스트

---

### 12 - Transactional Outbox 패턴

DB 쓰기와 Kafka 발행을 하나의 트랜잭션으로 묶는 **dual-write 문제**를 푸는 가장 실무적인 방법인 **Outbox 패턴**을 구현합니다.

```
POST /orders
     │
     ▼
┌─────────────────────────────────────┐
│  단일 DB 트랜잭션                       │
│   INSERT INTO orders                 │
│   INSERT INTO outbox                 │
│   COMMIT                             │
└──────────────┬──────────────────────┘
               │
               ▼
        ┌──────────────┐    poll (FOR UPDATE SKIP LOCKED)
        │  PostgreSQL  │ ◀─────────────┐
        │  outbox      │               │
        └──────────────┘     ┌─────────┴─────────┐
                             │  Outbox Relay     │
                             │  (백그라운드 폴러)    │
                             └─────────┬─────────┘
                                       ▼
                                   ┌───────┐
                                   │ Kafka │
                                   └───┬───┘
                                       ▼
                              event_id 기반 멱등 컨슈머
```

**배우는 것:**
- Dual-write 문제와 왜 try/except, 2PC, Kafka Transactions가 답이 아닌가
- `orders` + `outbox` 테이블을 같은 DB 트랜잭션으로 묶기
- 폴링 릴레이 패턴 (`SELECT ... FOR UPDATE SKIP LOCKED`)으로 안전한 동시성
- `event_id`(UUID) + dedup 테이블로 At-least-once를 멱등 처리로 흡수
- **Inbox 패턴**: 컨슈머 측 dual-write 도 같은 트랜잭션으로 묶기 (Outbox 의 짝꿍)
- Polling Relay vs Debezium(CDC) 트레이드오프
- Kafka가 죽어도 사용자 트랜잭션은 흔들리지 않는 장애 격리

---

### 13 - Resilience Patterns

외부 의존이 죽었을 때 우리 시스템 *전체* 가 같이 무너지지 않게 하는 **Resilience 패턴 4종** 을 직접 구현합니다.

```
[POST /payments]
     │
     ▼
   retry + backoff      ← 일시적 흔들림을 흡수
     │
     ▼
   circuit breaker      ← 죽은 게이트웨이로의 호출을 즉시 차단
     │  (CLOSED → OPEN → HALF_OPEN → CLOSED)
     ▼
   timeout              ← 응답 없는 호출을 빨리 끊음
     │
     ▼
   외부 결제 게이트웨이
   (HEALTHY / SLOW / DEAD / FLAKY 모드를 흉내내는 mock)
```

**배우는 것:**
- "Resilience" 라는 우산 용어와 그 안에 자리잡은 패턴들의 지도
- **Cascading failure(연쇄 장애)** 가 어떻게 시스템 전체를 죽이는가 + 왜 retry 만으론 못 막는가
- **Circuit Breaker** 직접 구현 (CLOSED / OPEN / HALF_OPEN 상태 머신)
- Timeout / Retry / Circuit Breaker 를 어떤 *순서* 로 쌓아야 하는가
- 모드 변경이 가능한 mock 게이트웨이로 회로 동작을 두 눈으로 확인

## 패키지 의존성 요약

| 패키지 | 용도 | 도입 챕터 |
|--------|------|-----------|
| `fastapi` | 웹 프레임워크 | 02 |
| `uvicorn` | ASGI 서버 | 02 |
| `aiokafka` | 비동기 Kafka 클라이언트 | 02 |
| `pydantic-settings` | 환경변수 설정 관리 | 02 |
| `redis` | 멱등성 키 저장 | 08 |
| `structlog` | 구조화된 로깅 | 10 |
| `pytest` + `testcontainers` | 통합 테스트 | 10 |
| `httpx` | 비동기 HTTP 클라이언트 (테스트용) | 10 |
| `sqlalchemy` + `asyncpg` | PostgreSQL 비동기 ORM | 12 |
| `httpx` | 외부 HTTP 호출 (resilience 시연) | 13 |

## 빠른 시작

```bash
# 1. 레포 클론
git clone <repository-url>
cd fastapi-kafka

# 2. 원하는 챕터로 이동
cd 01-kafka-basics

# 3. 실행 (Kafka + 앱이 모두 뜹니다)
docker-compose up

# 4. kafka-ui 접속
open http://localhost:8080
```

## 참고 자료

- [Apache Kafka 공식 문서](https://kafka.apache.org/documentation/)
- [aiokafka 문서](https://aiokafka.readthedocs.io/)
- [FastAPI 공식 문서](https://fastapi.tiangolo.com/)
- [kafbat/kafka-ui](https://github.com/kafbat/kafka-ui)
