# Chapter 11: 멀티 서비스 아키텍처 (Capstone)

> FastAPI + Kafka 학습 시리즈의 최종 캡스톤 프로젝트
> 6개의 마이크로서비스가 Kafka 3-broker 클러스터를 통해 비동기 통신하는 실전 아키텍처

---

## 1. 이 챕터에서 배우는 것

| 주제 | 설명 |
|------|------|
| **API Gateway 패턴** | 모든 클라이언트 요청의 단일 진입점 구현 |
| **CQRS 패턴** | 명령(Command)과 조회(Query) 책임 분리 |
| **이벤트 소싱 개념** | 상태 변경을 이벤트 로그로 기록하고 재구성 |
| **3-Broker 클러스터** | KRaft 모드 Kafka 클러스터의 고가용성 구성 |
| **Saga 패턴** | 분산 트랜잭션을 이벤트 기반 보상 트랜잭션으로 처리 |
| **서비스 간 느슨한 결합** | Kafka를 메시지 버스로 활용한 비동기 통신 |
| **헬스 체크 집계** | Gateway에서 전체 서비스 상태를 한번에 확인 |

---

## 2. 전체 아키텍처 다이어그램

```
                         ┌──────────────────────────────────────────────┐
                         │              Client (curl / Browser)         │
                         └───────────────────┬──────────────────────────┘
                                             │
                                             ▼
                         ┌──────────────────────────────────────────────┐
                         │          API Gateway (포트 8000)              │
                         │  POST /api/orders        → order-service     │
                         │  GET  /api/orders        → order-service     │
                         │  GET  /api/notifications → notification-svc  │
                         │  GET  /api/query/orders  → query-service     │
                         │  GET  /health            → 전체 서비스 집계      │ 
                         └──────┬──────────┬───────────┬────────────────┘
                                │          │           │
                    ┌───────────┘          │           └──────────┐
                    ▼                      ▼                      ▼
            ┌──────────────┐    ┌────────────────────┐   ┌──────────────────┐
            │ order-service│    │notification-service│   │  query-service   │
            │  (포트 8001)  │    │    (포트 8004)      │   │  (포트 8005)      │
            │              │    │                    │   │  CQRS 읽기 모델    │
            └──────┬───────┘    └────────▲───────────┘   └───────▲──────────┘
                   │                     │                       │
                   │  order.created      │ 모든 토픽 구독         │ 모든 토픽 구독
                   ▼                     │                       │
    ┌──────────────────────────────────────────────────────────────────────┐
    │                                                                      │
    │              Kafka 3-Broker 클러스터 (KRaft 모드)                       │
    │                                                                      │
    │      kafka-0 (controller+broker)  kafka-1 (broker)  kafka-2 (broker) │
    │                                                                      │
    │        토픽: order.created │ payment.result │ inventory.result        │
    │                        notification.sent                             │
    │                                                                      │
    └───────────────┬──────────────────────┬───────────────────────────────┘
                    │                      │
                    ▼                      ▼
          ┌──────────────────┐   ┌────────────────────┐
          │ payment-service  │   │ inventory-service  │
          │   (포트 8002)     │   │    (포트 8003)      │
          │                  │   │                    │
          │ order.created →  │   │ order.created →    │
          │ payment.result   │   │ inventory.result   │
          └──────────────────┘   └────────────────────┘
```

### 이벤트 흐름 (Event Flow)

```
1. 클라이언트 → Gateway → order-service: POST /api/orders (주문 생성)

2. order-service → Kafka [order.created]: 주문 생성 이벤트 발행

3. 이벤트 소비 (병렬):
   ├─ payment-service: 결제 처리 → Kafka [payment.result]
   ├─ inventory-service: 재고 확인 → Kafka [inventory.result]
   ├─ notification-service: "주문 접수" 알림 발송
   └─ query-service: 읽기 모델에 주문 추가

4. 결과 이벤트 소비:
   ├─ order-service: 주문 상태 업데이트
   ├─ notification-service: "결제 완료/재고 확인" 알림 발송
   └─ query-service: 읽기 모델 업데이트

5. 클라이언트 → Gateway → query-service: GET /api/query/orders (통합 조회)
```

---

## 3. API Gateway 패턴

### 왜 API Gateway가 필요한가?

마이크로서비스 아키텍처에서 클라이언트가 각 서비스에 직접 접근하면:
- 클라이언트가 모든 서비스의 주소를 알아야 함
- 서비스 추가/변경 시 클라이언트도 수정 필요
- 인증/인가, 로깅, 레이트 리밋 등 공통 관심사를 각 서비스에 구현해야 함

**API Gateway는 이 문제를 해결하는 단일 진입점(Single Entry Point)입니다.**

### 이 프로젝트의 Gateway 구현

```python
# httpx 비동기 HTTP 클라이언트로 내부 서비스에 요청 프록시
async def proxy_request(method, service_url, path, body=None):
    url = f"{service_url}{path}"
    response = await http_client.request(method=method, url=url, json=body)
    return JSONResponse(status_code=response.status_code, content=response.json())
```

### Gateway의 주요 역할

| 역할 | 설명 |
|------|------|
| 요청 라우팅 | URL 경로 기반으로 적절한 서비스로 전달 |
| 헬스 체크 집계 | 모든 서비스 상태를 동시에 확인하여 통합 결과 반환 |
| 에러 핸들링 | 서비스 장애 시 503, 타임아웃 시 504 반환 |
| 프로토콜 통일 | 클라이언트는 Gateway의 API만 알면 됨 |

### 프로덕션에서의 API Gateway

실제 프로덕션에서는 다음을 추가로 구현합니다:
- **인증/인가**: JWT 토큰 검증
- **레이트 리밋**: 과도한 요청 제한
- **서킷 브레이커**: 장애 서비스 자동 차단
- **로드 밸런싱**: 서비스 인스턴스 간 부하 분산
- **요청 변환**: API 버전 관리, 요청/응답 변환

---

## 4. CQRS 패턴 설명

### CQRS (Command Query Responsibility Segregation)

**명령(쓰기)과 조회(읽기)의 책임을 분리하는 패턴**

```
                    ┌─────────────────────┐
                    │      Client         │
                    └──────┬──────┬───────┘
                           │      │
                   Command │      │ Query
                   (쓰기)   │      │ (읽기)
                           ▼      ▼
                  ┌──────────┐  ┌──────────┐
                  │  order   │  │  query   │
                  │ service  │  │ service  │
                  │ (Write)  │  │ (Read)   │
                  └────┬─────┘  └────▲─────┘
                       │             │
                       │  이벤트       │ 이벤트 소비
                       ▼             │
                    ┌──────────────────┐
                    │     Kafka        │
                    └──────────────────┘
```

### 왜 CQRS를 사용하는가?

| 장점 | 설명 |
|------|------|
| **독립적 확장** | 읽기가 많으면 query-service만 스케일 아웃 |
| **최적화된 모델** | 쓰기는 정규화, 읽기는 비정규화된 모델 사용 |
| **성능 향상** | 읽기 시 JOIN 없이 미리 집계된 데이터 반환 |
| **느슨한 결합** | 쓰기 모델 변경이 읽기 모델에 직접 영향 없음 |

### 이 프로젝트의 CQRS 구현

**쓰기 모델 (order-service):**
- 주문 생성 API → Kafka로 이벤트 발행
- 정규화된 주문 데이터 저장

**읽기 모델 (query-service):**
- 모든 이벤트 토픽 구독 → 인메모리 읽기 뷰 구축
- 주문 + 결제 + 재고 정보를 하나의 비정규화된 뷰로 집계
- 필터링, 통계 등 다양한 조회 API 제공

### 최종 일관성 (Eventual Consistency)

CQRS에서 쓰기와 읽기 모델 사이에는 약간의 지연이 있습니다:
- 주문 생성 직후 query-service에서 바로 조회되지 않을 수 있음
- Kafka를 통해 이벤트가 전달된 후에야 읽기 모델이 업데이트됨
- 이를 **최종 일관성(Eventual Consistency)**이라 하며, 결국에는 일관된 상태가 됨

---

## 5. 이벤트 소싱 개념 소개

### 이벤트 소싱 (Event Sourcing)이란?

**현재 상태를 직접 저장하는 대신, 상태를 변경한 모든 이벤트를 시간순으로 기록하는 패턴**

```
전통적인 방식 (상태 저장):
  주문 테이블: { order_id: "123", status: "COMPLETED", ... }
  → 마지막 상태만 알 수 있음

이벤트 소싱 방식 (이벤트 기록):
  이벤트 로그:
    1. ORDER_CREATED    { order_id: "123", items: [...] }     10:00:00
    2. PAYMENT_COMPLETED { order_id: "123", amount: 50000 }   10:00:02
    3. INVENTORY_RESERVED { order_id: "123", warehouse: "WH-001" } 10:00:03
  → 이벤트를 순서대로 적용하면 현재 상태를 재구성 가능
  → "왜 이 상태가 되었는지" 전체 히스토리를 추적 가능
```

### query-service의 이벤트 소싱 구현

```python
# 이벤트를 적용하여 현재 상태 도출 (이벤트 소싱의 핵심)
def process_event(event, topic):
    event_type = event.get("event_type")
    order_id = event.get("order_id")

    # 이벤트 로그에 기록 (전체 히스토리)
    event_log.append({...})

    # 이벤트 타입에 따라 읽기 모델 업데이트
    if event_type == "ORDER_CREATED":
        orders_read_model[order_id] = { ... }  # 새 뷰 생성
    elif event_type == "PAYMENT_COMPLETED":
        orders_read_model[order_id]["payment_status"] = "COMPLETED"  # 상태 반영
```

### 이벤트 소싱의 장점

| 장점 | 설명 |
|------|------|
| **완전한 감사 로그** | 모든 변경 이력이 자동으로 기록됨 |
| **시간 여행 쿼리** | 특정 시점의 상태를 재구성 가능 |
| **디버깅 용이** | "왜 이 상태가 되었는지" 이벤트 추적 가능 |
| **다양한 읽기 뷰** | 같은 이벤트로 다양한 목적의 뷰 생성 가능 |

---

## 6. 3-Broker 클러스터 구성 설명

### KRaft 모드 Kafka 클러스터

```
┌─────────────────────────────────────────────────────────────┐
│                    KRaft 클러스터                             │
│                                                             │
│  ┌───────────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │    kafka-0        │  │   kafka-1    │  │   kafka-2    │  │
│  │                   │  │              │  │              │  │
│  │ [Controller]      │  │ [Broker]     │  │ [Broker]     │  │
│  │ [Broker]          │  │              │  │              │  │
│  │                   │  │              │  │              │  │
│  │ 메타데이터 관리       │  │ 데이터 저장     │  │ 데이터 저장     │  │
│  │ + 데이터 저장        │  │              │  │              │  │
│  │                   │  │              │  │              │  │
│  │ 포트: 9092, 9093   │  │ 포트: 9092    │  │ 포트: 9092     │  │
│  └───────────────────┘  └──────────────┘  └──────────────┘  │
│                                                             │
│  Replication Factor: 3  │  Min ISR: 2  │  Partitions: 3     │
└─────────────────────────────────────────────────────────────┘
```

### 핵심 설정 항목

| 설정 | 값 | 설명 |
|------|-----|------|
| `PROCESS_ROLES` | controller,broker / broker | 노드의 역할 지정 |
| `CONTROLLER_QUORUM_VOTERS` | 0@kafka-0:9093 | 컨트롤러 투표자 목록 |
| `DEFAULT_REPLICATION_FACTOR` | 3 | 토픽 데이터를 3개 브로커에 복제 |
| `MIN_INSYNC_REPLICAS` | 2 | 최소 2개 레플리카 동기화 필요 |
| `NUM_PARTITIONS` | 3 | 토픽당 기본 3개 파티션 |

### KRaft vs Zookeeper

| 항목 | Zookeeper 모드 | KRaft 모드 |
|------|---------------|------------|
| 메타데이터 관리 | 외부 Zookeeper 클러스터 | Kafka 자체 내장 |
| 운영 복잡도 | Zookeeper + Kafka 이중 관리 | Kafka만 관리 |
| 장애 복구 속도 | 상대적으로 느림 | 빠른 컨트롤러 페일오버 |
| 확장성 | Zookeeper가 병목 | 메타데이터 파티셔닝 가능 |

### 고가용성 보장

- **Replication Factor 3**: 1개 브로커 장애 시에도 데이터 유실 없음
- **Min ISR 2**: 최소 2개 레플리카에 쓰기 확인 후 응답
- **acks=all**: 프로듀서가 모든 ISR 레플리카 확인을 기다림

---

## 7. 프로덕션 체크리스트

### 인프라

- [ ] Kafka 브로커를 별도의 서버/노드에 분산 배치
- [ ] 컨트롤러 노드를 3개 이상으로 구성 (쿼럼 안정성)
- [ ] 브로커 디스크 I/O 모니터링 (SSD 권장)
- [ ] 네트워크 대역폭 모니터링

### 데이터 저장

- [ ] 인메모리 저장소를 실제 데이터베이스로 교체 (PostgreSQL, MongoDB 등)
- [ ] 이벤트 로그를 영구 저장소에 보관 (이벤트 스토어)
- [ ] 데이터 백업 및 복구 전략 수립

### 서비스 안정성

- [ ] 서킷 브레이커 패턴 적용 (Circuitbreaker 라이브러리)
- [ ] 재시도 로직 고도화 (지수 백오프 + 지터)
- [ ] 데드 레터 큐(DLQ) 구성 (처리 실패 메시지 보관)
- [ ] 멱등성 키(Idempotency Key) 구현 (중복 이벤트 처리 방지)

### API Gateway

- [ ] JWT 기반 인증/인가 추가
- [ ] 레이트 리밋 구현 (토큰 버킷 알고리즘)
- [ ] 서킷 브레이커 적용 (장애 서비스 자동 차단)
- [ ] 요청/응답 로깅 및 트레이싱 (OpenTelemetry)

### 모니터링

- [ ] Prometheus + Grafana 메트릭 수집
- [ ] 분산 트레이싱 (Jaeger / Zipkin)
- [ ] 구조화된 로깅 (JSON 포맷)
- [ ] 알림 설정 (Slack, PagerDuty 등)

### 배포

- [ ] Kubernetes 매니페스트 작성
- [ ] CI/CD 파이프라인 구축
- [ ] 블루-그린 또는 카나리 배포 전략
- [ ] 컨테이너 이미지 취약점 스캔

---

## 8. 실행 방법

### 사전 요구사항

- Docker 및 Docker Compose 설치
- 최소 4GB 이상의 메모리 (Kafka 3-broker + 6 서비스)

### 서비스 시작

```bash
# 전체 서비스 빌드 및 시작
docker compose up --build

# 백그라운드 실행
docker compose up --build -d

# 로그 확인
docker compose logs -f

# 특정 서비스 로그만 확인
docker compose logs -f order-service payment-service
```

### 서비스 접속 URL

| 서비스 | URL | 설명 |
|--------|-----|------|
| API Gateway | http://localhost:8000 | 클라이언트 진입점 |
| Gateway Docs | http://localhost:8000/docs | Swagger UI |
| Order Service | http://localhost:8001/docs | 주문 서비스 |
| Payment Service | http://localhost:8002/docs | 결제 서비스 |
| Inventory Service | http://localhost:8003/docs | 재고 서비스 |
| Notification Service | http://localhost:8004/docs | 알림 서비스 |
| Query Service | http://localhost:8005/docs | CQRS 읽기 모델 |
| Kafka UI | http://localhost:8080 | Kafka 관리 도구 |

### 서비스 중지

```bash
# 서비스 중지 및 볼륨 삭제
docker compose down -v
```

---

## 9. 실습 가이드

### 실습 1: 주문 생성 및 전체 흐름 확인

```bash
# 1. Gateway를 통해 주문 생성
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-001",
    "items": [
      {"product_id": "PROD-001", "product_name": "노트북", "quantity": 1, "price": 1500000},
      {"product_id": "PROD-002", "product_name": "키보드", "quantity": 2, "price": 89000}
    ],
    "shipping_address": "서울시 강남구"
  }'

# 2. 잠시 대기 (이벤트 처리 시간)
sleep 3

# 3. Gateway를 통해 주문 목록 조회
curl http://localhost:8000/api/orders

# 4. 알림 목록 확인 (알림 서비스가 수신한 이벤트 기반 알림)
curl http://localhost:8000/api/notifications

# 5. CQRS 읽기 모델에서 통합 조회 (주문 + 결제 + 재고 정보 집계)
curl http://localhost:8000/api/query/orders
```

### 실습 2: CQRS 상세 조회

```bash
# 1. 주문 생성
ORDER_ID=$(curl -s -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-002",
    "items": [
      {"product_id": "PROD-003", "product_name": "마우스", "quantity": 3, "price": 45000}
    ]
  }' | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")

echo "주문 ID: $ORDER_ID"

# 2. 이벤트 처리 대기
sleep 3

# 3. query-service에서 상세 조회 (이벤트 타임라인 포함)
curl http://localhost:8005/orders/$ORDER_ID | python3 -m json.tool

# 4. 전체 이벤트 로그 확인
curl http://localhost:8005/events | python3 -m json.tool

# 5. 주문 통계 확인
curl http://localhost:8005/stats | python3 -m json.tool
```

### 실습 3: 대량 주문으로 분산 처리 확인

```bash
# 10개 주문을 연속 생성
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8000/api/orders \
    -H "Content-Type: application/json" \
    -d "{
      \"user_id\": \"user-$(printf '%03d' $i)\",
      \"items\": [
        {\"product_id\": \"PROD-001\", \"product_name\": \"노트북\", \"quantity\": 1, \"price\": $(( RANDOM % 2000000 + 500000 ))}
      ]
    }" &
done
wait

# 5초 대기 후 결과 확인
sleep 5

# 주문 통계 확인 (상태별 분포)
curl http://localhost:8005/stats | python3 -m json.tool

# 알림 개수 확인
curl http://localhost:8004/notifications | python3 -c "import sys,json; data=json.load(sys.stdin); print(f'총 알림 수: {data[\"total\"]}')"
```

### 실습 4: 헬스 체크 및 장애 대응

```bash
# 1. 전체 서비스 헬스 체크
curl http://localhost:8000/health | python3 -m json.tool

# 2. 특정 서비스 중지 후 헬스 체크 변화 확인
docker compose stop payment-service

# 3. Gateway 헬스 체크 - payment-service가 unhealthy로 표시됨
curl http://localhost:8000/health | python3 -m json.tool

# 4. 서비스 복구
docker compose start payment-service

# 5. 복구 후 다시 헬스 체크
sleep 5
curl http://localhost:8000/health | python3 -m json.tool
```

### 실습 5: Kafka UI로 클러스터 모니터링

1. http://localhost:8080 접속
2. **Brokers** 탭: 3개 브로커 상태 확인
3. **Topics** 탭: order.created, payment.result, inventory.result 토픽 확인
4. **Consumers** 탭: 각 서비스의 컨슈머 그룹 및 오프셋 확인
5. **Messages** 탭: 각 토픽의 메시지 내용 직접 확인

### 실습 6: 결제 실패율 조정

```bash
# payment-service의 실패율을 50%로 올려서 보상 트랜잭션 관찰
docker compose stop payment-service
PAYMENT_FAILURE_RATE=0.5 docker compose up payment-service -d

# 주문 생성 후 결과 비교
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8000/api/orders \
    -H "Content-Type: application/json" \
    -d '{"user_id": "test-user", "items": [{"product_id": "P1", "product_name": "테스트", "quantity": 1, "price": 10000}]}'
  echo
done

sleep 5
curl http://localhost:8005/stats | python3 -m json.tool
```

---

## 10. 핵심 코드 해설

### 10.1 API Gateway - 프록시 요청 함수

```python
# gateway/app/main.py
async def proxy_request(method, service_url, path, body=None, params=None):
    """내부 서비스로 요청을 프록시하는 핵심 함수"""
    url = f"{service_url}{path}"
    try:
        response = await http_client.request(method=method, url=url, json=body, params=params)
        return JSONResponse(status_code=response.status_code, content=response.json())
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail=f"서비스 연결 불가: {url}")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"서비스 응답 타임아웃: {url}")
```

**포인트:**
- `httpx.AsyncClient`를 전역으로 재사용하여 커넥션 풀 효율 극대화
- 서비스 장애 시 적절한 HTTP 상태 코드로 응답 (503 서비스 불가, 504 타임아웃)

### 10.2 CQRS 읽기 모델 - 이벤트 프로세서

```python
# query-service/app/consumer.py
def process_event(event, topic):
    """이벤트를 읽기 모델에 반영 - 이벤트 소싱의 핵심"""
    event_type = event.get("event_type")
    order_id = event.get("order_id")

    # 1. 이벤트 로그에 무조건 기록 (이벤트 소싱)
    event_log.append({...})

    # 2. 이벤트 타입에 따라 읽기 모델 업데이트
    if event_type == "ORDER_CREATED":
        # 새로운 주문 뷰 생성 (비정규화된 데이터)
        orders_read_model[order_id] = {
            "order_id": order_id,
            "status": "CREATED",
            "payment_status": "PENDING",
            "inventory_status": "PENDING",
            ...
        }
    elif event_type == "PAYMENT_COMPLETED":
        # 결제 완료 상태 반영
        view = orders_read_model[order_id]
        view["payment_status"] = "COMPLETED"
        # 결제 + 재고 모두 완료 시 주문 완료로 전환
        if view["inventory_status"] == "RESERVED":
            view["status"] = "COMPLETED"
```

**포인트:**
- 모든 이벤트를 시간순으로 기록 (이벤트 로그) → 이벤트 리플레이 가능
- 비정규화된 읽기 뷰: 주문 + 결제 + 재고 정보를 하나의 객체에 집계
- 다양한 이벤트 타입에 대한 상태 전이 로직 구현

### 10.3 알림 서비스 - 느슨한 결합의 좋은 예

```python
# notification-service/app/consumer.py
# 3개의 토픽을 동시에 구독
consumer = AIOKafkaConsumer(
    TOPIC_ORDER_CREATED,      # 주문 생성 → "주문 접수됨" 알림
    TOPIC_PAYMENT_RESULT,     # 결제 결과 → "결제 완료/실패" 알림
    TOPIC_INVENTORY_RESULT,   # 재고 결과 → "재고 확인/부족" 알림
    ...
)
```

**포인트:**
- 알림 서비스는 다른 서비스의 코드를 전혀 모름 (느슨한 결합)
- 알림 서비스가 다운되어도 주문/결제/재고 흐름은 영향 없음
- 서비스 복구 시 Kafka에 쌓인 이벤트를 순차 처리 (메시지 유실 없음)

### 10.4 프로듀서 안전 설정

```python
# 모든 서비스의 프로듀서 공통 설정
producer = AIOKafkaProducer(
    acks="all",              # 모든 ISR 레플리카 확인 (데이터 유실 방지)
    enable_idempotence=True, # 중복 전송 방지 (네트워크 재시도 시)
)
```

**포인트:**
- `acks="all"` + `min.insync.replicas=2`: 최소 2개 레플리카에 기록 확인
- `enable_idempotence=True`: 프로듀서 재시도로 인한 중복 메시지 방지
- `key=order_id`: 같은 주문의 이벤트가 항상 같은 파티션으로 전달 (순서 보장)

---

## 프로젝트 구조

```
11-multi-service-architecture/
├── README.md                    # 이 문서
├── docker-compose.yml           # 전체 인프라 + 서비스 구성
├── shared/
│   └── events.py                # 공유 이벤트 모델 (Pydantic)
├── gateway/                     # API Gateway (포트 8000)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       ├── main.py              # httpx 기반 프록시 라우팅
│       └── config.py            # 내부 서비스 URL 설정
├── order-service/               # 주문 서비스 (포트 8001)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       ├── main.py              # 주문 생성/조회 API
│       ├── config.py
│       ├── producer.py          # order.created 이벤트 발행
│       ├── consumer.py          # payment/inventory 결과 소비
│       └── models.py            # 주문 데이터 모델
├── payment-service/             # 결제 서비스 (포트 8002)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       ├── main.py              # 결제 조회 API
│       ├── config.py
│       ├── consumer.py          # order.created 소비 → 결제 처리
│       └── producer.py          # payment.result 이벤트 발행
├── inventory-service/           # 재고 서비스 (포트 8003)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       ├── main.py              # 재고 조회 API
│       ├── config.py
│       ├── consumer.py          # order.created 소비 → 재고 확인
│       └── producer.py          # inventory.result 이벤트 발행
├── notification-service/        # 알림 서비스 (포트 8004)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       ├── main.py              # 알림 조회 API
│       ├── config.py
│       ├── consumer.py          # 모든 이벤트 소비 → 알림 생성
│       └── notifier.py          # 알림 발송 시뮬레이터
└── query-service/               # 쿼리 서비스 - CQRS (포트 8005)
    ├── Dockerfile
    ├── requirements.txt
    └── app/
        ├── __init__.py
        ├── main.py              # 통합 조회/통계 API
        ├── config.py
        └── consumer.py          # 모든 이벤트 소비 → 읽기 모델 구축
```
