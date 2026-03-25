# 08 - Exactly-Once & Idempotency (멱등성)

## 1. 이 챕터에서 배우는 것

- Kafka의 메시지 전달 보장(Delivery Guarantee) 3가지 수준 이해
- **멱등성 프로듀서(Idempotent Producer)** 설정과 동작 원리
- **Consumer 측 멱등성**을 Redis를 활용하여 직접 구현하는 방법
- 동일한 메시지를 두 번 보내도 한 번만 처리되는 것을 실습으로 확인

## 2. 메시지 전달 보장 비교

| 구분 | 설명 | 데이터 손실 | 중복 처리 | 구현 복잡도 |
|------|------|:-----------:|:---------:|:-----------:|
| **At-most-once** | 메시지를 최대 한 번 전달. 실패 시 재전송하지 않음 | 가능 | 없음 | 낮음 |
| **At-least-once** | 메시지를 최소 한 번 전달. 실패 시 재전송 | 없음 | 가능 | 중간 |
| **Exactly-once** | 메시지를 정확히 한 번 전달 | 없음 | 없음 | 높음 |

> 대부분의 실무 시스템은 **At-least-once + Consumer 멱등성**으로 Exactly-once 효과를 달성합니다.

## 3. 멱등성 프로듀서 (Idempotent Producer)

Kafka 0.11+부터 프로듀서 단에서 중복 전송을 방지하는 기능을 제공합니다.

```python
producer = AIOKafkaProducer(
    enable_idempotence=True,  # 멱등성 활성화
    acks="all",               # 모든 ISR 복제본 확인
)
```

**동작 원리:**
- 프로듀서에 **PID(Producer ID)** 와 **시퀀스 번호**가 부여됨
- 브로커는 (PID, 파티션, 시퀀스번호) 조합으로 중복 감지
- 네트워크 재전송으로 인한 **브로커 레벨 중복**을 방지

**한계:**
- 프로듀서가 재시작되면 새로운 PID가 할당되므로 **애플리케이션 레벨 중복**은 방지 못함
- 따라서 Consumer 측에서도 별도의 멱등성 처리가 필요

## 4. Consumer 측 멱등성 구현 (Redis 기반)

```
[메시지 수신] → [Redis에서 idempotency_key 확인]
                     │
            ┌────────┴────────┐
            │                 │
      [이미 처리됨]      [신규 메시지]
      → skip & log      → 비즈니스 로직 실행
                        → Redis에 key 저장 (TTL 24h)
                        → offset commit
```

### 핵심 흐름:
1. 메시지에 고유한 `idempotency_key`를 포함시켜 전송
2. Consumer가 메시지를 받으면 Redis에서 해당 key가 이미 처리되었는지 확인
3. 처리된 적 없으면 → 비즈니스 로직 실행 + Redis에 기록 + offset commit
4. 이미 처리되었으면 → 스킵하고 로그만 남김

## 5. 왜 Exactly-once가 어려운가

### 분산 시스템의 근본적 한계
- **네트워크 파티션**: 메시지 전송 성공 여부를 확인할 수 없는 순간이 존재
- **프로세스 장애**: Consumer가 처리 도중 죽으면 "처리했지만 commit 못한" 상태 발생
- **Two Generals Problem**: 두 시스템 간 합의를 100% 보장하는 것은 이론적으로 불가능

### 실무적 접근
| 전략 | 설명 |
|------|------|
| Kafka Transactions | 프로듀서-컨슈머를 하나의 트랜잭션으로 묶음 (Kafka 내부 한정) |
| Idempotency Key + Redis | 외부 저장소를 활용한 중복 방지 (가장 범용적) |
| Outbox Pattern | DB + 메시지 브로커 간 일관성 보장 |
| Deduplication Table | DB에 처리 이력 테이블을 두어 중복 확인 |

> 이 챕터에서는 **Idempotency Key + Redis** 방식을 구현합니다.

## 6. 실행 방법 및 실습

### 실행

```bash
cd 08-exactly-once-and-idempotency
docker compose up --build -d
```

### API 테스트

#### 포인트 적립 요청
```bash
curl -X POST http://localhost:8000/points \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user-001", "points": 100, "idempotency_key": "tx-abc-123"}'
```

#### 중복 전송 테스트 (핵심!)
```bash
# 동일한 이벤트를 2번 전송 → 1번만 처리되는지 확인
curl -X POST http://localhost:8000/points/duplicate-test \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user-001", "points": 500, "idempotency_key": "dup-test-001"}'
```

#### 잔액 확인
```bash
# 중복 테스트 후 잔액이 500이면 성공 (1000이면 중복 처리된 것)
curl http://localhost:8000/balances
```

#### 처리된 키 확인
```bash
curl http://localhost:8000/processed-keys
```

#### 헬스체크
```bash
curl http://localhost:8000/health
```

### Kafka UI
- http://localhost:8080 에서 토픽과 메시지 확인 가능

### 종료
```bash
docker compose down -v
```

## 7. 핵심 코드 해설

### IdempotencyStore (idempotency.py)
```python
class IdempotencyStore:
    """Redis 기반 멱등성 저장소"""

    async def is_processed(self, idempotency_key: str) -> bool:
        """이미 처리된 키인지 확인"""
        return await self.redis.exists(f"idempotency:{idempotency_key}") > 0

    async def mark_processed(self, idempotency_key: str, ttl: int = 86400):
        """처리 완료로 표시 (기본 TTL: 24시간)"""
        await self.redis.set(f"idempotency:{idempotency_key}", "1", ex=ttl)
```

### Producer 멱등성 설정 (producer.py)
```python
producer = AIOKafkaProducer(
    enable_idempotence=True,  # PID + 시퀀스 번호로 브로커 레벨 중복 방지
    acks="all",               # 모든 ISR 복제본이 확인해야 성공
)
```

### Consumer 중복 처리 방지 (consumer.py)
```python
# 1. 멱등성 키 확인
if await idempotency_store.is_processed(idempotency_key):
    logger.info(f"중복 메시지 스킵: {idempotency_key}")
    continue

# 2. 비즈니스 로직 실행 (포인트 적립)
balances[user_id] = balances.get(user_id, 0) + points

# 3. 처리 완료 표시
await idempotency_store.mark_processed(idempotency_key)
```

### 핵심 포인트
- `enable_idempotence=True`는 **네트워크 재전송**으로 인한 중복만 방지
- **애플리케이션 레벨 중복**(같은 요청 2번 전송)은 Redis 멱등성 키로 방지
- TTL을 설정하여 Redis 메모리를 효율적으로 관리
