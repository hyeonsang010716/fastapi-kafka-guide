# 07 - 에러 핸들링과 재시도 (Error Handling & Retry)

## 1. 이 챕터에서 배우는 것

- Kafka Producer/Consumer의 에러 핸들링 전략
- Exponential Backoff을 활용한 재시도 패턴
- Dead Letter Queue(DLQ) 패턴으로 실패 메시지 관리
- 수동 오프셋 커밋으로 메시지 유실 방지
- `acks` 설정을 통한 메시지 내구성 보장

---

## 2. Producer acks 설정 비교

| 설정 | 동작 | 내구성 | 성능 | 사용 사례 |
|------|------|--------|------|-----------|
| `acks=0` | 응답을 기다리지 않음 | 낮음 (유실 가능) | 가장 빠름 | 로그, 메트릭 등 유실 허용 데이터 |
| `acks=1` | 리더 브로커만 확인 | 중간 | 빠름 | 일반적인 이벤트 처리 |
| `acks=all` | 모든 ISR 복제본 확인 | 높음 (유실 최소화) | 느림 | 결제, 주문 등 중요 데이터 |

> 이 챕터에서는 결제 데이터를 다루므로 `acks=all`을 사용합니다.

---

## 3. 재시도 전략: Exponential Backoff

재시도 시 **고정 간격**으로 반복하면 장애 상황에서 서버에 부하가 집중됩니다.
**Exponential Backoff**은 재시도 간격을 지수적으로 늘려 이를 완화합니다.

```
대기 시간 = base_delay * (2 ^ attempt) + jitter

시도 1: 1.0 * (2^0) + jitter ≈ 1.0 ~ 1.5초
시도 2: 1.0 * (2^1) + jitter ≈ 2.0 ~ 2.5초
시도 3: 1.0 * (2^2) + jitter ≈ 4.0 ~ 4.5초
```

**Jitter(무작위 지연)** 를 추가하는 이유:
- 여러 클라이언트가 동시에 재시도하면 "Thundering Herd" 문제 발생
- 무작위 지연으로 재시도 시점을 분산시켜 서버 부하를 줄임

---

## 4. Dead Letter Queue (DLQ) 패턴

최대 재시도를 초과해도 실패한 메시지를 별도 토픽으로 이동하여 관리합니다.

```
                        ┌─────────────────────────┐
                        │       Producer          │
                        │     POST /payments      │
                        └──────────┬──────────────┘
                                   │
                                   ▼
                        ┌─────────────────────────┐
                        │     "payments" 토픽      │
                        └──────────┬──────────────┘
                                   │
                                   ▼
                        ┌─────────────────────────┐
                        │       Consumer          │
                        │     (결제 처리 시도)       │
                        └──────┬──────────┬───────┘
                               │          │
                          성공  │          │ 실패 (재시도 초과)
                               │          │
                               ▼          ▼
                    ┌───────────────┐  ┌─────────────────────┐
                    │   처리 완료     │  │ "payments-dlq" 토픽  │
                    │ GET /processed│  └──────────┬──────────┘
                    └───────────────┘             │
                                                 ▼
                                      ┌─────────────────────┐
                                      │    DLQ Consumer     │
                                      │    (로깅/모니터링)     │
                                      │    GET /failed      │
                                      └─────────────────────┘
```

---

## 5. 수동 오프셋 커밋의 중요성

### 자동 커밋의 문제점

```
enable_auto_commit=True (기본값)
```

1. Consumer가 메시지를 가져옴
2. **자동 커밋 실행** (처리 완료 여부와 무관)
3. 처리 중 오류 발생
4. 이미 커밋됐으므로 메시지 유실!

### 수동 커밋으로 해결

```
enable_auto_commit=False
```

1. Consumer가 메시지를 가져옴
2. 메시지 처리 시도
3. 성공 → 오프셋 커밋 (다음 메시지로 진행)
4. 실패 → DLQ 전송 후 커밋 (무한 루프 방지)

> 핵심: **처리 완료 후에만 커밋**하여 메시지 유실을 방지합니다.

---

## 6. 실행 방법 및 실습

### 실행

```bash
cd 07-error-handling-and-retry
docker compose up --build
```

### 결제 이벤트 전송

```bash
# 단건 결제
curl -X POST http://localhost:8000/payments \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user-1", "amount": 50000, "currency": "KRW", "description": "테스트 결제"}'

# 여러 건 전송하여 성공/실패 분포 확인
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8000/payments \
    -H "Content-Type: application/json" \
    -d "{\"user_id\": \"user-$i\", \"amount\": $((i * 1000)), \"description\": \"결제 $i\"}"
  echo ""
done
```

### 결과 확인

```bash
# 성공한 결제 조회
curl http://localhost:8000/processed | python3 -m json.tool

# 실패한 결제 조회 (DLQ)
curl http://localhost:8000/failed | python3 -m json.tool

# 헬스 체크
curl http://localhost:8000/health | python3 -m json.tool
```

### Kafka UI에서 확인

- http://localhost:8080 접속
- `payments` 토픽: 원본 결제 이벤트
- `payments-dlq` 토픽: 최대 재시도 후에도 실패한 메시지

### 종료

```bash
docker compose down -v
```

---

## 7. 핵심 코드 해설

### retry.py — Exponential Backoff

```python
async def retry_with_backoff(func, max_retries=3, base_delay=1.0):
    for attempt in range(max_retries):
        try:
            return await func()
        except Exception as e:
            if attempt < max_retries - 1:
                # 지수적으로 증가하는 대기 시간 + 무작위 지연
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                await asyncio.sleep(delay)
    raise last_exception  # 모든 재시도 실패 시 예외 발생
```

### producer.py — 안전한 메시지 전송

```python
producer = AIOKafkaProducer(
    acks="all",     # 모든 ISR이 메시지를 확인해야 성공
    retries=3,      # 일시적 실패에 대한 자동 재시도
)
```

### consumer.py — 수동 커밋 + DLQ 전송

```python
consumer = AIOKafkaConsumer(
    enable_auto_commit=False,  # 수동 커밋 활성화
)

async for msg in consumer:
    try:
        result = await retry_with_backoff(process_payment)  # 재시도
    except Exception:
        await send_to_dlq(payment, error, MAX_RETRIES)      # DLQ 전송
    await consumer.commit()  # 처리 완료 후 커밋
```
