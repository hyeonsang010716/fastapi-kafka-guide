# 13 — Resilience Patterns: 죽은 서비스가 우리 시스템까지 죽이지 않게

> 이 글은 *retry/DLQ 는 알지만 그것만으로 부족한 이유* 를 알고 싶은 백엔드 개발자를 대상으로 합니다. 07 챕터(에러 처리/재시도)를 한 번 읽고 오시는 걸 권장해요.

---

## 0. 이 글을 다 읽고 나면

- "Resilience(회복 탄력성)" 라는 우산 용어가 머릿속에 자리잡습니다. 07 의 retry/DLQ 가 사실 이 우산 안에 있었다는 걸 알게 됩니다.
- **Cascading failure(연쇄 장애)** 가 어떻게 일어나는지, 왜 retry 만으로는 못 막는지 손에 잡힙니다.
- **Circuit Breaker** 패턴을 *직접 구현* 해 보고, 죽은 게이트웨이에 대한 호출이 어떻게 즉시 차단되는지 두 눈으로 확인합니다.
- Timeout / Retry / Circuit Breaker 를 어떤 순서로 쌓아야 하는지 알게 됩니다.

---

## 1. 어느 화요일 밤, 한 시간 만에 시스템 전체가 죽은 이야기

쇼핑몰 백엔드의 결제 서비스가 외부 결제 게이트웨이(PG사) 를 호출하고 있었습니다. 평소엔 50ms 만에 응답이 옵니다. 그러다 화요일 밤 23시 12분, PG사 쪽에 장애가 시작됩니다.

```
T+0   PG사 응답이 점점 느려짐 (50ms → 5초)
T+30s 결제 서비스의 httpx 호출이 5초씩 잡혀 있음
T+1m  retry 가 작동 — 한 요청당 5초 × 3 = 15초 동안 스레드를 점유
T+2m  결제 서비스 워커 스레드 풀이 다 잡힘 → 새 요청을 못 받음
T+3m  주문 서비스가 결제 서비스를 호출하지만 응답 없음 → 거기도 같이 멈춤
T+5m  주문 API 가 사용자에게 timeout. 카프카 컨슈머는 lag 폭증.
T+10m 알림/재고 서비스도 같이 마비. **PG사는 아직 살아 있는데 우리만 다 죽음.**
T+1h  PG사 부활. 그런데 우리 시스템은 한 시간 동안 죽어 있었음.
```

이게 **cascading failure** — 한 외부 의존이 흔들렸을 뿐인데 우리 시스템 *전체가* 같이 무너지는 사고예요. 분산 시스템에서 가장 흔한 사망 원인입니다.

**재미있는 부분:** retry 가 "도와주려고" 한 일이 사실 *문제를 키웠습니다.* 죽은 PG사로 우리가 더 많은 요청을 보내면서 회복도 늦어지고, 우리 자원도 더 빨리 고갈됐어요.

이 사건을 막을 수 있었던 패턴이 하나 있어요. **Circuit Breaker.** "이미 죽은 거 알아, 더 이상 두드리지 마" 라고 *우리 쪽에서* 차단하는 패턴. 이 챕터의 주인공입니다.

---

## 2. Resilience 라는 우산 — 07 챕터를 다시 보기

먼저 어휘 정리부터. 사실 우리는 07 챕터에서 retry, exponential backoff, DLQ 를 만났을 때 이미 *resilience 의 일부* 를 만지고 있었어요. 다만 그 우산 이름을 정식으로 부르지 않았을 뿐이에요.

> **Resilience (회복 탄력성)** = 시스템이 장애 상황에서 *버티고, 흡수하고, 회복하는* 능력. 이걸 만드는 패턴들의 묶음.

이 챕터에서 다룰 패턴들이 그 묶음 안에 어떻게 자리잡는지 보면:

```
                Resilience 의 패턴 가족
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   "잠깐의 흔들림을 흡수"           "더 큰 사고를 막음"                       │
│   ─────────────────────         ───────────────────                 │ 
│   • Timeout                     • Circuit Breaker  ← 이번 챕터의 주인공 │
│   • Retry          ← 07 챕터     • Bulkhead                          │
│   • Backoff        ← 07 챕터     • Backpressure                      │
│                                 • Fallback                          │
│                                                                     │
│   "최후의 보루"                                                        │
│   ─────────────                                                     │
│   • DLQ            ← 07 챕터                                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

07 의 retry/DLQ 가 왼쪽 박스를 채웠다면, 이번 챕터는 *오른쪽 위* 를 채웁니다. 둘은 대체재가 아니라 **같이 쓰는 보호막** 이에요.

---

## 3. 4개 패턴을 한 줄씩

| 패턴 | 한 줄로 |
|------|--------|
| **Timeout** | "응답 없는 호출을 무한히 기다리지 않는다." 자원 고갈 방지의 가장 기본. |
| **Retry + Backoff** | "일시적 장애는 자동 재시도. 단, 점점 간격을 늘려 죽은 서비스를 더 죽이지 않는다." |
| **Circuit Breaker** | "이미 죽은 걸 아는 서비스로의 호출을 *우리 쪽에서* 즉시 차단한다." |
| **Bulkhead** | "한 컴포넌트의 장애가 다른 컴포넌트의 자원까지 잡아먹지 못하게 격벽을 친다." |

이 챕터에서는 앞의 세 개를 *직접 코드로* 만들고, Bulkhead 는 §9 에서 개념만 짚습니다.

---

## 4. Timeout — 가장 단순하고 가장 중요한 한 줄

**모든 외부 호출에는 timeout 이 있어야 한다.** 예외 없습니다.

이걸 빠뜨리면 §1 시나리오의 *T+30s* 줄이 일어납니다. 호출이 끝나지 않은 채로 스레드/커넥션이 한없이 잡혀요. 우리 코드에서는 `httpx.AsyncClient` 의 timeout 으로 한 줄에 처리합니다.

```python
http = httpx.AsyncClient(
    base_url=GATEWAY_URL,
    timeout=httpx.Timeout(REQUEST_TIMEOUT_SEC),   # ← 이 한 줄
)
```

가장 단순한 패턴이지만, 운영 사고의 30~40% 정도가 이 한 줄을 깜빡해서 일어납니다. 새 외부 호출을 도입할 때 *timeout 을 적었는가?* 가 첫 번째 코드 리뷰 체크리스트.

---

## 5. Retry + Backoff — 07 의 우리 친구

07 챕터의 핵심을 한 함수로 정리한 게 `app/retry.py` 의 `retry_with_backoff` 입니다.

```python
async def retry_with_backoff(func, *, max_attempts=3, base_delay_ms=200, ...):
    attempt = 0
    while True:
        attempt += 1
        try:
            return await func()
        except do_not_retry:        # 재시도 의미 없는 예외는 즉시 위로
            raise
        except retriable as exc:
            if attempt >= max_attempts:
                raise
            delay = (base_delay_ms / 1000) * (2 ** (attempt - 1)) + random.random() * 0.05
            await asyncio.sleep(delay)
```

여기서 새로 도입한 디테일이 한 줄. **`do_not_retry` 파라미터.** 어떤 예외는 재시도해도 의미가 없습니다. 가장 대표적인 게 *"회로가 열려 있다"* 는 신호예요. 그건 위로 던져서 즉시 호출자에게 알려야 합니다. 이게 다음 §6 과의 연결고리.

---

## 6. Circuit Breaker — 이 챕터의 주인공

### 6.1 아이디어

> *"죽은 서비스가 살아날 때까지 우리가 거기 두드리는 걸 멈춘다. 그래야 우리도 살고 그쪽도 살아난다."*

전기 회로 차단기와 똑같습니다. 합선이 나면 *차단기가 떨어져서* 더 큰 화재를 막죠. 다시 켜는 건 *합선이 해결된 다음에* 합니다.

### 6.2 3가지 상태

```
   ┌────────────┐                    ┌────────────┐
   │   CLOSED   │  연속 실패 N회 ───►   │   OPEN     │
   │  (정상)     │                    │  (차단)     │
   │ 호출 통과    │  ◄───── 시험 성공    │ 호출 즉시 실패 │
   └────────────┘                    └─────┬──────┘
        ▲                                   │
        │                                   │ recovery_timeout 경과
        │  시험 호출 성공                      │
        │                                   ▼
        │                            ┌────────────┐
        └────── 시험 호출 실패  ────    │ HALF_OPEN  │
                                    │ (회복 시도)   │
                                    │ 1건만 통과    │
                                    └────────────┘
```

| 상태 | 무엇? | 호출 시 동작 |
|------|------|-------------|
| **CLOSED** | 정상. 회로 닫혀 있음 | 호출이 통과. 실패하면 카운트 +1. 임계치 도달 → OPEN |
| **OPEN** | 차단. 회로 열림 | 호출 즉시 `CircuitBreakerOpenError`. *진짜 호출은 안 일어남.* recovery_timeout 후 HALF_OPEN |
| **HALF_OPEN** | 회복 시도 중 | 시험 호출 1건만 통과. 성공 → CLOSED, 실패 → OPEN |

### 6.3 `app/circuit_breaker.py` 핵심

```python
class CircuitBreaker:
    async def call(self, func, *args, **kwargs):
        await self._before_call()         # OPEN 이면 여기서 즉시 raise
        try:
            result = await func(*args, **kwargs)
        except self.expected_exception:
            await self._on_failure()       # 카운트 증가, 임계치 도달 시 OPEN
            raise
        else:
            await self._on_success()       # 카운트 리셋, HALF_OPEN 이었으면 CLOSED
            return result
```

핵심 한 가지는 `_before_call`:

```python
async def _before_call(self):
    async with self._lock:
        if self._state == CircuitState.OPEN:
            if self._should_attempt_recovery():
                # 시험 호출 한 건만 통과시키기 위해 HALF_OPEN 으로
                self._state = CircuitState.HALF_OPEN
            else:
                raise CircuitBreakerOpenError(self.name)
```

OPEN 상태면 **진짜 호출은 안 일어나고** 즉시 예외를 던집니다. 이게 §1 시나리오의 자원 고갈을 막는 결정적 한 줄이에요. 우리 워커 스레드는 *5초 timeout 을 기다리지 않고* 바로 다음 일로 넘어갑니다.

### 6.4 임계치/복구 시간을 어떻게 정해야 하나

이 챕터의 기본값:

```python
failure_threshold = 5      # 연속 5회 실패 시 OPEN
recovery_timeout  = 10초    # 10초 후 HALF_OPEN 시도
```

운영에서 정할 때의 가이드:

- **failure_threshold** — 너무 작으면 일시적 흔들림에도 회로가 자꾸 열림. 너무 크면 진짜 죽었을 때 오래 못 알아챔. 보통 **3~10 사이**.
- **recovery_timeout** — 너무 짧으면 죽은 서비스를 또 두드림. 너무 길면 부활했는데도 한참 못 씀. 보통 **외부 서비스의 평균 다운타임 + 여유**. 5~60초 사이가 일반적.

> Netflix Hystrix 같은 옛 도구는 *"최근 N개 호출 중 X% 실패"* 같은 윈도우 기반 임계치를 썼어요. 더 정확하지만 구현이 복잡합니다. 학습용으로는 **연속 N회 실패** 가 충분히 좋아요.

---

## 7. 셋을 합치는 순서가 중요하다

세 패턴을 한 호출에 다 적용하면 *어느 순서로 쌓느냐* 에 따라 동작이 달라져요. 우리 `gateway_client.py` 의 적용 순서는 이렇습니다.

```
[가장 바깥]     retry
                │
                ▼
            circuit breaker
                │
                ▼
            timeout (httpx)
                │
                ▼
[가장 안쪽]  실제 HTTP 호출
```

**왜 이 순서인가**:

- **Timeout 이 가장 안쪽**: "한 번의 호출" 이 무한히 안 잡히게 보호.
- **Circuit Breaker 가 그 위**: 한 번의 호출 결과(성공/실패)를 기준으로 회로 상태를 판단.
- **Retry 가 가장 바깥**: 한 번의 시도가 실패하면 재시도. **단, 회로가 열려 있으면 재시도 자체를 안 해야** 합니다 (재시도해도 어차피 즉시 실패).

이 마지막 디테일이 코드에서 이렇게 표현돼요.

```python
return await retry_with_backoff(
    _call_once,
    retriable=(httpx.HTTPError,),
    do_not_retry=(CircuitBreakerOpenError,),   # ← 여기
)
```

**거꾸로 쌓으면 어떻게 되나?** 가령 *retry 안쪽에 circuit breaker* 를 두면, retry 가 회로가 열린 상태에서 N회 retry 를 해버립니다. 의미 없는 즉시 실패를 N번 반복하면서 backoff 만 늘어나요. 패턴이 의도한 대로 안 동작합니다.

---

## 8. 직접 실행해 보기

### 8.1 띄우기

```bash
cd 13-resilience-patterns
docker compose up --build -d
```

올라오는 컨테이너:

| 컨테이너 | 무엇 | 어디 |
|----------|------|------|
| `resilience-gateway` | 흉내낸 외부 결제 게이트웨이 (모드 변경 가능) | http://localhost:9000 |
| `resilience-client` | 우리 결제 서비스 (resilience 적용) | http://localhost:8000 |

### 8.2 정상 흐름 — HEALTHY

```bash
curl -X POST http://localhost:8000/payments \
  -H 'Content-Type: application/json' \
  -d '{"order_id": "abc-1", "amount": 4900}'
# → 200 OK, transaction_id 들어 있음
```

회로 상태 확인:

```bash
curl http://localhost:8000/breaker
# → state: CLOSED, failure_count: 0
```

### 8.3 게이트웨이를 *죽은* 상태로 바꿔 본다 — DEAD

```bash
curl -X POST http://localhost:9000/admin/mode \
  -H 'Content-Type: application/json' \
  -d '{"mode": "DEAD"}'
```

이제 결제를 시도하면:

```bash
# 5번 정도 연속 호출
for i in 1 2 3 4 5 6; do
  curl -s -o /dev/null -w "attempt $i → %{http_code}\n" \
    -X POST http://localhost:8000/payments \
    -H 'Content-Type: application/json' \
    -d "{\"order_id\": \"x-$i\", \"amount\": 100}"
done
```

처음 몇 번은 502 (gateway error) 가 떨어지면서 retry 가 동작하지만, 임계치(5회)를 넘는 순간부터 **503 (서킷 오픈)** 으로 바뀝니다.

```bash
curl http://localhost:8000/breaker
# → state: OPEN, opened_at: ...
```

**이 시점부터는** 결제 호출이 *진짜 게이트웨이까지 가지 않습니다.* 클라이언트에서 즉시 차단돼요. 응답 시간을 측정해 보면 차이가 극명합니다.

```bash
# DEAD 모드인데 회로가 OPEN 인 상태
time curl -s -o /dev/null -X POST http://localhost:8000/payments \
  -H 'Content-Type: application/json' -d '{"order_id":"y-1","amount":100}'
# → real 0m0.0xx s   (즉시 실패. timeout 5초를 안 기다림.)
```

### 8.4 부활을 시연해 본다 — DEAD → HEALTHY

게이트웨이를 다시 살리면:

```bash
curl -X POST http://localhost:9000/admin/mode \
  -H 'Content-Type: application/json' \
  -d '{"mode": "HEALTHY"}'
```

회로는 여전히 OPEN 입니다 (recovery_timeout 10초가 지나기 전이므로). 10초 정도 기다린 뒤 호출하면:

```bash
sleep 11
curl -X POST http://localhost:8000/payments \
  -H 'Content-Type: application/json' \
  -d '{"order_id": "z-1", "amount": 100}'
```

내부적으로 일어나는 일:
1. 회로가 자동으로 HALF_OPEN 이 됨.
2. 시험 호출 1건이 게이트웨이로 진짜 나감.
3. 게이트웨이가 200 으로 응답.
4. 회로가 CLOSED 로 돌아옴. `failure_count = 0`.

```bash
curl http://localhost:8000/breaker
# → state: CLOSED, failure_count: 0
```

**이게 우리가 원하던 그림입니다.** 자동으로 차단하고, 자동으로 회복합니다. 운영자 개입 없이.

### 8.5 간헐 장애 — FLAKY

```bash
curl -X POST http://localhost:9000/admin/mode \
  -H 'Content-Type: application/json' -d '{"mode": "FLAKY"}'
```

50% 확률로 실패합니다. 결제 호출을 여러 번 해보면 retry 가 어떻게 동작하는지 보입니다 — 한 요청 안에서 자동 재시도가 일어나서 *대부분 결국 성공* 합니다. 간헐 장애는 retry 만으로 거의 다 흡수됩니다. circuit breaker 까지는 거의 안 가요. 이게 두 패턴의 *역할 분담* 이에요.

### 8.6 종료

```bash
docker compose down
```

---

## 9. Bulkhead — 한 발 더

Bulkhead 는 배의 *격벽* 에서 따온 이름입니다. 한 칸이 침수돼도 배 전체가 가라앉지 않게 격벽으로 나누어 둔 구조.

소프트웨어에선:

- **외부 호출 풀을 분리** — 결제 API 호출용 스레드 풀, 검색 API 호출용 풀을 *각각* 둠. 결제가 죽어도 검색은 산다.
- **세마포어로 동시 호출 수 제한** — 결제 호출은 동시에 최대 50개. 그 이상은 즉시 거절.
- **DB 커넥션 풀 분리** — 분석 쿼리용과 트랜잭션용 풀을 분리.

이 챕터에서는 코드로 다루지 않지만, 위 §1 시나리오의 *T+2m* 줄(워커 풀 고갈)을 막아주는 또 다른 보호막입니다. circuit breaker 가 *호출 자체를 차단* 한다면, bulkhead 는 *자원을 격리* 합니다. 둘은 보완 관계예요.

---

## 10. 운영 체크리스트

학습용 코드를 프로덕션으로 옮길 때 추가로 챙기는 것들.

- **모든 외부 호출에 timeout** — 한 번 더 강조. 코드 리뷰 체크리스트 1번.
- **메트릭 노출** — circuit breaker 상태, 회로 전이 횟수, retry 횟수, p99 latency. Prometheus + Grafana 가 표준.
- **알람**
  - 회로가 5분 이상 OPEN 으로 머물러 있음 → 운영자 호출
  - retry 비율이 10% 넘게 5분 지속 → 게이트웨이 이상 신호
- **임계치 튜닝** — 운영 데이터를 보고 `failure_threshold`, `recovery_timeout` 을 조정. 처음 도입 후 한 달은 자주 봄.
- **회로별 격리** — 외부 의존 *별로* 회로를 따로 둠. 결제 게이트웨이의 회로와 검색 API 의 회로는 별개여야 함.
- **검증된 라이브러리 고려** — Python 에선 [pybreaker](https://github.com/danielfm/pybreaker), [tenacity](https://github.com/jd/tenacity), [aiobreaker](https://github.com/arlyon/aiobreaker) 등이 있음. 개념을 익힌 뒤에는 라이브러리 사용을 권장.

---

## 11. 자주 묻는 질문

**Q1. 모든 외부 호출에 circuit breaker 를 적용해야 하나요?**
*외부 의존(다른 서비스, DB, 외부 API)* 이라면 거의 그렇습니다. 메모리 캐시, 같은 프로세스 내부 함수에는 의미 없어요.

**Q2. 회로마다 따로 둬야 하나요, 하나로 충분한가요?**
**의존마다 따로** 둡니다. 결제 게이트웨이 회로가 열렸다고 검색 API 호출까지 막으면 안 되니까요. 우리 코드에서는 `gateway_breaker` 가 게이트웨이 전용 회로예요.

**Q3. retry 와 circuit breaker, 둘 중 하나만 쓰면 안 되나요?**
- retry 만: 죽은 서비스를 계속 두드려서 사고를 키움.
- breaker 만: 일시적 흔들림(예: 네트워크 패킷 한 번 떨어짐)에 너무 민감하게 회로가 열림.
- **둘을 합쳐야** 일시적 흔들림은 retry 로 흡수하고, 진짜 죽음은 breaker 로 차단합니다.

**Q4. 회로가 자주 열렸다 닫혔다 하면(flapping) 어떡하죠?**
임계치를 너무 작게 잡았거나, 외부 서비스가 *진짜* 불안정한 거예요. 전자라면 `failure_threshold` 를 키우거나 윈도우 기반 임계치로 전환. 후자라면 외부 서비스 팀과 대화할 시간.

**Q5. saga / outbox 와는 어떻게 결합되나요?**
서로 다른 층입니다. **outbox 는 "이벤트 발행의 일관성"**, **resilience 는 "외부 호출의 신뢰성"** 을 다룹니다. 12 챕터 outbox 컨슈머에서 외부 결제 게이트웨이를 호출한다면, 그 호출에 이번 챕터의 보호막 3종을 *그대로* 씌우면 됩니다.

**Q6. DLQ 와의 관계는요?**
DLQ 는 "재시도해도 안 되는 메시지의 영구 격리" 입니다. 이번 챕터의 패턴들은 *호출 단계* 의 보호막이고, DLQ 는 *메시지 처리 단계* 의 마지막 안전망이에요. 보통 같이 씁니다.

**Q7. circuit breaker 가 열린 동안 우리 사용자에게 뭐라고 보여주나요?**
선택지가 있어요:
- 가장 흔함: 503 또는 사용자에게 "잠시 후 다시 시도" 라고 안내
- Fallback 패턴: 캐시된 응답 또는 디폴트 값 반환 (예: 추천 API 가 죽으면 인기 상품 디폴트 반환)
- Queue 로 비동기화: 사용자 요청을 큐에 넣고 나중에 처리 (결제처럼 *결과를 알아야 하는* 호출에는 부적합)

---

## 마무리 — 한 줄로

> **timeout 이 한 호출을 보호하고, retry + backoff 가 일시적 흔들림을 흡수하고, circuit breaker 가 진짜 죽음을 차단하고, bulkhead 가 자원을 격리한다. 이 네 패턴이 합쳐져 한 외부 의존의 장애가 우리 시스템 전체를 무너뜨리지 않게 만드는 것이 Resilience 다.**

이 챕터까지 읽으셨다면, 분산 시스템에서 만나는 *대부분의 신뢰성 문제* 가 어느 가족의 어느 패턴으로 해결되는지 분류할 수 있게 됐을 거예요.

| 문제 | 가족 | 패턴 |
|------|------|------|
| DB ↔ Kafka 일관성 | 일관성 | Outbox + Inbox (12) |
| 다중 서비스 거래 일관성 | 일관성 | Saga (09) |
| 같은 메시지 두 번 처리 | 멱등성 | event_id dedup (08, 12) |
| 일시적 호출 실패 | resilience | Retry + Backoff (07, 13) |
| 처리 영구 실패 | resilience | DLQ (07) |
| 외부 의존 장애 전염 | resilience | Circuit Breaker (13) |
| 외부 의존 자원 고갈 | resilience | Timeout, Bulkhead (13) |

이 표가 머릿속에 들어오면 다음 마이크로서비스 사고 회의에서 *"이건 어느 패턴 누락이었는지"* 정확히 짚을 수 있게 됩니다. 그게 이 코스의 목표였어요.

---

## 부록 A. 디렉토리 구조

```
13-resilience-patterns/
├── README.md                ← 이 글
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── app/
    ├── __init__.py
    ├── config.py
    ├── circuit_breaker.py   ← ★ 직접 구현한 회로 차단기
    ├── retry.py             ← retry + backoff
    ├── gateway_client.py    ← timeout + retry + breaker 합쳐 쓰는 진입점
    ├── client_main.py       ← FastAPI: POST /payments
    └── gateway_main.py      ← 흉내낸 외부 결제 게이트웨이
```

(★) 표시된 파일이 이 챕터의 본체. 나머지는 시연용 보일러플레이트.

## 부록 B. 한눈에 보는 흐름

```
[POST /payments]
     │
     ▼
   retry_with_backoff
     │  (httpx.HTTPError 면 재시도, CircuitBreakerOpenError 면 즉시 위로)
     ▼
   circuit breaker (.call)
     │  (CLOSED → 통과 / OPEN → 즉시 실패 / HALF_OPEN → 시험 1건만)
     ▼
   httpx.AsyncClient.post (timeout=2초)
     │
     ▼
   외부 결제 게이트웨이
```
