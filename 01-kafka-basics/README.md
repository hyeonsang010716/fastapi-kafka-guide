# 01 - Kafka 기초

## 1. 이 챕터에서 배우는 것

- Apache Kafka가 무엇인지, 왜 사용하는지 이해한다.
- Kafka의 핵심 구성 요소(Broker, Topic, Partition, Offset, Producer, Consumer)를 학습한다.
- HTTP 동기 방식과 Kafka 비동기 방식의 차이를 비교한다.
- ZooKeeper 없이 동작하는 KRaft 모드를 이해한다.
- Docker Compose로 Kafka를 직접 실행하고 kafka-ui로 클러스터를 확인한다.
- Kafka CLI를 사용하여 토픽을 생성하고 메시지를 주고받는다.

---

## 2. 핵심 개념

### Broker

Kafka 클러스터를 구성하는 서버 노드. 메시지를 저장하고 클라이언트 요청을 처리한다.

```
┌─────────────────────────────────────────┐
│            Kafka Cluster                │
│                                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│  │ Broker 0 │ │ Broker 1 │ │ Broker 2 │ │
│  └──────────┘ └──────────┘ └──────────┘ │
└─────────────────────────────────────────┘
```

### Topic

메시지가 분류되어 저장되는 논리적 채널. 우편함의 카테고리라고 생각하면 된다.

```
Kafka Broker
├── Topic: "orders"
├── Topic: "payments"
└── Topic: "notifications"
```

### Partition

Topic을 물리적으로 나눈 단위. 병렬 처리와 확장성을 제공한다.

```
Topic: "orders" (3 partitions)

  Partition 0: [ msg0 | msg3 | msg6 | msg9  | ... ]
  Partition 1: [ msg1 | msg4 | msg7 | msg10 | ... ]
  Partition 2: [ msg2 | msg5 | msg8 | msg11 | ... ]
```

### Offset

각 파티션 내에서 메시지의 순서를 나타내는 고유 번호. Consumer가 어디까지 읽었는지 추적하는 데 사용된다.

```
Partition 0:
  Offset:  0     1     2     3     4     5
         ┌─────┬─────┬─────┬─────┬─────┬─────┐
         │ A   │ B   │ C   │ D   │ E   │ F   │
         └─────┴─────┴─────┴─────┴─────┴─────┘
                              ^
                              └── Consumer 현재 위치 (offset=3)
```

### Producer & Consumer

- **Producer**: 메시지를 Topic에 보내는 클라이언트.
- **Consumer**: Topic에서 메시지를 읽는 클라이언트. Consumer Group 단위로 묶여 파티션을 분배받는다.

```
                    ┌───────────────────────┐
                    │     Kafka Broker      │
                    │                       │
 ┌──────────┐       │  ┌─────────────────┐  │      ┌────────────────────┐
 │ Producer ├─────> │  │ Topic: "orders" │  │─────>│ Consumer Group     │
 └──────────┘       │  │                 │  │      │  ┌──────────────┐  │
                    │  │  Partition 0 ───┼──┼─────>│  │ Consumer A   │  │
 ┌──────────┐       │  │  Partition 1 ───┼──┼─────>│  │ Consumer B   │  │
 │ Producer ├─────> │  │  Partition 2 ───┼──┼─────>│  │ Consumer C   │  │
 └──────────┘       │  └─────────────────┘  │      │  └──────────────┘  │
                    └───────────────────────┘      └────────────────────┘
```

---

## 3. HTTP 동기 방식 vs Kafka 비동기 방식

### HTTP 동기 방식

클라이언트가 요청을 보내면 서버의 응답이 올 때까지 **대기**한다.
중간에 서버가 다운되면 요청이 유실될 수 있다.

```
Client ──── POST /order ────> Order Service ──── POST /pay ────> Payment Service
  ^                                                                    │
  │                                                                    │
  └────────────────────── 200 OK (전체 완료까지 대기) <────────────────┘
```

**문제점:**
- 한 서비스가 느리면 전체가 느려진다 (강한 결합).
- 중간 서비스가 실패하면 전체 요청이 실패한다.
- 트래픽 급증 시 모든 서비스가 동시에 부하를 받는다.

### Kafka 비동기 방식

Producer가 메시지를 Kafka에 넣으면 **즉시 반환**된다.
Consumer가 자신의 속도에 맞춰 메시지를 처리한다.

```
Client ── POST /order ──> Order Service ── produce ──> [ Kafka Topic ]
  ^                            │                            │
  │     202 Accepted (즉시)     │                            │
  └────────────────────────────┘                            │
                                                   consume (비동기)
                                                            │
                                                            v
                                                    Payment Service
```

**장점:**
- 서비스 간 **느슨한 결합** (decoupling).
- Consumer가 다운되어도 메시지는 Kafka에 보존된다 (내구성).
- 트래픽 급증 시 Kafka가 **버퍼** 역할을 한다 (배압 처리).

| 항목 | HTTP 동기 | Kafka 비동기 |
|------|----------|-------------|
| 결합도 | 강한 결합 | 느슨한 결합 |
| 응답 속도 | 전체 처리 완료 후 | 즉시 (202 Accepted) |
| 장애 전파 | 한 서비스 장애 시 전체 영향 | 장애가 격리됨 |
| 메시지 유실 | 서버 다운 시 유실 가능 | Kafka에 보존 |
| 확장성 | 모든 서비스가 동일 부하 | Consumer 독립 확장 |

---

## 4. KRaft 모드란?

Kafka는 원래 클러스터 메타데이터 관리를 위해 **ZooKeeper**라는 별도의 시스템이 필요했다.
KRaft(Kafka Raft) 모드는 ZooKeeper 없이 Kafka 자체적으로 메타데이터를 관리하는 방식이다.

```
[ 기존 방식 ]                      [ KRaft 방식 ]

┌────────────┐                    ┌────────────────────────┐
│ ZooKeeper  │ <── 메타데이터 ──     │ Kafka Broker           │
└────────────┘                    │  + Controller (내장)    │
      ^                           │  + Raft 합의 알고리즘     │
      │                           └────────────────────────┘
┌─────┴──────┐
│ Kafka      │                    => ZooKeeper 불필요!
│ Broker     │                    => 운영 복잡도 감소
└────────────┘                    => 더 빠른 메타데이터 처리
```

이 프로젝트에서는 KRaft 모드를 사용한다. `docker-compose.yml`에서 다음 설정이 핵심이다:

- `KAFKA_CFG_PROCESS_ROLES=controller,broker` : 하나의 노드가 Controller + Broker 역할 모두 수행
- `KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093` : Raft 투표 참여자 설정
- `KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER` : Controller 통신용 리스너

---

## 5. 실행 방법

```bash
# 01-kafka-basics 디렉토리로 이동
cd 01-kafka-basics

# Kafka + kafka-ui 실행
docker compose up -d

# 로그 확인
docker compose logs -f kafka

# 종료
docker compose down
```

정상 실행되면 `kafka` 컨테이너와 `kafka-ui` 컨테이너가 각각 뜬다.

---

## 6. 실습: kafka-ui로 클러스터 확인

브라우저에서 [http://localhost:8080](http://localhost:8080) 에 접속한다.

확인할 것:
- **Dashboard**: `local` 클러스터가 연결되어 있는지 확인
- **Brokers**: Broker 0이 표시되는지 확인
- **Topics**: 아직 사용자 토픽은 없지만, 내부 토픽(`__consumer_offsets` 등)이 보일 수 있다

---

## 7. Kafka CLI로 토픽 생성/조회

Kafka 컨테이너 안에서 CLI 명령어를 실행한다.

### 토픽 생성

```bash
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic my-first-topic \
  --partitions 3 \
  --replication-factor 1
```

### 토픽 목록 조회

```bash
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

### 콘솔 Producer로 메시지 보내기

```bash
docker exec -it kafka kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic my-first-topic
```

프롬프트가 나오면 메시지를 입력하고 Enter. `Ctrl+C`로 종료.

### 콘솔 Consumer로 메시지 읽기

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic my-first-topic \
  --from-beginning
```

kafka-ui에서도 해당 토픽과 메시지를 확인할 수 있다.

---

## 8. 다음 챕터 미리보기

**02 - FastAPI + Kafka Producer**

다음 챕터에서는 Python FastAPI 애플리케이션에서 Kafka Producer를 구현한다.
- `aiokafka` 라이브러리를 사용하여 비동기 Producer를 만든다.
- REST API 엔드포인트로 메시지를 받아 Kafka 토픽에 전송한다.
- JSON 직렬화/역직렬화를 다룬다.
