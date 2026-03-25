# Kafka CLI 명령어 모음

이 문서는 `docker exec`를 통해 Kafka 컨테이너 내부에서 실행하는 CLI 명령어를 정리한 것이다.

---

## 토픽 관리

### 토픽 생성

```bash
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic <토픽이름> \
  --partitions <파티션수> \
  --replication-factor <복제계수>
```

예시:

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

### 토픽 상세 정보 조회

```bash
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic <토픽이름>
```

예시:

```bash
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic my-first-topic
```

출력 예시:

```
Topic: my-first-topic   TopicId: xxxx   PartitionCount: 3   ReplicationFactor: 1
  Topic: my-first-topic   Partition: 0   Leader: 0   Replicas: 0   Isr: 0
  Topic: my-first-topic   Partition: 1   Leader: 0   Replicas: 0   Isr: 0
  Topic: my-first-topic   Partition: 2   Leader: 0   Replicas: 0   Isr: 0
```

### 토픽 삭제

```bash
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --delete \
  --topic <토픽이름>
```

---

## 메시지 송수신

### 콘솔 Producer (메시지 보내기)

```bash
docker exec -it kafka kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic <토픽이름>
```

실행하면 `>` 프롬프트가 나타난다. 메시지를 입력하고 Enter를 누르면 전송된다.
종료: `Ctrl+C`

#### 키(key)를 포함하여 보내기

```bash
docker exec -it kafka kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic <토픽이름> \
  --property parse.key=true \
  --property key.separator=:
```

입력 형식: `key:value` (예: `user1:{"action":"login"}`)

### 콘솔 Consumer (메시지 읽기)

#### 처음부터 읽기

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic <토픽이름> \
  --from-beginning
```

#### 지금부터 새 메시지만 읽기

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic <토픽이름>
```

#### 키와 값 함께 출력하기

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic <토픽이름> \
  --from-beginning \
  --property print.key=true \
  --property key.separator=:
```

#### Consumer Group 지정하여 읽기

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic <토픽이름> \
  --group <그룹이름>
```

---

## Consumer Group 관리

### Consumer Group 목록 조회

```bash
docker exec -it kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --list
```

### Consumer Group 상세 조회 (offset 확인)

```bash
docker exec -it kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group <그룹이름>
```

출력에서 `LAG` 컬럼이 아직 처리하지 않은 메시지 수를 나타낸다.

### Consumer Group offset 초기화

```bash
docker exec -it kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group <그룹이름> \
  --topic <토픽이름> \
  --reset-offsets \
  --to-earliest \
  --execute
```
