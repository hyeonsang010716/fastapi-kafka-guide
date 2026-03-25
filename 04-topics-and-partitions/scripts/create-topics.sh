#!/bin/bash
# ──────────────────────────────────────────────
# 토픽 생성 스크립트
# 각 토픽을 서로 다른 파티션 수로 생성하여 파티션의 역할을 실습합니다.
# ──────────────────────────────────────────────

KAFKA_BROKER="kafka:9092"

echo "========================================="
echo " Kafka 토픽 생성 시작"
echo "========================================="

# orders: 3개 파티션 — 주문 ID를 키로 사용하여 파티션 분배를 확인
echo "[1/3] 'orders' 토픽 생성 (파티션 3개)..."
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" \
  --create \
  --if-not-exists \
  --topic orders \
  --partitions 3 \
  --replication-factor 1

# logs: 1개 파티션 — 순서가 중요한 로그는 단일 파티션으로 순서 보장
echo "[2/3] 'logs' 토픽 생성 (파티션 1개)..."
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" \
  --create \
  --if-not-exists \
  --topic logs \
  --partitions 1 \
  --replication-factor 1

# events: 6개 파티션 — 높은 처리량을 위해 파티션을 많이 설정
echo "[3/3] 'events' 토픽 생성 (파티션 6개)..."
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" \
  --create \
  --if-not-exists \
  --topic events \
  --partitions 6 \
  --replication-factor 1

echo ""
echo "========================================="
echo " 생성된 토픽 목록"
echo "========================================="
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --list

echo ""
echo "========================================="
echo " 토픽 상세 정보"
echo "========================================="
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --describe

echo ""
echo "토픽 생성 완료!"
