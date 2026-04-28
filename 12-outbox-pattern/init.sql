-- =============================================================================
-- Outbox Pattern 스키마
-- =============================================================================
-- orders: 비즈니스 데이터(주문)
-- outbox: 발행 대기/완료 이벤트 큐 (orders와 같은 트랜잭션에서 함께 INSERT)
--
-- 핵심 아이디어:
--   하나의 트랜잭션에서 두 테이블에 동시에 쓰면, "DB 커밋 = 이벤트 발행 예약"이
--   원자적으로 보장된다. Kafka 발행은 비동기 릴레이가 별도로 처리한다.
-- =============================================================================

CREATE TABLE IF NOT EXISTS orders (
    order_id     UUID         PRIMARY KEY,
    user_id      VARCHAR(255) NOT NULL,
    items        JSONB        NOT NULL,
    total_price  NUMERIC(12, 2) NOT NULL,
    status       VARCHAR(50)  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL    PRIMARY KEY,
    event_id        UUID         NOT NULL UNIQUE,            -- 컨슈머 측 멱등성 키
    aggregate_type  VARCHAR(255) NOT NULL,                   -- 예: "Order"
    aggregate_id    VARCHAR(255) NOT NULL,                   -- 예: order_id (Kafka 메시지 key)
    event_type      VARCHAR(255) NOT NULL,                   -- 예: "OrderCreated"
    topic           VARCHAR(255) NOT NULL,
    payload         JSONB        NOT NULL,
    headers         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ  NULL                        -- NULL = 미발행
);

-- 부분 인덱스: 미발행 이벤트만 인덱싱하여 폴링 쿼리 비용을 최소화
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox (id)
    WHERE published_at IS NULL;

-- 같은 aggregate(주문)의 이벤트를 시간순으로 모아 보고 싶을 때 사용
CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
    ON outbox (aggregate_type, aggregate_id, id);
