-- Deduplication audit log — long-term record beyond Redis TTL
CREATE TABLE IF NOT EXISTS dedup_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    message_id      TEXT        NOT NULL,
    topic           TEXT        NOT NULL,
    status          TEXT        NOT NULL,  -- PROCESSING | COMPLETED | FAILED | SKIPPED
    result          JSONB,
    error           TEXT,
    attempts        INT         NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dedup_audit_log_message_id
    ON dedup_audit_log (message_id);

CREATE INDEX IF NOT EXISTS idx_dedup_audit_log_topic_status
    ON dedup_audit_log (topic, status);

-- Orders table — idempotency key enforced at DB level
CREATE TABLE IF NOT EXISTS orders (
    id              BIGSERIAL PRIMARY KEY,
    dedup_message_id TEXT       UNIQUE NOT NULL,
    customer_id     TEXT        NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'CREATED',
    payload         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Transactional outbox — write here inside your business-logic transaction,
-- then let OutboxRelay publish the rows to Kafka asynchronously.
CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL PRIMARY KEY,
    topic           TEXT        NOT NULL,
    message_id      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'PENDING',  -- PENDING | PUBLISHED | FAILED
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_message_id
    ON outbox (message_id);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (status, id)
    WHERE status = 'PENDING';
