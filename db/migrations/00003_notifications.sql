-- +goose Up

CREATE TABLE notifications
(
    id                UUID PRIMARY KEY      DEFAULT uuidv7(),

    -- Idempotency key.
    event_id          VARCHAR(100) NOT NULL UNIQUE,

    -- order_shipped, payment_failed, etc.
    notification_type VARCHAR(50)  NOT NULL,

    -- transactional | social | marketing
    category          VARCHAR(20)  NOT NULL,

    -- Business recipient identity.
    recipient_id      VARCHAR(100) NOT NULL,

    -- Immutable business/event payload.
    payload           JSONB        NOT NULL DEFAULT '{}'::jsonb,

    -- Distributed tracing metadata.
    trace_id          VARCHAR(100),

    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT chk_notifications_category
        CHECK (
            category IN (
                         'transactional',
                         'social',
                         'marketing'
                )
            ),

    CONSTRAINT chk_notifications_payload_object
        CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX idx_notifications_recipient_created
    ON notifications (recipient_id, created_at DESC);

CREATE INDEX idx_notifications_trace_id
    ON notifications (trace_id) WHERE trace_id IS NOT NULL;


-- +goose Down

DROP TABLE IF EXISTS notifications;