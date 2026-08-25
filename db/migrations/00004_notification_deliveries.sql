-- +goose Up

CREATE TABLE notification_deliveries
(
    id              UUID PRIMARY KEY      DEFAULT uuidv7(),

    notification_id UUID         NOT NULL
        REFERENCES notifications (id)
            ON DELETE CASCADE,

    -- push | email | sms
    channel         VARCHAR(20)  NOT NULL,

    -- Provider name snapshot.
    provider        VARCHAR(50),

    -- Snapshot of the destination at delivery creation time.
    destination     VARCHAR(500) NOT NULL,

    -- pending | processing | sent | retry | failed | cancelled
    status          VARCHAR(30)  NOT NULL DEFAULT 'pending',

    -- Number of retries after the initial provider attempt.
    retry_count     INT          NOT NULL DEFAULT 0,

    -- Maximum number of retries allowed for this delivery.
    -- This is a snapshot of the policy at delivery creation time.
    max_retries     INT          NOT NULL DEFAULT 5,

    -- Required when status = retry.
    next_retry_at   TIMESTAMPTZ,

    -- Worker lease information.
    locked_by       VARCHAR(100),
    locked_at       TIMESTAMPTZ,

    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    sent_at         TIMESTAMPTZ,

    CONSTRAINT chk_delivery_channel
        CHECK (
            channel IN (
                        'push',
                        'email',
                        'sms'
                )
            ),

    CONSTRAINT chk_delivery_status
        CHECK (
            status IN (
                       'pending',
                       'processing',
                       'sent',
                       'retry',
                       'failed',
                       'cancelled'
                )
            ),

    CONSTRAINT chk_delivery_retry_count
        CHECK (
            retry_count >= 0
                AND retry_count <= max_retries
            ),

    CONSTRAINT chk_delivery_max_retries
        CHECK (max_retries >= 0),

    -- Retry must have a scheduled retry time.
    CONSTRAINT chk_delivery_retry_schedule
        CHECK (
            status <> 'retry'
                OR next_retry_at IS NOT NULL
            ),

    -- Only processing deliveries may hold a worker lock.
    CONSTRAINT chk_delivery_lock
        CHECK (
            (
                status = 'processing'
                    AND locked_by IS NOT NULL
                    AND locked_at IS NOT NULL
                )
                OR
            (
                status <> 'processing'
                    AND locked_by IS NULL
                    AND locked_at IS NULL
                )
            ),

    -- sent_at only makes sense for successfully sent deliveries.
    CONSTRAINT chk_delivery_sent_at
        CHECK (
            status = 'sent'
                OR sent_at IS NULL
            )
);

-- One notification can have at most one delivery per channel.
CREATE UNIQUE INDEX uq_notification_delivery_channel
    ON notification_deliveries (notification_id, channel);

-- Main lookup for delivery workers.
CREATE INDEX idx_deliveries_ready
    ON notification_deliveries (
                                next_retry_at,
                                created_at,
                                id
        ) WHERE status IN ('pending', 'retry');

-- Used to inspect/reclaim abandoned processing jobs.
CREATE INDEX idx_deliveries_locked
    ON notification_deliveries (locked_at, id) WHERE status = 'processing'
      AND locked_at IS NOT NULL;

-- Lookup all deliveries belonging to a notification.
CREATE INDEX idx_deliveries_notification
    ON notification_deliveries (notification_id);


-- +goose Down

DROP TABLE IF EXISTS notification_deliveries;