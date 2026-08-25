-- +goose Up

CREATE TABLE notification_delivery_attempts
(
    id                  UUID PRIMARY KEY     DEFAULT uuidv7(),

    delivery_id         UUID        NOT NULL
        REFERENCES notification_deliveries (id)
            ON DELETE CASCADE,

    attempt_number      INT         NOT NULL,

    -- Provider name snapshot for this particular attempt.
    provider            VARCHAR(50) NOT NULL,
    provider_message_id VARCHAR(255),

    -- success | failed
    status              VARCHAR(20) NOT NULL,

    http_status_code    INT,

    error_type          VARCHAR(30),
    error_message       TEXT,

    response            JSONB,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_delivery_attempt_number
        UNIQUE (delivery_id, attempt_number),

    CONSTRAINT chk_attempt_number
        CHECK (attempt_number > 0),

    CONSTRAINT chk_attempt_status
        CHECK (
            status IN (
                       'success',
                       'failed'
                )
            ),

    CONSTRAINT chk_attempt_http_status
        CHECK (
            http_status_code IS NULL
                OR (
                http_status_code >= 100
                    AND http_status_code <= 599
                )
            )
);

CREATE INDEX idx_delivery_attempts_delivery
    ON notification_delivery_attempts (
                                       delivery_id,
                                       attempt_number
        );


-- +goose Down

DROP TABLE IF EXISTS notification_delivery_attempts;