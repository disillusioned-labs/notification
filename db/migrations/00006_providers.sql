-- +goose Up
CREATE TABLE providers
(
    name       VARCHAR(50) PRIMARY KEY,
    -- email | sms | push
    type       VARCHAR(20) NOT NULL,
    config     JSONB       NOT NULL DEFAULT '{}',
    priority   INT         NOT NULL DEFAULT 100,
    is_active  BOOLEAN     NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_provider_type
        CHECK (
            type IN (
                     'email',
                     'sms',
                     'push'
                )
            ),

    CONSTRAINT chk_provider_priority
        CHECK (priority >= 0)
);

CREATE INDEX idx_providers_type_active
    ON providers (type, is_active);


-- +goose Down
DROP TABLE IF EXISTS providers;