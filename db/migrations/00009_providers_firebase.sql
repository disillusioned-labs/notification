-- +goose Up
-- The push provider row activates the push channel. It only takes effect once
-- the notification process runs with Firebase service-account credentials
-- (FIREBASE_SERVICE_ACCOUNT_FILE / _JSON); without them the provider is not
-- registered in-process and push targets are skipped at creation time.
INSERT INTO providers (name, type, config, priority, is_active)
VALUES ('firebase', 'push', '{}'::jsonb, 100, true);

-- +goose Down
DELETE FROM providers WHERE name = 'firebase';
