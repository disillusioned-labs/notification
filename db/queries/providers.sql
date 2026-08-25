-- name: GetProviderByName :one
SELECT *
FROM providers
WHERE name = sqlc.arg(name);


-- name: ListActiveProvidersByType :many
SELECT *
FROM providers
WHERE type = sqlc.arg(type)
  AND is_active = true
ORDER BY priority ASC, name ASC;


-- name: UpsertProvider :one
INSERT INTO providers (name,
                       type,
                       config,
                       priority,
                       is_active)
VALUES (sqlc.arg(name),
        sqlc.arg(type),
        sqlc.arg(config),
        sqlc.arg(priority),
        sqlc.arg(is_active)) ON CONFLICT (name)
DO
UPDATE SET
    type = EXCLUDED.type,
    config = EXCLUDED.config,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active
    RETURNING *;


-- name: DeactivateProvider :one
UPDATE providers
SET is_active = false
WHERE name = sqlc.arg(name) RETURNING *;