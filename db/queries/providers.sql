-- name: GetProviderByName :one
SELECT *
FROM providers
WHERE name = $1;

-- name: ListActiveProvidersByType :many
SELECT *
FROM providers
WHERE type = $1
  AND is_active = true
ORDER BY priority ASC, name ASC;
