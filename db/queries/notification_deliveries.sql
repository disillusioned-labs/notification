-- name: CreateNotificationDelivery :one
INSERT INTO notification_deliveries (
    notification_id,
    channel,
    provider,
    destination,
    max_retries
)
VALUES (
           sqlc.arg(notification_id),
           sqlc.arg(channel),
           sqlc.arg(provider),
           sqlc.arg(destination),
           sqlc.arg(max_retries)
       )
    RETURNING *;


-- name: GetDeliveryByID :one
SELECT *
FROM notification_deliveries
WHERE id = sqlc.arg(id);


-- name: GetDeliveryByNotificationAndChannel :one
SELECT *
FROM notification_deliveries
WHERE notification_id = sqlc.arg(notification_id)
  AND channel = sqlc.arg(channel);


-- name: ListDeliveriesByNotificationID :many
SELECT *
FROM notification_deliveries
WHERE notification_id = sqlc.arg(notification_id)
ORDER BY created_at ASC, id ASC;


-- name: ClaimNextDelivery :one
WITH candidate AS (
    SELECT id
    FROM notification_deliveries
    WHERE status = 'pending'
       OR (
        status = 'retry'
            AND next_retry_at <= now()
        )
    ORDER BY
        COALESCE(next_retry_at, created_at),
        created_at,
        id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE notification_deliveries AS d
SET
    status = 'processing',
    locked_by = sqlc.arg(worker_id),
    locked_at = now()
    FROM candidate
WHERE d.id = candidate.id
    RETURNING d.*;


-- name: ClaimDeliveries :many
WITH candidates AS (
    SELECT id
    FROM notification_deliveries
    WHERE status = 'pending'
       OR (
        status = 'retry'
            AND next_retry_at <= now()
        )
    ORDER BY
        COALESCE(next_retry_at, created_at),
        created_at,
        id
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(batch_size)
)
UPDATE notification_deliveries AS d
SET
    status = 'processing',
    locked_by = sqlc.arg(worker_id),
    locked_at = now()
    FROM candidates
WHERE d.id = candidates.id
    RETURNING d.*;


-- name: MarkDeliverySent :one
UPDATE notification_deliveries
SET
    status = 'sent',
    sent_at = now(),
    next_retry_at = NULL,
    locked_by = NULL,
    locked_at = NULL
WHERE id = sqlc.arg(id)
  AND status = 'processing'
  AND locked_by = sqlc.arg(worker_id)
    RETURNING *;


-- name: ScheduleDeliveryRetry :one
UPDATE notification_deliveries
SET
    status = 'retry',
    retry_count = retry_count + 1,
    next_retry_at = sqlc.arg(next_retry_at),
    locked_by = NULL,
    locked_at = NULL
WHERE id = sqlc.arg(id)
  AND status = 'processing'
  AND locked_by = sqlc.arg(worker_id)
  AND retry_count < max_retries
    RETURNING *;


-- name: MarkDeliveryFailed :one
UPDATE notification_deliveries
SET
    status = 'failed',
    next_retry_at = NULL,
    locked_by = NULL,
    locked_at = NULL
WHERE id = sqlc.arg(id)
  AND status = 'processing'
  AND locked_by = sqlc.arg(worker_id)
    RETURNING *;


-- name: CancelDelivery :one
UPDATE notification_deliveries
SET
    status = 'cancelled',
    next_retry_at = NULL,
    locked_by = NULL,
    locked_at = NULL
WHERE id = sqlc.arg(id)
  AND status IN ('pending', 'retry')
    RETURNING *;


-- name: ReclaimStaleDeliveries :many
UPDATE notification_deliveries
SET
    status = CASE
                 WHEN retry_count < max_retries THEN 'retry'
                 ELSE 'failed'
        END,
    retry_count = CASE
                      WHEN retry_count < max_retries THEN retry_count + 1
                      ELSE retry_count
        END,
    next_retry_at = CASE
                        WHEN retry_count < max_retries THEN now()
                        ELSE NULL
        END,
    locked_by = NULL,
    locked_at = NULL
WHERE status = 'processing'
  AND locked_at < now() - sqlc.arg(lease_timeout)::interval
RETURNING *;