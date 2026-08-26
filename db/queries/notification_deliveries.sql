-- name: CreateNotificationDelivery :one
INSERT INTO notification_deliveries (notification_id,
                                     channel,
                                     provider,
                                     destination,
                                     status,
                                     retry_count,
                                     max_retries)
VALUES ($1,
        $2,
        $3,
        $4,
        'pending',
        0,
        $5) RETURNING *;


-- name: GetDeliveryByID :one
SELECT id,
       notification_id,
       channel,
       provider,
       destination,
       status,
       retry_count,
       max_retries,
       next_retry_at,
       locked_by,
       locked_at,
       created_at,
       updated_at,
       sent_at
FROM notification_deliveries
WHERE id = $1;


-- name: GetDeliveryWithNotification :one
SELECT d.id,
       d.notification_id,
       d.channel,
       d.provider,
       d.destination,
       d.status,
       d.retry_count,
       d.max_retries,
       d.next_retry_at,
       d.locked_by,
       d.locked_at,
       d.created_at,
       d.updated_at,
       d.sent_at,
       n.notification_type,
       n.category,
       n.payload AS notification_payload
FROM notification_deliveries AS d
         JOIN notifications AS n
              ON n.id = d.notification_id
WHERE d.id = $1;


-- name: ClaimDelivery :one
UPDATE notification_deliveries
SET status     = 'processing',
    locked_at  = NOW(),
    locked_by  = $2,
    updated_at = NOW()
WHERE id = $1
  AND (
    status = 'pending'
        OR (
        status = 'retry'
            AND next_retry_at <= NOW()
        )
    )
  AND (
    locked_at IS NULL
        OR locked_at < NOW() - INTERVAL '5 minutes'
    )
    RETURNING
    id,
    notification_id,
    channel,
    provider,
    destination,
    status,
    retry_count,
    max_retries,
    next_retry_at,
    locked_by,
    locked_at,
    created_at,
    updated_at,
    sent_at;


-- name: ReclaimAbandonedDeliveries :many
UPDATE notification_deliveries
SET status     = 'pending',
    locked_by  = NULL,
    locked_at  = NULL,
    updated_at = NOW()
WHERE status = 'processing'
  AND locked_at IS NOT NULL
  AND locked_at < $1 RETURNING *;


-- name: MarkDeliverySent :execrows
UPDATE notification_deliveries
SET status        = 'sent',
    sent_at       = NOW(),
    next_retry_at = NULL,
    locked_at     = NULL,
    locked_by     = NULL,
    updated_at    = NOW()
WHERE id = $1
  AND status = 'processing'
  AND locked_by = $2;


-- name: MarkDeliveryRetry :execrows
UPDATE notification_deliveries
SET status        = 'retry',
    retry_count   = retry_count + 1,
    next_retry_at = $2,
    locked_at     = NULL,
    locked_by     = NULL,
    updated_at    = NOW()
WHERE id = $1
  AND status = 'processing'
  AND locked_by = $3
  AND retry_count < max_retries;


-- name: MarkDeliveryFailed :execrows
UPDATE notification_deliveries
SET status        = 'failed',
    next_retry_at = NULL,
    locked_at     = NULL,
    locked_by     = NULL,
    updated_at    = NOW()
WHERE id = $1
  AND status = 'processing'
  AND locked_by = $2;


-- name: ListReadyRetryDeliveries :many
SELECT id
FROM notification_deliveries
WHERE status = 'retry'
  AND next_retry_at IS NOT NULL
  AND next_retry_at <= $1
ORDER BY next_retry_at,
         created_at,
         id LIMIT $2;
