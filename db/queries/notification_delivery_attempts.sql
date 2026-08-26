-- name: CreateDeliveryAttempt :one
INSERT INTO notification_delivery_attempts (
    delivery_id,
    attempt_number,
    provider,
    provider_message_id,
    status,
    http_status_code,
    error_type,
    error_message,
    response
)
VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7,
           $8,
           $9
       )
    RETURNING *;


-- name: GetDeliveryAttempts :many
SELECT
    id,
    delivery_id,
    attempt_number,
    provider,
    provider_message_id,
    status,
    http_status_code,
    error_type,
    error_message,
    response,
    created_at
FROM notification_delivery_attempts
WHERE delivery_id = $1
ORDER BY attempt_number ASC;


-- name: GetLatestDeliveryAttempt :one
SELECT
    id,
    delivery_id,
    attempt_number,
    provider,
    provider_message_id,
    status,
    http_status_code,
    error_type,
    error_message,
    response,
    created_at
FROM notification_delivery_attempts
WHERE delivery_id = $1
ORDER BY attempt_number DESC
    LIMIT 1;
