-- name: CreateDeliveryAttempt :one
INSERT INTO notification_delivery_attempts (delivery_id,
                                            attempt_number,
                                            provider,
                                            provider_message_id,
                                            status,
                                            http_status_code,
                                            error_type,
                                            error_message,
                                            response)
VALUES (sqlc.arg(delivery_id),
        sqlc.arg(attempt_number),
        sqlc.arg(provider),
        sqlc.arg(provider_message_id),
        sqlc.arg(status),
        sqlc.arg(http_status_code),
        sqlc.arg(error_type),
        sqlc.arg(error_message),
        sqlc.arg(response)) RETURNING *;


-- name: GetDeliveryAttempt :one
SELECT *
FROM notification_delivery_attempts
WHERE delivery_id = sqlc.arg(delivery_id)
  AND attempt_number = sqlc.arg(attempt_number);


-- name: ListDeliveryAttempts :many
SELECT *
FROM notification_delivery_attempts
WHERE delivery_id = sqlc.arg(delivery_id)
ORDER BY attempt_number ASC;