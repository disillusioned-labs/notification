-- name: GetNotificationByEventID :one
SELECT *
FROM notifications
WHERE event_id = sqlc.arg(event_id);


-- name: CreateNotification :one
INSERT INTO notifications (
    event_id,
    notification_type,
    category,
    recipient_id,
    payload,
    trace_id
)
VALUES (
           sqlc.arg(event_id),
           sqlc.arg(notification_type),
           sqlc.arg(category),
           sqlc.arg(recipient_id),
           sqlc.arg(payload),
           sqlc.arg(trace_id)
       )
    RETURNING *;