-- name: NotificationExistsByEventID :one
SELECT EXISTS (SELECT 1
               FROM notifications
               WHERE event_id = $1);

-- name: GetNotificationByID :one
SELECT event_id,
       notification_type,
       category,
       recipient_id,
       payload,
       trace_id
FROM notifications
WHERE id = $1;


-- name: CreateNotification :one
INSERT INTO notifications (event_id,
                           notification_type,
                           category,
                           recipient_id,
                           payload,
                           trace_id)
VALUES ($1,
        $2,
        $3,
        $4,
        $5,
        $6) RETURNING *;
