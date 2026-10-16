-- +goose Up
DROP INDEX uq_notification_delivery_channel;

CREATE UNIQUE INDEX uq_notification_delivery_non_push_channel
    ON notification_deliveries (notification_id, channel)
    WHERE channel <> 'push';

CREATE UNIQUE INDEX uq_notification_delivery_push_destination
    ON notification_deliveries (notification_id, destination)
    WHERE channel = 'push';

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM notification_deliveries
        WHERE channel = 'push'
        GROUP BY notification_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot restore one-push-delivery-per-notification while fan-out rows exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX uq_notification_delivery_push_destination;
DROP INDEX uq_notification_delivery_non_push_channel;

CREATE UNIQUE INDEX uq_notification_delivery_channel
    ON notification_deliveries (notification_id, channel);
