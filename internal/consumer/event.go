package consumer

import (
	"fmt"
	"strconv"

	"github.com/disillusioned-labs/notification/internal/service/notification"
	"github.com/disillusioned-labs/platform/kafka"
)

const (
	headerEventID       = "event-id"
	headerEventType     = "event-type"
	headerEventVersion  = "event-version"
	headerSourceService = "source-service"
	headerAggregateType = "aggregate-type"
	headerAggregateID   = "aggregate-id"
)

func decodeNotificationEvent(
	record kafka.Record,
) (notification.NotificationEvent, error) {
	if record.Topic == "" {
		return notification.NotificationEvent{}, fmt.Errorf(
			"kafka record topic is empty",
		)
	}

	eventID, err := kafka.RequiredHeader(record.Headers, headerEventID)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	eventType, err := kafka.RequiredHeader(record.Headers, headerEventType)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	versionValue, err := kafka.RequiredHeader(record.Headers, headerEventVersion)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	version, err := strconv.Atoi(versionValue)
	if err != nil {
		return notification.NotificationEvent{}, fmt.Errorf(
			"invalid %s %q: %w",
			headerEventVersion,
			versionValue,
			err,
		)
	}

	sourceService, err := kafka.RequiredHeader(
		record.Headers,
		headerSourceService,
	)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	aggregateType, err := kafka.RequiredHeader(
		record.Headers,
		headerAggregateType,
	)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	aggregateID, err := kafka.RequiredHeader(
		record.Headers,
		headerAggregateID,
	)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	event := notification.NotificationEvent{
		EventID:       eventID,
		EventType:     eventType,
		EventVersion:  version,
		SourceService: sourceService,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Topic:         record.Topic,
		Payload:       record.Value,
	}

	if err := event.Validate(); err != nil {
		return notification.NotificationEvent{}, fmt.Errorf(
			"validate notification event: %w",
			err,
		)
	}

	return event, nil
}
