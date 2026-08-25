package consumer

import (
	"fmt"
	"strconv"

	"github.com/disillusioned-labs/notification/internal/platform/kafka"
	"github.com/disillusioned-labs/notification/internal/service/notification"
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

	eventID, err := requiredHeader(record.Headers, headerEventID)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	eventType, err := requiredHeader(record.Headers, headerEventType)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	versionValue, err := requiredHeader(record.Headers, headerEventVersion)
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

	sourceService, err := requiredHeader(
		record.Headers,
		headerSourceService,
	)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	aggregateType, err := requiredHeader(
		record.Headers,
		headerAggregateType,
	)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	aggregateID, err := requiredHeader(
		record.Headers,
		headerAggregateID,
	)
	if err != nil {
		return notification.NotificationEvent{}, err
	}

	event := notification.NotificationEvent{
		ID:            eventID,
		Type:          eventType,
		Version:       version,
		SourceService: sourceService,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
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

func requiredHeader(
	headers []kafka.RecordHeader,
	key string,
) (string, error) {
	value, ok := kafka.HeaderString(headers, key)
	if !ok || value == "" {
		return "", fmt.Errorf(
			"missing required header %q",
			key,
		)
	}

	return value, nil
}
