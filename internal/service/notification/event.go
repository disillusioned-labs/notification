package notification

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/disillusioned-labs/platform/contract/notification"
)

type NotificationEvent struct {
	EventID       string
	EventType     string
	EventVersion  int
	SourceService string
	AggregateType string
	AggregateID   string
	TraceID       string
	Topic         string
	Payload       []byte
}

func (e NotificationEvent) Validate() error {
	if e.EventID == "" {
		return errors.New("event id is required")
	}

	if e.EventType == "" {
		return errors.New("event type is required")
	}

	if e.EventVersion <= 0 {
		return errors.New("event version must be greater than zero")
	}

	if e.SourceService == "" {
		return errors.New("source service is required")
	}

	if e.AggregateType == "" {
		return errors.New("aggregate type is required")
	}

	if e.AggregateID == "" {
		return errors.New("aggregate id is required")
	}

	if e.Topic == "" {
		return errors.New("topic is required")
	}

	if len(e.Payload) == 0 {
		return errors.New("event payload is required")
	}

	return nil
}

func (e NotificationEvent) DecodePayload() (any, error) {
	switch e.EventType {
	case notification.EventTypeCreated:
		var payload notification.CreatedEvent

		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			return nil, fmt.Errorf(
				"decode notification.created payload: %w",
				err,
			)
		}

		return payload, nil

	case EventTypeNotificationDeliveryRequested:
		var payload NotificationDeliveryRequestedEvent

		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			return nil, fmt.Errorf(
				"decode notification.delivery.requested payload: %w",
				err,
			)
		}

		return payload, nil

	case EventTypeNotificationDeliveryRetry:
		var payload NotificationDeliveryRetryEvent

		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			return nil, fmt.Errorf(
				"decode notification.delivery.retry payload: %w",
				err,
			)
		}

		return payload, nil

	default:
		return nil, fmt.Errorf(
			"unsupported event type %q",
			e.EventType,
		)
	}
}

const (
	EventTypeNotificationDeliveryRequested = "notification.delivery.requested"
	EventTypeNotificationDeliveryRetry     = "notification.delivery.retry"
)

type NotificationDeliveryRequestedEvent struct {
	DeliveryID string `json:"delivery_id"`
}

func (e NotificationDeliveryRequestedEvent) Validate() error {
	if e.DeliveryID == "" {
		return errors.New("delivery_id is required")
	}

	return nil
}

type NotificationDeliveryRetryEvent struct {
	DeliveryID string `json:"delivery_id"`
}

func (e NotificationDeliveryRetryEvent) Validate() error {
	if e.DeliveryID == "" {
		return errors.New("delivery_id is required")
	}

	return nil
}
