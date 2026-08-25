package notification

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	EventTypeNotificationCreated           = "notification.created"
	EventTypeNotificationDeliveryRequested = "notification.delivery.requested"
	EventTypeNotificationDeliveryRetry     = "notification.delivery.retry"
)

type NotificationEvent struct {
	ID            string
	Type          string
	Version       int
	SourceService string
	AggregateType string
	AggregateID   string
	Payload       []byte
}

func (e NotificationEvent) Validate() error {
	if e.ID == "" {
		return errors.New("event id is required")
	}

	if e.Type == "" {
		return errors.New("event type is required")
	}

	if e.Version <= 0 {
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

	if len(e.Payload) == 0 {
		return errors.New("event payload is required")
	}

	return nil
}

func (e NotificationEvent) DecodePayload() (any, error) {
	switch e.Type {
	case EventTypeNotificationCreated:
		var payload NotificationCreatedEvent

		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			return nil, fmt.Errorf("decode notification.created payload: %w", err)
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
		return nil, fmt.Errorf("unsupported event type %q", e.Type)
	}
}

type NotificationCreatedEvent struct {
	NotificationID string                `json:"notification_id"`
	Recipient      NotificationRecipient `json:"recipient"`
	Channels       []NotificationChannel `json:"channels"`
	Payload        json.RawMessage       `json:"payload"`
}

type NotificationRecipient struct {
	UserID string `json:"user_id"`
}

type NotificationChannel struct {
	Channel  string `json:"channel"`
	Template string `json:"template"`
}

type NotificationDeliveryRequestedEvent struct {
	DeliveryID string `json:"delivery_id"`
}

type NotificationDeliveryRetryEvent struct {
	DeliveryID string `json:"delivery_id"`
}
