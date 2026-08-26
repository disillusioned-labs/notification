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
	EventID       string
	EventType     string
	EventVersion  int
	SourceService string
	AggregateType string
	AggregateID   string
	TraceID       string
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

	if len(e.Payload) == 0 {
		return errors.New("event payload is required")
	}

	return nil
}

func (e NotificationEvent) DecodePayload() (any, error) {
	switch e.EventType {
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
		return nil, fmt.Errorf("unsupported event type %q", e.EventType)
	}
}

type NotificationCreatedEvent struct {
	NotificationType string               `json:"notification_type"`
	Category         string               `json:"category"`
	RecipientID      string               `json:"recipient_id"`
	Targets          []NotificationTarget `json:"targets"`
	Payload          json.RawMessage      `json:"payload"`
}

type NotificationTarget struct {
	Channel     string `json:"channel"`
	Destination string `json:"destination"`
}

func (e NotificationCreatedEvent) Validate() error {
	if e.NotificationType == "" {
		return errors.New("notification type is required")
	}

	if e.Category == "" {
		return errors.New("category is required")
	}

	if e.RecipientID == "" {
		return errors.New("recipient id is required")
	}

	if len(e.Targets) == 0 {
		return errors.New("at least one target is required")
	}

	if len(e.Payload) == 0 {
		return errors.New("payload is required")
	}

	if !json.Valid(e.Payload) {
		return errors.New("payload must be valid JSON")
	}

	seenChannels := make(map[string]struct{}, len(e.Targets))

	for i, target := range e.Targets {
		if err := target.Validate(); err != nil {
			return fmt.Errorf("target[%d]: %w", i, err)
		}

		if _, exists := seenChannels[target.Channel]; exists {
			return fmt.Errorf(
				"target[%d]: duplicate channel %q",
				i,
				target.Channel,
			)
		}

		seenChannels[target.Channel] = struct{}{}
	}

	return nil
}

func (t NotificationTarget) Validate() error {
	if t.Channel == "" {
		return errors.New("channel is required")
	}

	switch t.Channel {
	case "email", "sms", "push":
	default:
		return fmt.Errorf("unsupported channel %q", t.Channel)
	}

	if t.Destination == "" {
		return errors.New("destination is required")
	}

	return nil
}

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
