package template

import (
	"fmt"
	"sort"
	"strings"
)

type Payload interface {
	Validate() error
}

type PayloadFactory func() Payload

type PayloadRegistry struct {
	factories map[string]PayloadFactory
}

func NewPayloadRegistry() *PayloadRegistry {
	return &PayloadRegistry{
		factories: make(map[string]PayloadFactory),
	}
}

func (r *PayloadRegistry) Register(
	notificationType string,
	factory PayloadFactory,
) error {
	notificationType = strings.TrimSpace(notificationType)

	if notificationType == "" {
		return fmt.Errorf("notification type is required")
	}

	if factory == nil {
		return fmt.Errorf(
			"payload factory for %q is nil",
			notificationType,
		)
	}

	if _, exists := r.factories[notificationType]; exists {
		return fmt.Errorf(
			"payload for notification type %q already registered",
			notificationType,
		)
	}

	r.factories[notificationType] = factory

	return nil
}

func (r *PayloadRegistry) New(
	notificationType string,
) (Payload, bool) {
	factory, ok := r.factories[notificationType]
	if !ok {
		return nil, false
	}

	return factory(), true
}

func (r *PayloadRegistry) Types() []string {
	types := make([]string, 0, len(r.factories))

	for notificationType := range r.factories {
		types = append(types, notificationType)
	}

	sort.Strings(types)

	return types
}
