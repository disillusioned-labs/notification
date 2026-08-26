package provider

import (
	"fmt"
	"sync"
)

type Registry interface {
	Get(name string) (Provider, error)
	Register(name string, p Provider) error
}

type registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

func NewRegistry() Registry {
	return &registry{
		providers: make(map[string]Provider),
	}
}

func (r *registry) Register(
	name string,
	p Provider,
) error {
	if name == "" {
		return fmt.Errorf("provider name must not be empty")
	}

	if p == nil {
		return fmt.Errorf("provider %q must not be nil", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.providers[name]; exists {
		return fmt.Errorf(
			"provider %q already registered",
			name,
		)
	}

	r.providers[name] = p

	return nil
}

func (r *registry) Get(name string) (Provider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p, ok := r.providers[name]
	if !ok {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrProviderNotFound,
			name,
		)
	}

	if p == nil {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrProviderUnavailable,
			name,
		)
	}

	return p, nil
}
