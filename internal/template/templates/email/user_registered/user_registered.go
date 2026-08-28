package user_registered

import (
	"errors"
	"strings"
)

type UserRegistered struct {
	Name string `json:"name"`
}

func (p *UserRegistered) Validate() error {
	if strings.TrimSpace(p.Name) == "" {
		return errors.New("name is required")
	}

	return nil
}
