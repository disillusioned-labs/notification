package user_registered

import (
	"errors"
	"strings"
)

type UserRegistered struct {
	FirstName string `json:"first_name"`
}

func (p *UserRegistered) Validate() error {
	if strings.TrimSpace(p.FirstName) == "" {
		return errors.New("first_name is required")
	}

	return nil
}
