package password_changed

import (
	"errors"
	"strings"
)

type PasswordChanged struct {
	Name string `json:"name"`
}

func (p *PasswordChanged) Validate() error {
	if strings.TrimSpace(p.Name) == "" {
		return errors.New("name is required")
	}
	return nil
}
