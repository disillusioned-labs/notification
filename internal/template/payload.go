package template

import (
	"errors"
	"strings"
)

type UserRegisteredPayload struct {
	Name string `json:"first_name"`
}

func (p *UserRegisteredPayload) Validate() error {
	if strings.TrimSpace(p.Name) == "" {
		return errors.New("first_name is required")
	}

	return nil
}

type emailPayload struct {
	Subject string `json:"subject"`
	HTML    string `json:"html"`
	Text    string `json:"text"`
}
