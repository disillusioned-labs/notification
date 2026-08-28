package template

import (
	"errors"
	"strings"
)

type UserRegisteredPayload struct {
	FirstName string `json:"first_name"`
}

func (p *UserRegisteredPayload) Validate() error {
	if strings.TrimSpace(p.FirstName) == "" {
		return errors.New("first_name is required")
	}

	return nil
}

type emailPayload struct {
	Subject string `json:"subject"`
	HTML    string `json:"html"`
	Text    string `json:"text"`
}
