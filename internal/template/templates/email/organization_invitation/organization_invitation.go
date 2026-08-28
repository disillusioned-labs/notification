package organization_invitation

import (
	"errors"
	"strings"
)

type OrganizationInvitation struct {
	OrganizationName string `json:"organization_name"`
	InviterName      string `json:"inviter_name"`
	InviteURL        string `json:"invite_url"`
}

func (p *OrganizationInvitation) Validate() error {
	if strings.TrimSpace(p.OrganizationName) == "" {
		return errors.New("organization_name is required")
	}

	if strings.TrimSpace(p.InviterName) == "" {
		return errors.New("inviter_name is required")
	}

	if strings.TrimSpace(p.InviteURL) == "" {
		return errors.New("invite_url is required")
	}

	return nil
}
