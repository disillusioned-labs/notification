package organization_invitation_accepted

import (
	"errors"
	"strings"
)

type OrganizationInvitationAccepted struct {
	OrganizationName string `json:"organization_name"`
	InviteeName      string `json:"invitee_name"`
	InviterName      string `json:"inviter_name"`
}

func (p *OrganizationInvitationAccepted) Validate() error {
	if strings.TrimSpace(p.OrganizationName) == "" {
		return errors.New("organization_name is required")
	}

	if strings.TrimSpace(p.InviteeName) == "" {
		return errors.New("invitee_name is required")
	}

	if strings.TrimSpace(p.InviterName) == "" {
		return errors.New("inviter_name is required")
	}

	return nil
}
