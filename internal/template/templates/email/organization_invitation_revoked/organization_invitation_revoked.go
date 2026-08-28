package organization_invitation_revoked

import (
	"errors"
	"strings"
)

type OrganizationInvitationRevoked struct {
	OrganizationName string `json:"organization_name"`
	InviteeName      string `json:"invitee_name"`
	RevokerName      string `json:"revoker_name"`
}

func (p *OrganizationInvitationRevoked) Validate() error {
	if strings.TrimSpace(p.OrganizationName) == "" {
		return errors.New("organization_name is required")
	}

	if strings.TrimSpace(p.InviteeName) == "" {
		return errors.New("invitee_name is required")
	}

	if strings.TrimSpace(p.RevokerName) == "" {
		return errors.New("revoker_name is required")
	}

	return nil
}
