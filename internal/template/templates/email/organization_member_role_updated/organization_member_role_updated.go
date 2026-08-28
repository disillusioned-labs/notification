package organization_member_role_updated

import (
	"errors"
	"strings"
)

type OrganizationMemberRoleUpdated struct {
	OrganizationName string `json:"organization_name"`
	MemberName       string `json:"member_name"`
	OldRole          string `json:"old_role"`
	NewRole          string `json:"new_role"`
}

func (p *OrganizationMemberRoleUpdated) Validate() error {
	if strings.TrimSpace(p.OrganizationName) == "" {
		return errors.New("organization_name is required")
	}

	if strings.TrimSpace(p.MemberName) == "" {
		return errors.New("member_name is required")
	}

	if strings.TrimSpace(p.OldRole) == "" {
		return errors.New("old_role is required")
	}

	if strings.TrimSpace(p.NewRole) == "" {
		return errors.New("new_role is required")
	}

	return nil
}
