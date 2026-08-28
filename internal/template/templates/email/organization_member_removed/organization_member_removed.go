package organization_member_removed

import (
	"errors"
	"strings"
)

type OrganizationMemberRemoved struct {
	OrganizationName string `json:"organization_name"`
	MemberName       string `json:"member_name"`
	RemoverName      string `json:"remover_name"`
}

func (p *OrganizationMemberRemoved) Validate() error {
	if strings.TrimSpace(p.OrganizationName) == "" {
		return errors.New("organization_name is required")
	}

	if strings.TrimSpace(p.MemberName) == "" {
		return errors.New("member_name is required")
	}

	if strings.TrimSpace(p.RemoverName) == "" {
		return errors.New("remover_name is required")
	}

	return nil
}
