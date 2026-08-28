package organization_deleted

import (
	"errors"
	"strings"
)

type OrganizationDeleted struct {
	OrganizationName string `json:"organization_name"`
	MemberName       string `json:"member_name"`
}

func (p *OrganizationDeleted) Validate() error {
	if strings.TrimSpace(p.OrganizationName) == "" {
		return errors.New("organization_name is required")
	}

	if strings.TrimSpace(p.MemberName) == "" {
		return errors.New("member_name is required")
	}

	return nil
}
