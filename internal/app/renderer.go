package app

import (
	"fmt"

	"github.com/disillusioned-labs/notification/internal/template"
	"github.com/disillusioned-labs/notification/internal/template/templates/email/organization_deleted"
	"github.com/disillusioned-labs/notification/internal/template/templates/email/organization_invitation"
	"github.com/disillusioned-labs/notification/internal/template/templates/email/organization_invitation_accepted"
	"github.com/disillusioned-labs/notification/internal/template/templates/email/organization_invitation_revoked"
	"github.com/disillusioned-labs/notification/internal/template/templates/email/organization_member_removed"
	"github.com/disillusioned-labs/notification/internal/template/templates/email/organization_member_role_updated"
	"github.com/disillusioned-labs/notification/internal/template/templates/email/user_registered"
)

func buildRenderer() (template.Renderer, error) {
	payloadRegistry := template.NewPayloadRegistry()

	payloads := map[string]func() template.Payload{
		"user_registered":                  func() template.Payload { return &user_registered.UserRegistered{} },
		"organization_invitation":          func() template.Payload { return &organization_invitation.OrganizationInvitation{} },
		"organization_invitation_accepted": func() template.Payload { return &organization_invitation_accepted.OrganizationInvitationAccepted{} },
		"organization_invitation_revoked":  func() template.Payload { return &organization_invitation_revoked.OrganizationInvitationRevoked{} },
		"organization_member_removed":      func() template.Payload { return &organization_member_removed.OrganizationMemberRemoved{} },
		"organization_member_role_updated": func() template.Payload { return &organization_member_role_updated.OrganizationMemberRoleUpdated{} },
		"organization_deleted":             func() template.Payload { return &organization_deleted.OrganizationDeleted{} },
	}

	for name, newPayload := range payloads {
		if err := payloadRegistry.Register(name, newPayload); err != nil {
			return nil, fmt.Errorf(
				"register %s payload: %w",
				name,
				err,
			)
		}
	}

	renderer, err := template.NewLocalRenderer(
		payloadRegistry,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"initialize notification renderer: %w",
			err,
		)
	}

	return renderer, nil
}
