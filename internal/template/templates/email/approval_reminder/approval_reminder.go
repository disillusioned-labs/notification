package approval_reminder

import (
	"errors"
	"strings"
)

// ApprovalReminder is emitted by expense's reminder worker for an active
// approval step that has been waiting longer than the configured threshold.
type ApprovalReminder struct {
	ApproverName           string `json:"approver_name"`
	TransactionID          string `json:"transaction_id"`
	TransactionDescription string `json:"transaction_description"`
	AmountDisplay          string `json:"amount_display"`
	WaitingDays            int    `json:"waiting_days"`
}

func (p *ApprovalReminder) Validate() error {
	if strings.TrimSpace(p.ApproverName) == "" {
		return errors.New("approver_name is required")
	}

	if strings.TrimSpace(p.TransactionID) == "" {
		return errors.New("transaction_id is required")
	}

	if strings.TrimSpace(p.AmountDisplay) == "" {
		return errors.New("amount_display is required")
	}

	if p.WaitingDays < 1 {
		return errors.New("waiting_days must be >= 1")
	}

	return nil
}
