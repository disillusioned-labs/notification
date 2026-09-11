package approval_step_activated

import (
	"errors"
	"strings"
)

// ApprovalStepActivated is emitted by expense when an approval step goes
// active and the approver's turn starts.
type ApprovalStepActivated struct {
	ApproverName           string `json:"approver_name"`
	TransactionID          string `json:"transaction_id"`
	TransactionDescription string `json:"transaction_description"`
	AmountDisplay          string `json:"amount_display"`
	Step                   int    `json:"step"`
}

func (p *ApprovalStepActivated) Validate() error {
	if strings.TrimSpace(p.ApproverName) == "" {
		return errors.New("approver_name is required")
	}

	if strings.TrimSpace(p.TransactionID) == "" {
		return errors.New("transaction_id is required")
	}

	if strings.TrimSpace(p.AmountDisplay) == "" {
		return errors.New("amount_display is required")
	}

	if p.Step < 1 {
		return errors.New("step must be >= 1")
	}

	return nil
}
