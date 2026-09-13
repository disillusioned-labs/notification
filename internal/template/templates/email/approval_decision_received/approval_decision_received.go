package approval_decision_received

import (
	"errors"
	"strings"
)

// ApprovalDecisionReceived is emitted by expense when a decision lands on a
// transaction; the recipient is the transaction's creator.
type ApprovalDecisionReceived struct {
	CreatorName            string `json:"creator_name"`
	TransactionID          string `json:"transaction_id"`
	TransactionDescription string `json:"transaction_description"`
	AmountDisplay          string `json:"amount_display"`
	Decision               string `json:"decision"` // "approved" | "rejected"
	DeciderName            string `json:"decider_name"`
}

// TransactionRef lets the push renderer attach the transaction id to
// the message data map, so a client can open the transaction on tap.
func (p *ApprovalDecisionReceived) TransactionRef() string { return p.TransactionID }

func (p *ApprovalDecisionReceived) Validate() error {
	if strings.TrimSpace(p.CreatorName) == "" {
		return errors.New("creator_name is required")
	}

	if strings.TrimSpace(p.TransactionID) == "" {
		return errors.New("transaction_id is required")
	}

	if strings.TrimSpace(p.AmountDisplay) == "" {
		return errors.New("amount_display is required")
	}

	if p.Decision != "approved" && p.Decision != "rejected" {
		return errors.New("decision must be approved or rejected")
	}

	if strings.TrimSpace(p.DeciderName) == "" {
		return errors.New("decider_name is required")
	}

	return nil
}
