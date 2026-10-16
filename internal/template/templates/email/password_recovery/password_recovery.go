package password_recovery

import (
	"errors"
	"net/url"
	"strings"
	"time"
)

type PasswordRecovery struct {
	Name        string    `json:"name"`
	RecoveryURL string    `json:"recovery_url"`
	ExpiresAt   time.Time `json:"expires_at"`
}

func (p *PasswordRecovery) Validate() error {
	u, err := url.Parse(p.RecoveryURL)
	if strings.TrimSpace(p.Name) == "" || !p.ExpiresAt.After(time.Now()) || err != nil || !u.IsAbs() || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return errors.New("name and a safe recovery URL are required")
	}
	switch {
	case u.Scheme == "https":
	case u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"):
	default:
		return errors.New("recovery URL must use HTTPS")
	}
	return nil
}
