package fcm

import (
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// serviceAccount is the subset of a Firebase service-account JSON file this
// provider needs. The private key never leaves this package and is never
// logged.
type serviceAccount struct {
	ProjectID   string `json:"project_id"`
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
}

// loadServiceAccount reads credentials from the inline JSON or, when the
// inline value is empty, from the file at path. Exactly one source must be
// configured; ambiguity is a deployment error, not a preference.
func loadServiceAccount(inlineJSON, filePath string) (*serviceAccount, error) {
	if inlineJSON != "" && filePath != "" {
		return nil, errors.New(
			"fcm: set either FIREBASE_SERVICE_ACCOUNT_JSON or FIREBASE_SERVICE_ACCOUNT_FILE, not both",
		)
	}

	raw := inlineJSON
	if raw == "" {
		if strings.TrimSpace(filePath) == "" {
			return nil, errors.New("fcm: service account credentials are required")
		}

		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("fcm: read service account file: %w", err)
		}
		raw = string(data)
	}

	var sa serviceAccount
	if err := json.Unmarshal([]byte(raw), &sa); err != nil {
		return nil, fmt.Errorf("fcm: decode service account json: %w", err)
	}

	switch {
	case strings.TrimSpace(sa.ProjectID) == "":
		return nil, errors.New("fcm: service account is missing project_id")
	case strings.TrimSpace(sa.ClientEmail) == "":
		return nil, errors.New("fcm: service account is missing client_email")
	case strings.TrimSpace(sa.PrivateKey) == "":
		return nil, errors.New("fcm: service account is missing private_key")
	}

	return &sa, nil
}

// parsePrivateKey decodes the PEM RSA private key from the service account.
// Go's stdlib rejects the escaped newlines as-is, so they are restored first.
func (sa *serviceAccount) parsePrivateKey() (*rsa.PrivateKey, error) {
	key := strings.ReplaceAll(sa.PrivateKey, "\\n", "\n")
	parsed, err := parsePEMKey([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("fcm: parse service account private key: %w", err)
	}
	return parsed, nil
}

func (sa *serviceAccount) validate() error {
	_, err := sa.parsePrivateKey()
	return err
}

// tokenCache keeps one OAuth2 access token alive for the process. FCM v1
// calls are authenticated with a bearer token minted from the service
// account's JWT assertion; minting on every send would double the requests
// and rate-limit quickly.
type tokenCache struct {
	clientEmail string
	projectID   string
	key         *rsa.PrivateKey

	accessToken string
	expiresAt   time.Time
}

func newTokenCache(sa *serviceAccount) (*tokenCache, error) {
	key, err := sa.parsePrivateKey()
	if err != nil {
		return nil, err
	}

	return &tokenCache{
		clientEmail: sa.ClientEmail,
		projectID:   sa.ProjectID,
		key:         key,
	}, nil
}
