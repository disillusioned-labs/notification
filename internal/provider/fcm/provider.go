package fcm

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/disillusioned-labs/notification/internal/provider"
)

const (
	ProviderName = "firebase"
	ChannelPush  = "push"
)

// Package vars rather than consts so tests can point them at an httptest
// server; production code must not reassign them.
var (
	tokenEndpoint    = "https://oauth2.googleapis.com/token"
	sendEndpointFmt  = "https://fcm.googleapis.com/v1/projects/%s/messages:send"
	firebaseScope    = "https://www.googleapis.com/auth/firebase.messaging"
	tokenRefreshLead = time.Minute
)

// Config carries the Firebase service account credentials. Exactly one of the
// two sources must be set; an unset Config means the provider is not built and
// the push channel stays disabled.
type Config struct {
	ServiceAccountJSON string
	ServiceAccountFile string
}

// Provider sends push notifications through the FCM HTTP v1 API. It depends
// only on the standard library: the service-account JWT assertion is signed
// here and exchanged for an OAuth2 access token, which is cached until shortly
// before expiry.
type Provider struct {
	httpClient *http.Client
	tokens     *tokenCache

	mu sync.Mutex
}

var _ provider.Provider = (*Provider)(nil)

// NewFCMProvider validates the credentials eagerly so a bad service account
// fails at boot, not at the first push.
func NewFCMProvider(cfg Config, httpClient *http.Client) (*Provider, error) {
	sa, err := loadServiceAccount(cfg.ServiceAccountJSON, cfg.ServiceAccountFile)
	if err != nil {
		return nil, err
	}

	cache, err := newTokenCache(sa)
	if err != nil {
		return nil, err
	}

	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &Provider{
		httpClient: httpClient,
		tokens:     cache,
	}, nil
}

// pushPayload is the rendered push message a template produces.
type pushPayload struct {
	Title string            `json:"title"`
	Body  string            `json:"body"`
	Data  map[string]string `json:"data,omitempty"`
}

func (p *Provider) Send(
	ctx context.Context,
	req provider.SendRequest,
) (provider.SendResult, error) {
	if err := ctx.Err(); err != nil {
		return provider.SendResult{
			ErrorType:    string(provider.ErrorTypeTimeout),
			ErrorMessage: err.Error(),
			Retryable:    true,
		}, err
	}

	if err := validateRequest(req); err != nil {
		return provider.SendResult{
			ErrorType:    string(provider.ErrorTypeInvalidRequest),
			ErrorMessage: err.Error(),
			Retryable:    false,
		}, err
	}

	payload, err := decodePushPayload(req.Payload)
	if err != nil {
		return provider.SendResult{
			ErrorType:    string(provider.ErrorTypeInvalidRequest),
			ErrorMessage: err.Error(),
			Retryable:    false,
		}, err
	}

	accessToken, err := p.accessToken(ctx)
	if err != nil {
		return provider.SendResult{
			ErrorType:    string(provider.ErrorTypeUnavailable),
			ErrorMessage: fmt.Sprintf("fcm: obtain access token: %v", err),
			Retryable:    true,
		}, err
	}

	message := map[string]any{
		"token": req.Destination,
		"notification": map[string]string{
			"title": payload.Title,
			"body":  payload.Body,
		},
	}
	if len(payload.Data) > 0 {
		message["data"] = payload.Data
	}

	body, err := json.Marshal(map[string]any{"message": message})
	if err != nil {
		return provider.SendResult{
			ErrorType:    string(provider.ErrorTypeInternal),
			ErrorMessage: err.Error(),
			Retryable:    false,
		}, err
	}

	sendURL := fmt.Sprintf(sendEndpointFmt, p.tokens.projectID)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, sendURL, bytes.NewReader(body))
	if err != nil {
		return provider.SendResult{
			ErrorType:    string(provider.ErrorTypeInternal),
			ErrorMessage: err.Error(),
			Retryable:    false,
		}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+accessToken)
	httpReq.Header.Set("Content-Type", "application/json; charset=UTF-8")

	resp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return provider.SendResult{
			ErrorType:    string(provider.ErrorTypeUnavailable),
			ErrorMessage: err.Error(),
			Retryable:    true,
		}, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		respBody = nil
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return mapSendError(resp.StatusCode, respBody)
	}

	var accepted struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(respBody, &accepted)

	// The response body carries only FCM's message name, never the
	// destination token, so it is safe to persist as the attempt record.
	return provider.SendResult{
		MessageID:      accepted.Name,
		HTTPStatusCode: int32(resp.StatusCode),
		Response:       respBody,
		Retryable:      false,
	}, nil
}

func validateRequest(req provider.SendRequest) error {
	if req.Channel != ChannelPush {
		return fmt.Errorf("fcm: unsupported channel %q", req.Channel)
	}

	if strings.TrimSpace(req.Destination) == "" {
		return errors.New("fcm: destination token is required")
	}

	return nil
}

func decodePushPayload(raw []byte) (pushPayload, error) {
	var payload pushPayload

	if len(raw) == 0 {
		return payload, errors.New("fcm: push payload is empty")
	}

	if err := json.Unmarshal(raw, &payload); err != nil {
		return payload, fmt.Errorf("fcm: decode push payload: %w", err)
	}

	if strings.TrimSpace(payload.Title) == "" || strings.TrimSpace(payload.Body) == "" {
		return payload, errors.New("fcm: push payload needs title and body")
	}

	return payload, nil
}

// mapSendError classifies FCM HTTP errors. 429/5xx are transient; 401 means
// the cached access token expired early and the next attempt mints a fresh
// one; everything else (400, 404, 410 - unregistered/invalid tokens) is a
// permanent failure for this destination.
func mapSendError(statusCode int, respBody []byte) (provider.SendResult, error) {
	errType := provider.ErrorTypeNotFound
	retryable := false

	switch {
	case statusCode == http.StatusTooManyRequests || statusCode >= 500:
		errType = provider.ErrorTypeRateLimited
		retryable = true
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		errType = provider.ErrorTypeUnavailable
		retryable = true
	}

	return provider.SendResult{
		HTTPStatusCode: int32(statusCode),
		Response:       respBody,
		ErrorType:      string(errType),
		ErrorMessage:   fmt.Sprintf("fcm: send failed with status %d", statusCode),
		Retryable:      retryable,
	}, fmt.Errorf("fcm: send failed with status %d", statusCode)
}

func parsePEMKey(raw []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("fcm: private key pem block not found")
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, errors.New("fcm: private key is neither PKCS1 nor PKCS8")
		}
	}

	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("fcm: private key is not RSA")
	}

	return rsaKey, nil
}

// accessToken returns a cached access token or mints a new one. The mutex is
// held across the HTTP exchange so concurrent deliveries under token rotation
// share one request instead of stampeding the token endpoint.
func (p *Provider) accessToken(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.tokens.accessToken != "" && time.Now().Before(p.tokens.expiresAt) {
		return p.tokens.accessToken, nil
	}

	assertion, err := p.signAssertion(time.Now())
	if err != nil {
		return "", err
	}

	form := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"assertion":  {assertion},
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint returned status %d", resp.StatusCode)
	}

	var granted struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(respBody, &granted); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}

	if granted.AccessToken == "" {
		return "", errors.New("token response carried no access token")
	}

	expiresIn := time.Duration(granted.ExpiresIn) * time.Second
	if expiresIn <= 0 {
		expiresIn = time.Hour
	}

	p.tokens.accessToken = granted.AccessToken
	p.tokens.expiresAt = time.Now().Add(expiresIn - tokenRefreshLead)

	return p.tokens.accessToken, nil
}

// signAssertion builds the signed JWT assertion for the OAuth2 token
// exchange: RS256 over {header, claims} with the Firebase messaging scope.
func (p *Provider) signAssertion(now time.Time) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))

	claims, err := json.Marshal(map[string]any{
		"iss":   p.tokens.clientEmail,
		"scope": firebaseScope,
		"aud":   tokenEndpoint,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	})
	if err != nil {
		return "", err
	}

	claimsB64 := base64.RawURLEncoding.EncodeToString(claims)
	signingInput := header + "." + claimsB64

	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, p.tokens.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}

	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
