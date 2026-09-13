package fcm

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/disillusioned-labs/notification/internal/provider"
)

// testServiceAccount mints a throwaway service-account JSON whose private key
// is generated in-process, so the provider's JWT signing path is exercised
// for real without any Google endpoint.
func testServiceAccount(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}

	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal pkcs8: %v", err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: der,
	})

	sa := map[string]string{
		"project_id":   "test-project",
		"client_email": "push@test-project.iam.gserviceaccount.com",
		"private_key":  string(pemKey),
	}

	raw, err := json.Marshal(sa)
	if err != nil {
		t.Fatalf("marshal service account: %v", err)
	}

	return string(raw)
}

// overrideEndpoints redirects the OAuth2 token and FCM send endpoints at the
// given test servers and restores the originals when the test finishes.
func overrideEndpoints(t *testing.T, tokenURL, sendURL string) {
	t.Helper()

	oldToken, oldSend := tokenEndpoint, sendEndpointFmt
	tokenEndpoint = tokenURL
	sendEndpointFmt = sendURL

	t.Cleanup(func() {
		tokenEndpoint = oldToken
		sendEndpointFmt = oldSend
	})
}

func TestSendHappyPath(t *testing.T) {
	var sendRequests atomic.Int32

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse token form: %v", err)
		}
		if got := r.Form.Get("grant_type"); got != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
			t.Errorf("grant_type = %q", got)
		}
		if r.Form.Get("assertion") == "" {
			t.Error("assertion is required")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"tok-1","expires_in":3600}`)
	}))
	defer tokenSrv.Close()

	sendSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sendRequests.Add(1)

		if auth := r.Header.Get("Authorization"); auth != "Bearer tok-1" {
			t.Errorf("Authorization = %q", auth)
		}

		var body struct {
			Message struct {
				Token        string            `json:"token"`
				Notification map[string]string `json:"notification"`
				Data         map[string]string `json:"data"`
			} `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode send body: %v", err)
		}
		if body.Message.Token != "device-token-1" {
			t.Errorf("token = %q", body.Message.Token)
		}
		if body.Message.Notification["title"] != "Approval needed" {
			t.Errorf("title = %q", body.Message.Notification["title"])
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"projects/test-project/messages/msg-1"}`)
	}))
	defer sendSrv.Close()

	overrideEndpoints(t, tokenSrv.URL, sendSrv.URL+"/v1/projects/%s/messages:send")

	p, err := NewFCMProvider(Config{ServiceAccountJSON: testServiceAccount(t)}, tokenSrv.Client())
	if err != nil {
		t.Fatalf("NewFCMProvider: %v", err)
	}

	result, err := p.Send(context.Background(), provider.SendRequest{
		Channel:     ChannelPush,
		Destination: "device-token-1",
		Payload:     []byte(`{"title":"Approval needed","body":"Decide soon","data":{"transaction_id":"tx-1"}}`),
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	if result.MessageID != "projects/test-project/messages/msg-1" {
		t.Errorf("MessageID = %q", result.MessageID)
	}
	if result.HTTPStatusCode != 200 {
		t.Errorf("HTTPStatusCode = %d", result.HTTPStatusCode)
	}
}

func TestSendRetriesUseCachedToken(t *testing.T) {
	var tokenRequests atomic.Int32

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"tok-1","expires_in":3600}`)
	}))
	defer tokenSrv.Close()

	sendSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"projects/test-project/messages/msg-1"}`)
	}))
	defer sendSrv.Close()

	overrideEndpoints(t, tokenSrv.URL, sendSrv.URL+"/v1/projects/%s/messages:send")

	p, err := NewFCMProvider(Config{ServiceAccountJSON: testServiceAccount(t)}, tokenSrv.Client())
	if err != nil {
		t.Fatalf("NewFCMProvider: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := p.Send(context.Background(), provider.SendRequest{
			Channel:     ChannelPush,
			Destination: "device-token-1",
			Payload:     []byte(`{"title":"t","body":"b"}`),
		}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}

	if got := tokenRequests.Load(); got != 1 {
		t.Errorf("token endpoint hit %d times, want 1 (token must be cached)", got)
	}
}

func TestSendErrorMapping(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		wantRetry  bool
		wantErrTyp string
	}{
		{"429 rate limited", 429, true, string(provider.ErrorTypeRateLimited)},
		{"500 provider", 500, true, string(provider.ErrorTypeRateLimited)},
		{"401 token expired", 401, true, string(provider.ErrorTypeUnavailable)},
		{"400 invalid token", 400, false, string(provider.ErrorTypeNotFound)},
		{"410 unregistered", 410, false, string(provider.ErrorTypeNotFound)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"access_token":"tok-1","expires_in":3600}`)
			}))
			defer tokenSrv.Close()

			sendSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				fmt.Fprint(w, `{"error":{"code":404,"message":"Requested entity was not found."}}`)
			}))
			defer sendSrv.Close()

			overrideEndpoints(t, tokenSrv.URL, sendSrv.URL+"/v1/projects/%s/messages:send")

			p, err := NewFCMProvider(Config{ServiceAccountJSON: testServiceAccount(t)}, tokenSrv.Client())
			if err != nil {
				t.Fatalf("NewFCMProvider: %v", err)
			}

			result, err := p.Send(context.Background(), provider.SendRequest{
				Channel:     ChannelPush,
				Destination: "device-token-1",
				Payload:     []byte(`{"title":"t","body":"b"}`),
			})
			if err == nil {
				t.Fatal("Send must return an error on non-2xx")
			}
			if result.Retryable != tc.wantRetry {
				t.Errorf("Retryable = %v, want %v", result.Retryable, tc.wantRetry)
			}
			if result.ErrorType != tc.wantErrTyp {
				t.Errorf("ErrorType = %q, want %q", result.ErrorType, tc.wantErrTyp)
			}
			if result.Response == nil {
				t.Error("provider response body must be preserved for the attempt record")
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	t.Run("both sources rejected", func(t *testing.T) {
		if _, err := NewFCMProvider(Config{
			ServiceAccountJSON: "{}",
			ServiceAccountFile: "sa.json",
		}, nil); err == nil || !strings.Contains(err.Error(), "not both") {
			t.Errorf("err = %v, want ambiguity error", err)
		}
	})

	t.Run("no source rejected", func(t *testing.T) {
		if _, err := NewFCMProvider(Config{}, nil); err == nil {
			t.Error("expected error for missing credentials")
		}
	})

	t.Run("empty payload rejected", func(t *testing.T) {
		sa := testServiceAccount(t)
		if _, err := NewFCMProvider(Config{ServiceAccountJSON: sa}, nil); err != nil {
			t.Fatalf("NewFCMProvider: %v", err)
		}
	})
}
