package template

import (
	"context"
	"encoding/json"
	"io/fs"
	"testing"

	"github.com/disillusioned-labs/notification/internal/template/templates/email/approval_step_activated"
)

func buildTestRenderer(t *testing.T) *LocalRenderer {
	t.Helper()

	registry := NewPayloadRegistry()

	// The renderer validates every embedded template against a registered
	// payload, so cover all embedded types; the fake payload satisfies the
	// registry and is never rendered by these tests.
	entries, err := fs.ReadDir(templateFS, "templates/email")
	if err != nil {
		t.Fatalf("read embedded templates: %v", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		factory := func() Payload { return fakePayload{} }
		if entry.Name() == "approval_step_activated" {
			factory = func() Payload { return &approval_step_activated.ApprovalStepActivated{} }
		}

		if err := registry.Register(entry.Name(), factory); err != nil {
			t.Fatalf("register payload %s: %v", entry.Name(), err)
		}
	}

	r, err := NewLocalRenderer(registry)
	if err != nil {
		t.Fatalf("NewLocalRenderer: %v", err)
	}

	return r
}

type fakePayload struct{}

func (fakePayload) Validate() error { return nil }

func TestRenderPush(t *testing.T) {
	r := buildTestRenderer(t)

	raw, err := json.Marshal(approval_step_activated.ApprovalStepActivated{
		ApproverName:           "Budi",
		TransactionID:          "018f-tx",
		TransactionDescription: "Material gedung",
		AmountDisplay:          "Rp 15.000.000",
		Step:                   1,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	rendered, err := r.Render(context.Background(), "approval_step_activated", "push", raw)
	if err != nil {
		t.Fatalf("Render push: %v", err)
	}

	var out struct {
		Title string            `json:"title"`
		Body  string            `json:"body"`
		Data  map[string]string `json:"data"`
	}
	if err := json.Unmarshal(rendered, &out); err != nil {
		t.Fatalf("unmarshal rendered push: %v", err)
	}

	if out.Title == "" || out.Body == "" {
		t.Errorf("title/body must be non-empty, got %q / %q", out.Title, out.Body)
	}

	if want := "Material gedung"; !contains(out.Body, want) {
		t.Errorf("body %q should mention %q", out.Body, want)
	}

	if out.Data["transaction_id"] != "018f-tx" {
		t.Errorf("data.transaction_id = %q", out.Data["transaction_id"])
	}
	if out.Data["notification_type"] != "approval_step_activated" {
		t.Errorf("data.notification_type = %q", out.Data["notification_type"])
	}
}

func TestRenderPushMissingTemplateFails(t *testing.T) {
	r := buildTestRenderer(t)

	// user_registered has a payload + email template but deliberately no push
	// template: a push delivery for it must fail loudly at render time.
	raw, err := json.Marshal(approval_step_activated.ApprovalStepActivated{})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	if _, err := r.Render(context.Background(), "user_registered", "push", raw); err == nil {
		t.Fatal("expected error rendering push for a type without a push template")
	}
}

func TestRenderPushInvalidPayloadFails(t *testing.T) {
	r := buildTestRenderer(t)

	// Step 0 violates the payload's Validate(); render must refuse.
	if _, err := r.Render(
		context.Background(),
		"approval_step_activated",
		"push",
		[]byte(`{"approver_name":"Budi","transaction_id":"tx","amount_display":"Rp 1","step":0}`),
	); err == nil {
		t.Fatal("expected validation error for invalid payload")
	}
}

func TestRenderUnknownChannelFails(t *testing.T) {
	r := buildTestRenderer(t)

	if _, err := r.Render(context.Background(), "approval_step_activated", "sms", []byte(`{}`)); err == nil {
		t.Fatal("expected error for unsupported channel")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle ||
		len(needle) == 0 ||
		indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
