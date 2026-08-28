package resend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/disillusioned-labs/notification/internal/provider"
	resendSDK "github.com/resend/resend-go/v3"
)

const (
	ProviderName = "resend"
	ChannelEmail = "email"
)

type Config struct {
	APIKey string
	From   string
}

func (c Config) Validate() error {
	switch {
	case strings.TrimSpace(c.APIKey) == "":
		return errors.New("resend: api key is required")

	case strings.TrimSpace(c.From) == "":
		return errors.New("resend: from address is required")

	default:
		return nil
	}
}

type Provider struct {
	client *resendSDK.Client
	from   string
}

var _ provider.Provider = (*Provider)(nil)

func NewResendProvider(cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Provider{
		client: resendSDK.NewClient(cfg.APIKey),
		from:   strings.TrimSpace(cfg.From),
	}, nil
}

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`

	HTML string `json:"html,omitempty"`
	Text string `json:"text,omitempty"`

	ReplyTo string `json:"reply_to,omitempty"`
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

	payload, err := decodeEmailPayload(req.Payload)
	if err != nil {
		return provider.SendResult{
			ErrorType:    string(provider.ErrorTypeInvalidRequest),
			ErrorMessage: err.Error(),
			Retryable:    false,
		}, err
	}

	to := payload.To
	if to == "" {
		to = req.Destination
	}

	if err := validateEmailPayload(payload, to); err != nil {
		return provider.SendResult{
			ErrorType:    string(provider.ErrorTypeInvalidRequest),
			ErrorMessage: err.Error(),
			Retryable:    false,
		}, err
	}

	params := &resendSDK.SendEmailRequest{
		From: p.from,
		//To:      []string{to},
		To:      []string{"delivered@resend.dev"},
		Subject: payload.Subject,
		Html:    payload.HTML,
		Text:    payload.Text,
	}

	if payload.ReplyTo != "" {
		params.ReplyTo = payload.ReplyTo
	}

	options := &resendSDK.SendEmailOptions{}

	if req.IdempotencyKey != "" {
		options.IdempotencyKey = req.IdempotencyKey
	}

	result, err := p.client.Emails.SendWithOptions(
		ctx,
		params,
		options,
	)
	if err != nil {
		return mapError(err)
	}

	if result == nil {
		err := errors.New("resend: empty response")

		return provider.SendResult{
			ErrorType:    string(provider.ErrorTypeInternal),
			ErrorMessage: err.Error(),
			Retryable:    true,
		}, err
	}

	response, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		// The email has already been accepted by Resend.
		//
		// Never convert this into a failed Send(), otherwise the
		// notification service could retry an already accepted email.
		response = nil
	}

	return provider.SendResult{
		MessageID:      result.Id,
		HTTPStatusCode: 200,
		Response:       response,
		Retryable:      false,
	}, nil
}

func validateRequest(req provider.SendRequest) error {
	if req.Channel != ChannelEmail {
		return fmt.Errorf(
			"resend: unsupported channel %q",
			req.Channel,
		)
	}

	if strings.TrimSpace(req.Destination) == "" {
		return errors.New("resend: destination is required")
	}

	if len(req.IdempotencyKey) > 256 {
		return errors.New(
			"resend: idempotency key must not exceed 256 characters",
		)
	}

	return nil
}

func decodeEmailPayload(raw []byte) (emailPayload, error) {
	var payload emailPayload

	if len(raw) == 0 {
		return payload, errors.New(
			"resend: email payload is empty",
		)
	}

	if err := json.Unmarshal(raw, &payload); err != nil {
		return payload, fmt.Errorf(
			"resend: decode email payload: %w",
			err,
		)
	}

	return payload, nil
}

func validateEmailPayload(
	payload emailPayload,
	to string,
) error {
	if strings.TrimSpace(to) == "" {
		return errors.New("resend: recipient is required")
	}

	if strings.TrimSpace(payload.Subject) == "" {
		return errors.New("resend: subject is required")
	}

	if strings.TrimSpace(payload.HTML) == "" &&
		strings.TrimSpace(payload.Text) == "" {
		return errors.New(
			"resend: either html or text content is required",
		)
	}

	return nil
}
