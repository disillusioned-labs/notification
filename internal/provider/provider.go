package provider

import "context"

type Provider interface {
	Send(ctx context.Context, req SendRequest) (SendResult, error)
}

type SendRequest struct {
	Channel        string
	Destination    string
	Payload        []byte
	IdempotencyKey string
}

type SendResult struct {
	MessageID      string
	HTTPStatusCode int32
	Response       []byte

	ErrorType    string
	ErrorMessage string

	Retryable bool
}
