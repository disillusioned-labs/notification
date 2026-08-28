package resend

import (
	"context"
	"errors"
	"net"
	"syscall"

	"github.com/disillusioned-labs/notification/internal/provider"
	resendSDK "github.com/resend/resend-go/v3"
)

func mapError(err error) (provider.SendResult, error) {
	if err == nil {
		return provider.SendResult{}, nil
	}

	result := provider.SendResult{
		ErrorType:    string(provider.ErrorTypeInternal),
		ErrorMessage: err.Error(),
		Retryable:    true,
	}

	// Resend explicitly exposes rate-limit errors.
	//
	// 429 is dependency throttling, therefore retryable.
	var rateLimitErr *resendSDK.RateLimitError
	if errors.As(err, &rateLimitErr) {
		result.ErrorType = string(provider.ErrorTypeRateLimited)
		result.Retryable = true

		return result, err
	}

	// Context cancellation is NOT a provider failure.
	//
	// The caller should stop processing rather than schedule
	// another delivery merely because its own context was cancelled.
	if errors.Is(err, context.Canceled) {
		result.ErrorType = string(provider.ErrorTypeInternal)
		result.Retryable = false

		return result, err
	}

	// Timeout means we don't know whether Resend accepted the email.
	// Retry is safe because the caller supplies a stable idempotency key.
	if isTimeoutError(err) {
		result.ErrorType = string(provider.ErrorTypeTimeout)
		result.Retryable = true

		return result, err
	}

	// Temporary network failure.
	if isTemporaryNetworkError(err) {
		result.ErrorType = string(provider.ErrorTypeUnavailable)
		result.Retryable = true

		return result, err
	}

	// Unknown transport/infrastructure failure.
	//
	// Prefer retrying rather than silently dropping a notification.
	result.ErrorType = string(provider.ErrorTypeInternal)
	result.Retryable = true

	return result, err
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	return false
}

func isTemporaryNetworkError(err error) bool {
	if err == nil {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Temporary()
	}

	return false
}
