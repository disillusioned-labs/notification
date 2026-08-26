package provider

import "errors"

var (
	ErrProviderNotFound    = errors.New("provider not found")
	ErrProviderUnavailable = errors.New("provider unavailable")

	ErrInvalidRequest = errors.New("provider: invalid request")
	ErrUnauthorized   = errors.New("provider: unauthorized")
	ErrForbidden      = errors.New("provider: forbidden")
	ErrNotFound       = errors.New("provider: not found")
	ErrRateLimited    = errors.New("provider: rate limited")
	ErrUnavailable    = errors.New("provider: unavailable")
	ErrTimeout        = errors.New("provider: timeout")
	ErrInternal       = errors.New("provider: internal error")
)

type ErrorType string

const (
	ErrorTypeInvalidRequest ErrorType = "invalid_request"
	ErrorTypeUnauthorized   ErrorType = "unauthorized"
	ErrorTypeForbidden      ErrorType = "forbidden"
	ErrorTypeNotFound       ErrorType = "not_found"
	ErrorTypeRateLimited    ErrorType = "rate_limited"
	ErrorTypeUnavailable    ErrorType = "unavailable"
	ErrorTypeTimeout        ErrorType = "timeout"
	ErrorTypeInternal       ErrorType = "internal"
)
