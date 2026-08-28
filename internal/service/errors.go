// Package service holds cross-resource service bits: the domain error type
// and storage-error helpers. Each resource's business logic lives in its own
// subpackage (service/user, ...), exposing an interface plus an unexported
// implementation so handlers can mock the contract in tests.
//
// Domain errors are self-describing: each carries the HTTP status and
// machine-readable code it maps to. handler.WriteServiceError reads those
// fields, so adding a resource never means editing the shared handler layer.
package service

import (
	"errors"

	platformerrors "github.com/disillusioned-labs/platform/errors"
)

// Error is a domain error that knows how it surfaces over HTTP.
type Error = platformerrors.Error

// NewError builds a domain error for a resource-specific failure.
var NewError = platformerrors.NewError

var (
	ErrUnauthenticated = platformerrors.ErrUnauthenticated
	ErrForbidden       = platformerrors.ErrForbidden
	ErrNotFound        = platformerrors.ErrNotFound
	ErrConflict        = platformerrors.ErrConflict
	ErrInternal        = platformerrors.ErrInternal
)

// IsUniqueViolation reports whether err is a Postgres unique-constraint violation.
var IsUniqueViolation = platformerrors.IsUniqueViolation

// Notification-specific errors.
var (
	ErrDuplicateEvent       = errors.New("notification event already processed")
	ErrNotificationNotFound = errors.New("notification not found")
	ErrDeliveryNotFound     = errors.New("delivery not found")
)
