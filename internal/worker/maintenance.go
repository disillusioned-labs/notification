package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/disillusioned-labs/notification/internal/service/notification"
	"github.com/disillusioned-labs/notification/internal/service/outbox"
)

const (
	defaultReclaimInterval = time.Minute
	defaultCleanupInterval = time.Hour
	defaultLeaseTimeout    = 10 * time.Minute
)

// MaintenanceWorker runs the two low-frequency housekeeping jobs that keep
// delivery state and the outbox table healthy: reclaiming processing leases
// abandoned by dead workers, and deleting outbox events past their retention
// window. Both jobs are no-ops on healthy systems; the intervals exist so an
// operator can tighten them in dev and widen them in production.
type MaintenanceWorker struct {
	notifications   notification.NotificationService
	outbox          outbox.OutboxService
	reclaimInterval time.Duration
	cleanupInterval time.Duration
	leaseTimeout    time.Duration
	log             *slog.Logger
}

// MaintenanceOption customises a MaintenanceWorker; zero-value options keep
// the defaults.
type MaintenanceOption func(*MaintenanceWorker)

// WithReclaimInterval sets how often abandoned delivery leases are reclaimed.
func WithReclaimInterval(interval time.Duration) MaintenanceOption {
	return func(w *MaintenanceWorker) {
		if interval > 0 {
			w.reclaimInterval = interval
		}
	}
}

// WithCleanupInterval sets how often published outbox events are deleted.
func WithCleanupInterval(interval time.Duration) MaintenanceOption {
	return func(w *MaintenanceWorker) {
		if interval > 0 {
			w.cleanupInterval = interval
		}
	}
}

// WithLeaseTimeout sets the processing-lease age past which a delivery counts
// as abandoned. It must stay well above the slowest real provider call.
func WithLeaseTimeout(timeout time.Duration) MaintenanceOption {
	return func(w *MaintenanceWorker) {
		if timeout > 0 {
			w.leaseTimeout = timeout
		}
	}
}

// NewMaintenanceWorker builds the worker; notifications and outbox are the
// services owning the two jobs.
func NewMaintenanceWorker(
	notifications notification.NotificationService,
	outbox outbox.OutboxService,
	log *slog.Logger,
	opts ...MaintenanceOption,
) *MaintenanceWorker {
	w := &MaintenanceWorker{
		notifications:   notifications,
		outbox:          outbox,
		reclaimInterval: defaultReclaimInterval,
		cleanupInterval: defaultCleanupInterval,
		leaseTimeout:    defaultLeaseTimeout,
		log:             log,
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Run blocks until ctx is cancelled, running each job on its own ticker.
// Like the other workers, a failing tick is logged and retried on the next
// tick - maintenance failures must never take the process down.
func (w *MaintenanceWorker) Run(ctx context.Context) error {
	w.log.Info(
		"maintenance worker started",
		"reclaim_interval", w.reclaimInterval,
		"cleanup_interval", w.cleanupInterval,
		"lease_timeout", w.leaseTimeout,
	)

	reclaimTicker := time.NewTicker(w.reclaimInterval)
	defer reclaimTicker.Stop()

	cleanupTicker := time.NewTicker(w.cleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.log.Info("maintenance worker stopped")

			return nil

		case <-reclaimTicker.C:
			if _, err := w.notifications.ReclaimStaleDeliveries(
				ctx,
				w.leaseTimeout,
			); err != nil && ctx.Err() == nil {
				w.log.ErrorContext(
					ctx,
					"reclaim stale deliveries failed",
					"error", err,
				)
			}

		case <-cleanupTicker.C:
			if _, err := w.outbox.CleanupPublished(ctx); err != nil && ctx.Err() == nil {
				w.log.ErrorContext(
					ctx,
					"cleanup published outbox events failed",
					"error", err,
				)
			}
		}
	}
}
