package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/disillusioned-labs/notification/internal/service/outbox"
)

const (
	defaultOutboxInterval = time.Second
	defaultOutboxBatch    = 100
)

type OutboxWorker struct {
	batchSize  int
	interval   time.Duration
	instanceID string
	service    outbox.OutboxService
	log        *slog.Logger
}

type Option func(*OutboxWorker)

func WithInterval(interval time.Duration) Option {
	return func(worker *OutboxWorker) {
		if interval > 0 {
			worker.interval = interval
		}
	}
}

func WithBatchSize(batchSize int) Option {
	return func(worker *OutboxWorker) {
		if batchSize > 0 {
			worker.batchSize = batchSize
		}
	}
}

func NewOutboxWorker(
	instanceID string,
	service outbox.OutboxService,
	log *slog.Logger,
	opts ...Option,
) *OutboxWorker {
	worker := &OutboxWorker{
		interval:   defaultOutboxInterval,
		batchSize:  defaultOutboxBatch,
		instanceID: instanceID,
		service:    service,
		log:        log,
	}

	for _, opt := range opts {
		opt(worker)
	}

	return worker
}

func (w *OutboxWorker) Run(ctx context.Context) error {
	w.log.Info(
		"outbox worker started",
		"instance_id", w.instanceID,
		"interval", w.interval,
		"batch_size", w.batchSize,
	)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.log.Info(
				"outbox worker stopped",
				"instance_id", w.instanceID,
			)
			return nil

		case <-ticker.C:
			err := w.service.PublishPending(
				ctx,
				w.instanceID,
				w.batchSize,
			)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}

				w.log.ErrorContext(
					ctx,
					"outbox publish failed",
					"error", err,
					"instance_id", w.instanceID,
				)
			}
		}
	}
}
