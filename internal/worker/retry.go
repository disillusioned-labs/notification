package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/disillusioned-labs/notification/internal/service/notification"
)

const (
	defaultRetryInterval = 30 * time.Second
	defaultRetryBatch    = 100
)

type RetryWorker struct {
	batchSize int
	interval  time.Duration
	service   notification.NotificationService
	log       *slog.Logger
}

type RetryOption func(*RetryWorker)

func WithRetryInterval(interval time.Duration) RetryOption {
	return func(worker *RetryWorker) {
		if interval > 0 {
			worker.interval = interval
		}
	}
}

func WithRetryBatchSize(batchSize int) RetryOption {
	return func(worker *RetryWorker) {
		if batchSize > 0 {
			worker.batchSize = batchSize
		}
	}
}

func NewRetryWorker(
	service notification.NotificationService,
	log *slog.Logger,
	opts ...RetryOption,
) *RetryWorker {
	worker := &RetryWorker{
		interval:  defaultRetryInterval,
		batchSize: defaultRetryBatch,
		service:   service,
		log:       log,
	}

	for _, opt := range opts {
		opt(worker)
	}

	return worker
}

func (w *RetryWorker) Run(ctx context.Context) error {
	w.log.Info(
		"retry worker started",
		"interval", w.interval,
		"batch_size", w.batchSize,
	)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.log.Info("retry worker stopped")
			return nil

		case <-ticker.C:
			err := w.service.ProcessReadyRetries(
				ctx,
				w.batchSize,
			)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}

				w.log.ErrorContext(
					ctx,
					"process ready retries failed",
					"error", err,
				)
			}
		}
	}
}
