package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/disillusioned-labs/notification/internal/platform/kafka"
	"github.com/disillusioned-labs/notification/internal/platform/retry"
	"github.com/disillusioned-labs/notification/internal/service/notification"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("consumer/notification")

const eventTypeHeader = "event-type"

type Consumer struct {
	kafkaConsumer       *kafka.Consumer
	dlqPublisher        *kafka.DLQPublisher
	notificationService notification.NotificationService
	retryPolicy         retry.RetryPolicy
	metrics             *ConsumerMetrics
	log                 *slog.Logger
}

func NewConsumer(
	kafkaConsumer *kafka.Consumer,
	dlqPublisher *kafka.DLQPublisher,
	notificationService notification.NotificationService,
	retryPolicy retry.RetryPolicy,
	metrics *ConsumerMetrics,
	log *slog.Logger,
) *Consumer {
	return &Consumer{
		kafkaConsumer:       kafkaConsumer,
		dlqPublisher:        dlqPublisher,
		notificationService: notificationService,
		retryPolicy:         retryPolicy,
		metrics:             metrics,
		log:                 log,
	}
}

func (w *Consumer) Run(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("consumer is nil")
	}

	if w.kafkaConsumer == nil {
		return fmt.Errorf("kafka consumer is nil")
	}

	if w.dlqPublisher == nil {
		return fmt.Errorf("dlq publisher is nil")
	}

	if w.log == nil {
		return fmt.Errorf("consumer logger is nil")
	}

	if err := w.retryPolicy.Validate(); err != nil {
		return fmt.Errorf("validate retry policy: %w", err)
	}

	w.log.Info("consumer started")

	for {
		records, err := w.kafkaConsumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf("poll kafka: %w", err)
		}

		for _, record := range records {
			if err := w.processWithRetry(ctx, record); err != nil {
				return fmt.Errorf(
					"process kafka record topic=%s partition=%d offset=%d: %w",
					record.Topic,
					record.Partition,
					record.Offset,
					err,
				)
			}

			if err := w.kafkaConsumer.CommitRecords(ctx, record); err != nil {
				if w.metrics != nil {
					w.metrics.recordCommitFailed(
						ctx,
						record.Topic,
					)
				}

				return fmt.Errorf(
					"commit kafka record topic=%s partition=%d offset=%d: %w",
					record.Topic,
					record.Partition,
					record.Offset,
					err,
				)
			}
		}
	}
}

func (w *Consumer) processWithRetry(
	ctx context.Context,
	record kafka.Record,
) error {
	ctx, span := tracer.Start(
		ctx,
		"notification.consumer.process",
	)
	defer span.End()

	start := time.Now()
	eventType := recordEventType(record)

	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", record.Topic),
		attribute.Int64("messaging.kafka.partition", int64(record.Partition)),
		attribute.Int64("messaging.kafka.offset", record.Offset),
		attribute.String("event.type", eventType),
	)

	defer func() {
		if w.metrics != nil {
			w.metrics.recordProcessingDuration(
				ctx,
				start,
				record.Topic,
				eventType,
			)
		}
	}()

	var lastErr error

	for attempt := 1; attempt <= w.retryPolicy.MaxAttempts; attempt++ {
		err := w.processRecord(ctx, record)
		if err == nil {
			if w.metrics != nil {
				w.metrics.recordProcessed(
					ctx,
					record.Topic,
					eventType,
				)
			}

			return nil
		}

		lastErr = err
		errType := classifyError(err)

		if IsPermanent(err) {
			if w.metrics != nil {
				w.metrics.recordFailed(
					ctx,
					record.Topic,
					eventType,
					errType,
				)
			}

			if err := w.moveToDLQ(
				ctx,
				record,
				err,
				attempt,
			); err != nil {
				span.RecordError(err)
				span.SetStatus(
					codes.Error,
					"move record to dlq failed",
				)

				return err
			}

			if w.metrics != nil {
				w.metrics.recordDLQ(
					ctx,
					record.Topic,
					eventType,
					errType,
				)
			}

			return nil
		}

		if attempt == w.retryPolicy.MaxAttempts {
			break
		}

		if w.metrics != nil {
			w.metrics.recordRetried(
				ctx,
				record.Topic,
				eventType,
				errType,
			)
		}

		w.log.Warn(
			"retrying kafka record",
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
			"attempt", attempt+1,
			"max_attempts", w.retryPolicy.MaxAttempts,
			"error", err,
		)

		if err := w.retryPolicy.Wait(ctx, attempt); err != nil {
			span.RecordError(err)
			span.SetStatus(
				codes.Error,
				"retry wait failed",
			)

			return err
		}
	}

	errType := classifyError(lastErr)

	if w.metrics != nil {
		w.metrics.recordFailed(
			ctx,
			record.Topic,
			eventType,
			errType,
		)
	}

	if err := w.moveToDLQ(
		ctx,
		record,
		lastErr,
		w.retryPolicy.MaxAttempts,
	); err != nil {
		span.RecordError(err)
		span.SetStatus(
			codes.Error,
			"move record to dlq failed",
		)

		return err
	}

	if w.metrics != nil {
		w.metrics.recordDLQ(
			ctx,
			record.Topic,
			eventType,
			errType,
		)
	}

	return nil
}

func (w *Consumer) moveToDLQ(
	ctx context.Context,
	record kafka.Record,
	cause error,
	attempt int,
) error {
	if cause == nil {
		return fmt.Errorf("move kafka record to dlq: cause is nil")
	}

	metadata := kafka.DLQMetadata{
		SourceTopic:     record.Topic,
		SourcePartition: record.Partition,
		SourceOffset:    record.Offset,
		Attempt:         attempt,
		ErrorType:       "transient",
		ErrorCode:       "PROCESSING_FAILED",
	}

	if IsPermanent(cause) {
		metadata.ErrorType = "permanent"
		metadata.ErrorCode = "PERMANENT_PROCESSING_ERROR"
	}

	if err := w.dlqPublisher.Publish(
		ctx,
		record,
		metadata,
	); err != nil {
		return fmt.Errorf(
			"move kafka record to dlq: %w",
			err,
		)
	}

	return nil
}

func (w *Consumer) processRecord(
	ctx context.Context,
	record kafka.Record,
) error {
	event, err := decodeNotificationEvent(record)
	if err != nil {
		return Permanent(
			fmt.Errorf(
				"decode notification event: %w",
				err,
			),
		)
	}

	switch event.EventType {
	case notification.EventTypeNotificationCreated:
		return w.notificationService.CreateFromEvent(ctx, event)

	case notification.EventTypeNotificationDeliveryRequested:
		return w.notificationService.RequestDelivery(ctx, event)

	case notification.EventTypeNotificationDeliveryRetry:
		return w.notificationService.RetryDelivery(ctx, event)

	default:
		return Permanent(
			fmt.Errorf(
				"unsupported notification event type %q",
				event.EventType,
			),
		)
	}
}

func recordEventType(record kafka.Record) string {
	value, ok := kafka.HeaderString(
		record.Headers,
		eventTypeHeader,
	)
	if !ok || value == "" {
		return "unknown"
	}

	return value
}
