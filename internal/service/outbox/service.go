package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/disillusioned-labs/notification/internal/repository"
	"github.com/disillusioned-labs/platform/kafka"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("service/outbox")

const (
	defaultBatchSize  = 100
	defaultRetryDelay = 5 * time.Second
)

type OutboxService interface {
	PublishPending(ctx context.Context, instanceID string, batchSize int) error
}

type outboxService struct {
	repo     repository.Store
	producer kafka.Producer
	log      *slog.Logger
	metrics  Metrics
}

func NewOutboxService(
	repo repository.Store,
	producer kafka.Producer,
	log *slog.Logger,
	metrics Metrics,
) OutboxService {
	return &outboxService{
		repo:     repo,
		producer: producer,
		log:      log,
		metrics:  metrics,
	}
}

func (s *outboxService) PublishPending(
	ctx context.Context,
	instanceID string,
	batchSize int,
) error {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	events, err := s.repo.ClaimPendingOutboxEvents(
		ctx,
		repository.ClaimPendingOutboxEventsParams{
			Limit: int32(batchSize),
			LockedBy: pgtype.Text{
				String: instanceID,
				Valid:  true,
			},
		},
	)
	if err != nil {
		s.log.ErrorContext(
			ctx,
			"claim pending outbox events failed",
			"error", err,
			"instance_id", instanceID,
		)

		return fmt.Errorf("claim pending outbox events: %w", err)
	}

	s.metrics.lastPollTimestamp.Record(ctx, time.Now().Unix())
	s.metrics.eventsClaimed.Add(ctx, int64(len(events)))

	if len(events) == 0 {
		return nil
	}

	ctx, span := tracer.Start(ctx, "OutboxService.PublishPending")
	defer span.End()

	span.SetAttributes(
		attribute.String("outbox.instance_id", instanceID),
		attribute.Int("outbox.batch_size", batchSize),
	)

	span.SetAttributes(
		attribute.Int("outbox.events_claimed", len(events)),
	)

	for _, event := range events {
		if err := s.publishEvent(ctx, event); err != nil {
			continue
		}
	}

	return nil
}

func (s *outboxService) publishEvent(
	ctx context.Context,
	event repository.OutboxEvent,
) error {
	ctx, span := tracer.Start(ctx, "OutboxService.PublishEvent")
	defer span.End()

	span.SetAttributes(
		attribute.String("outbox.event_id", event.ID.String()),
		attribute.String("outbox.event_type", event.EventType),
		attribute.String("outbox.aggregate_type", event.AggregateType),
		attribute.String("outbox.aggregate_id", event.AggregateID.String()),
		attribute.Int("outbox.attempt_count", int(event.AttemptCount)),
	)

	start := time.Now()

	headers := make([]kafka.RecordHeader, 0, 8)

	carrier := kafka.NewHeaderCarrier(&headers)
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	headers = append(
		headers,
		kafka.RecordHeader{
			Key:   "event-id",
			Value: []byte(event.ID.String()),
		},
		kafka.RecordHeader{
			Key:   "event-type",
			Value: []byte(event.EventType),
		},
		kafka.RecordHeader{
			Key:   "event-version",
			Value: []byte(strconv.Itoa(int(event.EventVersion))),
		},
		kafka.RecordHeader{
			Key:   "source-service",
			Value: []byte("identity"),
		},
		kafka.RecordHeader{
			Key:   "aggregate-type",
			Value: []byte(event.AggregateType),
		},
		kafka.RecordHeader{
			Key:   "aggregate-id",
			Value: []byte(event.AggregateID.String()),
		},
	)

	if event.TraceID.Valid {
		headers = append(
			headers,
			kafka.RecordHeader{
				Key:   "trace-id",
				Value: []byte(event.TraceID.String),
			},
		)
	}

	err := s.producer.Publish(
		ctx,
		kafka.Record{
			Topic:   event.Topic,
			Key:     []byte(event.AggregateID.String()),
			Value:   event.Payload,
			Headers: headers,
		},
	)

	s.metrics.publishDuration.Record(ctx, time.Since(start).Seconds())

	if err != nil {
		s.metrics.eventsPublishFailed.Add(ctx, 1)
		span.RecordError(err)
		span.SetStatus(
			codes.Error,
			"publish kafka event failed",
		)

		s.log.ErrorContext(
			ctx,
			"publish outbox event failed",
			"error", err,
			"event_id", event.ID,
			"event_type", event.EventType,
		)

		backoff := time.Duration(min(event.AttemptCount+1, 6)) * defaultRetryDelay
		nextAttemptAt := time.Now().Add(backoff)
		lastError := err.Error()

		markErr := s.repo.MarkOutboxEventFailed(
			ctx,
			repository.MarkOutboxEventFailedParams{
				ID: event.ID,
				NextAttemptAt: pgtype.Timestamptz{
					Time:  nextAttemptAt,
					Valid: true,
				},
				LastError: pgtype.Text{
					String: lastError,
					Valid:  true,
				},
			},
		)
		if markErr != nil {
			s.metrics.eventsMarkFailed.Add(ctx, 1)
			span.RecordError(markErr)

			s.log.ErrorContext(
				ctx,
				"mark outbox event failed",
				"error", markErr,
				"event_id", event.ID,
				"event_type", event.EventType,
			)

			return fmt.Errorf("mark outbox event failed: %w", markErr)
		}

		return fmt.Errorf("publish outbox event: %w", err)
	}

	err = s.repo.MarkOutboxEventPublished(
		ctx,
		event.ID,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(
			codes.Error,
			"mark outbox event published failed",
		)

		s.log.ErrorContext(
			ctx,
			"mark outbox event published failed",
			"error", err,
			"event_id", event.ID,
			"event_type", event.EventType,
		)

		return fmt.Errorf("mark outbox event published: %w", err)
	}

	s.metrics.eventsPublished.Add(ctx, 1)

	s.log.DebugContext(
		ctx,
		"outbox event published",
		"event_id", event.ID,
		"event_type", event.EventType,
	)

	return nil
}
