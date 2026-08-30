package notification

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/disillusioned-labs/notification/internal/constant"
	"github.com/disillusioned-labs/notification/internal/provider"
	"github.com/disillusioned-labs/notification/internal/repository"
	"github.com/disillusioned-labs/notification/internal/service"
	"github.com/disillusioned-labs/notification/internal/template"
	"github.com/disillusioned-labs/platform/contract/notification"
	"github.com/disillusioned-labs/platform/pgutil"
	"github.com/disillusioned-labs/platform/retry"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("service/notification")

const (
	notificationEventVersion = 1
	deliveryAggregateType    = "notification_delivery"
)

type NotificationService interface {
	CreateFromEvent(ctx context.Context, event NotificationEvent) error
	RequestDelivery(ctx context.Context, event NotificationEvent) error
	RetryDelivery(ctx context.Context, event NotificationEvent) error
	ProcessReadyRetries(ctx context.Context, limit int) error
}

type notificationService struct {
	instanceID string
	repo       repository.Store
	providers  provider.Registry
	renderer   template.Renderer
	retry      retry.RetryPolicy
	metrics    Metrics
	log        *slog.Logger
}

func NewNotificationService(instanceID string, repo repository.Store, providers provider.Registry, renderer template.Renderer, retry retry.RetryPolicy, metrics Metrics, log *slog.Logger) NotificationService {
	return &notificationService{
		instanceID: instanceID,
		repo:       repo,
		providers:  providers,
		renderer:   renderer,
		retry:      retry,
		metrics:    metrics,
		log:        log,
	}
}

func (n *notificationService) CreateFromEvent(
	ctx context.Context,
	event NotificationEvent,
) error {
	ctx, span := tracer.Start(ctx, "NotificationService.CreateFromEvent")
	defer span.End()

	payload, err := event.DecodePayload()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode notification.created payload")

		return fmt.Errorf("decode notification.created payload: %w", err)
	}

	created, ok := payload.(notification.CreatedEvent)
	if !ok {
		err := fmt.Errorf(
			"unexpected payload type %T",
			payload,
		)

		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid notification.created payload")

		return err
	}

	if err := created.Validate(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid notification.created payload")

		return fmt.Errorf("validate notification.created payload: %w", err)
	}

	//if err := n.validateNotificationPayload(
	//	created.NotificationType,
	//	created.Payload,
	//); err != nil {
	//	span.RecordError(err)
	//	span.SetStatus(
	//		codes.Error,
	//		"invalid notification business payload",
	//	)
	//
	//	return fmt.Errorf(
	//		"validate notification business payload: %w",
	//		err,
	//	)
	//}

	exists, err := n.repo.NotificationExistsByEventID(
		ctx,
		event.EventID,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "check notification event")
		n.log.ErrorContext(ctx, "check notification event failed", "error", err, "event_id", event.EventID)

		return fmt.Errorf("check notification event: %w", err)
	}

	if exists {
		n.log.InfoContext(
			ctx,
			"notification event already processed",
			"event_id", event.EventID,
		)

		return nil
	}

	err = n.repo.ExecTx(ctx, func(q repository.Querier) error {
		notification, err := q.CreateNotification(
			ctx,
			repository.CreateNotificationParams{
				EventID:          event.EventID,
				NotificationType: created.NotificationType,
				Category:         created.Category,
				RecipientID:      created.RecipientID,
				Payload:          created.Payload,
				TraceID:          pgutil.TextFromString(event.TraceID),
			},
		)
		if err != nil {
			if service.IsUniqueViolation(err) {
				return service.ErrDuplicateEvent
			}

			return fmt.Errorf("create notification: %w", err)
		}

		for _, target := range created.Targets {
			providers, err := q.ListActiveProvidersByType(
				ctx,
				target.Channel,
			)
			if err != nil {
				return fmt.Errorf(
					"list providers for channel %q: %w",
					target.Channel,
					err,
				)
			}

			if len(providers) == 0 {
				return fmt.Errorf(
					"no active provider configured for channel %q",
					target.Channel,
				)
			}

			provider := providers[0]

			delivery, err := q.CreateNotificationDelivery(
				ctx,
				repository.CreateNotificationDeliveryParams{
					NotificationID: notification.ID,
					Channel:        target.Channel,
					Provider:       provider.Name,
					Destination:    target.Destination,
					MaxRetries:     int32(n.retry.MaxAttempts),
				},
			)
			if err != nil {
				return fmt.Errorf(
					"create %s delivery: %w",
					target.Channel,
					err,
				)
			}

			if err := service.Emit(
				ctx,
				q,
				deliveryAggregateType,
				delivery.ID,
				EventTypeNotificationDeliveryRequested,
				notificationEventVersion,
				event.Topic,
				NotificationDeliveryRequestedEvent{
					DeliveryID: delivery.ID.String(),
				},
			); err != nil {
				return fmt.Errorf("create delivery requested outbox event: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, service.ErrDuplicateEvent) {
			n.log.InfoContext(
				ctx,
				"notification event already processed",
				"event_id", event.EventID,
			)

			return nil
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, "create notification")
		n.log.ErrorContext(ctx, "create notification failed", "error", err, "event_id", event.EventID)

		return fmt.Errorf("create notification: %w", err)
	}

	return nil
}

func (n *notificationService) RequestDelivery(
	ctx context.Context,
	event NotificationEvent,
) error {
	ctx, span := tracer.Start(ctx, "NotificationService.RequestDelivery")
	defer span.End()

	payload, err := event.DecodePayload()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode delivery requested payload")

		return fmt.Errorf("decode delivery requested payload: %w", err)
	}

	request, ok := payload.(NotificationDeliveryRequestedEvent)
	if !ok {
		err := fmt.Errorf(
			"unexpected payload type %T",
			payload,
		)

		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid delivery requested payload")

		return err
	}

	if err := request.Validate(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid delivery requested payload")

		return fmt.Errorf("validate delivery requested payload: %w", err)
	}

	deliveryID, err := uuid.Parse(request.DeliveryID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid delivery id")

		return fmt.Errorf("parse delivery id: %w", err)
	}

	delivery, err := n.repo.GetDeliveryByID(
		ctx,
		deliveryID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			span.RecordError(err)
			span.SetStatus(codes.Error, "delivery not found")

			return service.ErrDeliveryNotFound
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, "get delivery")
		n.log.ErrorContext(ctx, "get delivery failed", "error", err, "delivery_id", deliveryID)

		return fmt.Errorf("get delivery: %w", err)
	}

	switch delivery.Status {
	case constant.DeliveryStatusSent:
		return nil

	case constant.DeliveryStatusPending:
		// Continue.

	default:
		n.log.InfoContext(
			ctx,
			"delivery is not eligible for initial processing",
			"delivery_id", request.DeliveryID,
			"status", delivery.Status,
		)

		return nil
	}

	return n.processDelivery(
		ctx,
		deliveryID,
	)
}

func (n *notificationService) RetryDelivery(
	ctx context.Context,
	event NotificationEvent,
) error {
	ctx, span := tracer.Start(ctx, "NotificationService.RetryDelivery")
	defer span.End()

	payload, err := event.DecodePayload()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode delivery retry payload")

		return fmt.Errorf("decode delivery retry payload: %w", err)
	}

	retryEvent, ok := payload.(NotificationDeliveryRetryEvent)
	if !ok {
		err := fmt.Errorf(
			"unexpected payload type %T",
			payload,
		)

		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid delivery retry payload")

		return err
	}

	if err := retryEvent.Validate(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid delivery retry payload")

		return fmt.Errorf("validate delivery retry payload: %w", err)
	}

	deliveryID, err := uuid.Parse(retryEvent.DeliveryID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid delivery id")

		return fmt.Errorf("parse delivery id: %w", err)
	}

	delivery, err := n.repo.GetDeliveryByID(
		ctx,
		deliveryID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			span.RecordError(err)
			span.SetStatus(codes.Error, "delivery not found")

			return service.ErrDeliveryNotFound
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, "get delivery")
		n.log.ErrorContext(ctx, "get delivery failed", "error", err, "delivery_id", deliveryID)

		return fmt.Errorf("get delivery: %w", err)
	}

	switch delivery.Status {
	case constant.DeliveryStatusSent:
		return nil

	case constant.DeliveryStatusRetry:
		// Continue.

	default:
		n.log.InfoContext(
			ctx,
			"delivery is not eligible for retry",
			"delivery_id", retryEvent.DeliveryID,
			"status", delivery.Status,
		)

		return nil
	}

	if delivery.NextRetryAt.Valid &&
		delivery.NextRetryAt.Time.After(time.Now()) {
		return nil
	}

	return n.processDelivery(
		ctx,
		deliveryID,
	)
}

func (n *notificationService) ProcessReadyRetries(
	ctx context.Context,
	limit int,
) error {
	ctx, span := tracer.Start(ctx, "NotificationService.ProcessReadyRetries")
	defer span.End()

	ids, err := n.repo.ListReadyRetryDeliveries(
		ctx,
		repository.ListReadyRetryDeliveriesParams{
			NextRetryAt: pgtype.Timestamptz{
				Time:  time.Now(),
				Valid: true,
			},
			Limit: int32(limit),
		},
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "list ready retry deliveries")
		n.log.ErrorContext(ctx, "list ready retry deliveries failed", "error", err)

		return fmt.Errorf("list ready retry deliveries: %w", err)
	}

	if len(ids) == 0 {
		n.metrics.retryReady.Record(ctx, 0)
		return nil
	}

	span.SetAttributes(attribute.Int("notification.retry_ready", len(ids)),)
	n.metrics.retryReady.Record(ctx, float64(len(ids)))

	for _, id := range ids {
		if err := n.processDelivery(ctx, id); err != nil {
			return err
		}
	}

	return nil
}

func (n *notificationService) processDelivery(
	ctx context.Context,
	deliveryID uuid.UUID,
) error {
	ctx, span := tracer.Start(ctx, "NotificationService.processDelivery")
	defer span.End()

	delivery, err := n.repo.ClaimDelivery(
		ctx,
		repository.ClaimDeliveryParams{
			ID:       deliveryID,
			LockedBy: pgutil.TextFromString(n.instanceID),
		},
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Another worker claimed the delivery, or the delivery
			// is no longer eligible for processing.
			return nil
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, "claim delivery")
		n.log.ErrorContext(ctx, "claim delivery failed", "error", err, "delivery_id", deliveryID)

		return fmt.Errorf("claim delivery: %w", err)
	}

	p, err := n.providers.Get(delivery.Provider)
	if err != nil {
		return n.handleProviderResolutionFailure(
			ctx,
			delivery,
			err,
		)
	}

	notification, err := n.repo.GetNotificationByID(
		ctx,
		delivery.NotificationID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err := fmt.Errorf(
				"notification %q not found",
				delivery.NotificationID,
			)

			span.RecordError(err)
			span.SetStatus(codes.Error, "notification not found")

			return service.ErrNotificationNotFound
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, "get notification payload")

		return fmt.Errorf("get notification payload: %w", err)
	}

	renderedPayload, err := n.renderer.Render(
		ctx,
		notification.NotificationType,
		delivery.Channel,
		notification.Payload,
	)
	if err != nil {
		return n.handleRenderingFailure(
			ctx,
			delivery,
			err,
		)
	}

	result, sendErr := p.Send(
		ctx,
		provider.SendRequest{
			Channel:        delivery.Channel,
			Destination:    delivery.Destination,
			Payload:        renderedPayload,
			IdempotencyKey: deliveryID.String(),
		},
	)

	if sendErr == nil {
		return n.handleProviderSuccess(
			ctx,
			delivery,
			result,
		)
	}

	return n.handleProviderFailure(
		ctx,
		delivery,
		result,
		sendErr,
	)
}

func (n *notificationService) handleProviderSuccess(
	ctx context.Context,
	delivery repository.NotificationDelivery,
	result provider.SendResult,
) error {
	ctx, span := tracer.Start(ctx, "NotificationService.handleProviderSuccess")
	defer span.End()

	attemptNumber := delivery.RetryCount + 1

	err := n.repo.ExecTx(ctx, func(q repository.Querier) error {
		_, err := q.CreateDeliveryAttempt(
			ctx,
			repository.CreateDeliveryAttemptParams{
				DeliveryID:        delivery.ID,
				AttemptNumber:     attemptNumber,
				Provider:          delivery.Provider,
				ProviderMessageID: pgutil.TextFromString(result.MessageID),
				Status:            constant.AttemptStatusSuccess,
				HttpStatusCode:    pgutil.Int4(result.HTTPStatusCode),
				ErrorType:         pgtype.Text{},
				ErrorMessage:      pgtype.Text{},
				Response:          result.Response,
			},
		)
		if err != nil {
			return fmt.Errorf("create successful delivery attempt: %w", err)
		}

		rows, err := q.MarkDeliverySent(
			ctx,
			repository.MarkDeliverySentParams{
				ID:       delivery.ID,
				LockedBy: pgutil.TextFromString(n.instanceID),
			},
		)
		if err != nil {
			return fmt.Errorf("mark delivery sent: %w", err)
		}

		if rows != 1 {
			return fmt.Errorf("mark delivery sent: expected 1 row, affected %d", rows)
		}

		return nil
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "persist provider success")

		return fmt.Errorf("persist provider success: %w", err)
	}

	n.log.InfoContext(
		ctx,
		"notification delivery sent",
		"delivery_id", delivery.ID,
		"provider", delivery.Provider,
		"attempt_number", attemptNumber,
		"provider_message_id", result.MessageID,
	)

	n.metrics.deliverySent.Add(ctx, 1, providerAttr(delivery.Provider))

	return nil
}
func (n *notificationService) handleProviderFailure(
	ctx context.Context,
	delivery repository.NotificationDelivery,
	result provider.SendResult,
	sendErr error,
) error {
	ctx, span := tracer.Start(ctx, "NotificationService.handleProviderFailure")
	defer span.End()

	attemptNumber := delivery.RetryCount + 1

	errorType := result.ErrorType
	errorMessage := result.ErrorMessage

	if errorMessage == "" && sendErr != nil {
		errorMessage = sendErr.Error()
	}

	// retry_count represents retries that have already been used.
	//
	// retry_count = 0 -> initial attempt just failed
	// retry_count = 1 -> first retry already happened
	//
	// Therefore another retry is available while:
	//
	// retry_count < max_retries
	retryNumber := delivery.RetryCount + 1

	retryable := result.Retryable &&
		delivery.RetryCount < delivery.MaxRetries

	var nextRetryAt pgtype.Timestamptz
	var nextRetryTime time.Time

	if retryable {
		nextRetryTime = time.Now().UTC().Add(
			n.retry.Delay(int(retryNumber)),
		)

		nextRetryAt = pgtype.Timestamptz{
			Time:  nextRetryTime,
			Valid: true,
		}
	}

	err := n.repo.ExecTx(ctx, func(q repository.Querier) error {
		_, err := q.CreateDeliveryAttempt(
			ctx,
			repository.CreateDeliveryAttemptParams{
				DeliveryID:        delivery.ID,
				AttemptNumber:     attemptNumber,
				Provider:          delivery.Provider,
				ProviderMessageID: pgutil.TextFromString(result.MessageID),
				Status:            constant.AttemptStatusFailed,
				HttpStatusCode:    pgutil.Int4(result.HTTPStatusCode),
				ErrorType:         pgutil.TextFromString(errorType),
				ErrorMessage:      pgutil.TextFromString(errorMessage),
				Response:          result.Response,
			},
		)
		if err != nil {
			return fmt.Errorf("create failed delivery attempt: %w", err)
		}

		if !retryable {
			rows, err := q.MarkDeliveryFailed(
				ctx,
				repository.MarkDeliveryFailedParams{
					ID:       delivery.ID,
					LockedBy: pgutil.TextFromString(n.instanceID),
				},
			)
			if err != nil {
				return fmt.Errorf("mark delivery failed: %w", err)
			}

			if rows != 1 {
				return fmt.Errorf("mark delivery failed: expected 1 row, affected %d", rows)
			}

			return nil
		}

		rows, err := q.MarkDeliveryRetry(
			ctx,
			repository.MarkDeliveryRetryParams{
				ID:          delivery.ID,
				NextRetryAt: nextRetryAt,
				LockedBy:    pgutil.TextFromString(n.instanceID),
			},
		)
		if err != nil {
			return fmt.Errorf("mark delivery retry: %w", err)
		}

		if rows != 1 {
			return fmt.Errorf("mark delivery retry: expected 1 row, affected %d", rows)
		}

		return nil
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "persist provider failure")

		return fmt.Errorf("persist provider failure: %w", err)
	}

	if retryable {
		n.metrics.deliveryRetryScheduled.Add(ctx, 1, providerAttr(delivery.Provider))

		n.log.WarnContext(
			ctx,
			"notification delivery scheduled for retry",
			"delivery_id", delivery.ID,
			"provider", delivery.Provider,
			"attempt_number", attemptNumber,
			"retry_number", retryNumber,
			"max_retries", delivery.MaxRetries,
			"next_retry_at", nextRetryTime,
			"error_type", errorType,
			"error", errorMessage,
		)

		return nil
	}

	n.metrics.deliveryFailed.Add(ctx, 1, providerAttr(delivery.Provider))

	n.log.ErrorContext(
		ctx,
		"notification delivery failed",
		"delivery_id", delivery.ID,
		"provider", delivery.Provider,
		"attempt_number", attemptNumber,
		"retryable", result.Retryable,
		"retry_count", delivery.RetryCount,
		"max_retries", delivery.MaxRetries,
		"error_type", errorType,
		"error", errorMessage,
	)

	return nil
}

func (n *notificationService) handleProviderResolutionFailure(
	ctx context.Context,
	delivery repository.NotificationDelivery,
	providerErr error,
) error {
	ctx, span := tracer.Start(ctx, "NotificationService.handleProviderResolutionFailure")
	defer span.End()

	if providerErr == nil {
		providerErr = provider.ErrProviderUnavailable
	}

	attemptNumber := delivery.RetryCount + 1

	errorType := providerErrorType(providerErr)
	errorMessage := providerErr.Error()

	retryable := errors.Is(
		providerErr,
		provider.ErrProviderUnavailable,
	) && delivery.RetryCount < delivery.MaxRetries

	var nextRetryAt pgtype.Timestamptz

	if retryable {
		retryNumber := delivery.RetryCount + 1
		nextRetryTime := time.Now().UTC().Add(
			n.retry.Delay(int(retryNumber)),
		)

		nextRetryAt = pgtype.Timestamptz{
			Time:  nextRetryTime,
			Valid: true,
		}
	}

	err := n.repo.ExecTx(ctx, func(q repository.Querier) error {
		_, err := q.CreateDeliveryAttempt(
			ctx,
			repository.CreateDeliveryAttemptParams{
				DeliveryID:        delivery.ID,
				AttemptNumber:     attemptNumber,
				Provider:          delivery.Provider,
				ProviderMessageID: pgtype.Text{},
				Status:            constant.AttemptStatusFailed,
				HttpStatusCode:    pgtype.Int4{},
				ErrorType:         pgutil.TextFromString(errorType),
				ErrorMessage:      pgutil.TextFromString(errorMessage),
				Response:          nil,
			},
		)
		if err != nil {
			return fmt.Errorf("create provider resolution attempt: %w", err)
		}

		if retryable {
			rows, err := q.MarkDeliveryRetry(
				ctx,
				repository.MarkDeliveryRetryParams{
					ID:          delivery.ID,
					NextRetryAt: nextRetryAt,
					LockedBy:    pgutil.TextFromString(n.instanceID),
				},
			)
			if err != nil {
				return fmt.Errorf("mark delivery retry: %w", err)
			}

			if rows != 1 {
				return fmt.Errorf("mark delivery retry: expected 1 row, affected %d", rows)
			}

			return nil
		}

		rows, err := q.MarkDeliveryFailed(
			ctx,
			repository.MarkDeliveryFailedParams{
				ID:       delivery.ID,
				LockedBy: pgutil.TextFromString(n.instanceID),
			},
		)
		if err != nil {
			return fmt.Errorf("mark delivery failed: %w", err)
		}

		if rows != 1 {
			return fmt.Errorf("mark delivery failed: expected 1 row, affected %d", rows)
		}

		return nil
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "persist provider resolution failure")

		return fmt.Errorf("persist provider resolution failure: %w", err)
	}

	if retryable {
		n.metrics.deliveryRetryScheduled.Add(ctx, 1, providerAttr(delivery.Provider))

		n.log.WarnContext(
			ctx,
			"notification provider temporarily unavailable; delivery scheduled for retry",
			"delivery_id", delivery.ID,
			"provider", delivery.Provider,
			"attempt_number", attemptNumber,
			"retry_number", delivery.RetryCount+1,
			"max_retries", delivery.MaxRetries,
			"next_retry_at", nextRetryAt.Time,
			"error_type", errorType,
		)

		return nil
	}

	n.metrics.deliveryFailed.Add(ctx, 1, providerAttr(delivery.Provider))

	n.log.ErrorContext(
		ctx,
		"notification delivery failed during provider resolution",
		"delivery_id", delivery.ID,
		"provider", delivery.Provider,
		"attempt_number", attemptNumber,
		"retryable", false,
		"max_retries", delivery.MaxRetries,
		"error_type", errorType,
	)

	return nil
}

func (n *notificationService) handleRenderingFailure(
	ctx context.Context,
	delivery repository.NotificationDelivery,
	renderErr error,
) error {
	ctx, span := tracer.Start(ctx, "NotificationService.handleRenderingFailure")
	defer span.End()

	if renderErr == nil {
		renderErr = errors.New("notification rendering failed")
	}

	attemptNumber := delivery.RetryCount + 1

	err := n.repo.ExecTx(ctx, func(q repository.Querier) error {
		_, err := q.CreateDeliveryAttempt(
			ctx,
			repository.CreateDeliveryAttemptParams{
				DeliveryID:        delivery.ID,
				AttemptNumber:     attemptNumber,
				Provider:          delivery.Provider,
				ProviderMessageID: pgtype.Text{},
				Status:            constant.AttemptStatusFailed,
				HttpStatusCode:    pgtype.Int4{},
				ErrorType:         pgutil.TextFromString("template_rendering"),
				ErrorMessage:      pgutil.TextFromString(renderErr.Error()),
				Response:          nil,
			},
		)
		if err != nil {
			return fmt.Errorf("create rendering failure attempt: %w", err)
		}

		rows, err := q.MarkDeliveryFailed(
			ctx,
			repository.MarkDeliveryFailedParams{
				ID: delivery.ID,
				LockedBy: pgutil.TextFromString(
					n.instanceID,
				),
			},
		)
		if err != nil {
			return fmt.Errorf("mark delivery failed after rendering error: %w", err)
		}

		if rows != 1 {
			return fmt.Errorf("mark delivery failed: expected 1 row, affected %d", rows)
		}

		return nil
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "persist rendering failure")

		return fmt.Errorf("persist rendering failure: %w", err)
	}

	n.metrics.deliveryFailed.Add(ctx, 1, providerAttr(delivery.Provider))

	n.log.ErrorContext(
		ctx,
		"notification delivery failed during template rendering",
		"delivery_id", delivery.ID,
		"notification_id", delivery.NotificationID,
		"provider", delivery.Provider,
		"attempt_number", attemptNumber,
		"error_type", "template_rendering",
		"error", renderErr,
	)

	return nil
}

func providerErrorType(err error) string {
	switch {
	case errors.Is(err, provider.ErrProviderNotFound):
		return string(provider.ErrorTypeInternal)

	case errors.Is(err, provider.ErrProviderUnavailable):
		return string(provider.ErrorTypeUnavailable)

	default:
		return string(provider.ErrorTypeInternal)
	}
}
