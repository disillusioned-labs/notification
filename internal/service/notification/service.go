package notification

import (
	"context"
	"log/slog"

	"github.com/disillusioned-labs/notification/internal/repository"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("service/notification")

type NotificationService interface {
	CreateFromEvent(ctx context.Context, event NotificationCreatedEvent) error
	RequestDelivery(ctx context.Context, event NotificationDeliveryRequestedEvent) error
	RetryDelivery(ctx context.Context, event NotificationDeliveryRetryEvent) error
}

type notificationService struct {
	repo repository.Store
	log  *slog.Logger
}

func NewNotificationService(repo repository.Store, log *slog.Logger) NotificationService {
	return &notificationService{
		repo: repo,
		log:  log,
	}
}

func (n *notificationService) CreateFromEvent(ctx context.Context, event NotificationCreatedEvent) error {
	//TODO implement me
	panic("implement me")
}

func (n *notificationService) RequestDelivery(ctx context.Context, event NotificationDeliveryRequestedEvent) error {
	//TODO implement me
	panic("implement me")
}

func (n *notificationService) RetryDelivery(ctx context.Context, event NotificationDeliveryRetryEvent) error {
	//TODO implement me
	panic("implement me")
}
