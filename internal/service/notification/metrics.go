package notification

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("service/notification")

type Metrics struct {
	deliverySent           metric.Int64Counter
	deliveryFailed         metric.Int64Counter
	deliveryRetryScheduled metric.Int64Counter
	retryReady             metric.Float64Gauge
}

func NewMetrics() Metrics {
	deliverySent, _ := meter.Int64Counter(
		"notification.delivery.sent",
		metric.WithDescription("Deliveries successfully handed to the provider"),
	)

	deliveryFailed, _ := meter.Int64Counter(
		"notification.delivery.failed",
		metric.WithDescription("Deliveries marked failed (no retries left or non-retryable error)"),
	)

	deliveryRetryScheduled, _ := meter.Int64Counter(
		"notification.delivery.retry_scheduled",
		metric.WithDescription("Deliveries scheduled for another retry attempt"),
	)

	retryReady, _ := meter.Float64Gauge(
		"notification.retry_ready",
		metric.WithDescription("Number of retry deliveries due for processing at the last retry worker poll"),
	)

	return Metrics{
		deliverySent:           deliverySent,
		deliveryFailed:         deliveryFailed,
		deliveryRetryScheduled: deliveryRetryScheduled,
		retryReady:             retryReady,
	}
}

func providerAttr(provider string) metric.AddOption {
	return metric.WithAttributes(attribute.String("provider", provider))
}
