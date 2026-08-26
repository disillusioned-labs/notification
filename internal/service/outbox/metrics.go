package outbox

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("service/outbox")

type Metrics struct {
	lastPollTimestamp metric.Int64Gauge

	eventsClaimed             metric.Int64Counter
	eventsPublished           metric.Int64Counter
	eventsPublishFailed       metric.Int64Counter
	eventsMarkFailed          metric.Int64Counter
	eventsMarkPublishedFailed metric.Int64Counter

	publishDuration metric.Float64Histogram
}

func NewMetrics() Metrics {
	lastPollTimestamp, _ := meter.Int64Gauge(
		"outbox.worker.last_poll_timestamp_seconds",
		metric.WithDescription("Unix timestamp of the last successful outbox poll"),
	)

	eventsClaimed, _ := meter.Int64Counter(
		"outbox.events.claimed",
		metric.WithDescription("Total number of outbox events claimed"),
	)

	eventsPublished, _ := meter.Int64Counter(
		"outbox.events.published",
		metric.WithDescription("Total number of outbox events successfully published"),
	)

	eventsPublishFailed, _ := meter.Int64Counter(
		"outbox.events.publish_failed",
		metric.WithDescription("Total number of outbox events that failed to publish"),
	)

	eventsMarkFailed, _ := meter.Int64Counter(
		"outbox.events.mark_failed",
		metric.WithDescription("Total number of outbox events that failed to be marked as failed"),
	)

	eventsMarkPublishedFailed, _ := meter.Int64Counter(
		"outbox.events.mark_published_failed",
		metric.WithDescription("Total number of outbox events that failed to be marked as published"),
	)

	publishDuration, _ := meter.Float64Histogram(
		"outbox.publish.duration",
		metric.WithDescription("Time spent publishing an outbox event"),
		metric.WithUnit("s"),
	)

	return Metrics{
		lastPollTimestamp:         lastPollTimestamp,
		eventsClaimed:             eventsClaimed,
		eventsPublished:           eventsPublished,
		eventsPublishFailed:       eventsPublishFailed,
		eventsMarkFailed:          eventsMarkFailed,
		eventsMarkPublishedFailed: eventsMarkPublishedFailed,
		publishDuration:           publishDuration,
	}
}
