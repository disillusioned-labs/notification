package service

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/trace"

	"github.com/disillusioned-labs/notification/internal/repository"
)

// Emit writes one domain event into the transactional outbox. Call it inside
// the same ExecTx as the state change it announces, so a committed state
// change can never be missing its event (and vice versa).
func Emit(ctx context.Context, q repository.Querier, aggregateType string, aggregateID uuid.UUID, eventType string, eventVersion int32, topic string, payload any) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	spanCtx := trace.SpanContextFromContext(ctx)
	var traceID pgtype.Text
	if spanCtx.IsValid() {
		traceID = pgtype.Text{
			String: spanCtx.TraceID().String(),
			Valid:  true,
		}
	}

	_, err = q.CreateOutboxEvent(ctx, repository.CreateOutboxEventParams{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     eventType,
		EventVersion:  eventVersion,
		Topic:         topic,
		Payload:       payloadJSON,
		TraceID:       traceID,
	})
	return err
}
