Siap. Kalau `architecture.md` lo mau jadi **source of truth**, gue akan masukin contract ini dan tetap mempertahankan base structure repo lo. Gue juga akan rapihin bagian worker supaya nggak misleading soal Kafka offset, internal queue, retry, dan idempotency.

Ini versi `architecture.md` yang gue rekomendasikan:

````md
# Notification Service Architecture

## 1. Overview

The notification service is an event-driven service responsible for:

- consuming notification events from Kafka
- creating notifications and deliveries
- processing deliveries
- calling external notification providers
- tracking delivery attempts
- retrying transient failures
- maintaining notification and delivery state in PostgreSQL

The service is designed around:

- Kafka for asynchronous event transport
- PostgreSQL as the source of truth for notification and delivery state
- Transactional Outbox on producer services
- At-least-once event delivery
- Idempotent event processing
- Bounded worker concurrency
- Explicit retry and failure handling
- OpenTelemetry for tracing and metrics

---

# 2. High-Level Architecture

```text
                         ┌──────────────────────────┐
                         │     PRODUCER SERVICES     │
                         │                          │
                         │ Order Service             │
                         │ Social Service            │
                         │ Marketing Service         │
                         └────────────┬─────────────┘
                                      │
                                      │ Transactional Outbox
                                      ▼
                         ┌──────────────────────────┐
                         │        Kafka Producer    │
                         └────────────┬─────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              KAFKA CLUSTER                               │
│                                                                         │
│  notification.transactional                                             │
│  ├── P0                                                                │
│  ├── P1                                                                │
│  ├── P2                                                                │
│  └── ...                                                               │
│                                                                         │
│  notification.social                                                    │
│  ├── P0                                                                │
│  ├── P1                                                                │
│  └── ...                                                               │
│                                                                         │
│  notification.marketing                                                 │
│  ├── P0                                                                │
│  ├── P1                                                                │
│  └── ...                                                               │
└──────────────┬──────────────────┬──────────────────────┬────────────────┘
               │                  │                      │
               ▼                  ▼                      ▼
┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐
│ Transactional      │ │ Social             │ │ Marketing          │
│ Consumer Group     │ │ Consumer Group     │ │ Consumer Group     │
└─────────┬──────────┘ └─────────┬──────────┘ └─────────┬──────────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐
│ Consumer Instances │ │ Consumer Instances │ │ Consumer Instances │
└─────────┬──────────┘ └─────────┬──────────┘ └─────────┬──────────┘
          │                      │                      │
          ▼                      ▼                      ▼
      Worker Pool            Worker Pool            Worker Pool
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                                 ▼
                     ┌────────────────────────┐
                     │   Notification Domain  │
                     │                        │
                     │ Create Notification    │
                     │ Create Delivery        │
                     │ Process Delivery       │
                     └────────────┬───────────┘
                                  │
                       ┌──────────┴──────────┐
                       ▼                     ▼
                 PostgreSQL            Provider Layer
                                             │
                                  ┌──────────┼──────────┐
                                  ▼          ▼          ▼
                                 FCM      SendGrid    Twilio
````

---

# 3. Repository Structure

The existing repository structure is retained.

```text
notification/
│
├── .github/
│
├── cmd/
│
├── db/
│   ├── migrations/
│   │   ├── 00001_extensions.sql
│   │   ├── 00002_functions.sql
│   │   ├── 00003_notifications.sql
│   │   ├── 00004_notification_deliveries.sql
│   │   ├── 00005_notification_delivery_attempts.sql
│   │   ├── 00006_providers.sql
│   │   └── migrations.go
│   │
│   └── queries/
│
├── internal/
│   ├── app/
│   │   ├── consumer.go
│   │   └── version.go
│   │
│   ├── config/
│   │
│   ├── consumer/
│   │   └── consumer.go
│   │
│   ├── platform/
│   │   ├── kafka/
│   │   │   ├── consumer.go
│   │   │   ├── header.go
│   │   │   ├── kafka.go
│   │   │   ├── producer.go
│   │   │   └── record.go
│   │   │
│   │   ├── postgres/
│   │   │
│   │   └── telemetry/
│   │       ├── logger.go
│   │       └── otel.go
│   │
│   ├── repository/
│   │   ├── db.go
│   │   └── store.go
│   │
│   └── service/
│       └── errors.go
│
├── migrations/
│
├── architecture.md
├── Dockerfile
├── Makefile
├── docker-compose.yml
├── go.mod
├── go.sum
└── sqlc.yaml
```

The architecture should evolve within this structure rather than introducing unnecessary top-level packages.

---

# 4. Kafka Architecture

Kafka topics represent **notification workload boundaries**.

Event types represent **event semantics**.

They are intentionally separate.

```text
Topic
│
├── notification.transactional
├── notification.social
└── notification.marketing

Event Type
│
├── notification.created
├── notification.delivery.requested
└── notification.delivery.retry
```

## Topic Rules

Topics are explicitly configured.

The topic must not be derived from the event type.

Bad:

```text
event_type = notification.created
topic      = notification.created
```

Correct:

```text
topic      = notification.transactional
event_type = notification.created
```

This allows each workload to scale independently.

For example:

```text
transactional → 20 instances
social        → 5 instances
marketing     → 2 instances
```

---

# 5. Kafka Consumer Groups

Each topic has a dedicated consumer group.

```text
notification.transactional
        │
        ▼
notification-transactional

notification.social
        │
        ▼
notification-social

notification.marketing
        │
        ▼
notification-marketing
```

Consumer instances belonging to the same consumer group share partition ownership.

Example:

```text
notification.transactional
│
└── notification-transactional
       │
       ├── T1
       ├── T2
       └── T3
```

Kafka determines partition assignment across instances.

The number of active consumers for a topic should not exceed the number of partitions when maximum parallelism is desired.

---

# 6. Kafka Event Contract

Every Kafka event consists of:

```text
Kafka Record
│
├── Topic
├── Key
├── Headers
└── Value
```

The application-facing abstraction remains:

```go
type Record struct {
    Topic   string
    Key     []byte
    Value   []byte
    Headers []kgo.RecordHeader
}
```

The Kafka infrastructure layer is responsible for translating this abstraction to/from the underlying Kafka client.

---

# 7. Kafka Topic Contract

Supported notification topics:

```text
notification.transactional
notification.social
notification.marketing
```

### `notification.transactional`

Used for business-critical notification workloads.

Typical events:

```text
notification.created
notification.delivery.requested
notification.delivery.retry
```

### `notification.social`

Used for social and activity-related notification workloads.

Typical events:

```text
notification.created
notification.delivery.requested
notification.delivery.retry
```

### `notification.marketing`

Used for campaign and promotional notification workloads.

Typical events:

```text
notification.created
notification.delivery.requested
notification.delivery.retry
```

The same event type may exist on different topics when the workload semantics differ.

---

# 8. Kafka Key Contract

The Kafka key is used for partitioning and ordering.

Default:

```text
Kafka Key = aggregate_id
```

Example:

```text
aggregate_id = order-123
Kafka key    = order-123
```

This allows events belonging to the same aggregate to maintain partition ordering.

The key must not be:

```text
event_id
Kafka offset
random UUID
```

when aggregate-level ordering is required.

Different topics may choose different aggregate types depending on their business semantics.

Examples:

```text
transactional → order_id
social        → user_id
marketing     → campaign_id
```

The selected key must be documented as part of the topic contract.

---

# 9. Kafka Headers Contract

Standard event headers:

```text
event-id
event-type
event-version
source-service
aggregate-type
aggregate-id
```

OpenTelemetry propagation:

```text
traceparent
tracestate
```

## `event-id`

Globally unique identifier for a logical event.

Example:

```text
01K3ABCDEF123...
```

`event-id` is the primary identifier used for consumer idempotency.

Kafka offsets must never be treated as event identifiers.

---

## `event-type`

Identifies the semantic type of the event.

Examples:

```text
notification.created
notification.delivery.requested
notification.delivery.retry
```

`event-type` does not determine the Kafka topic.

---

## `event-version`

Identifies the schema/contract version of the event.

Example:

```text
event-version: 1
```

This is not the version of the aggregate or database entity.

---

## `source-service`

Identifies the service that produced the event.

Examples:

```text
order-service
social-service
marketing-service
identity-service
```

---

## `aggregate-type`

Identifies the aggregate associated with the event.

Examples:

```text
order
user
campaign
notification
delivery
```

---

## `aggregate-id`

Identifies the aggregate associated with the event.

Example:

```text
order-123
```

The aggregate ID normally corresponds to the Kafka key.

---

## OpenTelemetry Headers

Distributed tracing is propagated using the OpenTelemetry text map propagator.

The service should inject the current trace context into Kafka headers.

The notification service extracts the context before processing the event.

Custom `trace-id` headers should not be required when standard OpenTelemetry propagation is available.

---

# 10. Kafka Event Value

The Kafka value contains the business payload only.

Example:

```json
{
  "notification_id": "notif-123",
  "recipient": {
    "user_id": "user-456"
  },
  "channels": [
    {
      "channel": "push",
      "template": "order_created"
    },
    {
      "channel": "email",
      "template": "order_created"
    }
  ],
  "payload": {
    "order_id": "order-123",
    "order_number": "ORD-001"
  }
}
```

Kafka-specific metadata must not be duplicated in the business payload.

Do not include:

```text
topic
partition
offset
consumer_group
Kafka timestamp
```

inside the event payload.

---

# 11. Event Types

The notification service initially supports:

```text
notification.created
notification.delivery.requested
notification.delivery.retry
```

## `notification.created`

Requests the notification domain to create a notification and its associated deliveries.

Flow:

```text
notification.created
        │
        ▼
Notification Service
        │
        ├── Create Notification
        └── Create Deliveries
```

---

## `notification.delivery.requested`

Requests processing of an existing delivery.

Flow:

```text
notification.delivery.requested
        │
        ▼
Delivery Service
        │
        ▼
Provider
```

---

## `notification.delivery.retry`

Requests retry processing for an existing delivery.

Payload:

```json
{
  "delivery_id": "delivery-123"
}
```

The retry event must not duplicate the full notification payload.

The notification service retrieves the latest delivery state from PostgreSQL.

Flow:

```text
notification.delivery.retry
        │
        ▼
delivery_id
        │
        ▼
PostgreSQL
        │
        ▼
Atomic Claim
        │
        ▼
Provider
```

---

# 12. Example Event

Example `notification.created` event:

### Topic

```text
notification.transactional
```

### Key

```text
order-123
```

### Headers

```text
event-id: 01K3ABCDEF123
event-type: notification.created
event-version: 1
source-service: order-service
aggregate-type: order
aggregate-id: order-123
traceparent: 00-...
```

### Value

```json
{
  "notification_id": "notif-123",
  "recipient": {
    "user_id": "user-456"
  },
  "channels": [
    {
      "channel": "push",
      "template": "order_created"
    },
    {
      "channel": "email",
      "template": "order_created"
    }
  ],
  "payload": {
    "order_id": "order-123",
    "order_number": "ORD-001"
  }
}
```

---

# 13. Producer Architecture

Producer services use the Transactional Outbox pattern.

```text
┌──────────────────────┐
│ Producer Service     │
│                      │
│ Business Transaction │
└──────────┬───────────┘
           │
           │ same DB transaction
           ▼
┌──────────────────────┐
│ PostgreSQL           │
│                      │
│ business data        │
│ outbox events        │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Outbox Publisher     │
│                      │
│ claim pending events │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Kafka Producer       │
└──────────┬───────────┘
           │
           ▼
         Kafka
```

The Outbox pattern guarantees that the business transaction and event creation are committed atomically.

Kafka publishing remains at-least-once.

---

# 14. Producer Record Construction

Producer services should construct Kafka records explicitly.

Example:

```go
headers := make([]kgo.RecordHeader, 0, 8)

carrier := kafka.NewHeaderCarrier(&headers)
otel.GetTextMapPropagator().Inject(ctx, carrier)

headers = append(
    headers,
    kafka.RecordHeader("event-id", event.ID.String()),
    kafka.RecordHeader("event-type", event.EventType),
    kafka.RecordHeader("event-version", strconv.Itoa(int(event.EventVersion))),
    kafka.RecordHeader("source-service", "order-service"),
    kafka.RecordHeader("aggregate-type", event.AggregateType),
    kafka.RecordHeader("aggregate-id", event.AggregateID.String()),
)

err := producer.Publish(ctx, kafka.Record{
    Topic:   "notification.transactional",
    Key:     []byte(event.AggregateID.String()),
    Value:   event.Payload,
    Headers: headers,
})
```

The topic is selected explicitly.

---

# 15. At-Least-Once Delivery

The producer must be treated as at-least-once.

Example failure:

```text
1. Publish Kafka event       ✓
2. Kafka accepts event       ✓
3. Producer crashes          X
4. Mark outbox published     X
5. Event becomes eligible
6. Event is published again  ✓
```

Therefore:

```text
Transactional Outbox
        +
At-Least-Once Delivery
        +
Consumer Idempotency
```

is the expected delivery model.

Exactly-once business processing must not be assumed from Kafka alone.

---

# 16. Consumer Architecture

Each consumer instance contains:

```text
Kafka Consumer
      │
      ▼
Event Validation
      │
      ▼
Internal Bounded Queue
      │
      ├── Worker 1
      ├── Worker 2
      ├── Worker 3
      └── Worker N
             │
             ▼
       Notification Domain
```

Example:

```text
notification.transactional
        │
        ▼
notification-transactional
        │
        ▼
       T1
        │
        ▼
┌─────────────────────┐
│ Kafka Consumer      │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Bounded Work Queue  │
└─────────┬───────────┘
          │
    ┌─────┼─────┬─────┐
    ▼     ▼     ▼     ▼
   W1    W2    W3    WN
    │     │     │     │
    └─────┴─────┴─────┘
              │
              ▼
      Notification Domain
```

Worker concurrency must be bounded.

The initial worker count should be configurable.

Example:

```text
WORKER_COUNT=10
QUEUE_SIZE=100
```

The system must not create an unbounded number of goroutines.

---

# 17. Consumer Processing Lifecycle

A Kafka event follows:

```text
Kafka Record
     │
     ▼
Extract Headers
     │
     ▼
Validate Event Contract
     │
     ▼
Deserialize Payload
     │
     ▼
Check Idempotency
     │
     ▼
Process Domain Operation
     │
     ▼
Persist State
     │
     ▼
Mark Processing Complete
     │
     ▼
Kafka Offset Commit
```

An event must only be considered successfully processed after the required business operation has completed successfully.

---

# 18. Idempotency

Consumers must be idempotent.

The primary deduplication identifier is:

```text
event-id
```

Example:

```text
Kafka
 │
 ├── event-id = 01ABC
 │
 └── event-id = 01ABC
```

The first event:

```text
01ABC
  │
  ▼
process
  │
  ▼
success
```

The second event:

```text
01ABC
  │
  ▼
already processed
  │
  ▼
skip safely
```

Database constraints should enforce uniqueness where appropriate.

The consumer must not rely only on an in-memory deduplication cache.

---

# 19. Notification Processing

The notification domain owns notification state.

```text
Kafka Event
    │
    ▼
Notification Service
    │
    ├── Create Notification
    │
    └── Create Delivery
```

PostgreSQL is the source of truth for:

```text
notifications
notification_deliveries
notification_delivery_attempts
providers
preferences
```

---

# 20. Delivery Processing

A delivery represents one attemptable notification channel.

Example:

```text
Notification
│
├── Push Delivery
├── Email Delivery
└── SMS Delivery
```

Delivery processing:

```text
Delivery
   │
   ▼
Atomic Claim
   │
   ▼
Provider Selection
   │
   ▼
Provider Call
   │
   ├───────────────┐
   ▼               ▼
 Success         Failure
   │               │
   ▼               ▼
  SENT         Retry / DLQ
```

Delivery state is persisted in PostgreSQL.

---

# 21. Provider Layer

External providers are abstracted behind a provider interface.

```text
Notification Domain
        │
        ▼
Provider Interface
        │
   ┌────┼────┐
   ▼    ▼    ▼
  FCM SendGrid Twilio
```

The domain must not depend directly on provider-specific SDKs.

Provider implementations belong under the platform/integration layer.

Provider failures must be classified into:

```text
Transient
Permanent
Unknown
```

Transient errors may be retried.

Permanent errors should not be retried indefinitely.

---

# 22. Retry Architecture

Retry state is stored in PostgreSQL.

Example:

```text
notification_deliveries
├── status
├── attempt_count
├── next_retry_at
├── last_error
└── ...
```

The retry worker finds eligible deliveries:

```text
PostgreSQL
     │
     ▼
next_retry_at <= now()
     │
     ▼
Atomic Claim
     │
     ▼
Publish notification.delivery.retry
     │
     ▼
Kafka
     │
     ▼
Normal Consumer Pipeline
```

Retry events contain references rather than a copy of the original notification state.

```json
{
  "delivery_id": "delivery-123"
}
```

The database remains the source of truth.

---

# 23. Retry Backoff

Retries use bounded exponential backoff with jitter.

Conceptually:

```text
attempt 1 → short delay
attempt 2 → longer delay
attempt 3 → longer delay
...
attempt N → maximum delay
```

The retry system must have:

* maximum retry count
* maximum retry delay
* jitter
* permanent failure classification
* DLQ handling

Retry state must never cause an infinite retry loop.

---

# 24. Dead Letter Queue

Events that cannot be processed successfully after the configured retry policy are moved to a DLQ.

Conceptually:

```text
Kafka
  │
  ▼
Consumer
  │
  ▼
Processing Failure
  │
  ▼
Retry
  │
  ├── success
  │
  └── maximum retries exceeded
              │
              ▼
             DLQ
```

DLQ records must retain enough metadata to investigate the failure, including:

```text
event-id
event-type
event-version
source-service
original-topic
aggregate-id
error
```

DLQ handling must be observable and operationally recoverable.

---

# 25. Offset Management

Kafka offsets represent transport progress, not business state.

An offset should only advance after successful processing.

With an internal worker pool, completion ordering must be handled carefully.

Example:

```text
Partition 0

offset 100 → Worker 1 → slow
offset 101 → Worker 2 → done
offset 102 → Worker 3 → done
```

The consumer must not blindly commit offset `102` while offset `100` is still processing.

The commit position must represent the highest safely processed contiguous offset.

The implementation should track completion per partition.

---

# 26. Backpressure

The internal work queue must be bounded.

Example:

```text
Kafka
  │
  ▼
Consumer
  │
  ▼
┌─────────────────────┐
│ Queue size = 100    │
└─────────┬───────────┘
          │
      ┌───┼───┐
      ▼   ▼   ▼
     W1  W2  W3
```

When workers cannot keep up:

```text
queue full
    │
    ▼
consumer slows/stops accepting work
    │
    ▼
Kafka consumer lag increases
```

Kafka lag is preferable to allowing unbounded in-memory memory growth.

---

# 27. Graceful Shutdown

Shutdown sequence:

```text
1. Stop accepting new Kafka work
2. Stop polling / pause consumption
3. Stop enqueueing new work
4. Wait for in-flight workers
5. Commit successfully processed offsets
6. Close Kafka consumer
7. Close database connections
8. Shutdown telemetry
```

The service should respect a configurable shutdown timeout.

In-flight work must not be abandoned silently.

---

# 28. Observability

The service must expose:

### Kafka

```text
consumer lag
records consumed
records failed
records processed
rebalance count
```

### Worker Pool

```text
queue depth
queue capacity
active workers
processing duration
```

### Notification

```text
notifications created
deliveries created
deliveries processed
deliveries sent
deliveries failed
```

### Provider

```text
provider request count
provider latency
provider errors
provider rate limits
```

### Retry

```text
retry count
retry exhaustion
DLQ count
```

All logs and traces should include:

```text
event-id
event-type
aggregate-id
delivery-id
```

where applicable.

---

# 29. Schema Evolution

Event schemas are versioned using:

```text
event-version
```

Example:

```text
event-type: notification.created
event-version: 1
```

A backward-compatible change should preserve the event type and evolve the schema.

Example:

```text
v1
{
    "order_id": "123"
}

v2
{
    "order_id": "123",
    "order_number": "ORD-123"
}
```

Consumers must tolerate supported compatible versions.

Incompatible changes should introduce an explicit migration strategy rather than silently changing the existing contract.

Schema definitions should be version-controlled.

A Schema Registry may be introduced when the number of producers/consumers and independently evolving schemas justifies centralized compatibility enforcement.

---

# 30. Design Principles

The notification service follows these principles:

1. **Topics represent workload boundaries.**
2. **Event types represent event semantics.**
3. **Topics are explicitly configured.**
4. **Kafka keys represent the ordering/partitioning strategy.**
5. **Event metadata is transported separately from business payload.**
6. **PostgreSQL is the source of truth for notification and delivery state.**
7. **Kafka delivery is treated as at-least-once.**
8. **Consumers must be idempotent.**
9. **Worker concurrency must be bounded.**
10. **Kafka lag is preferred over unbounded in-memory queues.**
11. **Retry state belongs to delivery state, not the original event.**
12. **Retry events reference database state rather than copying it.**
13. **Offsets represent transport progress, not business state.**
14. **External providers are accessed through an abstraction layer.**
15. **Provider failures must be classified before retrying.**
16. **Tracing is propagated through standard OpenTelemetry context propagation.**
17. **Schema changes must follow an explicit compatibility policy.**

---

# 31. End-to-End Flow

## Normal Flow

```text
Producer Service
      │
      ▼
DB Transaction
      │
      ├── Business Data
      └── Outbox Event
              │
              ▼
        Outbox Publisher
              │
              ▼
            Kafka
              │
              ▼
       Consumer Group
              │
              ▼
        Kafka Consumer
              │
              ▼
       Bounded Work Queue
              │
       ┌──────┼──────┐
       ▼      ▼      ▼
      W1     W2     WN
       │      │      │
       └──────┼──────┘
              ▼
      Notification Domain
              │
       ┌──────┴──────┐
       ▼             ▼
 PostgreSQL      Provider Layer
                     │
                ┌────┼────┐
                ▼    ▼    ▼
               FCM SendGrid Twilio
```

## Retry Flow

```text
Provider Failure
      │
      ▼
Delivery State
      │
      ▼
next_retry_at
      │
      ▼
Retry Worker
      │
      ▼
notification.delivery.retry
      │
      ▼
Kafka
      │
      ▼
Consumer
      │
      ▼
Atomic Delivery Claim
      │
      ▼
Provider
```

---

# 32. Current Implementation Strategy

Implementation should proceed incrementally.

### Phase 1 — Kafka Contract

Implement and validate:

```text
topic
key
event-id
event-type
event-version
source-service
aggregate-type
aggregate-id
traceparent
```

### Phase 2 — Consumer

Implement:

```text
Kafka Consumer
      ↓
Header Validation
      ↓
Payload Validation
      ↓
Event Dispatch
```

### Phase 3 — Notification Domain

Implement:

```text
notification.created
      ↓
Create Notification
      ↓
Create Deliveries
```

### Phase 4 — Idempotency

Implement database-backed event deduplication.

### Phase 5 — Delivery Processing

Implement:

```text
delivery.requested
      ↓
Atomic Claim
      ↓
Provider
      ↓
Persist Result
```

### Phase 6 — Worker Pool

Introduce bounded worker concurrency after single-event processing is correct.

### Phase 7 — Offset Tracking

Implement safe partition-aware offset commit semantics.

### Phase 8 — Retry and DLQ

Implement:

```text
retry
backoff
jitter
maximum attempts
DLQ
```

### Phase 9 — Production Hardening

Add:

```text
metrics
tracing
graceful shutdown
backpressure
provider rate limits
load testing
failure testing
```

```

**Yang paling penting:** gue sengaja nggak memasukkan struktur folder `worker/`, `notification/`, `delivery/`, `provider/`, dll yang baru ke `architecture.md`, karena repo lo **sudah punya base structure sendiri**. Kita biarkan implementasinya tumbuh dari structure yang ada, bukan architecture doc memaksa folder baru.
```
