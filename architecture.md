Bisa. Ini versi **Markdown yang sudah dibenerin**, terutama supaya section tidak nyangkut jadi satu `code block`.

````markdown
# Notification Service Architecture

## 1. Overview

The Notification Service is an event-driven service responsible for:

- consuming notification events from Kafka
- creating notifications and notification deliveries
- processing notification deliveries
- calling external notification providers
- tracking delivery attempts
- scheduling and processing retries
- maintaining notification and delivery state in PostgreSQL

The service follows:

- Kafka for asynchronous event transport
- PostgreSQL as the source of truth for notification and delivery state
- Transactional Outbox for reliable event publication
- At-least-once event delivery
- Database-backed idempotency
- Bounded worker concurrency
- Explicit retry and failure handling
- OpenTelemetry for distributed tracing

## 2. High-Level Architecture

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
   │      Outbox Publisher    │
   └────────────┬─────────────┘
                │
                ▼
       ┌──────────────┐
       │     Kafka    │
       └──────┬───────┘
              │
   ┌──────────┼───────────────────┐
   │          │                   │
   ▼          ▼                   ▼
transactional  social             marketing
topic          topic              topic
   │           │                  │
   ▼           ▼                  ▼
Consumer Group Consumer Group     Consumer Group
   │           │                  │
   └───────────┼──────────────────┘
               │
               ▼
        Bounded Worker Pool
               │
               ▼
   ┌────────────────────────┐
   │ Notification Service   │
   │                        │
   │ Create Notification    │
   │ Create Delivery        │
   │ Process Delivery       │
   └────────────┬───────────┘
                │
       ┌────────┴────────────┐
       │                     │
       ▼                     ▼
   PostgreSQL          Provider Layer
       │
   ┌───┼───────────┐
   ▼   ▼           ▼
  Push Email       SMS
````

## 3. Core Components

### Kafka

Kafka is the asynchronous transport layer.

Kafka carries:

* notification creation events
* delivery request events
* delivery retry events

Kafka is not the source of truth for notification state.

### PostgreSQL

PostgreSQL is the source of truth for:

* notifications
* notification deliveries
* delivery attempts
* providers
* outbox events

Kafka offsets are transport state and must not be used as business state.

### Notification Service

The notification service contains the domain logic for:

* `notification.created`
* `notification.delivery.requested`
* `notification.delivery.retry`

The service is responsible for validating events, maintaining state, claiming deliveries, and coordinating provider execution.

### Provider Layer

The provider layer abstracts external notification providers.

Examples:

* FCM
* SendGrid
* Twilio

The domain must not depend directly on provider-specific SDKs.

## 4. Kafka Topic Architecture

Notification workloads are separated by category.

```text
notification.transactional
notification.social
notification.marketing
```

Topics represent workload boundaries.

Event types represent event semantics.

These concepts are intentionally separate.

For example:

Topic:

```text
notification.transactional
```

Event:

```text
notification.created
```

The event type must not determine the Kafka topic.

## 5. Consumer Groups

Each notification topic has its own consumer group.

```text
notification.transactional
│
└── notification-transactional

notification.social
│
└── notification-social

notification.marketing
│
└── notification-marketing
```

Consumer instances in the same group share Kafka partitions.

Example:

```text
notification.transactional
│
▼
notification-transactional
│
├── Consumer 1
├── Consumer 2
└── Consumer 3
```

Scaling the number of consumers does not increase parallelism beyond the number of partitions.

## 6. Kafka Event Contract

Each Kafka record contains:

* Topic
* Key
* Headers
* Value

Kafka metadata and business payload are kept separate.

## 7. Kafka Headers

The standard event headers are:

* `event-id`
* `event-version`
* `source-service`
* `aggregate-type`
* `aggregate-id`

Distributed tracing is propagated using OpenTelemetry propagation headers.

Typically:

```text
traceparent
tracestate
```

### `event-id`

Globally unique identifier for the logical event.

Example:

```text
01K3ABCDEF123...
```

Consumers use this identifier for idempotency.

Kafka offsets must never be treated as event IDs.

### `event-version`

Schema version of the event.

Example:

```text
event-version: 1
```

### `source-service`

Service that produced the event.

Example:

```text
source-service: order
```

### `aggregate-type`

Logical aggregate associated with the event.

Examples:

```text
order
notification
notification_delivery
```

### `aggregate-id`

Identifier of the associated aggregate.

Examples:

```text
order-123
delivery-123
```

The Kafka key should normally correspond to the aggregate ID when ordering for that aggregate is required.

## 8. Kafka Key

Kafka keys determine partitioning and therefore influence ordering.

The default strategy is:

```text
Kafka Key = aggregate_id
```

For example:

```text
aggregate-id = order-123
Kafka key    = order-123
```

This ensures events for the same aggregate are routed to the same Kafka partition.

The exact aggregate/key strategy is part of each topic's contract.

## 9. Event Value Contract

The Kafka value contains the business payload only.

Kafka-specific metadata must not be duplicated inside the payload.

Do not put these fields inside the value:

```text
topic
partition
offset
consumer group
Kafka timestamp
```

## 10. Notification Events

The notification service supports three event types:

```text
notification.created
notification.delivery.requested
notification.delivery.retry
```

## 11. notification.created

`notification.created` requests creation of a notification and its deliveries.

The payload is:

```json
{
  "notification_type": "order_shipped",
  "recipient_id": "user-123",
  "category": "transactional",
  "targets": [
    {
      "channel": "email",
      "destination": "user@example.com"
    },
    {
      "channel": "push",
      "destination": "device-token"
    }
  ],
  "payload": {
    "order_id": "order-123",
    "tracking_number": "JNE123"
  }
}
```

### Fields

### notification_type

Business notification type.

Examples:

```text
order_shipped
payment_failed
order_delivered
```

### recipient_id

Business identity of the notification recipient.

Example:

```text
user-123
```

The notification service does not resolve the recipient into provider-specific destinations using this field.

### category

Notification category.

Supported categories:

* transactional
* social
* marketing

### targets

The concrete delivery destinations requested by the producer.

Each target contains:

```json
{
  "channel": "email",
  "destination": "user@example.com"
}
```

Supported channels:

* email
* sms
* push

The destination is captured at delivery creation time.

This prevents provider execution from depending on mutable recipient data.

### payload

Immutable business data required by the notification.

Example:

```json
{
  "order_id": "order-123",
  "tracking_number": "JNE123"
}
```

The notification service stores this payload as JSONB.

## 12. notification.created Processing

The processing flow is:

```text
notification.created
│
▼
Validate Event
│
▼
Decode Payload
│
▼
Validate Payload
│
▼
Check event_id
│
├── already exists → success / ignore
│
▼
BEGIN TRANSACTION
│
├── Create Notification
│
├── Create Delivery
│
└── Create Outbox Event
│
▼
COMMIT
```

The notification, delivery, and corresponding outbox event are committed atomically.

## 13. Idempotency

Notification creation is idempotent using:

```text
event_id
```

The database enforces uniqueness:

```text
notifications.event_id UNIQUE
```

The service also performs an existence check before attempting creation.

However, the existence check is not considered the final protection against races.

The unique database constraint is authoritative.

Example:

```text
Consumer A ──┐
             ├── event_id = X
Consumer B ──┘

A → INSERT → success
B → INSERT → unique violation
```

The unique violation is treated as a duplicate event and therefore successful processing.

## 14. Notification Delivery

Each target from notification.created produces one notification delivery.

Example:

```text
Notification
│
├── Email Delivery
│
└── Push Delivery
```

A notification may have multiple deliveries.

The database enforces at most one delivery per channel for a notification.

```text
(notification_id, channel) UNIQUE
```

## 15. Provider Selection

Providers are selected when the delivery is created.

Providers have:

* name
* type
* priority
* is_active

The service selects the highest-priority active provider for the target channel.

The selected provider is stored as a snapshot on:

```text
notification_deliveries.provider
```

Provider selection is not repeated during delivery execution.

This prevents a delivery from silently switching providers after it has been created.

## 16. Delivery State

A delivery can have these states:

* pending
* processing
* sent
* retry
* failed
* cancelled

The delivery is the unit of provider execution and retry.

## 17. Delivery Request Event

After a delivery is created, the notification service creates an outbox event:

```text
event-type:
notification.delivery.requested
```

Payload:

```json
{
  "delivery_id": "delivery-123"
}
```

The event references the delivery rather than duplicating the notification data.

## 18. Transactional Outbox

Notification creation uses the Transactional Outbox pattern.

The transaction contains:

```text
BEGIN

Create notification

Create delivery

Create notification.delivery.requested outbox event

COMMIT
```

The outbox event is therefore guaranteed to exist if the delivery exists.

The service must never:

```text
COMMIT delivery
↓
publish Kafka
↓
hope publishing succeeds
```

because a process crash between those operations could lose the event.

Instead:

```text
DB transaction
│
├── delivery
└── outbox event
│
▼
COMMIT
│
▼
Outbox Publisher
│
▼
Kafka
```

## 19. Outbox Event Contract

The outbox table contains:

* id
* aggregate_type
* aggregate_id
* event_type
* event_version
* payload
* created_at
* published_at
* trace_id
* attempt_count
* next_attempt_at
* locked_at
* locked_by
* last_error

For delivery events:

```text
aggregate_type = notification_delivery
aggregate_id   = delivery.id
```

## 20. Outbox Publisher

The Outbox Publisher is responsible for:

* claiming pending outbox events
* publishing them to Kafka
* marking successful events as published
* scheduling retries for failed publication
* releasing stale locks

Flow:

```text
PostgreSQL
│
▼
Claim Pending Events
│
▼
Publish Kafka
│
├── success
│     │
│     ▼
│   Mark Published
│
└── failure
│
▼
Schedule Retry
```

Multiple publisher instances may run concurrently.

`FOR UPDATE SKIP LOCKED` is used to safely distribute work between publishers.

## 21. Outbox At-Least-Once Semantics

Outbox publishing is at-least-once.

A crash may occur after Kafka accepts an event but before `published_at` is persisted.

Example:

```text
Publish Kafka        ✓
Process crashes      X
Mark published       X
```

The event may subsequently be published again.

Consumers must therefore remain idempotent.

Exactly-once business processing must not be assumed.

## 22. notification.delivery.requested

This event requests processing of an existing pending delivery.

Payload:

```json
{
  "delivery_id": "delivery-123"
}
```

Processing:

```text
delivery.requested
│
▼
Validate Event
│
▼
Parse delivery_id
│
▼
Load Delivery
│
▼
Check State
│
├── sent       → ignore
├── pending    → process
└── other      → ignore
```

## 23. Delivery Claiming

Before calling a provider, the delivery must be atomically claimed.

```text
pending
│
▼
Atomic Claim
│
▼
processing
```

The claim stores worker ownership:

```text
locked_by
locked_at
```

Only the worker that successfully claims the delivery may execute provider processing.

If another worker attempts to claim the same delivery, it receives no eligible row and safely stops processing it.

This prevents concurrent provider execution for the same delivery.

## 24. Provider Execution

After claiming a delivery:

```text
Delivery
│
▼
Load Notification Payload
│
▼
Use Provider Snapshot
│
▼
Provider.Send(...)
│
├── success
│
└── failure
```

Provider execution must use:

```text
delivery.provider
delivery.destination
delivery.notification_id
```

The provider must not be re-selected by priority during execution.

## 25. Delivery Attempts

Every provider execution is recorded in:

```text
notification_delivery_attempts
```

An attempt contains:

* delivery_id
* attempt_number
* provider
* provider_message_id
* status
* http_status_code
* error_type
* error_message
* response
* created_at

Supported attempt statuses:

* success
* failed

Each delivery has a unique attempt number.

```text
(delivery_id, attempt_number) UNIQUE
```

## 26. Successful Delivery

When the provider succeeds:

```text
processing
│
▼
provider success
│
├── create successful attempt
│
└── mark delivery sent
```

The delivery becomes:

```text
status = sent
sent_at = now()
```

The worker lock is removed.

## 27. Failed Delivery

When provider execution fails:

```text
processing
│
▼
provider failure
│
├── create failed attempt
│
└── classify error
```

Failures are classified as:

* transient
* permanent
* unknown

Transient failures may be retried.

Permanent failures should transition to:

```text
failed
```

without further retry.

Unknown failures should follow the configured safety policy rather than being retried indefinitely.

## 28. Retry Event

When a delivery is eligible for retry:

```text
notification.delivery.retry
```

is published.

Payload:

```json
{
  "delivery_id": "delivery-123"
}
```

The retry event contains only the delivery reference.

The notification and delivery state remain in PostgreSQL.

## 29. Retry Processing

Retry processing follows:

```text
delivery.retry
│
▼
Load Delivery
│
▼
Check Status
│
├── sent  → ignore
│
├── retry → continue
│
└── other → ignore
│
▼
Check next_retry_at
│
├── not due → ignore
│
└── due
│
▼
Atomic Claim
│
▼
Provider
```

This makes retry processing idempotent.

## 30. Retry Policy

Retry count is stored on the delivery:

```text
retry_count
max_retries
```

Retry scheduling uses bounded exponential backoff with jitter.

The retry policy must have:

* maximum retries
* maximum delay
* jitter

Retries must never continue indefinitely.

## 31. Retry State

The delivery remains the source of truth for retry state.

Example:

```text
status         = retry
retry_count    = 2
max_retries    = 5
next_retry_at  = 2026-08-26T12:00:00Z
```

The retry event itself does not contain this state.

This prevents stale retry events from becoming authoritative.

## 32. Worker Architecture

Kafka consumption and provider execution are separated by a bounded internal work queue.

```text
Kafka Consumer
│
▼
Validate Record
│
▼
Bounded Queue
│
┌────┼────┬────┐
▼    ▼    ▼    ▼
W1   W2   W3   WN
│    │    │    │
└────┴────┴────┘
│
▼
Notification Service
```

The queue must be bounded.

Example:

```text
worker_count = 10
queue_size   = 100
```

The exact values are configuration.

## 33. Backpressure

When the worker queue is full, Kafka consumption must slow down.

Preferred behavior:

```text
Workers busy
│
▼
Queue full
│
▼
Consumer stops accepting additional work
│
▼
Kafka lag increases
```

Kafka lag is preferable to unbounded memory growth.

The service must not create unbounded goroutines for Kafka records.

## 34. Kafka Offset Management

Kafka offsets represent transport progress, not business state.

An offset may only be committed after the corresponding event has been successfully processed.

With concurrent workers, completion order can differ from Kafka offset order.

Example:

```text
offset 100 → Worker A → processing
offset 101 → Worker B → completed
offset 102 → Worker C → completed
```

Offset 102 must not be committed while offset 100 remains incomplete.

The consumer must track completion per partition and commit only the highest safely completed contiguous offset.

## 35. Error Classification

Errors are divided into:

* Permanent
* Transient
* Unknown

### Permanent

The event cannot succeed through retry.

Examples:

* invalid event schema
* invalid event ID
* unsupported event version
* invalid delivery ID
* delivery does not exist

Permanent failures should not be retried indefinitely.

### Transient

The operation may succeed later.

Examples:

* database temporary failure
* Kafka temporary failure
* provider timeout
* provider rate limit
* temporary network failure

Transient errors may be retried.

### Unknown

Unexpected failures that have not been explicitly classified.

Unknown failures should follow the consumer's safety policy and must be observable.

## 36. Dead Letter Queue

Events that cannot be successfully processed after the configured retry policy are routed to a DLQ.

The DLQ record should retain:

* event-id
* event-type
* event-version
* source-service
* aggregate-type
* aggregate-id
* original-topic
* error

The DLQ exists for operational recovery and investigation.

## 37. Database Schema

The core tables are:

* notifications
* notification_deliveries
* notification_delivery_attempts
* providers
* outbox_events

Relationships:

```text
notifications
│
└── notification_deliveries
    │
    └── notification_delivery_attempts
```

Outbox:

```text
outbox_events
│
└── Kafka
```

## 38. Notifications Table

The notification stores the immutable business notification.

Important fields:

* id
* event_id
* notification_type
* category
* recipient_id
* payload
* trace_id
* created_at
* updated_at

`event_id` is unique and provides event-level idempotency.

`payload` contains the immutable business payload.

## 39. Notification Deliveries Table

A delivery stores the concrete execution state.

Important fields:

* id
* notification_id
* channel
* provider
* destination
* status
* retry_count
* max_retries
* next_retry_at
* locked_by
* locked_at
* created_at
* updated_at
* sent_at

Provider and destination are snapshots.

## 40. Notification Delivery Attempts Table

Attempts provide an immutable execution history.

Example:

```text
Delivery
│
├── Attempt 1 → failed
├── Attempt 2 → failed
└── Attempt 3 → success
```

This makes provider behavior and retry history auditable.

## 41. Providers Table

Providers are configured by channel.

Important fields:

* name
* type
* config
* priority
* is_active
* created_at
* updated_at

Supported provider types:

* email
* sms
* push

Active providers are ordered by priority.

Provider configuration must be handled securely and must not expose secrets in logs.

## 42. Provider Selection and Snapshotting

Provider selection happens at delivery creation:

```text
Target
│
▼
Active Providers
│
▼
Priority
│
▼
Selected Provider
│
▼
Delivery Snapshot
```

After this point, the delivery owns the provider selection.

If provider configuration changes later, existing deliveries continue using their stored provider snapshot.

## 43. Graceful Shutdown

Shutdown sequence:

```text
Stop accepting new Kafka work
Stop polling Kafka
Stop enqueueing new records
Wait for in-flight workers
Commit safely completed offsets
Close Kafka consumer
Close database connections
Shutdown telemetry
```

Shutdown must use a configurable timeout.

## 44. Observability

The service should expose metrics for:

**Kafka**

* consumer lag
* records consumed
* records processed
* records failed
* rebalance count

**Workers**

* queue depth
* queue capacity
* active workers
* processing latency

**Notifications**

* notifications created
* deliveries created

**Deliveries**

* deliveries processed
* deliveries sent
* deliveries failed

**Providers**

* provider requests
* provider latency
* provider failures
* provider rate limits

**Retries**

* retry count
* retry exhaustion
* DLQ count

## 45. Distributed Tracing

OpenTelemetry is used for distributed tracing.

The producer injects trace context into Kafka headers.

The consumer extracts the trace context before processing.

Conceptually:

```text
Producer Span
│
▼
Kafka Headers
│
▼
Consumer Span
│
▼
Service Span
│
▼
Provider Span
```

Relevant identifiers should be included as span attributes:

```text
event.id
event.type
aggregate.type
aggregate.id
delivery.id
provider
```

Sensitive destination data must not be added to traces or logs.

## 46. Event Schema Evolution

Events are versioned using:

```text
event-version
```

Example:

```text
event-type: notification.created
event-version: 1
```

Compatible changes should preserve the event type.

Breaking changes should introduce an explicit versioning strategy.

Consumers must reject unsupported event versions explicitly rather than silently processing an incompatible schema.

Event schemas must be version-controlled.

## 47. Security

The service must treat Kafka payloads and provider destinations as untrusted input.

Validation must happen before persistence or provider execution.

Sensitive data must not be written to logs.

In particular, avoid logging:

* email addresses
* phone numbers
* push tokens
* provider credentials
* provider API keys
* full notification payloads

Provider credentials must be securely managed and never stored in source code.

## 48. End-to-End Normal Flow

```text
    Producer Service
    │
    ▼
    Business Transaction
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
    Notification Consumer
    │
    ▼
    Validate Event
    │
    ▼
    CreateFromEvent
    │
    ▼
    PostgreSQL TX
    │
    ┌───────┼────────┐
    │       │        │
    ▼       ▼        ▼
    Notification Delivery Outbox
    │
    ▼
    COMMIT
    │
    ▼
    Outbox Publisher
    │
    ▼
    Kafka
    │
    ▼
    delivery.requested
    │
    ▼
    RequestDelivery
    │
    ▼
    Atomic Claim
    │
    ▼
    Provider
    │
    ┌────┴────┐
    ▼         ▼
    Success    Failure
    │         │
    ▼         ▼
    SENT     Retry / Failed
```

## 49. End-to-End Retry Flow

```text
    Provider
    │
    ▼
    Transient Failure
    │
    ▼
    Delivery Attempt
    │
    ▼
    Delivery State
    │
    ├── retry_count++
    └── next_retry_at
    │
    ▼
    Retry Worker
    │
    ▼
    Outbox Event
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
    RetryDelivery
    │
    ▼
    Check next_retry_at
    │
    ▼
    Atomic Claim
    │
    ▼
    Provider
```

## 50. Core Design Principles

* Kafka is the transport layer, not the source of truth.
* PostgreSQL is the source of truth for notification and delivery state.
* Topics represent workload boundaries.
* Event types represent event semantics.
* Kafka keys represent the partitioning and ordering strategy.
* Event metadata is transported through Kafka headers.
* Business data is transported through the Kafka value.
* Events are delivered at least once.
* Consumers must be idempotent.
* `event-id` is the primary event idempotency key.
* Database uniqueness is the final protection against duplicate processing.
* Notification creation, delivery creation, and their outbox events are committed atomically.
* Outbox publishing is at-least-once.
* Delivery state is the source of truth for provider execution.
* Provider selection is snapshotted at delivery creation.
* Provider selection is not repeated during delivery execution.
* Delivery claiming must be atomic.
* Provider execution must be bounded by worker concurrency.
* Internal queues must be bounded.
* Kafka lag is preferred over unbounded memory growth.
* Retry events reference delivery state rather than duplicating notification state.
* Retry state belongs to PostgreSQL delivery state.
* Provider failures must be explicitly classified.
* Retries must be bounded.
* Kafka offsets represent transport progress, not business state.
* Concurrent consumers must commit only safely completed contiguous offsets.
* Distributed tracing uses standard OpenTelemetry propagation.
* Sensitive destination and provider data must not be logged.
* Event schemas are versioned and compatibility must be explicit.
* External providers are accessed through an abstraction layer.

## 51. Current Implementation Boundary

The current implementation is intentionally divided into these responsibilities:

```text
Kafka Consumer
│
├── Decode Kafka record
├── Validate event metadata
├── Decode event payload
└── Dispatch by event type
│
▼
Notification Service
│
├── CreateFromEvent
│      ├── Create notification
│      ├── Create deliveries
│      └── Create delivery.requested outbox events
│
├── RequestDelivery
│      └── Process pending delivery
│
└── RetryDelivery
       └── Process due retry delivery
```

Provider execution is isolated behind processDelivery and will be implemented through the provider abstraction.

The Outbox Publisher is responsible only for reliably publishing persisted outbox events to Kafka.

This separation keeps:

```text
Kafka transport
Notification domain
Delivery state
Provider execution
Outbox publication
```

independent and independently testable.

```

**Catatan penting:** di atas aku mempertahankan isi teknisnya, tapi formatting Markdown-nya sekarang benar: heading tidak masuk ke `code block`, JSON jadi `json`, diagram jadi `text`, dan list menjadi bullet list.
```
