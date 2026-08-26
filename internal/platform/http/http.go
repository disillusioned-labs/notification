package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultMaxResponseBodySize = 1 << 20 // 1 MiB
	defaultHTTPTimeout         = 10 * time.Second
	defaultUserAgent           = "disillusioned-labs-http-client/1.0"

	// instrumentationName identifies this package as the source of spans
	// and metrics, mirroring how otelpgx tags every DB span. Adjust to your
	// actual module path so it lines up with your other services' names.
	//
	// This is the instrumentation scope, not the service name - the service
	// name (semconv.ServiceName) comes from the resource that
	// telemetry.Setup builds, and every span/metric from every package
	// inherits it automatically.
	instrumentationName = "github.com/disillusioned-labs/notification/internal/platform/http"
)

// ErrResponseTooLarge is returned when the response body exceeds the
// configured limit. Callers should treat this as a definitive error rather
// than silently consuming a truncated payload.
var ErrResponseTooLarge = errors.New("http response body exceeds configured limit")

// Doer abstracts *http.Client so HTTPClient can be unit-tested with a fake
// transport instead of hitting the network.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Logger is a minimal structured-logging interface. Most logging libraries
// (slog, zap's SugaredLogger, logrus, etc.) already satisfy this shape or
// can be trivially adapted to it.
type Logger interface {
	Warn(msg string, kv ...any)
}

type noopLogger struct{}

func (noopLogger) Warn(string, ...any) {}

// handleErr reports err to the global OTel error handler, mirroring
// otelhttp's own internal helper of the same name. The nil check matters:
// otel.Handle's default handler does an unconditional log.Print(err) with
// no nil guard of its own, so calling it unconditionally here would log a
// spurious "<nil>" line on every successful instrument creation.
func handleErr(err error) {
	if err != nil {
		otel.Handle(err)
	}
}

// HTTPClient is a hardened JSON HTTP client: bounded response size and
// OpenTelemetry tracing/metrics for every call.
type HTTPClient struct {
	client          Doer
	maxResponseSize int64
	userAgent       string
	logger          Logger

	tracer trace.Tracer

	// callDuration times a whole logical Do call.
	callDuration metric.Float64Histogram

	// callsFailedCounter counts Do calls that returned a non-nil error to
	// the caller.
	callsFailedCounter metric.Int64Counter

	// callsInFlight tracks logical Do calls currently in progress. otelhttp
	// v0.56 does not wire up its own active-requests instrument, so without
	// this there is no way to see request concurrency/saturation at all.
	callsInFlight metric.Int64UpDownCounter

	// responseTooLargeCounter counts responses rejected by
	// WithMaxResponseSize. A sustained nonzero rate means either the limit
	// is too tight for a legitimate response or an upstream is sending
	// unexpectedly large payloads - worth its own alert since it fails
	// silently as an ordinary-looking error otherwise.
	responseTooLargeCounter metric.Int64Counter
}

// Option configures an HTTPClient.
type Option func(*HTTPClient)

// WithMaxResponseSize overrides the default 1 MiB response body cap.
func WithMaxResponseSize(n int64) Option {
	return func(c *HTTPClient) { c.maxResponseSize = n }
}

// WithUserAgent overrides the default User-Agent header.
func WithUserAgent(ua string) Option {
	return func(c *HTTPClient) { c.userAgent = ua }
}

// WithLogger injects a logger used for close warnings. Defaults to a
// no-op logger if nil.
func WithLogger(l Logger) Option {
	return func(c *HTTPClient) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithTracerProvider sets the TracerProvider used to build this client's
// tracer. Defaults to otel.GetTracerProvider() (the global one) if unset -
// pass a no-op or in-memory provider in tests to assert on spans without
// wiring a real exporter.
//
// If you're using telemetry.Setup, its Option wires the global provider via
// otel.SetTracerProvider before returning, so as long as NewHTTPClient runs
// after telemetry.Setup in your wiring code, the default here already picks
// it up - you only need this option for tests or a non-global provider.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *HTTPClient) {
		if tp != nil {
			c.tracer = tp.Tracer(instrumentationName)
		}
	}
}

// WithMeterProvider sets the MeterProvider used for this client's metrics.
// Defaults to otel.GetMeterProvider() (the global one) if unset. Like
// otelpgx.RecordStats, this reads the provider once at construction time,
// so telemetry.Setup must run - and its WithMetrics option must be passed -
// before NewHTTPClient is called, or every instrument here silently stays
// a no-op.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *HTTPClient) {
		if mp == nil {
			return
		}
		meter := mp.Meter(instrumentationName)

		callDuration, err := meter.Float64Histogram(
			"http.client.call.duration",
			metric.WithDescription("Duration of one HTTPClient.Do call"),
			metric.WithUnit("ms"),
		)
		handleErr(err)
		c.callDuration = callDuration

		callsFailedCounter, err := meter.Int64Counter(
			"http.client.calls.failed",
			metric.WithDescription("Number of HTTPClient.Do calls that returned an error to the caller"),
			metric.WithUnit("{call}"),
		)
		handleErr(err)
		c.callsFailedCounter = callsFailedCounter

		callsInFlight, err := meter.Int64UpDownCounter(
			"http.client.calls.inflight",
			metric.WithDescription("Number of HTTPClient.Do calls currently in progress"),
			metric.WithUnit("{call}"),
		)
		handleErr(err)
		c.callsInFlight = callsInFlight

		responseTooLargeCounter, err := meter.Int64Counter(
			"http.client.response.body_limit_exceeded",
			metric.WithDescription("Number of responses rejected for exceeding the configured max size"),
			metric.WithUnit("{response}"),
		)
		handleErr(err)
		c.responseTooLargeCounter = responseTooLargeCounter
	}
}

// NewHTTPClient builds an HTTPClient. Pass nil to use a default *http.Client
// whose transport is wrapped with otelhttp, giving every outbound request a
// child span plus trace-context propagation for free. If you supply your
// own Doer, wrap its RoundTripper yourself (see WrapTransport) to get the
// same propagation - HTTPClient only controls the transport it creates.
//
// otelhttp reads the propagator from otel.GetTextMapPropagator() at request
// time, so it automatically uses whatever telemetry.Setup installed
// (TraceContext + Baggage) with no extra wiring here - construction order
// only matters for the tracer/meter (see WithTracerProvider/WithMeterProvider),
// not for propagation.
func NewHTTPClient(client Doer, opts ...Option) *HTTPClient {
	if client == nil {
		client = &http.Client{
			Timeout:   defaultHTTPTimeout,
			Transport: WrapTransport(http.DefaultTransport),
		}
	}
	c := &HTTPClient{
		client:          client,
		maxResponseSize: defaultMaxResponseBodySize,
		userAgent:       defaultUserAgent,
		logger:          noopLogger{},
		tracer:          otel.GetTracerProvider().Tracer(instrumentationName),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.callDuration == nil {
		// Same lazy default as the tracer: read the global meter provider
		// if the caller didn't inject one via WithMeterProvider. Checking
		// callDuration (rather than another metric) as the "did the user set
		// this up" sentinel because it's set unconditionally by
		// WithMeterProvider whenever mp is non-nil.
		WithMeterProvider(otel.GetMeterProvider())(c)
	}
	return c
}

// WrapTransport wraps rt with otelhttp so requests sent through it get a
// client span and W3C trace-context propagation headers. Use this when you
// build your own http.Client/Doer to pass into NewHTTPClient, e.g.:
//
//	httpClient := &http.Client{Transport: provider.WrapTransport(myRoundTripper)}
//	client := provider.NewHTTPClient(httpClient)
func WrapTransport(rt http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(rt)
}

// Do executes an HTTP request with JSON body marshaling and a bounded
// response read.
//
// The whole call runs inside one span, so the http.client.call.duration
// metric records the duration of the single HTTP round trip.
func (c *HTTPClient) Do(
	ctx context.Context,
	method string,
	url string,
	headers map[string]string,
	body any,
) (statusCode int, response []byte, err error) {
	payload, err := marshalBody(body)
	if err != nil {
		return 0, nil, err
	}

	ctx, span := c.tracer.Start(ctx, spanName(method, url),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.HTTPRequestMethodKey.String(method),
			semconv.URLFull(url),
		),
	)
	defer span.End()

	methodAttr := metric.WithAttributes(semconv.HTTPRequestMethodKey.String(method))

	c.callsInFlight.Add(ctx, 1, methodAttr)
	defer c.callsInFlight.Add(ctx, -1, methodAttr)

	start := time.Now()
	// Deferred so every exit path records exactly once.
	// The marshal-error return above happens before this defer is registered,
	// so it is correctly excluded: that failure never reached the network
	// and shouldn't count as a failed call or in-flight request.
	defer func() {
		c.callDuration.Record(ctx, float64(time.Since(start).Milliseconds()), methodAttr)
		if err != nil {
			c.callsFailedCounter.Add(ctx, 1, metric.WithAttributes(
				semconv.HTTPRequestMethodKey.String(method),
				semconv.ErrorType(err),
			))
		}
	}()

	statusCode, response, err = c.doOnce(ctx, method, url, headers, payload)
	if err != nil {
		finishSpan(span, statusCode, err)
		return statusCode, response, err
	}

	finishSpan(span, statusCode, nil)
	return statusCode, response, nil
}

func (c *HTTPClient) doOnce(
	ctx context.Context,
	method, url string,
	headers map[string]string,
	payload []byte,
) (int, []byte, error) {
	req, err := buildRequest(ctx, method, url, headers, payload, c.userAgent)
	if err != nil {
		return 0, nil, err
	}

	// req carries ctx, which is a child of the span started in Do. Because
	// the transport is otelhttp-wrapped (see NewHTTPClient/WrapTransport),
	// c.client.Do below creates its own child span for this specific round
	// trip and injects W3C traceparent/tracestate headers automatically -
	// no manual propagation code needed here.
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("execute http request: %w", err)
	}
	defer func() {
		// Drain the body so the underlying connection can be reused by the
		// transport's connection pool, then close it.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		if cerr := resp.Body.Close(); cerr != nil {
			c.logger.Warn("close response body failed", "err", cerr)
		}
	}()

	data, err := readLimited(resp.Body, c.maxResponseSize)
	if err != nil {
		if errors.Is(err, ErrResponseTooLarge) {
			c.responseTooLargeCounter.Add(ctx, 1, metric.WithAttributes(
				semconv.HTTPRequestMethodKey.String(method),
			))
		}
		return resp.StatusCode, nil, fmt.Errorf("read http response: %w", err)
	}

	return resp.StatusCode, data, nil
}

// finishSpan records the final outcome of a Do call on its span. A non-nil
// err or a >=400 status marks the span as an error so it surfaces in
// trace-based alerting/SLOs, matching how HTTP semantic conventions define
// client-span status.
func finishSpan(span trace.Span, statusCode int, err error) {
	if statusCode > 0 {
		span.SetAttributes(semconv.HTTPResponseStatusCode(statusCode))
	}
	switch {
	case err != nil:
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	case statusCode >= 400:
		span.SetStatus(codes.Error, fmt.Sprintf("http %d", statusCode))
	default:
		span.SetStatus(codes.Ok, "")
	}
}

// spanName builds a low-cardinality span name from method + host + path.
// The query string is deliberately dropped: query params often carry
// high-cardinality IDs or secrets (API keys, tokens) that would blow up
// span cardinality and leak sensitive data into trace backends - the same
// reason otelpgx trims SQL text down to the statement shape.
func spanName(method, rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return method
	}
	return method + " " + u.Host + u.Path
}

func marshalBody(body any) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request body: %w", err)
	}
	return payload, nil
}

func buildRequest(
	ctx context.Context,
	method, url string,
	headers map[string]string,
	payload []byte,
	userAgent string,
) (*http.Request, error) {
	var bodyReader io.Reader
	if payload != nil {
		bodyReader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}

	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", userAgent)
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	return req, nil
}

// readLimited reads up to limit+1 bytes so it can distinguish "body exactly
// at the limit" from "body was truncated", returning ErrResponseTooLarge in
// the latter case instead of silently handing back a partial payload.
func readLimited(r io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, ErrResponseTooLarge
	}
	return data, nil
}
