// Package config loads and validates application configuration from environment
// variables (e.g. SERVER_PORT=9090), with a local .env file as a development
// convenience and production-safe defaults underneath.
//
// Variable names are unprefixed, sharing a namespace with everything else in
// the environment. "service" is named defensively so it does not collide with
// generic NAME/ENV; "otel" deliberately does the opposite and adopts the
// OpenTelemetry SDK's own spelling (see OTelConfig). Check any new key against
// what an orchestrator might already set (Kubernetes injects <SERVICE>_PORT
// for every Service in the namespace).
//
// .env.example is the single list of available settings, written exactly as
// a deployment sets them - there is deliberately one vocabulary, not a
// config.yaml shadowing the env vars.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config is the root of all application settings, one field per subsystem.
type Config struct {
	Service  ServiceConfig  `mapstructure:"service"`
	Postgres PostgresConfig `mapstructure:"postgres"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
	OTel     OTelConfig     `mapstructure:"otel"`
	Log      LogConfig      `mapstructure:"log"`
}

// ServiceConfig identifies this service in logs and telemetry.
//
// It is a section rather than two root keys so the variables read SERVICE_NAME
// and SERVICE_ENV. Bare NAME and ENV are the two most collision-prone names in
// an unprefixed environment - generic enough that unrelated tooling sets them.
type ServiceConfig struct {
	Name string `mapstructure:"name"`
	Env  string `mapstructure:"env"`
}

// Env values accepted in SERVICE_ENV; they gate log formatting defaults and are
// stamped on every span and log record.
const (
	EnvDevelopment = "development"
	EnvStaging     = "staging"
	EnvProduction  = "production"
)

// PostgresConfig holds the pgx pool settings and startup-migration flag.
type PostgresConfig struct {
	DSN             string        `mapstructure:"dsn"`
	MaxConns        int32         `mapstructure:"max_conns"`
	MinConns        int32         `mapstructure:"min_conns"`
	MaxConnLifetime time.Duration `mapstructure:"max_conn_lifetime"`
	// Migrate runs embedded goose migrations at boot. Defaults to false so the
	// production image is safe without a config file: concurrent replicas all
	// racing to migrate on rollout is not something to opt into by accident.
	// .env.example turns it on for local dev.
	Migrate bool `mapstructure:"migrate"`
	// QueryExecMode selects pgx's statement protocol. Leave "cache_statement"
	// when talking to Postgres directly. Behind a connection pooler in
	// transaction mode (pgbouncer, RDS Proxy, Supabase's 6543 port) server-side
	// prepared statements break, and this must be "simple_protocol" or
	// "exec" - a boilerplate-level footgun worth a knob.
	QueryExecMode string `mapstructure:"query_exec_mode"`
}

// LogValue redacts the DSN's password so logging a Config (or a
// PostgresConfig) can never leak credentials.
func (p PostgresConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("dsn", RedactDSN(p.DSN)),
		slog.Int64("max_conns", int64(p.MaxConns)),
		slog.Int64("min_conns", int64(p.MinConns)),
		slog.Duration("max_conn_lifetime", p.MaxConnLifetime),
		slog.Bool("migrate", p.Migrate),
		slog.String("query_exec_mode", p.QueryExecMode),
	)
}

// keywordPassword matches the password field of a libpq keyword/value DSN
// ("host=db password=secret sslmode=require"), which url.Parse cannot redact.
var keywordPassword = regexp.MustCompile(`(?i)\bpassword\s*=\s*('(?:[^']|'')*'|\S+)`)

// RedactDSN replaces the password in a DSN with "xxxxx", handling both accepted
// pgx forms: URL ("postgres://user:pw@host/db") and libpq keyword/value
// ("host=... password=..."). Unparseable input is reported as redacted rather
// than echoed, so a malformed DSN carrying a secret still never reaches the logs.
func RedactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	// Keyword/value form has no scheme; url.Parse would return it untouched
	// and leak the password, so handle it first.
	if !strings.Contains(dsn, "://") {
		return keywordPassword.ReplaceAllString(dsn, "password=xxxxx")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "[unparseable dsn redacted]"
	}
	return u.Redacted()
}

// RedisMode controls how the app treats its Redis dependency.
type RedisMode string

const (
	// RedisModeDisabled skips Redis entirely: no connection, no caching.
	RedisModeDisabled RedisMode = "disabled"
	// RedisModeOptional degrades gracefully: an unreachable Redis logs a
	// warning and the app runs uncached.
	RedisModeOptional RedisMode = "optional"
	// RedisModeRequired makes an unreachable Redis fatal at startup (fail-fast).
	RedisModeRequired RedisMode = "required"
)

// RedisConfig holds the Redis connection settings; Mode decides how hard the
// app depends on it (see RedisMode).
type RedisConfig struct {
	Mode     RedisMode `mapstructure:"mode"`
	Addr     string    `mapstructure:"addr"`
	Password string    `mapstructure:"password"`
	DB       int       `mapstructure:"db"`
}

// LogValue keeps the Redis password out of logs.
func (r RedisConfig) LogValue() slog.Value {
	pw := ""
	if r.Password != "" {
		pw = "xxxxx"
	}
	return slog.GroupValue(
		slog.String("mode", string(r.Mode)),
		slog.String("addr", r.Addr),
		slog.String("password", pw),
		slog.Int("db", r.DB),
	)
}

// CacheConfig tunes the object cache built on Redis.
type CacheConfig struct {
	DefaultTTL time.Duration `mapstructure:"default_ttl"`
}

type KafkaConfig struct {
	Brokers     []string            `mapstructure:"brokers"`
	ClientID    string              `mapstructure:"client_id"`
	PingTimeout time.Duration       `mapstructure:"ping_timeout"`
	Producer    KafkaProducerConfig `mapstructure:"producer"`
	Consumer    KafkaConsumerConfig `mapstructure:"consumer"`
}

type KafkaProducerConfig struct {
	RecordRetries         int64         `mapstructure:"record_retries"`
	RecordDeliveryTimeout time.Duration `mapstructure:"record_delivery_timeout"`
}

type KafkaConsumerConfig struct {
	Group    string      `mapstructure:"group"`
	Topic    string      `mapstructure:"topic"`
	DLQTopic string      `mapstructure:"dlq_topic"`
	Retry    RetryConfig `mapstructure:"retry"`
}

type RetryConfig struct {
	MaxAttempts  int           `mapstructure:"max_attempts"`
	InitialDelay time.Duration `mapstructure:"initial_delay"`
	MaxDelay     time.Duration `mapstructure:"max_delay"`
}

// OTelConfig controls OTLP export of traces and metrics.
//
// The keys spell out to the OpenTelemetry SDK's own variable names on purpose.
// The SDK reads those names from the real environment itself, so matching its
// spelling and meaning is what keeps an operator's OTEL_EXPORTER_OTLP_ENDPOINT
// from being silently ignored. Two deliberate deviations:
//   - OTEL_SERVICE_NAME is not read; SERVICE_NAME owns the notification because the
//     logger uses it too.
//   - OTEL_METRIC_EXPORT_INTERVAL is milliseconds, per spec, unlike every other
//     interval here.
//
// Unregistered OTEL_* still reach the exporter from the real environment
// (_HEADERS, _TIMEOUT, _COMPRESSION) - but not from .env, which is parsed into
// Viper rather than exported into the process.
type OTelConfig struct {
	// SDKDisabled turns off both signals; one signal is disabled with
	// TracesExporter/MetricsExporter = "none".
	SDKDisabled bool `mapstructure:"sdk_disabled"`

	// TracesExporter and MetricsExporter accept "otlp" or "none".
	TracesExporter  string `mapstructure:"traces_exporter"`
	MetricsExporter string `mapstructure:"metrics_exporter"`

	// Endpoint is the collector base URL. Its scheme decides transport
	// security, which is why there is no separate insecure flag.
	Endpoint string `mapstructure:"exporter_otlp_endpoint"`
	// TracesEndpoint and MetricsEndpoint override Endpoint for one signal.
	TracesEndpoint  string `mapstructure:"exporter_otlp_traces_endpoint"`
	MetricsEndpoint string `mapstructure:"exporter_otlp_metrics_endpoint"`

	// TracesSampler is a spec sampler name; see telemetry.NewSampler.
	TracesSampler string `mapstructure:"traces_sampler"`
	// TracesSamplerArg is the ratio for the traceidratio samplers.
	TracesSamplerArg float64 `mapstructure:"traces_sampler_arg"`

	// MetricExportIntervalMillis is the push period, in milliseconds.
	MetricExportIntervalMillis int `mapstructure:"metric_export_interval"`
}

// TracesEnabled reports whether spans should be exported.
func (o OTelConfig) TracesEnabled() bool {
	return !o.SDKDisabled && o.TracesExporter == OTelExporterOTLP
}

// MetricsEnabled reports whether metrics should be exported. There is no scrape
// endpoint, so disabling this means no metrics at all.
func (o OTelConfig) MetricsEnabled() bool {
	return !o.SDKDisabled && o.MetricsExporter == OTelExporterOTLP
}

// TraceEndpoint resolves the per-signal override against the base endpoint.
func (o OTelConfig) TraceEndpoint() string {
	if o.TracesEndpoint != "" {
		return o.TracesEndpoint
	}
	return o.Endpoint
}

// MetricEndpoint resolves the per-signal override against the base endpoint.
func (o OTelConfig) MetricEndpoint() string {
	if o.MetricsEndpoint != "" {
		return o.MetricsEndpoint
	}
	return o.Endpoint
}

// MetricExportInterval converts the spec's millisecond integer to a Duration.
func (o OTelConfig) MetricExportInterval() time.Duration {
	return time.Duration(o.MetricExportIntervalMillis) * time.Millisecond
}

// Exporter values accepted in OTEL_TRACES_EXPORTER / OTEL_METRICS_EXPORTER.
const (
	// OTelExporterOTLP pushes over OTLP/gRPC.
	OTelExporterOTLP = "otlp"
	// OTelExporterNone disables the signal.
	OTelExporterNone = "none"
)

// OTelSamplers are the OTEL_TRACES_SAMPLER values this app implements. The
// spec's jaeger_remote and xray are rejected by validation rather than ignored:
// a sampler that silently is not the one you asked for is how an incident ends
// up with no trace to look at.
var OTelSamplers = []string{
	"always_on",
	"always_off",
	"traceidratio",
	"parentbased_always_on",
	"parentbased_always_off",
	"parentbased_traceidratio",
}

// LogConfig sets slog level ("debug".."error") and format ("text" or "json").
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// DotEnvFile is the optional local overrides file, loaded from the working
// directory. It is git-ignored; .env.example documents every key.
const DotEnvFile = ".env"

// Load builds the configuration from environment variables (e.g. POSTGRES_DSN),
// falling back to the defaults in setDefaults.
//
// A .env file in the working directory is loaded into the environment first as
// a local development convenience; real environment variables always win, so
// the precedence is environment > .env > defaults. Deployments set variables
// directly and ship no file.
func Load() (*Config, error) {
	dotEnv, err := parseDotEnv(DotEnvFile)
	if err != nil {
		return nil, err
	}

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	setDefaults(v)

	// Layer .env in as defaults rather than by setting process environment
	// variables. Viper resolves AutomaticEnv before defaults, so a real
	// environment variable still wins for free - and Load leaves no global
	// state behind, which keeps it idempotent and safe to call from tests.
	for _, key := range v.AllKeys() {
		if value, ok := dotEnv[envKey(key)]; ok {
			v.SetDefault(key, value)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	cfg.Kafka.Brokers = normalizeKafkaBrokers(cfg.Kafka.Brokers)
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

// validate rejects every value the app would otherwise silently misinterpret.
// A boilerplate is copied far more often than it is read, so an unset or
// fat-fingered override must fail at boot rather than degrade in production.
func (c *Config) validate() error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if c.Service.Name == "" {
		fail("service.name must not be empty")
	}
	switch c.Service.Env {
	case EnvDevelopment, EnvStaging, EnvProduction:
	default:
		fail("service.env must be one of %s|%s|%s, got %q",
			EnvDevelopment, EnvStaging, EnvProduction, c.Service.Env)
	}

	if strings.TrimSpace(c.Postgres.DSN) == "" {
		fail("postgres.dsn must not be empty")
	}
	if c.Postgres.MaxConns < 1 {
		fail("postgres.max_conns must be >= 1, got %d", c.Postgres.MaxConns)
	}
	if c.Postgres.MinConns < 0 {
		fail("postgres.min_conns must not be negative, got %d", c.Postgres.MinConns)
	}
	if c.Postgres.MinConns > c.Postgres.MaxConns {
		fail("postgres.min_conns (%d) must be <= postgres.max_conns (%d)",
			c.Postgres.MinConns, c.Postgres.MaxConns)
	}
	switch c.Postgres.QueryExecMode {
	case "cache_statement", "cache_describe", "describe_exec", "exec", "simple_protocol":
	default:
		fail("postgres.query_exec_mode must be one of cache_statement|cache_describe|describe_exec|exec|simple_protocol, got %q",
			c.Postgres.QueryExecMode)
	}

	if len(c.Kafka.Brokers) == 0 {
		fail("kafka.brokers must not be empty")
	}

	for i, broker := range c.Kafka.Brokers {
		if strings.TrimSpace(broker) == "" {
			fail("kafka.brokers[%d] must not be empty", i)
		}
	}

	if strings.TrimSpace(c.Kafka.ClientID) == "" {
		fail("kafka.client_id must not be empty")
	}

	if c.Kafka.PingTimeout <= 0 {
		fail(
			"kafka.ping_timeout must be > 0, got %s",
			c.Kafka.PingTimeout,
		)
	}

	// Producer.
	if c.Kafka.Producer.RecordRetries < 0 {
		fail(
			"kafka.producer.record_retries must be >= 0, got %d",
			c.Kafka.Producer.RecordRetries,
		)
	}

	if c.Kafka.Producer.RecordDeliveryTimeout <= 0 {
		fail(
			"kafka.producer.record_delivery_timeout must be > 0, got %s",
			c.Kafka.Producer.RecordDeliveryTimeout,
		)
	}

	// Consumer.
	if strings.TrimSpace(c.Kafka.Consumer.Group) == "" {
		fail("kafka.consumer.group must not be empty")
	}

	if strings.TrimSpace(c.Kafka.Consumer.Topic) == "" {
		fail("kafka.consumer.topic must not be empty")
	}

	if strings.TrimSpace(c.Kafka.Consumer.DLQTopic) == "" {
		fail("kafka.consumer.dlq_topic must not be empty")
	}

	if c.Kafka.Consumer.Topic == c.Kafka.Consumer.DLQTopic {
		fail(
			"kafka.consumer.topic and kafka.consumer.dlq_topic must be different",
		)
	}

	retry := c.Kafka.Consumer.Retry

	if retry.MaxAttempts < 1 {
		fail(
			"kafka.consumer.retry.max_attempts must be >= 1, got %d",
			retry.MaxAttempts,
		)
	}

	if retry.InitialDelay <= 0 {
		fail(
			"kafka.consumer.retry.initial_delay must be > 0, got %s",
			retry.InitialDelay,
		)
	}

	if retry.MaxDelay <= 0 {
		fail(
			"kafka.consumer.retry.max_delay must be > 0, got %s",
			retry.MaxDelay,
		)
	}

	if retry.MaxDelay < retry.InitialDelay {
		fail(
			"kafka.consumer.retry.max_delay (%s) must be >= initial_delay (%s)",
			retry.MaxDelay,
			retry.InitialDelay,
		)
	}

	for _, e := range []struct {
		key   string
		value string
	}{
		{"otel.traces_exporter", c.OTel.TracesExporter},
		{"otel.metrics_exporter", c.OTel.MetricsExporter},
	} {
		if e.value != OTelExporterOTLP && e.value != OTelExporterNone {
			fail("%s must be %s or %s, got %q", e.key, OTelExporterOTLP, OTelExporterNone, e.value)
		}
	}
	// Only validated per signal: a broken endpoint must not block boot for a
	// deployment that exports neither.
	for _, e := range []struct {
		key      string
		endpoint string
		enabled  bool
	}{
		{"otel.exporter_otlp_traces_endpoint", c.OTel.TraceEndpoint(), c.OTel.TracesEnabled()},
		{"otel.exporter_otlp_metrics_endpoint", c.OTel.MetricEndpoint(), c.OTel.MetricsEnabled()},
	} {
		if !e.enabled {
			continue
		}
		if err := validateOTLPEndpoint(e.endpoint); err != nil {
			fail("%s: %w", e.key, err)
		}
	}
	if c.OTel.TracesEnabled() {
		if !slices.Contains(OTelSamplers, c.OTel.TracesSampler) {
			fail("otel.traces_sampler must be one of %s, got %q",
				strings.Join(OTelSamplers, "|"), c.OTel.TracesSampler)
		}
		if c.OTel.TracesSamplerArg < 0 || c.OTel.TracesSamplerArg > 1 {
			fail("otel.traces_sampler_arg must be in 0.0..1.0, got %v", c.OTel.TracesSamplerArg)
		}
	}
	if c.OTel.MetricsEnabled() && c.OTel.MetricExportIntervalMillis <= 0 {
		fail("otel.metric_export_interval must be > 0 milliseconds, got %d",
			c.OTel.MetricExportIntervalMillis)
	}

	// Both are matched case-insensitively downstream; validate the same way so
	// a typo fails here instead of silently falling back to info/json.
	switch strings.ToLower(c.Log.Level) {
	case "debug", "info", "warn", "error":
	default:
		fail("log.level must be one of debug|info|warn|error, got %q", c.Log.Level)
	}
	switch strings.ToLower(c.Log.Format) {
	case "text", "json":
	default:
		fail("log.format must be text or json, got %q", c.Log.Format)
	}

	return errors.Join(errs...)
}

// validateOTLPEndpoint enforces the spec's URL form. The scheme selects TLS, so
// a bare "host:4317" - what everyone types first - has no defined transport
// security and is rejected with the fix spelled out.
func validateOTLPEndpoint(endpoint string) error {
	if endpoint == "" {
		return errors.New("must be set while the signal is exported")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("must be a URL, got %q", endpoint)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf(
			"must include an http:// or https:// scheme, which selects transport security; got %q (try %q)",
			endpoint, "http://"+endpoint)
	}
	if u.Host == "" {
		return fmt.Errorf("must include a host, got %q", endpoint)
	}
	return nil
}

// setDefaults registers every key with its production-safe value; Viper's
// AutomaticEnv only resolves keys it already knows, so an unregistered key
// would be invisible even when its variable is set.
func setDefaults(v *viper.Viper) {
	v.SetDefault("service.name", "notification")
	v.SetDefault("service.env", EnvDevelopment)

	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", "10s")
	v.SetDefault("server.write_timeout", "30s")
	v.SetDefault("server.idle_timeout", "60s")
	v.SetDefault("server.shutdown_timeout", "20s")
	v.SetDefault("server.request_timeout", "20s")
	v.SetDefault("server.drain_delay", "5s")
	// Off by default; the port is pre-filled so enabling it needs one variable.
	v.SetDefault("pprof.enabled", false)
	v.SetDefault("pprof.port", 6060)

	v.SetDefault("postgres.dsn", "postgres://app:app@localhost:5433/app?sslmode=disable")
	v.SetDefault("postgres.max_conns", 25)
	v.SetDefault("postgres.min_conns", 2)
	v.SetDefault("postgres.max_conn_lifetime", "1h")
	v.SetDefault("postgres.migrate", false)
	v.SetDefault("postgres.query_exec_mode", "cache_statement")

	v.SetDefault("redis.mode", string(RedisModeOptional))
	v.SetDefault("redis.addr", "localhost:6380")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

	v.SetDefault("cache.default_ttl", "5m")

	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.client_id", "notification")
	v.SetDefault("kafka.ping_timeout", "5s")

	v.SetDefault("kafka.producer.record_retries", int64(5))
	v.SetDefault("kafka.producer.record_delivery_timeout", "30s")

	v.SetDefault("kafka.consumer.retry.max_attempts", 3)
	v.SetDefault("kafka.consumer.retry.initial_delay", "500ms")
	v.SetDefault("kafka.consumer.retry.max_delay", "10s")

	v.SetDefault("otel.sdk_disabled", false)
	v.SetDefault("otel.traces_exporter", OTelExporterOTLP)
	v.SetDefault("otel.metrics_exporter", OTelExporterOTLP)
	v.SetDefault("otel.exporter_otlp_endpoint", "http://localhost:4317")
	// Empty means "inherit the base endpoint"; registered anyway because
	// AutomaticEnv only resolves keys Viper already knows.
	v.SetDefault("otel.exporter_otlp_traces_endpoint", "")
	v.SetDefault("otel.exporter_otlp_metrics_endpoint", "")
	v.SetDefault("otel.traces_sampler", "parentbased_traceidratio")
	v.SetDefault("otel.traces_sampler_arg", 1.0)
	// Milliseconds, per spec - this is the OTel SDK's own default.
	v.SetDefault("otel.metric_export_interval", 60000)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
}

func normalizeKafkaBrokers(brokers []string) []string {
	var result []string

	for _, value := range brokers {
		for broker := range strings.SplitSeq(value, ",") {
			broker = strings.TrimSpace(broker)
			if broker != "" {
				result = append(result, broker)
			}
		}
	}

	return result
}
