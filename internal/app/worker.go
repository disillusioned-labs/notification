// Package app owns the application lifecycle: bootstrap infrastructure,
// wire dependencies (see di.go), serve, and shut down gracefully.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/disillusioned-labs/notification/internal/config"
	"github.com/disillusioned-labs/notification/internal/platform/kafka"
	"github.com/disillusioned-labs/notification/internal/platform/postgres"
	"github.com/disillusioned-labs/notification/internal/platform/retry"
	"github.com/disillusioned-labs/notification/internal/platform/telemetry"
	"github.com/disillusioned-labs/notification/internal/provider"
	"github.com/disillusioned-labs/notification/internal/provider/resend"
	"github.com/disillusioned-labs/notification/internal/repository"
	"github.com/disillusioned-labs/notification/internal/service/outbox"
	"github.com/disillusioned-labs/notification/internal/worker"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// otelFlushTimeout bounds the trace flush at exit: if the OTLP collector is
// unreachable, the batch exporter blocks indefinitely and the process never
// exits.
const otelWorkerFlushTimeout = 5 * time.Second

// RunConsumer boots the worker process with the given configuration and blocks
// until the process is told to stop. The caller owns loading and validating cfg.
func RunWorker(cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log := telemetry.NewLogger(
		cfg.Log.Level,
		telemetry.Format(cfg.Log.Format),
		telemetry.Env(cfg.Service.Env),
		telemetry.Service(cfg.Service.Name),
	)
	slog.SetDefault(log)

	log.Info(
		"starting",
		"service", cfg.Service.Name,
		"build", buildInfo(),
		"role", "worker",
	)

	// -------------------------------------------------------------------------
	// Telemetry
	// -------------------------------------------------------------------------
	otelOpts := []telemetry.Option{
		telemetry.WithBuild(version, commit),
	}

	if cfg.OTel.TracesEnabled() {
		sampler, err := telemetry.NewSampler(
			cfg.OTel.TracesSampler,
			cfg.OTel.TracesSamplerArg,
		)
		if err != nil {
			return fmt.Errorf("configure trace sampler: %w", err)
		}

		otelOpts = append(
			otelOpts,
			telemetry.WithTracing(
				cfg.OTel.TraceEndpoint(),
				sampler,
			),
		)
	}

	if cfg.OTel.MetricsEnabled() {
		otelOpts = append(
			otelOpts,
			telemetry.WithMetrics(
				cfg.OTel.MetricEndpoint(),
				cfg.OTel.MetricExportInterval(),
			),
		)
	}

	shutdownOtel, err := telemetry.Setup(
		ctx,
		cfg.Service.Name,
		cfg.Service.Env,
		otelOpts...,
	)
	if err != nil {
		return fmt.Errorf("setup telemetry: %w", err)
	}

	log.Info(
		"telemetry configured",
		"traces", exportTarget(
			cfg.OTel.TracesEnabled(),
			cfg.OTel.TraceEndpoint(),
		),
		"metrics", exportTarget(
			cfg.OTel.MetricsEnabled(),
			cfg.OTel.MetricEndpoint(),
		),
		"metric_export_interval",
		cfg.OTel.MetricExportInterval(),
	)

	defer func() {
		flushCtx, cancel := context.WithTimeout(
			context.Background(),
			otelWorkerFlushTimeout,
		)
		defer cancel()

		if err := shutdownOtel(flushCtx); err != nil {
			log.Error("otel shutdown failed", "error", err)
		}
	}()

	// -------------------------------------------------------------------------
	// PostgreSQL
	// -------------------------------------------------------------------------
	pool, err := postgres.NewPool(
		ctx,
		cfg.Postgres.DSN,
		postgres.MaxConns(cfg.Postgres.MaxConns),
		postgres.MinConns(cfg.Postgres.MinConns),
		postgres.MaxConnLifetime(cfg.Postgres.MaxConnLifetime),
		postgres.QueryExecMode(cfg.Postgres.QueryExecMode),
	)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	log.Info("connected to postgres", "postgres", cfg.Postgres)

	if cfg.Postgres.Migrate {
		if err := postgres.Migrate(ctx, pool, log); err != nil {
			return fmt.Errorf("run migrations: %w", err)
		}
	}
	repo := repository.NewStore(pool)

	// -------------------------------------------------------------------------
	// Retry
	// -------------------------------------------------------------------------
	retryPolicy := retry.RetryPolicy{
		MaxAttempts:  cfg.Kafka.Consumer.Retry.MaxAttempts,
		InitialDelay: cfg.Kafka.Consumer.Retry.InitialDelay,
		MaxDelay:     cfg.Kafka.Consumer.Retry.MaxDelay,
	}

	if err := retryPolicy.Validate(); err != nil {
		return fmt.Errorf(
			"validate consumer retry policy: %w",
			err,
		)
	}

	// -------------------------------------------------------------------------
	// Provider
	// -------------------------------------------------------------------------
	resendConfig := resend.Config{
		APIKey: cfg.Resend.APIKey,
		From:   cfg.Resend.From,
	}
	resendProvider, err := resend.NewResendProvider(resendConfig)
	if err != nil {
		return fmt.Errorf(
			"failed create resend provider: %w",
			err,
		)
	}

	providers := provider.NewRegistry()
	err = providers.Register("resend", resendProvider)
	if err != nil {
		return fmt.Errorf(
			"failed register resend provider: %w",
			err,
		)
	}

	// -------------------------------------------------------------------------
	// Kafka
	// -------------------------------------------------------------------------
	kafkaOpts := []kafka.Option{
		kgo.ConsumerGroup(cfg.Kafka.Consumer.Group),
		kgo.ConsumeTopics(cfg.Kafka.Consumer.Topic),
	}

	kafkaClient, err := kafka.New(
		ctx,
		cfg.Kafka,
		kafkaOpts...,
	)
	if err != nil {
		return fmt.Errorf("connect kafka: %w", err)
	}
	defer kafkaClient.Close()

	log.Info(
		"connected to kafka",
		"brokers", cfg.Kafka.Brokers,
		"client_id", cfg.Kafka.ClientID,
		"consumer_group", cfg.Kafka.Consumer.Group,
		"consumer_topic", cfg.Kafka.Consumer.Topic,
		"dlq_topic", cfg.Kafka.Consumer.DLQTopic,
	)

	kafkaProducer := kafka.NewProducer(kafkaClient)

	// -------------------------------------------------------------------------
	// Outbox
	// -------------------------------------------------------------------------
	outboxMetrics := outbox.NewMetrics()
	outboxService := outbox.NewOutboxService(
		repo,
		kafkaProducer,
		log,
		outboxMetrics,
	)

	outboxWorker := worker.NewOutboxWorker(
		cfg.Service.InstanceID,
		outboxService,
		log,
		worker.WithInterval(time.Second),
		worker.WithBatchSize(100),
	)

	// -------------------------------------------------------------------------
	// Run
	// -------------------------------------------------------------------------
	g, runCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return outboxWorker.Run(runCtx)
	})

	<-runCtx.Done()

	signalled := ctx.Err() != nil

	log.Info(
		"shutdown initiated",
		"cause", shutdownCause(signalled),
	)

	stop()

	// Worker.Run observes runCtx cancellation and exits gracefully.
	if err := g.Wait(); err != nil {
		return err
	}

	log.Info("shutdown complete")

	return nil
}
