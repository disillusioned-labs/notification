package app

import (
	"fmt"
	"log/slog"

	"github.com/disillusioned-labs/notification/internal/config"
	platformgrpc "github.com/disillusioned-labs/platform/grpc"
)

func newIdentityClient(cfg *config.Config, log *slog.Logger) (*platformgrpc.Client, error) {
	opts := []platformgrpc.Option{
		platformgrpc.WithUnaryTimeout(cfg.GRPCClient.Timeout),
		platformgrpc.WithMaxRecvMsgSize(cfg.GRPCClient.MaxRecvMsgSize),
		platformgrpc.WithMaxSendMsgSize(cfg.GRPCClient.MaxSendMsgSize),
		platformgrpc.WithLogger(log),
	}
	if cfg.GRPCClient.TLS.Enabled {
		tlsConfig, err := platformgrpc.NewTLSConfig(
			cfg.GRPCClient.TLS.CAFile,
			cfg.GRPCClient.TLS.CertFile,
			cfg.GRPCClient.TLS.KeyFile,
			cfg.GRPCClient.TLS.ServerName,
			cfg.GRPCClient.TLS.MutualTLS,
		)
		if err != nil {
			return nil, fmt.Errorf("build identity gRPC TLS config: %w", err)
		}
		opts = append(opts, platformgrpc.WithTLS(tlsConfig))
	}
	if cfg.Identity.GRPCServiceToken != "" {
		credentials, err := platformgrpc.NewServiceTokenCredentials(
			cfg.Identity.GRPCServiceToken,
			cfg.GRPCClient.TLS.Enabled,
		)
		if err != nil {
			return nil, fmt.Errorf("configure identity gRPC service credentials: %w", err)
		}
		opts = append(opts, platformgrpc.WithPerRPCCredentials(credentials))
	}
	client, err := platformgrpc.NewClient(cfg.Identity.GRPCTarget, opts...)
	if err != nil {
		return nil, fmt.Errorf("create identity gRPC client: %w", err)
	}
	return client, nil
}
