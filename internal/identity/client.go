// Package identity holds the gRPC client the notification service uses to
// read data identity owns. It exists so the service depends on a narrow
// consumer-defined interface rather than the full IdentityService contract.
package identity

import (
	"context"
	"fmt"

	identitypb "github.com/disillusioned-labs/platform/contract/identity"
	platformgrpc "github.com/disillusioned-labs/platform/grpc"
)

// DeviceTokenResolver returns the push tokens of a user's active devices.
// The notification service calls it before fanning a push delivery out per
// device; an unreachable identity surfaces as an error so the delivery event
// is retried, never silently dropped.
type DeviceTokenResolver interface {
	GetActiveDeviceTokens(ctx context.Context, userID string) ([]string, error)
}

type GRPCIdentityClient struct {
	client identitypb.IdentityServiceClient
}

func NewGRPCIdentityClient(conn *platformgrpc.Client) *GRPCIdentityClient {
	return &GRPCIdentityClient{
		client: identitypb.NewIdentityServiceClient(conn.Conn()),
	}
}

func (c *GRPCIdentityClient) GetActiveDeviceTokens(ctx context.Context, userID string) ([]string, error) {
	resp, err := c.client.GetDeviceTokens(ctx, &identitypb.GetDeviceTokensRequest{
		UserId: userID,
	})
	if err != nil {
		return nil, fmt.Errorf("identity.GetDeviceTokens: %w", err)
	}

	return resp.GetTokens(), nil
}

// NoopResolver answers "no devices" without calling identity. It backs
// deployments that have not configured IDENTITY_GRPC_TARGET: push targets
// resolve to nothing and are skipped, while email deliveries still flow.
type NoopResolver struct{}

func NewNoopResolver() *NoopResolver { return &NoopResolver{} }

func (NoopResolver) GetActiveDeviceTokens(context.Context, string) ([]string, error) {
	return nil, nil
}
