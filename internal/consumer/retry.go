package consumer

import (
	"context"
	"fmt"
	"time"
)

type RetryPolicy struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

func (p RetryPolicy) Validate() error {
	if p.MaxAttempts <= 0 {
		return fmt.Errorf("max attempts must be greater than zero")
	}

	if p.InitialDelay < 0 {
		return fmt.Errorf("initial delay must not be negative")
	}

	if p.MaxDelay < 0 {
		return fmt.Errorf("max delay must not be negative")
	}

	if p.MaxDelay > 0 && p.InitialDelay > p.MaxDelay {
		return fmt.Errorf(
			"initial delay (%s) must be <= max delay (%s)",
			p.InitialDelay,
			p.MaxDelay,
		)
	}

	return nil
}

// Wait waits before the next retry attempt.
//
// attempt is the attempt that just failed:
//
//	attempt=1 -> InitialDelay
//	attempt=2 -> InitialDelay * 2
//	attempt=3 -> InitialDelay * 4
//
// The delay is capped at MaxDelay.
//
// If InitialDelay is zero, retries happen immediately.
// If MaxDelay is zero, no maximum cap is applied.
func (p RetryPolicy) Wait(
	ctx context.Context,
	attempt int,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if attempt <= 0 {
		return fmt.Errorf("retry attempt must be greater than zero")
	}

	delay := p.delay(attempt)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p RetryPolicy) delay(attempt int) time.Duration {
	if p.InitialDelay <= 0 {
		return 0
	}

	delay := p.InitialDelay

	for i := 1; i < attempt; i++ {
		if p.MaxDelay > 0 && delay >= p.MaxDelay {
			return p.MaxDelay
		}

		if delay > time.Duration(1<<62) {
			if p.MaxDelay > 0 {
				return p.MaxDelay
			}

			return time.Duration(1<<63 - 1)
		}

		delay *= 2

		if p.MaxDelay > 0 && delay >= p.MaxDelay {
			return p.MaxDelay
		}
	}

	return delay
}
