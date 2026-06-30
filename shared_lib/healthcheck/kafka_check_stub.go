//go:build !kafka

package healthcheck

import "context"

func messageBrokerCheck(_ context.Context, _ HealthCheckConfig) error {
	return nil
}
