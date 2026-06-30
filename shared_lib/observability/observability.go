package observability

import (
	"context"

	log "github.com/sirupsen/logrus"
	metrics "lib/shared_lib/metrics"
	"lib/shared_lib/trace"
)

// Init wires tracing and metrics with shared configuration.
func Init(ctx context.Context, serviceName, serviceVersion string) func() {
	log.Trace("Observability Init")

	traceShutdown := trace.Init(ctx, serviceName, serviceVersion)
	metricsShutdown := metrics.Init(ctx, serviceName, serviceVersion)

	return func() {
		metricsShutdown()
		traceShutdown()
	}
}
