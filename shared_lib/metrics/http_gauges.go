package metrics

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ActiveRequests struct {
	count atomic.Int64
}

var (
	activeRequests     *ActiveRequests
	activeRequestsOnce sync.Once
)

func DefaultActiveRequests() *ActiveRequests {
	log.Trace("DefaultActiveRequests")
	activeRequestsOnce.Do(func() {
		activeRequests = &ActiveRequests{}
		registerActiveRequestsGauge("transport", activeRequests)
	})
	return activeRequests
}

func (a *ActiveRequests) Inc() {
	a.count.Add(1)
}

func (a *ActiveRequests) Dec() {
	a.count.Add(-1)
}

func registerActiveRequestsGauge(meterName string, active *ActiveRequests) {
	log.Trace("registerActiveRequestsGauge")
	if meterName == "" {
		meterName = "transport"
	}
	name := fmt.Sprintf("exchange.%s", meterName)
	meter := otel.Meter(name)
	fallbackName := fmt.Sprintf("%s-fallback", name)

	gauge := MustObservableGauge(meter, "http_active_requests", "Active HTTP requests", "1", fallbackName)
	attrs := []attribute.KeyValue{attribute.String("boundary", string(BoundaryHTTPServer))}

	_, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		observer.ObserveInt64(gauge, active.count.Load(), metric.WithAttributes(attrs...))
		return nil
	}, gauge)
	if err == nil {
		log.Info("Registered http active requests gauge")
	}
}
