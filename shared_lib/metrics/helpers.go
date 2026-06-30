package metrics

import (
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

func MustCounter(meter metric.Meter, name, description, fallbackMeterName string) metric.Int64Counter {
	counter, err := meter.Int64Counter(name, metric.WithDescription(description), metric.WithUnit("1"))
	if err == nil {
		return counter
	}
	if fallbackMeterName == "" {
		fallbackMeterName = "metrics-fallback"
	}
	fallback := metricnoop.NewMeterProvider().Meter(fallbackMeterName)
	counter, _ = fallback.Int64Counter(name, metric.WithDescription(description), metric.WithUnit("1"))
	return counter
}

func MustHistogram(meter metric.Meter, name, description, unit, fallbackMeterName string) metric.Float64Histogram {
	histogram, err := meter.Float64Histogram(name, metric.WithDescription(description), metric.WithUnit(unit))
	if err == nil {
		return histogram
	}
	if fallbackMeterName == "" {
		fallbackMeterName = "metrics-fallback"
	}
	fallback := metricnoop.NewMeterProvider().Meter(fallbackMeterName)
	histogram, _ = fallback.Float64Histogram(name, metric.WithDescription(description), metric.WithUnit(unit))
	return histogram
}

func MustObservableGauge(meter metric.Meter, name, description, unit, fallbackMeterName string) metric.Int64ObservableGauge {
	gauge, err := meter.Int64ObservableGauge(name, metric.WithDescription(description), metric.WithUnit(unit))
	if err == nil {
		return gauge
	}
	if fallbackMeterName == "" {
		fallbackMeterName = "metrics-fallback"
	}
	fallback := metricnoop.NewMeterProvider().Meter(fallbackMeterName)
	gauge, _ = fallback.Int64ObservableGauge(name, metric.WithDescription(description), metric.WithUnit(unit))
	return gauge
}

func MustGauge(meter metric.Meter, name, description, unit, fallbackMeterName string) metric.Int64Gauge {
	gauge, err := meter.Int64Gauge(name, metric.WithDescription(description), metric.WithUnit(unit))
	if err == nil {
		return gauge
	}
	if fallbackMeterName == "" {
		fallbackMeterName = "metrics-fallback"
	}
	fallback := metricnoop.NewMeterProvider().Meter(fallbackMeterName)
	gauge, _ = fallback.Int64Gauge(name, metric.WithDescription(description), metric.WithUnit(unit))
	return gauge
}
