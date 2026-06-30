package logs

import (
	"context"
	env "lib/shared_lib/env"
	"os"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/uptrace/opentelemetry-go-extra/otellogrus"
	"go.opentelemetry.io/otel/trace"
)

var (
	openTelemetryHookOnce sync.Once
	traceFieldHookOnce    sync.Once
)

type traceFieldHook struct{}

func (traceFieldHook) Levels() []log.Level {
	return log.AllLevels
}

func (traceFieldHook) Fire(entry *log.Entry) error {
	if entry.Context == nil {
		return nil
	}
	spanContext := trace.SpanContextFromContext(entry.Context)
	if !spanContext.IsValid() {
		return nil
	}
	if _, ok := entry.Data["trace_id"]; !ok {
		entry.Data["trace_id"] = spanContext.TraceID().String()
	}
	if _, ok := entry.Data["span_id"]; !ok {
		entry.Data["span_id"] = spanContext.SpanID().String()
	}
	return nil
}

func WithContext(ctx context.Context) *log.Entry {
	return log.WithContext(ctx)
}

func debugTags() {
	log.SetLevel(log.TraceLevel)
	log.SetOutput(os.Stdout)
}

func releaseTags() {
	log.SetLevel(log.InfoLevel)
	log.SetOutput(os.Stdout)
}

func InstallTraceFieldHook() {
	traceFieldHookOnce.Do(func() {
		log.AddHook(traceFieldHook{})
	})
}

func InstallOpenTelemetryHook() {
	openTelemetryHookOnce.Do(func() {
		log.AddHook(otellogrus.NewHook(otellogrus.WithLevels(
			log.PanicLevel,
			log.FatalLevel,
			log.ErrorLevel,
			log.WarnLevel,
			log.InfoLevel,
		)))
	})
}

func Init() {
	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})
	InstallTraceFieldHook()
	levelOverride := env.WithDefaultString("LOG_LEVEL", "")
	if strings.TrimSpace(levelOverride) != "" {
		if parsed, err := log.ParseLevel(strings.ToLower(levelOverride)); err == nil {
			log.SetLevel(parsed)
			log.SetOutput(os.Stdout)
			if env.IsProduction() {
				InstallOpenTelemetryHook()
			}
			return
		}
	}
	if env.IsLocalDev() {
		debugTags()
	} else {
		releaseTags()
		if env.IsProduction() {
			InstallOpenTelemetryHook()
		}
	}
}
