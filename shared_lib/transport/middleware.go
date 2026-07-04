package transport

import (
	"context"

	"fmt"
	"net/http"
	"strconv"
	"time"

	"encoding/json"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
	metrics "lib/shared_lib/metrics"
)

type handlerFunc func(ctx context.Context, r *http.Request) (int, []byte, error)

type headerProvider interface {
	HTTPHeaders() map[string]string
}

func Middleware(tracer trace.Tracer, spanName string, next handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		metrics.DefaultActiveRequests().Inc()
		defer metrics.DefaultActiveRequests().Dec()
		ctx := r.Context()
		ctx, span := tracer.Start(ctx, spanName)
		defer span.End()
		statusCode, body, err := next(ctx, r)
		statusLabel := strconv.Itoa(statusCode)
		metrics.Default().RecordRequest(ctx, metrics.BoundaryHTTPServer, spanName, statusLabel)
		if statusCode >= http.StatusBadRequest {
			metrics.Default().RecordError(
				ctx,
				metrics.BoundaryHTTPServer,
				spanName,
				metrics.ClassifyHTTPStatus(statusCode),
				statusLabel,
			)
		}
		metrics.Default().RecordDuration(ctx, metrics.BoundaryHTTPServer, spanName, statusLabel, time.Since(start).Seconds())
		w.Header().Set("content-type", "application/json")
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			if provider, ok := err.(headerProvider); ok {
				for key, value := range provider.HTTPHeaders() {
					w.Header().Set(key, value)
				}
			}
		}
		w.WriteHeader(statusCode)
		if err != nil {
			if statusCode >= http.StatusInternalServerError {
				log.WithContext(ctx).WithError(err).Warn("request failed")
			}
			writeRespError(w, fmt.Sprintf("Error: %s", err.Error()))
		} else if _, err := w.Write(body); err != nil {
			log.WithContext(ctx).WithError(err).Error("write response failed")
			writeRespError(w, "Error: Failed to write response")
		}
	}
}

type ErrorMessage struct {
	Message string `json:"message"`
}

func writeRespError(w http.ResponseWriter, errMsg string) {

	errMessage := ErrorMessage{Message: errMsg}
	body, err := json.Marshal(errMessage)
	if err != nil {
		log.WithError(err).Error("Failed to encode response message")
		body = fmt.Appendf(nil, `{"message": "%s"}`, errMsg)
	}
	if _, err := w.Write(body); err != nil {
		log.WithError(err).Error("Failed to write response message")
	}
}
