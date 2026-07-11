package modelserving

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"inference_service/pkg/domain"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type LoadTriggerConfig struct {
	Endpoint       string
	RequestTimeout time.Duration
	HTTPClient     *http.Client
}

type HTTPLoadTrigger struct {
	endpoint string
	client   *http.Client
}

func NewHTTPLoadTrigger(config LoadTriggerConfig) (*HTTPLoadTrigger, error) {
	log.Trace("NewHTTPLoadTrigger")

	if strings.TrimSpace(config.Endpoint) == "" {
		return nil, domain.ErrValidationFailed.Extend("model serving endpoint is required")
	}
	client := config.HTTPClient
	if client == nil {
		if config.RequestTimeout <= 0 {
			return nil, domain.ErrValidationFailed.Extend("model serving request timeout must be greater than zero")
		}
		client = &http.Client{
			Timeout:   config.RequestTimeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		}
	}
	return &HTTPLoadTrigger{
		endpoint: strings.TrimRight(strings.TrimSpace(config.Endpoint), "/"),
		client:   client,
	}, nil
}

func (t *HTTPLoadTrigger) TriggerModelLoad(ctx context.Context, _ uuid.UUID, modelID uuid.UUID) error {
	log.Trace("HTTPLoadTrigger TriggerModelLoad")

	url := fmt.Sprintf("%s/v1/private/served-models/%s/load", t.endpoint, modelID.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("model serving load trigger returned status %d", resp.StatusCode)
	}
	return nil
}
