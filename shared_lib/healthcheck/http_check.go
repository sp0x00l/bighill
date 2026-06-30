package healthcheck

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

func httpCheck(ctx context.Context, config HealthCheckConfig) error {
	log.Trace("Monitor HTTP Healthcheck")

	if len(config.HttpCheckTargets) == 0 {
		return nil
	}

	timeout := config.HttpCheckTimeoutSec
	if timeout <= 0 {
		timeout = config.ServiceLatencyThresholdSec
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	client := &http.Client{Timeout: timeout}

	for name, url := range config.HttpCheckTargets {
		if url == "" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("%s health request failed: %w", name, err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("%s health request failed: %w", name, err)
		}
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if resp.StatusCode >= http.StatusBadRequest {
			body := strings.TrimSpace(string(bodyBytes))
			if body != "" {
				return fmt.Errorf("%s health status %s: %s", name, resp.Status, body)
			}
			return fmt.Errorf("%s health status %s", name, resp.Status)
		}
	}

	return nil
}
