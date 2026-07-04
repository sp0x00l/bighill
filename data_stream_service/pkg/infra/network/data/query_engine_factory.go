package data

import (
	domainErrors "data_stream_service/pkg/domain"
	"data_stream_service/pkg/infra"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

func NewQueryEngine(config infra.QueryEngineConfig) (QueryEngine, error) {
	log.Trace("NewQueryEngine")

	switch strings.ToLower(strings.TrimSpace(config.Mode)) {
	case "", "local":
		return NewLocalQueryEngine(), nil
	case "datafusion":
		return NewDataFusionQueryEngine(config)
	case "lakehouse":
		return NewLakehouseQueryEngine(config)
	case "registry", "datasource":
		return NewRegistryQueryEngine(config)
	default:
		return nil, domainErrors.ErrValidationFailed.Extend(fmt.Sprintf("unsupported query engine mode %q", config.Mode))
	}
}

func queryEngineTimeout(config infra.QueryEngineConfig) time.Duration {
	log.Trace("queryEngineTimeout")

	if config.TimeoutSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(config.TimeoutSec) * time.Second
}
