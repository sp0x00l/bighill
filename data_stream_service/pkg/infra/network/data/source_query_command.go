package data

import (
	streamdomain "data_stream_service/pkg/domain"
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

type sourceQueryCommand struct {
	UserID            string                  `json:"userId"`
	OrgID             string                  `json:"orgId"`
	SourceType        streamdomain.SourceType `json:"sourceType"`
	SourceConnectorID string                  `json:"sourceConnectorId"`
	SQL               string                  `json:"sql"`
	Database          string                  `json:"database"`
	Collection        string                  `json:"collection"`
	Limit             int64                   `json:"limit"`
}

func parseSourceQueryCommand(command string) (*sourceQueryCommand, error) {
	log.Trace("parseSourceQueryCommand")

	command = strings.TrimSpace(command)
	if command == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("query command is required")
	}

	var query sourceQueryCommand
	if err := json.Unmarshal([]byte(command), &query); err != nil {
		return nil, streamdomain.ErrValidationFailed.Extend(fmt.Sprintf("registry query command must be JSON: %v", err))
	}

	query.SourceConnectorID = strings.TrimSpace(query.SourceConnectorID)
	query.UserID = strings.TrimSpace(query.UserID)
	query.OrgID = strings.TrimSpace(query.OrgID)
	query.SQL = strings.TrimSpace(query.SQL)
	query.Database = strings.TrimSpace(query.Database)
	query.Collection = strings.TrimSpace(query.Collection)

	if query.UserID == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("registry query command requires userId")
	}
	if query.OrgID == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("registry query command requires orgId")
	}
	if query.SourceConnectorID == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("registry query command requires sourceConnectorId")
	}
	if query.SourceType == streamdomain.SourceTypeUnknown {
		query.SourceType = streamdomain.SourceTypePostgres
	}
	if !sourceTypeSupportsRegistryQuery(query.SourceType) {
		return nil, streamdomain.ErrValidationFailed.Extend(fmt.Sprintf("unsupported source type %q for registry query engine", query.SourceType.String()))
	}
	if query.SourceType == streamdomain.SourceTypeMongoDB {
		if query.SQL == "" && (query.Database == "" || query.Collection == "") {
			return nil, streamdomain.ErrValidationFailed.Extend("mongo registry query command requires database and collection")
		}
		return &query, nil
	}
	if query.SQL == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("registry query command requires sql")
	}

	return &query, nil
}

func sourceTypeSupportsRegistryQuery(sourceType streamdomain.SourceType) bool {
	switch sourceType {
	case streamdomain.SourceTypePostgres,
		streamdomain.SourceTypeMySQL,
		streamdomain.SourceTypeOracle,
		streamdomain.SourceTypeMongoDB,
		streamdomain.SourceTypeClickHouse:
		return true
	default:
		return false
	}
}
