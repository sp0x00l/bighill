package data

import (
	domainErrors "data_stream_service/pkg/domain"
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

type sourceQueryCommand struct {
	UserID            string `json:"userId"`
	SourceType        string `json:"sourceType"`
	SourceConnectorID string `json:"sourceConnectorId"`
	SQL               string `json:"sql"`
	Database          string `json:"database"`
	Collection        string `json:"collection"`
	Limit             int64  `json:"limit"`
}

func parseSourceQueryCommand(command string) (*sourceQueryCommand, error) {
	log.Trace("parseSourceQueryCommand")

	command = strings.TrimSpace(command)
	if command == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("query command is required")
	}

	var query sourceQueryCommand
	if err := json.Unmarshal([]byte(command), &query); err != nil {
		return nil, domainErrors.ErrValidationFailed.Extend(fmt.Sprintf("registry query command must be JSON: %v", err))
	}

	query.SourceType = strings.ToLower(strings.TrimSpace(query.SourceType))
	query.SourceConnectorID = strings.TrimSpace(query.SourceConnectorID)
	query.UserID = strings.TrimSpace(query.UserID)
	query.SQL = strings.TrimSpace(query.SQL)
	query.Database = strings.TrimSpace(query.Database)
	query.Collection = strings.TrimSpace(query.Collection)

	if query.UserID == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("registry query command requires userId")
	}
	if query.SourceConnectorID == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("registry query command requires sourceConnectorId")
	}
	if query.SourceType == "" {
		query.SourceType = "postgres"
	}
	if query.SourceType == "mongo" {
		if query.SQL == "" && (query.Database == "" || query.Collection == "") {
			return nil, domainErrors.ErrValidationFailed.Extend("mongo registry query command requires database and collection")
		}
		return &query, nil
	}
	if query.SQL == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("registry query command requires sql")
	}

	return &query, nil
}
