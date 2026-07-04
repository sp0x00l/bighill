package data

import (
	streamdomain "data_stream_service/pkg/domain"
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

type lakehouseQueryCommand struct {
	UserID     string `json:"userId"`
	DatasetID  string `json:"datasetId"`
	SQL        string `json:"sql"`
	SnapshotID string `json:"snapshotId"`
}

func parseLakehouseQueryCommand(command string) (*lakehouseQueryCommand, error) {
	log.Trace("parseLakehouseQueryCommand")

	command = strings.TrimSpace(command)
	if command == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("query command is required")
	}

	var query lakehouseQueryCommand
	if err := json.Unmarshal([]byte(command), &query); err != nil {
		return nil, streamdomain.ErrValidationFailed.Extend(fmt.Sprintf("lakehouse query command must be JSON: %v", err))
	}

	query.UserID = strings.TrimSpace(query.UserID)
	query.DatasetID = strings.TrimSpace(query.DatasetID)
	query.SQL = strings.TrimSpace(query.SQL)
	query.SnapshotID = strings.TrimSpace(query.SnapshotID)

	if query.UserID == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("lakehouse query command requires userId")
	}
	if query.DatasetID == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("lakehouse query command requires datasetId")
	}
	if query.SQL == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("lakehouse query command requires sql")
	}

	return &query, nil
}
