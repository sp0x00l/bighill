package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type BuildTrajectoryDatasetCommand struct {
	OrgID              uuid.UUID
	UserID             uuid.UUID
	AgentLineage       string
	GoldenSplitVersion int
}

type AgentTrajectoryDataset struct {
	DatasetID          uuid.UUID
	OrgID              uuid.UUID
	AgentLineage       string
	GoldenSplitVersion int
	ContentHash        string
	DatasetURI         string
	Format             string
	LabelCount         int
	Manifest           json.RawMessage
	EffectiveBaseID    string
	AgentSpecHash      string
	ToolsetHash        string
	DataSnapshotHash   string
	CreatedByUserID    uuid.UUID
	CreatedAt          time.Time
}
