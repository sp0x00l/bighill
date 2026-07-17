package model

import (
	"errors"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

type OriginType int

const (
	Standard OriginType = iota
	Community
)

func (s OriginType) String() string {
	return [...]string{"standard", "community"}[s]
}

func (s OriginType) DBString() string {
	return [...]string{"STANDARD", "COMMUNITY"}[s]
}

func ToOriginType(s string) (OriginType, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "STANDARD":
		return Standard, nil
	case "COMMUNITY":
		return Community, nil
	default:
		return 0, errors.New("invalid OriginType")
	}
}

type StatusType int

const (
	Draft StatusType = iota
	Published
	Blacklisted
)

func (s StatusType) String() string {
	return [...]string{"draft", "published", "blacklisted"}[s]
}

func (s StatusType) DBString() string {
	return [...]string{"DRAFT", "PUBLISHED", "BLACKLISTED"}[s]
}

func ToStatusType(s string) (StatusType, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DRAFT":
		return Draft, nil
	case "PUBLISHED":
		return Published, nil
	case "BLACKLISTED":
		return Blacklisted, nil
	default:
		return 0, errors.New("invalid StatusType")
	}
}

type Dataset struct {
	ID                       uuid.UUID
	UserID                   uuid.UUID
	OrgID                    uuid.UUID
	Title                    string
	Description              string
	Origin                   OriginType
	Location                 string
	SourceType               StorageType
	SourceConnectorID        uuid.UUID
	SourceQuery              string
	SourceDatabase           string
	SourceCollection         string
	Status                   StatusType
	Category                 string
	TableNamespace           string
	TableName                string
	TableFormat              TableFormat
	CatalogProvider          CatalogProvider
	ProcessingProfile        ProcessingProfile
	SchemaVersion            int
	SchemaMetadata           string
	ProcessingState          ProcessingState
	DatasetVersion           int
	RawSnapshotID            uuid.UUID
	FeatureSnapshotID        uuid.UUID
	EmbeddingSnapshotID      uuid.UUID
	VectorStore              string
	CollectionName           string
	EmbeddingDimensions      int
	EmbeddingCount           int64
	EmbeddingStrategyVersion string
	EmbeddingChunkerName     string
	EmbeddingChunkerVersion  string
	EmbeddingChunkSize       int
	EmbeddingChunkOverlap    int
	EmbeddingProvider        string
	EmbeddingModel           string
	GraphSnapshotID          uuid.UUID
	GraphProvenanceHash      string
	GraphNodeCount           int64
	GraphEdgeCount           int64
}

func NewDataset(ID uuid.UUID) *Dataset {
	return &Dataset{ID: ID}
}

func NormalizeDatasetMetadata(dataset *Dataset) {
	if dataset == nil {
		return
	}
	if dataset.TableNamespace == "" {
		dataset.TableNamespace = "default"
	}
	if dataset.TableName == "" {
		dataset.TableName = defaultTableName(dataset.Title)
	}
	if dataset.SchemaVersion <= 0 {
		dataset.SchemaVersion = 1
	}
	if dataset.SchemaMetadata == "" {
		dataset.SchemaMetadata = "{}"
	}
	if dataset.DatasetVersion <= 0 {
		dataset.DatasetVersion = 1
	}
}

func defaultTableName(title string) string {
	if title != "" {
		name := sanitizeTableIdentifier(title)
		if name != "" {
			return name
		}
	}
	return "dataset"
}

func sanitizeTableIdentifier(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = regexp.MustCompile(`[^a-z0-9_]+`).ReplaceAllString(value, "_")
	value = strings.Trim(value, "_")
	if value == "" {
		return ""
	}
	if value[0] >= '0' && value[0] <= '9' {
		value = "dataset_" + value
	}
	return value
}
