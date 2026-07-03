package domain

import (
	"encoding/json"
	"fmt"
	"strings"
)

type SourceType int

const (
	SourceTypeUnknown SourceType = iota
	SourceTypeS3
	SourceTypeAzureStorage
	SourceTypeGCS
	SourceTypePostgres
	SourceTypeMySQL
	SourceTypeOracle
	SourceTypeMongoDB
	SourceTypeClickHouse
)

func (s SourceType) String() string {
	if s < SourceTypeS3 || s > SourceTypeClickHouse {
		return ""
	}
	return [...]string{
		"S3",
		"AZURE_STORAGE",
		"GCS",
		"POSTGRES",
		"MYSQL",
		"ORACLE",
		"MONGO",
		"CLICKHOUSE",
	}[s-SourceTypeS3]
}

func ToSourceType(value string) (SourceType, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "":
		return SourceTypeUnknown, nil
	case "S3":
		return SourceTypeS3, nil
	case "AZURE_STORAGE", "AZURE-STORAGE", "AZURE":
		return SourceTypeAzureStorage, nil
	case "GCS", "GOOGLE_CLOUD_STORAGE", "GOOGLE-CLOUD-STORAGE":
		return SourceTypeGCS, nil
	case "POSTGRES", "POSTGRESQL":
		return SourceTypePostgres, nil
	case "MYSQL":
		return SourceTypeMySQL, nil
	case "ORACLE":
		return SourceTypeOracle, nil
	case "MONGO", "MONGODB":
		return SourceTypeMongoDB, nil
	case "CLICKHOUSE":
		return SourceTypeClickHouse, nil
	default:
		return SourceTypeUnknown, fmt.Errorf("invalid source type %q", value)
	}
}

func (s *SourceType) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	sourceType, err := ToSourceType(value)
	if err != nil {
		return err
	}
	*s = sourceType
	return nil
}

func (s SourceType) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}
