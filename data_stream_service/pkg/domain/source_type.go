package domain

import sharedDomain "lib/shared_lib/domain"

type SourceType = sharedDomain.SourceType

const (
	SourceTypeUnknown      = sharedDomain.SourceTypeUnknown
	SourceTypeS3           = sharedDomain.SourceTypeS3
	SourceTypeAzureStorage = sharedDomain.SourceTypeAzureStorage
	SourceTypeGCS          = sharedDomain.SourceTypeGCS
	SourceTypePostgres     = sharedDomain.SourceTypePostgres
	SourceTypeMySQL        = sharedDomain.SourceTypeMySQL
	SourceTypeOracle       = sharedDomain.SourceTypeOracle
	SourceTypeMongoDB      = sharedDomain.SourceTypeMongoDB
	SourceTypeClickHouse   = sharedDomain.SourceTypeClickHouse
)

func ToSourceType(value string) (SourceType, error) {
	return sharedDomain.ToSourceType(value)
}
