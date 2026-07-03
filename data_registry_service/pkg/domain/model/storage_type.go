package model

import (
	"errors"
	"strings"

	sharedDomain "lib/shared_lib/domain"
)

type StorageType = sharedDomain.SourceType

const (
	UnknownStorageType = sharedDomain.SourceTypeUnknown
	S3                 = sharedDomain.SourceTypeS3
	AzureStorage       = sharedDomain.SourceTypeAzureStorage
	GoogleCloudStorage = sharedDomain.SourceTypeGCS
	Postgres           = sharedDomain.SourceTypePostgres
	MySQL              = sharedDomain.SourceTypeMySQL
	Oracle             = sharedDomain.SourceTypeOracle
	MongoDB            = sharedDomain.SourceTypeMongoDB
	ClickHouse         = sharedDomain.SourceTypeClickHouse
)

func ToStorageType(s string) (StorageType, error) {
	storageType, err := sharedDomain.ToSourceType(s)
	if err != nil {
		return UnknownStorageType, errors.New("invalid StorageType")
	}
	return storageType, nil
}

type TableFormat int

const (
	Parquet TableFormat = iota
	Iceberg
)

func (s TableFormat) String() string {
	return [...]string{"PARQUET", "ICEBERG"}[s]
}

func ToTableFormat(s string) (TableFormat, error) {
	switch strings.ToUpper(s) {
	case "PARQUET":
		return Parquet, nil
	case "ICEBERG":
		return Iceberg, nil
	default:
		return 0, errors.New("invalid TableFormat")
	}
}

type CtasFormat = TableFormat

func ToCtasFormat(s string) (CtasFormat, error) {
	return ToTableFormat(s)
}

type CatalogProvider int

const (
	LocalCatalog CatalogProvider = iota
	PolarisCatalog
)

func (s CatalogProvider) String() string {
	return [...]string{"LOCAL", "POLARIS"}[s]
}

func ToCatalogProvider(s string) (CatalogProvider, error) {
	switch strings.ToUpper(s) {
	case "LOCAL":
		return LocalCatalog, nil
	case "POLARIS":
		return PolarisCatalog, nil
	default:
		return 0, errors.New("invalid CatalogProvider")
	}
}

type AuthenticationType int

const (
	Anonymous AuthenticationType = iota
	Master
)

func (s AuthenticationType) String() string {
	return [...]string{"ANONYMOUS", "MASTER"}[s]
}

func ToAuthenticationType(s string) (AuthenticationType, error) {
	switch strings.ToUpper(s) {
	case "ANONYMOUS":
		return Anonymous, nil
	case "MASTER":
		return Master, nil
	default:
		return 0, errors.New("invalid AuthenticationType")
	}
}
