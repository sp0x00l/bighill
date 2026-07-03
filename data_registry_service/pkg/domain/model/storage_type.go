package model

import (
	"errors"
	"strings"
)

type StorageType int

const (
	S3 StorageType = iota
	AzureStorage
	GoogleCloudStorage
	Postgres
	MySQL
	Oracle
	MongoDB
	ClickHouse
)

func (s StorageType) String() string {
	if s < S3 || s > ClickHouse {
		return ""
	}
	return [...]string{"S3", "AZURE_STORAGE", "GCS", "POSTGRES", "MYSQL", "ORACLE", "MONGO", "CLICKHOUSE"}[s]
}

func ToStorageType(s string) (StorageType, error) {

	switch strings.ToUpper(s) {
	case "S3":
		return S3, nil
	case "AZURE_STORAGE":
		return AzureStorage, nil
	case "GCS":
		return GoogleCloudStorage, nil
	case "POSTGRES":
		return Postgres, nil
	case "MYSQL":
		return MySQL, nil
	case "ORACLE":
		return Oracle, nil
	case "MONGO":
		return MongoDB, nil
	case "CLICKHOUSE":
		return ClickHouse, nil
	default:
		return 0, errors.New("invalid StorageType")
	}
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
