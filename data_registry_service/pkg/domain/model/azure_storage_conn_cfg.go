package model

import (
	"errors"
	"strings"
)

type AzureVersion int

const (
	AzureV1 AzureVersion = iota
	AzureV2
)

func (s AzureVersion) String() string {
	return [...]string{"STORAGE_V1", "STORAGE_V2"}[s]
}

func ToAzureVersion(s string) (AzureVersion, error) {
	switch s {
	case "STORAGE_V1":
		return AzureV1, nil
	case "STORAGE_V2":
		return AzureV2, nil
	default:
		return 0, errors.New("invalid AzureVersion")
	}
}

type CredentialsType int

const (
	AccessKey CredentialsType = iota
	ActiveDirectory
)

func (c CredentialsType) String() string {
	return [...]string{"ACCESS_KEY", "AZURE_ACTIVE_DIRECTORY"}[c]
}

func ToCredentialsType(s string) (CredentialsType, error) {
	switch strings.ToUpper(s) {
	case "ACCESS_KEY":
		return AccessKey, nil
	case "AZURE_ACTIVE_DIRECTORY":
		return ActiveDirectory, nil
	default:
		return 0, errors.New("invalid CredentialsType")
	}
}

type AzureStorageConnCfg struct {
	CredentialsType   CredentialsType
	AccountKind       AzureVersion
	DefaultCtasFormat CtasFormat
	AccountName       string
	AccessKey         string
	ClientSecret      string
	RootPath          string
	ClientID          string
	TokenEndpoint     string
}

func (a *AzureStorageConnCfg) GetStorageType() StorageType {
	return AzureStorage
}
