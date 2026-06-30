package model

import "errors"

type AuthMode int

const (
	Auto AuthMode = iota
	ServiceAccountKeys
)

func (s AuthMode) String() string {
	return [...]string{"AUTO", "SERVICE_ACCOUNT_KEYS"}[s]
}

func ToAuthMode(s string) (AuthMode, error) {
	switch s {
	case "AUTO":
		return Auto, nil
	case "SERVICE_ACCOUNT_KEYS":
		return ServiceAccountKeys, nil
	default:
		return 0, errors.New("invalid AuthMode")
	}
}

type GoogleCloudStorageConnCfg struct {
	ProjectID         string
	AuthMode          AuthMode
	RootPath          string
	PrivateKeyId      string
	PrivateKey        string
	ClientEmail       string
	ClientId          string
	DefaultCtasFormat CtasFormat
}

func (a *GoogleCloudStorageConnCfg) GetStorageType() StorageType {
	return GoogleCloudStorage
}
