package model

type AwsS3StorageConnCfg struct {
	AccessKey          string
	AccessSecret       string
	AssumedRoleARN     string
	RootPath           string
	Secure             bool
	DefaultCtasFormat  CtasFormat
	WhitelistedBuckets []string
}

func (a *AwsS3StorageConnCfg) GetStorageType() StorageType {
	return S3
}
