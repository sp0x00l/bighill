package model

type ClickHouseConnCfg struct {
	Hostname           string
	Port               int
	DatabaseName       string
	Username           string
	Password           string
	AuthenticationType AuthenticationType
}

func (a *ClickHouseConnCfg) GetStorageType() StorageType {
	return ClickHouse
}
