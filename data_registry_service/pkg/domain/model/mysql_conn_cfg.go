package model

type MysqlDBConnCfg struct {
	Hostname           string
	Port               int
	DatabaseName       string
	Username           string
	Password           string
	AuthenticationType AuthenticationType
}

func (a *MysqlDBConnCfg) GetStorageType() StorageType {
	return MySQL
}
