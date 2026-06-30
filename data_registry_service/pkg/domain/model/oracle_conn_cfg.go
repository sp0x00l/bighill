package model

type OracleDBConnCfg struct {
	Hostname           string
	Port               int
	Instance           string
	Username           string
	Password           string
	SecretResourceUrl  string
	AuthenticationType AuthenticationType
}

func (a *OracleDBConnCfg) GetStorageType() StorageType {
	return Oracle
}
