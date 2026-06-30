package model

type PostgresDBConnCfg struct {
	Hostname           string
	Port               int
	DatabaseName       string
	Username           string
	Password           string
	SecretResourceUrl  string
	AuthenticationType AuthenticationType
}

func (a *PostgresDBConnCfg) GetStorageType() StorageType {
	return Postgres
}
