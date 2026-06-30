package model

type Host struct {
	Hostname string
	Port     int
}

type MongoDBConnCfg struct {
	HostList           []Host
	AuthenticationType AuthenticationType
	Username           string
	Password           string
	AuthDatabase       string
}

func (a *MongoDBConnCfg) GetStorageType() StorageType {
	return MongoDB
}
