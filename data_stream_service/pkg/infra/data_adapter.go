package infra

type ServerConnectionConfig struct {
	Hostname string
	Port     int
}

type QueryEngineConfig struct {
	Mode               string
	DataRoot           string
	BinaryPath         string
	TimeoutSec         int
	RegistryAddress    string
	RegistryDialMs     int
	RegistryCallMs     int
	RegistryRetryCount int
}

type DataConfig struct {
	Server      ServerConnectionConfig
	QueryEngine QueryEngineConfig
}
