package infra

type ServerConnectionConfig struct {
	Hostname          string
	Port              int
	TLSCertPath       string
	TLSKeyPath        string
	TLSClientCAPath   string
	RequireClientCert bool
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
