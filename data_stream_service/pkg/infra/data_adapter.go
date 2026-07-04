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
	PolarisBaseURL     string
	PolarisCatalog     string
	PolarisWarehouse   string
	PolarisCredential  string
	PolarisToken       string
	PolarisScope       string
	PolarisS3Endpoint  string
	PolarisS3AccessKey string
	PolarisS3SecretKey string
	PolarisS3Region    string
	PolarisS3PathStyle bool
}

type DataConfig struct {
	Server      ServerConnectionConfig
	QueryEngine QueryEngineConfig
}
