package infra

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInfra(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data stream infra unit test suite")
}

var _ = Describe("DataConfig", func() {
	It("carries server and query engine configuration", func() {
		cfg := DataConfig{
			Server: ServerConnectionConfig{
				Hostname:          "127.0.0.1",
				Port:              8815,
				RequireClientCert: true,
			},
			QueryEngine: QueryEngineConfig{
				Mode:               "flight",
				DataRoot:           "/data",
				RegistryAddress:    "127.0.0.1:50051",
				RegistryDialMs:     1000,
				RegistryCallMs:     2000,
				RegistryRetryCount: 3,
				PolarisS3Region:    "eu-west-1",
				PolarisS3PathStyle: true,
			},
		}

		Expect(cfg.Server.Port).To(Equal(8815))
		Expect(cfg.Server.RequireClientCert).To(BeTrue())
		Expect(cfg.QueryEngine.RegistryRetryCount).To(Equal(3))
		Expect(cfg.QueryEngine.PolarisS3Region).To(Equal("eu-west-1"))
	})
})
