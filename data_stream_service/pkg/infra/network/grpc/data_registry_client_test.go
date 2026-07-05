package grpc

import (
	"context"
	"testing"

	"data_stream_service/pkg/infra"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestGRPC(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data stream gRPC unit test suite")
}

var _ = Describe("DataRegistryClient", func() {
	It("rejects missing registry address", func() {
		_, err := NewDataRegistryClient(context.Background(), infra.QueryEngineConfig{
			RegistryDialMs:     100,
			RegistryCallMs:     100,
			RegistryRetryCount: 1,
		})

		Expect(err).To(MatchError(ContainSubstring("data registry grpc address is required")))
	})

	It("rejects invalid timeout and retry settings", func() {
		_, err := NewDataRegistryClient(context.Background(), infra.QueryEngineConfig{
			RegistryAddress:    "localhost:1",
			RegistryCallMs:     100,
			RegistryRetryCount: 1,
		})
		Expect(err).To(MatchError(ContainSubstring("dial timeout")))

		_, err = NewDataRegistryClient(context.Background(), infra.QueryEngineConfig{
			RegistryAddress:    "localhost:1",
			RegistryDialMs:     100,
			RegistryRetryCount: 1,
		})
		Expect(err).To(MatchError(ContainSubstring("call timeout")))

		_, err = NewDataRegistryClient(context.Background(), infra.QueryEngineConfig{
			RegistryAddress: "localhost:1",
			RegistryDialMs:  100,
			RegistryCallMs:  100,
		})
		Expect(err).To(MatchError(ContainSubstring("retry count")))
	})

	It("closes cleanly when no connection has been established", func() {
		client := &dataRegistryClient{}

		Expect(client.Close()).To(Succeed())
	})
})
