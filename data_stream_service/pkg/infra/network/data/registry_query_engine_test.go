package data

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	dataregistrypb "lib/data_contracts_lib/data_registry"
)

type dataRegistryClientStub struct {
	calls      int
	sourceType string
	connector  *dataregistrypb.SourceConnector
	err        error
}

func (s *dataRegistryClientStub) ReadSourceConnector(_ context.Context, _ uuid.UUID, _ uuid.UUID, sourceType string) (*dataregistrypb.SourceConnector, error) {
	s.calls++
	s.sourceType = sourceType
	if s.err != nil {
		return nil, s.err
	}
	return s.connector, nil
}

func (s *dataRegistryClientStub) ReadDatasetTable(context.Context, uuid.UUID, uuid.UUID, string) (*dataregistrypb.ReadDatasetTableResponse, error) {
	return nil, fmt.Errorf("should not be called")
}

func (s *dataRegistryClientStub) Close() error {
	return nil
}

var _ = Describe("registry query engine", func() {
	var (
		userID      uuid.UUID
		connectorID uuid.UUID
	)

	BeforeEach(func() {
		userID = uuid.New()
		connectorID = uuid.New()
	})

	It("forwards canonical enum source type names to data registry", func() {
		client := &dataRegistryClientStub{connector: &dataregistrypb.SourceConnector{}}
		engine := NewRegistryQueryEngineWithClient(client, time.Second)

		_, err := engine.Execute(context.Background(), &flight.Ticket{Ticket: registryCommand(userID, connectorID, "postgres", "select 1")})

		Expect(err).To(MatchError(ContainSubstring("source connector does not include postgres config")))
		Expect(client.calls).To(Equal(1))
		Expect(client.sourceType).To(Equal("POSTGRES"))
	})

	It("rejects object-store source types before reading connector credentials", func() {
		for _, sourceType := range []string{"S3", "GCS", "AZURE_STORAGE"} {
			client := &dataRegistryClientStub{err: fmt.Errorf("should not be called")}
			engine := NewRegistryQueryEngineWithClient(client, time.Second)

			_, err := engine.Execute(context.Background(), &flight.Ticket{Ticket: registryCommand(userID, connectorID, sourceType, "")})

			Expect(err).To(MatchError(ContainSubstring("unsupported source type")))
			Expect(client.calls).To(Equal(0))
		}
	})

	It("routes oracle as a supported SQL source", func() {
		client := &dataRegistryClientStub{connector: &dataregistrypb.SourceConnector{}}
		engine := NewRegistryQueryEngineWithClient(client, time.Second)

		_, err := engine.Execute(context.Background(), &flight.Ticket{Ticket: registryCommand(userID, connectorID, "oracle", "select 1 from dual")})

		Expect(err).To(MatchError(ContainSubstring("source connector does not include oracle config")))
		Expect(client.calls).To(Equal(1))
		Expect(client.sourceType).To(Equal("ORACLE"))
	})
})

func registryCommand(userID, connectorID uuid.UUID, sourceType, sql string) []byte {
	payload := map[string]any{
		"userId":            userID.String(),
		"sourceType":        sourceType,
		"sourceConnectorId": connectorID.String(),
	}
	if sql != "" {
		payload["sql"] = sql
	}
	return []byte(sourceQueryJSON(payload))
}
