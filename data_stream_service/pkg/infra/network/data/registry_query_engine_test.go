package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	dataregistrypb "lib/data_contracts_lib/data_registry"
)

type dataRegistryClientStub struct {
	calls      int
	userID     uuid.UUID
	orgID      uuid.UUID
	sourceType string
	connector  *dataregistrypb.SourceConnector
	err        error
}

func (s *dataRegistryClientStub) ReadSourceConnector(_ context.Context, _ uuid.UUID, userID, orgID uuid.UUID, sourceType string) (*dataregistrypb.SourceConnector, error) {
	s.calls++
	s.userID = userID
	s.orgID = orgID
	s.sourceType = sourceType
	if s.err != nil {
		return nil, s.err
	}
	return s.connector, nil
}

func (s *dataRegistryClientStub) ReadDatasetTable(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (*dataregistrypb.ReadDatasetTableResponse, error) {
	return nil, fmt.Errorf("should not be called")
}

func (s *dataRegistryClientStub) Close() error {
	return nil
}

var _ = Describe("registry query engine", func() {
	var (
		userID      uuid.UUID
		orgID       uuid.UUID
		connectorID uuid.UUID
	)

	BeforeEach(func() {
		userID = uuid.New()
		orgID = uuid.New()
		connectorID = uuid.New()
	})

	It("forwards canonical enum source type names to data registry", func() {
		client := &dataRegistryClientStub{connector: &dataregistrypb.SourceConnector{}}
		engine := NewRegistryQueryEngineWithClient(client, time.Second)

		_, err := engine.Execute(context.Background(), &flight.Ticket{Ticket: registryCommand(userID, orgID, connectorID, "postgres", "select 1")})

		Expect(err).To(MatchError(ContainSubstring("source connector does not include postgres config")))
		Expect(client.calls).To(Equal(1))
		Expect(client.userID).To(Equal(userID))
		Expect(client.orgID).To(Equal(orgID))
		Expect(client.sourceType).To(Equal("POSTGRES"))
	})

	It("rejects object-store source types before reading connector credentials", func() {
		for _, sourceType := range []string{"S3", "GCS", "AZURE_STORAGE"} {
			client := &dataRegistryClientStub{err: fmt.Errorf("should not be called")}
			engine := NewRegistryQueryEngineWithClient(client, time.Second)

			_, err := engine.Execute(context.Background(), &flight.Ticket{Ticket: registryCommand(userID, orgID, connectorID, sourceType, "")})

			Expect(err).To(MatchError(ContainSubstring("unsupported source type")))
			Expect(client.calls).To(Equal(0))
		}
	})

	It("routes oracle as a supported SQL source", func() {
		client := &dataRegistryClientStub{connector: &dataregistrypb.SourceConnector{}}
		engine := NewRegistryQueryEngineWithClient(client, time.Second)

		_, err := engine.Execute(context.Background(), &flight.Ticket{Ticket: registryCommand(userID, orgID, connectorID, "oracle", "select 1 from dual")})

		Expect(err).To(MatchError(ContainSubstring("source connector does not include oracle config")))
		Expect(client.calls).To(Equal(1))
		Expect(client.sourceType).To(Equal("ORACLE"))
	})

	It("rejects registry source configs that omit registry-owned connection fields", func() {
		cases := []struct {
			name       string
			sourceType string
			command    map[string]any
			connector  *dataregistrypb.SourceConnector
			wantError  string
		}{
			{
				name:       "postgres hostname",
				sourceType: "postgres",
				command:    registryCommandPayload(userID, orgID, connectorID, "postgres", "select 1"),
				connector: &dataregistrypb.SourceConnector{
					PostgresConfig: &dataregistrypb.PostgresSourceConfig{
						Port:         5432,
						DatabaseName: "pagila",
					},
				},
				wantError: "postgres source hostname is required",
			},
			{
				name:       "postgres port",
				sourceType: "postgres",
				command:    registryCommandPayload(userID, orgID, connectorID, "postgres", "select 1"),
				connector: &dataregistrypb.SourceConnector{
					PostgresConfig: &dataregistrypb.PostgresSourceConfig{
						Hostname:     "127.0.0.1",
						DatabaseName: "pagila",
					},
				},
				wantError: "postgres source port is required",
			},
			{
				name:       "postgres database name",
				sourceType: "postgres",
				command:    registryCommandPayload(userID, orgID, connectorID, "postgres", "select 1"),
				connector: &dataregistrypb.SourceConnector{
					PostgresConfig: &dataregistrypb.PostgresSourceConfig{
						Hostname: "127.0.0.1",
						Port:     5432,
					},
				},
				wantError: "postgres source database name is required",
			},
			{
				name:       "mysql hostname",
				sourceType: "mysql",
				command:    registryCommandPayload(userID, orgID, connectorID, "mysql", "select 1"),
				connector: &dataregistrypb.SourceConnector{
					MysqlConfig: &dataregistrypb.MySQLSourceConfig{
						Port:         3306,
						DatabaseName: "sakila",
					},
				},
				wantError: "mysql source hostname is required",
			},
			{
				name:       "mysql port",
				sourceType: "mysql",
				command:    registryCommandPayload(userID, orgID, connectorID, "mysql", "select 1"),
				connector: &dataregistrypb.SourceConnector{
					MysqlConfig: &dataregistrypb.MySQLSourceConfig{
						Hostname:     "127.0.0.1",
						DatabaseName: "sakila",
					},
				},
				wantError: "mysql source port is required",
			},
			{
				name:       "mysql database name",
				sourceType: "mysql",
				command:    registryCommandPayload(userID, orgID, connectorID, "mysql", "select 1"),
				connector: &dataregistrypb.SourceConnector{
					MysqlConfig: &dataregistrypb.MySQLSourceConfig{
						Hostname: "127.0.0.1",
						Port:     3306,
					},
				},
				wantError: "mysql source database name is required",
			},
			{
				name:       "clickhouse hostname",
				sourceType: "clickhouse",
				command:    registryCommandPayload(userID, orgID, connectorID, "clickhouse", "select 1"),
				connector: &dataregistrypb.SourceConnector{
					ClickhouseConfig: &dataregistrypb.ClickHouseSourceConfig{
						Port:         9000,
						DatabaseName: "mlops",
					},
				},
				wantError: "clickhouse source hostname is required",
			},
			{
				name:       "clickhouse port",
				sourceType: "clickhouse",
				command:    registryCommandPayload(userID, orgID, connectorID, "clickhouse", "select 1"),
				connector: &dataregistrypb.SourceConnector{
					ClickhouseConfig: &dataregistrypb.ClickHouseSourceConfig{
						Hostname:     "127.0.0.1",
						DatabaseName: "mlops",
					},
				},
				wantError: "clickhouse source port is required",
			},
			{
				name:       "clickhouse database name",
				sourceType: "clickhouse",
				command:    registryCommandPayload(userID, orgID, connectorID, "clickhouse", "select 1"),
				connector: &dataregistrypb.SourceConnector{
					ClickhouseConfig: &dataregistrypb.ClickHouseSourceConfig{
						Hostname: "127.0.0.1",
						Port:     9000,
					},
				},
				wantError: "clickhouse source database name is required",
			},
			{
				name:       "oracle hostname",
				sourceType: "oracle",
				command:    registryCommandPayload(userID, orgID, connectorID, "oracle", "select 1 from dual"),
				connector: &dataregistrypb.SourceConnector{
					OracleConfig: &dataregistrypb.OracleSourceConfig{
						Port:     1521,
						Instance: "xe",
					},
				},
				wantError: "oracle source hostname is required",
			},
			{
				name:       "oracle port",
				sourceType: "oracle",
				command:    registryCommandPayload(userID, orgID, connectorID, "oracle", "select 1 from dual"),
				connector: &dataregistrypb.SourceConnector{
					OracleConfig: &dataregistrypb.OracleSourceConfig{
						Hostname: "127.0.0.1",
						Instance: "xe",
					},
				},
				wantError: "oracle source port is required",
			},
			{
				name:       "oracle instance",
				sourceType: "oracle",
				command:    registryCommandPayload(userID, orgID, connectorID, "oracle", "select 1 from dual"),
				connector: &dataregistrypb.SourceConnector{
					OracleConfig: &dataregistrypb.OracleSourceConfig{
						Hostname: "127.0.0.1",
						Port:     1521,
					},
				},
				wantError: "oracle source instance is required",
			},
			{
				name:       "mongo hosts",
				sourceType: "mongo",
				command:    mongoRegistryCommandPayload(userID, orgID, connectorID),
				connector: &dataregistrypb.SourceConnector{
					MongoConfig: &dataregistrypb.MongoSourceConfig{},
				},
				wantError: "mongo source hosts are required",
			},
			{
				name:       "mongo host hostname",
				sourceType: "mongo",
				command:    mongoRegistryCommandPayload(userID, orgID, connectorID),
				connector: &dataregistrypb.SourceConnector{
					MongoConfig: &dataregistrypb.MongoSourceConfig{
						Hosts: []*dataregistrypb.MongoHost{{Port: 27017}},
					},
				},
				wantError: "mongo source host hostname is required",
			},
			{
				name:       "mongo host port",
				sourceType: "mongo",
				command:    mongoRegistryCommandPayload(userID, orgID, connectorID),
				connector: &dataregistrypb.SourceConnector{
					MongoConfig: &dataregistrypb.MongoSourceConfig{
						Hosts: []*dataregistrypb.MongoHost{{Hostname: "127.0.0.1"}},
					},
				},
				wantError: "mongo source host port is required",
			},
		}

		for _, tc := range cases {
			By(tc.name)
			client := &dataRegistryClientStub{connector: tc.connector}
			engine := NewRegistryQueryEngineWithClient(client, time.Second)

			_, err := engine.Execute(context.Background(), &flight.Ticket{Ticket: []byte(sourceQueryJSON(tc.command))})

			Expect(err).To(MatchError(ContainSubstring(tc.wantError)))
			Expect(client.calls).To(Equal(1))
			Expect(client.sourceType).To(Equal(strings.ToUpper(tc.sourceType)))
		}
	})
})

func registryCommand(userID, orgID, connectorID uuid.UUID, sourceType, sql string) []byte {
	payload := registryCommandPayload(userID, orgID, connectorID, sourceType, sql)
	return []byte(sourceQueryJSON(payload))
}

func registryCommandPayload(userID, orgID, connectorID uuid.UUID, sourceType, sql string) map[string]any {
	payload := map[string]any{
		"userId":            userID.String(),
		"orgId":             orgID.String(),
		"sourceType":        sourceType,
		"sourceConnectorId": connectorID.String(),
	}
	if sql != "" {
		payload["sql"] = sql
	}
	return payload
}

func mongoRegistryCommandPayload(userID, orgID, connectorID uuid.UUID) map[string]any {
	payload := registryCommandPayload(userID, orgID, connectorID, "mongo", "")
	payload["database"] = "sample"
	payload["collection"] = "movies"
	return payload
}
