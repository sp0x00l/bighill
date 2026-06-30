package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"data_stream_service/pkg/infra"
	"data_stream_service/pkg/infra/network/data"
	dataregistrypb "lib/data_contracts_lib/data_registry_service"
)

func TestDataStreamIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data stream integration test suite")
}

type registryFixture struct {
	dataregistrypb.UnimplementedDataRegistryServiceServer
	connectors map[string]*dataregistrypb.SourceConnector
}

func (f *registryFixture) ReadSourceConnector(_ context.Context, req *dataregistrypb.ReadSourceConnectorRequest) (*dataregistrypb.ReadSourceConnectorResponse, error) {
	connector, ok := f.connectors[req.GetConnectorId()]
	if !ok {
		return nil, grpcstatus.Error(codes.NotFound, "source connector not found")
	}
	if req.GetUserId() != connector.GetUserId() {
		return nil, grpcstatus.Error(codes.NotFound, "source connector not found")
	}
	if req.GetSourceType() != "" && req.GetSourceType() != connector.GetSourceType() {
		return nil, grpcstatus.Error(codes.FailedPrecondition, "source connector type mismatch")
	}
	return &dataregistrypb.ReadSourceConnectorResponse{Connector: connector}, nil
}

var _ = Describe("Data stream datasource integration", Ordered, func() {
	var (
		ctx          context.Context
		cancel       context.CancelFunc
		grpcServer   *grpc.Server
		listener     net.Listener
		queryEngine  data.QueryEngine
		userID       uuid.UUID
		connectorIDs map[string]string
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)
		userID = uuid.New()
		connectorIDs = map[string]string{
			"postgres":   uuid.NewString(),
			"mysql":      uuid.NewString(),
			"clickhouse": uuid.NewString(),
			"mongo":      uuid.NewString(),
		}

		var err error
		listener, err = net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())

		fixture := &registryFixture{connectors: map[string]*dataregistrypb.SourceConnector{
			connectorIDs["postgres"]: {
				Id:         connectorIDs["postgres"],
				UserId:     userID.String(),
				SourceType: "postgres",
				PostgresConfig: &dataregistrypb.PostgresSourceConfig{
					Hostname:           "127.0.0.1",
					Port:               5435,
					DatabaseName:       "pagila",
					Username:           "postgres",
					Password:           "mypassword",
					AuthenticationType: "MASTER",
				},
			},
			connectorIDs["mysql"]: {
				Id:         connectorIDs["mysql"],
				UserId:     userID.String(),
				SourceType: "mysql",
				MysqlConfig: &dataregistrypb.MySQLSourceConfig{
					Hostname:           "127.0.0.1",
					Port:               3306,
					DatabaseName:       "sakila",
					Username:           "user",
					Password:           "password",
					AuthenticationType: "MASTER",
				},
			},
			connectorIDs["clickhouse"]: {
				Id:         connectorIDs["clickhouse"],
				UserId:     userID.String(),
				SourceType: "clickhouse",
				ClickhouseConfig: &dataregistrypb.ClickHouseSourceConfig{
					Hostname:           "127.0.0.1",
					Port:               19000,
					DatabaseName:       "mlops",
					Username:           "user",
					Password:           "password",
					AuthenticationType: "MASTER",
				},
			},
			connectorIDs["mongo"]: {
				Id:         connectorIDs["mongo"],
				UserId:     userID.String(),
				SourceType: "mongo",
				MongoConfig: &dataregistrypb.MongoSourceConfig{
					Hosts:              []*dataregistrypb.MongoHost{{Hostname: "127.0.0.1", Port: 27017}},
					Username:           "root",
					Password:           "example",
					AuthDatabase:       "admin",
					AuthenticationType: "MASTER",
				},
			},
		}}

		grpcServer = grpc.NewServer()
		dataregistrypb.RegisterDataRegistryServiceServer(grpcServer, fixture)
		go func() {
			defer GinkgoRecover()
			if err := grpcServer.Serve(listener); err != nil {
				if !errors.Is(err, grpc.ErrServerStopped) {
					Fail(fmt.Sprintf("registry fixture gRPC server failed: %v", err))
				}
			}
		}()

		queryEngine, err = data.NewQueryEngine(infra.QueryEngineConfig{
			Mode:            "registry",
			RegistryAddress: listener.Addr().String(),
			TimeoutSec:      20,
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if closer, ok := queryEngine.(interface{ Close() error }); ok {
			Expect(closer.Close()).To(Succeed())
		}
		if grpcServer != nil {
			grpcServer.Stop()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("streams Arrow records from Postgres, MySQL, ClickHouse, and Mongo datasources", func() {
		postgres := execute(ctx, queryEngine, command(userID, connectorIDs["postgres"], "postgres", "SELECT actor_id, first_name FROM public.actor ORDER BY actor_id LIMIT 3", "", "", 0))
		Expect(postgres.TotalRecords).To(Equal(int64(3)))
		Expect(postgres.Schema.Field(0).Name).To(Equal("actor_id"))
		Expect(firstValue(postgres, "first_name")).To(Equal("PENELOPE"))

		mysql := execute(ctx, queryEngine, command(userID, connectorIDs["mysql"], "mysql", "SELECT actor_id, first_name FROM actor ORDER BY actor_id LIMIT 3", "", "", 0))
		Expect(mysql.TotalRecords).To(Equal(int64(3)))
		Expect(firstValue(mysql, "first_name")).To(Equal("PENELOPE"))

		clickhouse := execute(ctx, queryEngine, command(userID, connectorIDs["clickhouse"], "clickhouse", "SELECT title, release_year FROM movies WHERE has(genres, 'Silent') ORDER BY release_year, title LIMIT 3", "", "", 0))
		Expect(clickhouse.TotalRecords).To(Equal(int64(3)))
		Expect(firstValue(clickhouse, "title")).To(Equal("Capture of Boer Battery by British"))
		Expect(firstValue(clickhouse, "release_year")).To(BeNumerically("==", 1900))

		mongo := execute(ctx, queryEngine, command(userID, connectorIDs["mongo"], "mongo", "", "sample_db", "movies", 3))
		Expect(mongo.TotalRecords).To(Equal(int64(3)))
		Expect(mongo.Schema.FieldIndices("title")).NotTo(BeEmpty())
		Expect(firstValue(mongo, "title")).NotTo(BeEmpty())
	})

	It("returns validation errors for malformed commands and source errors for bad datasource queries", func() {
		_, err := queryEngine.GetSchema(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte("{")})
		Expect(err).To(MatchError(ContainSubstring("registry query command must be JSON")))

		_, err = queryEngine.GetSchema(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: command(userID, connectorIDs["clickhouse"], "clickhouse", "SELECT missing_column FROM movies LIMIT 1", "", "", 0)})
		Expect(err).To(MatchError(ContainSubstring("query clickhouse source")))

		_, err = queryEngine.GetSchema(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: command(userID, uuid.NewString(), "postgres", "SELECT 1", "", "", 0)})
		Expect(err).To(MatchError(ContainSubstring("read source connector")))
	})
})

func command(userID uuid.UUID, connectorID, sourceType, sql, databaseName, collection string, limit int64) []byte {
	payload := map[string]any{
		"userId":            userID.String(),
		"sourceType":        sourceType,
		"sourceConnectorId": connectorID,
	}
	if sql != "" {
		payload["sql"] = sql
	}
	if databaseName != "" {
		payload["database"] = databaseName
	}
	if collection != "" {
		payload["collection"] = collection
	}
	if limit > 0 {
		payload["limit"] = limit
	}
	bytes, err := json.Marshal(payload)
	Expect(err).NotTo(HaveOccurred())
	return bytes
}

func execute(ctx context.Context, queryEngine data.QueryEngine, command []byte) *data.QueryResult {
	result, err := queryEngine.Execute(ctx, &flight.Ticket{Ticket: command})
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() {
		for _, record := range result.Records {
			record.Release()
		}
	})
	return result
}

func firstValue(result *data.QueryResult, columnName string) any {
	Expect(result.Records).NotTo(BeEmpty())
	record := result.Records[0]
	Expect(record.NumRows()).To(BeNumerically(">", 0))
	indexes := result.Schema.FieldIndices(columnName)
	Expect(indexes).NotTo(BeEmpty())
	column := record.Column(indexes[0])

	switch values := column.(type) {
	case *array.String:
		return values.Value(0)
	case *array.Int16:
		return int64(values.Value(0))
	case *array.Int32:
		return int64(values.Value(0))
	case *array.Int64:
		return values.Value(0)
	default:
		return column.ValueStr(0)
	}
}
