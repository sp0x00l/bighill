package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"data_stream_service/pkg/infra"
	"data_stream_service/pkg/infra/network/data"
	dataregistrypb "lib/data_contracts_lib/data_registry"
)

func TestDataStreamIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data stream integration test suite")
}

type registryFixture struct {
	dataregistrypb.UnimplementedDataRegistryServiceServer
	connectors map[string]*dataregistrypb.SourceConnector
	tables     map[string]*dataregistrypb.ReadDatasetTableResponse
}

func (f *registryFixture) ReadSourceConnector(_ context.Context, req *dataregistrypb.ReadSourceConnectorRequest) (*dataregistrypb.ReadSourceConnectorResponse, error) {
	connector, ok := f.connectors[req.GetConnectorId()]
	if !ok {
		return nil, grpcstatus.Error(codes.NotFound, "source connector not found")
	}
	if req.GetUserId() != connector.GetUserId() {
		return nil, grpcstatus.Error(codes.NotFound, "source connector not found")
	}
	if req.GetOrgId() != connector.GetOrgId() {
		return nil, grpcstatus.Error(codes.NotFound, "source connector not found")
	}
	if req.GetSourceType() != "" && !strings.EqualFold(req.GetSourceType(), connector.GetSourceType()) {
		return nil, grpcstatus.Error(codes.FailedPrecondition, "source connector type mismatch")
	}
	return &dataregistrypb.ReadSourceConnectorResponse{Connector: connector}, nil
}

func (f *registryFixture) ReadDatasetTable(_ context.Context, req *dataregistrypb.ReadDatasetTableRequest) (*dataregistrypb.ReadDatasetTableResponse, error) {
	table, ok := f.tables[req.GetDatasetId()]
	if !ok {
		return nil, grpcstatus.Error(codes.NotFound, "dataset table not found")
	}
	if req.GetUserId() != table.GetUserId() {
		return nil, grpcstatus.Error(codes.NotFound, "dataset table not found")
	}
	if req.GetOrgId() != table.GetOrgId() {
		return nil, grpcstatus.Error(codes.NotFound, "dataset table not found")
	}
	return table, nil
}

var _ = Describe("Data stream datasource integration", Label("external-data-source"), Ordered, func() {
	var (
		ctx          context.Context
		cancel       context.CancelFunc
		grpcServer   *grpc.Server
		listener     net.Listener
		fixture      *registryFixture
		queryEngine  data.QueryEngine
		userID       uuid.UUID
		orgID        uuid.UUID
		connectorIDs map[string]string
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)
		userID = uuid.New()
		orgID = uuid.New()
		connectorIDs = map[string]string{
			"postgres":   uuid.NewString(),
			"mysql":      uuid.NewString(),
			"clickhouse": uuid.NewString(),
			"mongo":      uuid.NewString(),
		}

		var err error
		listener, err = net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())

		fixture = &registryFixture{connectors: map[string]*dataregistrypb.SourceConnector{
			connectorIDs["postgres"]: {
				Id:         connectorIDs["postgres"],
				UserId:     userID.String(),
				OrgId:      orgID.String(),
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
				OrgId:      orgID.String(),
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
				OrgId:      orgID.String(),
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
				OrgId:      orgID.String(),
				SourceType: "mongo",
				MongoConfig: &dataregistrypb.MongoSourceConfig{
					Hosts:              []*dataregistrypb.MongoHost{{Hostname: "127.0.0.1", Port: 27017}},
					Username:           "root",
					Password:           "example",
					AuthDatabase:       "admin",
					AuthenticationType: "MASTER",
				},
			},
		}, tables: map[string]*dataregistrypb.ReadDatasetTableResponse{}}

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
			Mode:               "registry",
			RegistryAddress:    listener.Addr().String(),
			RegistryDialMs:     1000,
			RegistryCallMs:     5000,
			RegistryRetryCount: 1,
			TimeoutSec:         20,
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
		requireExternalDatasourceFixtures()

		postgres := execute(ctx, queryEngine, command(userID, orgID, connectorIDs["postgres"], "postgres", "SELECT actor_id, first_name FROM public.actor ORDER BY actor_id LIMIT 3", "", "", 0))
		Expect(postgres.TotalRecords).To(Equal(int64(3)))
		Expect(postgres.Schema.Field(0).Name).To(Equal("actor_id"))
		Expect(firstValue(postgres, "first_name")).To(Equal("PENELOPE"))

		mysql := execute(ctx, queryEngine, command(userID, orgID, connectorIDs["mysql"], "mysql", "SELECT actor_id, first_name FROM actor ORDER BY actor_id LIMIT 3", "", "", 0))
		Expect(mysql.TotalRecords).To(Equal(int64(3)))
		Expect(firstValue(mysql, "first_name")).To(Equal("PENELOPE"))

		clickhouse := execute(ctx, queryEngine, command(userID, orgID, connectorIDs["clickhouse"], "clickhouse", "SELECT title, release_year FROM movies WHERE has(genres, 'Silent') ORDER BY release_year, title LIMIT 3", "", "", 0))
		Expect(clickhouse.TotalRecords).To(Equal(int64(3)))
		Expect(firstValue(clickhouse, "title")).To(Equal("Capture of Boer Battery by British"))
		Expect(firstValue(clickhouse, "release_year")).To(BeNumerically("==", 1900))

		mongo := execute(ctx, queryEngine, command(userID, orgID, connectorIDs["mongo"], "mongo", "", "sample_db", "movies", 3))
		Expect(mongo.TotalRecords).To(Equal(int64(3)))
		Expect(mongo.Schema.FieldIndices("title")).NotTo(BeEmpty())
		Expect(firstValue(mongo, "title")).NotTo(BeEmpty())
	})

	It("returns validation errors for malformed commands and source errors for bad datasource queries", func() {
		_, err := queryEngine.GetSchema(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte("{")})
		Expect(err).To(MatchError(ContainSubstring("registry query command must be JSON")))

		requireExternalDatasourceFixtures()

		_, err = queryEngine.GetSchema(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: command(userID, orgID, connectorIDs["clickhouse"], "clickhouse", "SELECT missing_column FROM movies LIMIT 1", "", "", 0)})
		Expect(err).To(MatchError(ContainSubstring("query clickhouse source")))

		_, err = queryEngine.GetSchema(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: command(userID, orgID, uuid.NewString(), "postgres", "SELECT 1", "", "", 0)})
		Expect(err).To(MatchError(ContainSubstring("read source connector")))
	})

	It("resolves materialized dataset tables through data registry before running the lakehouse query engine", func() {
		tmpDir := GinkgoT().TempDir()
		ipcPath := filepath.Join(tmpDir, "lakehouse.arrow")
		argsPath := filepath.Join(tmpDir, "lakehouse.args")
		binaryPath := filepath.Join(tmpDir, "fake-datafusion")
		datasetID := uuid.New()

		Expect(os.WriteFile(ipcPath, buildLakehouseIntegrationIPC(), 0600)).To(Succeed())
		Expect(os.WriteFile(binaryPath, []byte("#!/usr/bin/env sh\nprintf '%s\\n' \"$@\" > \"$FAKE_DATAFUSION_ARGS\"\ncat \"$FAKE_DATAFUSION_IPC\"\n"), 0700)).To(Succeed())
		Expect(os.Setenv("FAKE_DATAFUSION_IPC", ipcPath)).To(Succeed())
		Expect(os.Setenv("FAKE_DATAFUSION_ARGS", argsPath)).To(Succeed())
		DeferCleanup(os.Unsetenv, "FAKE_DATAFUSION_IPC")
		DeferCleanup(os.Unsetenv, "FAKE_DATAFUSION_ARGS")

		fixture.tables[datasetID.String()] = &dataregistrypb.ReadDatasetTableResponse{
			DatasetId:       datasetID.String(),
			UserId:          userID.String(),
			OrgId:           orgID.String(),
			DatasetVersion:  7,
			ProcessingState: "FEATURE_MATERIALIZED",
			StorageLocation: tmpDir,
			TableNamespace:  "features",
			TableName:       "movie_features",
			TableFormat:     "PARQUET",
			CatalogProvider: "LOCAL",
			SchemaVersion:   3,
			SchemaMetadata:  `{"columns":["title","year"]}`,
		}

		lakehouseEngine, err := data.NewQueryEngine(infra.QueryEngineConfig{
			Mode:               "lakehouse",
			BinaryPath:         binaryPath,
			RegistryAddress:    listener.Addr().String(),
			RegistryDialMs:     1000,
			RegistryCallMs:     5000,
			RegistryRetryCount: 1,
			TimeoutSec:         20,
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			if closer, ok := lakehouseEngine.(interface{ Close() error }); ok {
				Expect(closer.Close()).To(Succeed())
			}
		})

		result := execute(ctx, lakehouseEngine, lakehouseIntegrationCommand(userID, orgID, datasetID, "SELECT title, year FROM dataset ORDER BY year LIMIT 2"))
		Expect(result.TotalRecords).To(Equal(int64(2)))
		Expect(firstValue(result, "title")).To(Equal("Metropolis"))

		argsBytes, err := os.ReadFile(argsPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(argsBytes)).To(ContainSubstring("--data-root\n" + tmpDir))
		Expect(string(argsBytes)).To(ContainSubstring("SELECT title, year FROM dataset ORDER BY year LIMIT 2"))
	})

	It("rejects truncated lakehouse IPC from the DataFusion subprocess", func() {
		tmpDir := GinkgoT().TempDir()
		ipcPath := filepath.Join(tmpDir, "lakehouse-truncated.arrow")
		binaryPath := filepath.Join(tmpDir, "fake-datafusion")
		datasetID := uuid.New()

		Expect(os.WriteFile(ipcPath, buildLakehouseIntegrationIPCWithoutFooter(), 0600)).To(Succeed())
		Expect(os.WriteFile(binaryPath, []byte("#!/usr/bin/env sh\ncat \"$FAKE_DATAFUSION_IPC\"\n"), 0700)).To(Succeed())
		Expect(os.Setenv("FAKE_DATAFUSION_IPC", ipcPath)).To(Succeed())
		DeferCleanup(os.Unsetenv, "FAKE_DATAFUSION_IPC")

		fixture.tables[datasetID.String()] = &dataregistrypb.ReadDatasetTableResponse{
			DatasetId:       datasetID.String(),
			UserId:          userID.String(),
			OrgId:           orgID.String(),
			DatasetVersion:  8,
			ProcessingState: "FEATURE_MATERIALIZED",
			StorageLocation: tmpDir,
			TableNamespace:  "features",
			TableName:       "bad_features",
			TableFormat:     "PARQUET",
			CatalogProvider: "LOCAL",
			SchemaVersion:   3,
		}

		lakehouseEngine, err := data.NewQueryEngine(infra.QueryEngineConfig{
			Mode:               "lakehouse",
			BinaryPath:         binaryPath,
			RegistryAddress:    listener.Addr().String(),
			RegistryDialMs:     1000,
			RegistryCallMs:     5000,
			RegistryRetryCount: 1,
			TimeoutSec:         20,
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			if closer, ok := lakehouseEngine.(interface{ Close() error }); ok {
				Expect(closer.Close()).To(Succeed())
			}
		})

		_, err = lakehouseEngine.Execute(ctx, &flight.Ticket{Ticket: lakehouseIntegrationCommand(userID, orgID, datasetID, "SELECT title FROM dataset LIMIT 1")})
		Expect(err).To(MatchError(ContainSubstring("read query engine envelope footer")))
	})
})

func requireExternalDatasourceFixtures() {
	fixtures := []struct {
		host string
		port int
	}{
		{host: "127.0.0.1", port: 5435},
		{host: "127.0.0.1", port: 3306},
		{host: "127.0.0.1", port: 19000},
		{host: "127.0.0.1", port: 27017},
	}
	for _, fixture := range fixtures {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(fixture.host, fmt.Sprintf("%d", fixture.port)), 250*time.Millisecond)
		if err != nil {
			Fail("external datasource fixtures are not running; run scripts/start-data-sources.sh before running datasource integration specs")
		}
		_ = conn.Close()
	}
}

func command(userID, orgID uuid.UUID, connectorID, sourceType, sql, databaseName, collection string, limit int64) []byte {
	payload := map[string]any{
		"userId":            userID.String(),
		"orgId":             orgID.String(),
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

func lakehouseIntegrationCommand(userID, orgID, datasetID uuid.UUID, sql string) []byte {
	payload := map[string]any{
		"userId":    userID.String(),
		"orgId":     orgID.String(),
		"datasetId": datasetID.String(),
		"sql":       sql,
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

func buildLakehouseIntegrationIPC() []byte {
	return frameLakehouseIntegrationIPC(buildRawLakehouseIntegrationIPC(), 2)
}

func buildLakehouseIntegrationIPCWithoutFooter() []byte {
	raw := buildRawLakehouseIntegrationIPC()
	var output bytes.Buffer
	output.WriteString("BHIPC001")
	var rowCount [8]byte
	binary.LittleEndian.PutUint64(rowCount[:], 2)
	output.Write(rowCount[:])
	output.Write(raw)
	return output.Bytes()
}

func frameLakehouseIntegrationIPC(raw []byte, rows uint64) []byte {
	var output bytes.Buffer
	output.WriteString("BHIPC001")
	var rowCount [8]byte
	binary.LittleEndian.PutUint64(rowCount[:], rows)
	output.Write(rowCount[:])
	output.Write(raw)
	output.WriteString("BHIPCEND")
	return output.Bytes()
}

func buildRawLakehouseIntegrationIPC() []byte {
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "title", Type: arrow.BinaryTypes.String},
			{Name: "year", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.StringBuilder).AppendValues([]string{"Metropolis", "Solaris"}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{1927, 1972}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
	Expect(writer.Write(record)).To(Succeed())
	Expect(writer.Close()).To(Succeed())
	return output.Bytes()
}
