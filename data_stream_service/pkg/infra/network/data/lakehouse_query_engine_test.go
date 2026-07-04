package data

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	dataregistrypb "lib/data_contracts_lib/data_registry"
)

type lakehouseRegistryClientStub struct {
	calls      int
	datasetID  uuid.UUID
	userID     uuid.UUID
	snapshotID string
	table      *dataregistrypb.ReadDatasetTableResponse
	err        error
}

func (s *lakehouseRegistryClientStub) ReadSourceConnector(context.Context, uuid.UUID, uuid.UUID, string) (*dataregistrypb.SourceConnector, error) {
	return nil, fmt.Errorf("should not be called")
}

func (s *lakehouseRegistryClientStub) ReadDatasetTable(_ context.Context, datasetID, userID uuid.UUID, snapshotID string) (*dataregistrypb.ReadDatasetTableResponse, error) {
	s.calls++
	s.datasetID = datasetID
	s.userID = userID
	s.snapshotID = snapshotID
	if s.err != nil {
		return nil, s.err
	}
	return s.table, nil
}

func (s *lakehouseRegistryClientStub) Close() error {
	return nil
}

var _ = Describe("lakehouse query engine", func() {
	It("resolves a dataset table through data registry and runs local Parquet through DataFusion", func() {
		tmpDir := GinkgoT().TempDir()
		ipcPath := filepath.Join(tmpDir, "result.arrow")
		argsPath := filepath.Join(tmpDir, "args.txt")
		binaryPath := filepath.Join(tmpDir, "fake-datafusion")
		datasetID := uuid.New()
		userID := uuid.New()

		Expect(os.WriteFile(ipcPath, lakehouseArrowIPC(), 0600)).To(Succeed())
		Expect(os.WriteFile(binaryPath, []byte("#!/usr/bin/env sh\nprintf '%s\\n' \"$@\" > \"$FAKE_DATAFUSION_ARGS\"\ncat \"$FAKE_DATAFUSION_IPC\"\n"), 0700)).To(Succeed())
		Expect(os.Setenv("FAKE_DATAFUSION_IPC", ipcPath)).To(Succeed())
		Expect(os.Setenv("FAKE_DATAFUSION_ARGS", argsPath)).To(Succeed())
		DeferCleanup(os.Unsetenv, "FAKE_DATAFUSION_IPC")
		DeferCleanup(os.Unsetenv, "FAKE_DATAFUSION_ARGS")

		client := &lakehouseRegistryClientStub{table: &dataregistrypb.ReadDatasetTableResponse{
			DatasetId:       datasetID.String(),
			UserId:          userID.String(),
			StorageLocation: tmpDir,
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     "PARQUET",
			CatalogProvider: "LOCAL",
		}}
		engine := NewLakehouseQueryEngineWithClient(client, &dataFusionQueryEngine{
			allocator:  memory.NewGoAllocator(),
			binaryPath: binaryPath,
			timeout:    time.Second,
		}, time.Second)

		result, err := engine.Execute(context.Background(), &flight.Ticket{Ticket: lakehouseCommand(userID, datasetID, "SELECT * FROM dataset LIMIT 2", "")})
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			for _, record := range result.Records {
				record.Release()
			}
		}()

		Expect(result.TotalRecords).To(Equal(int64(2)))
		Expect(client.calls).To(Equal(1))
		Expect(client.datasetID).To(Equal(datasetID))
		Expect(client.userID).To(Equal(userID))

		argsBytes, err := os.ReadFile(argsPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(argsBytes)).To(ContainSubstring("--data-root\n" + tmpDir))
		Expect(string(argsBytes)).To(ContainSubstring("SELECT * FROM dataset LIMIT 2"))
	})

	It("routes Polaris Iceberg tables to the query executable with catalog arguments", func() {
		tmpDir := GinkgoT().TempDir()
		argsPath := filepath.Join(tmpDir, "args.txt")
		binaryPath := filepath.Join(tmpDir, "fake-datafusion")
		datasetID := uuid.New()
		userID := uuid.New()
		Expect(os.WriteFile(binaryPath, []byte("#!/usr/bin/env sh\nprintf '%s\\n' \"$@\" > \"$FAKE_DATAFUSION_ARGS\"\necho 'iceberg unavailable' >&2\nexit 42\n"), 0700)).To(Succeed())
		Expect(os.Setenv("FAKE_DATAFUSION_ARGS", argsPath)).To(Succeed())
		DeferCleanup(os.Unsetenv, "FAKE_DATAFUSION_ARGS")

		client := &lakehouseRegistryClientStub{table: &dataregistrypb.ReadDatasetTableResponse{
			DatasetId:       datasetID.String(),
			UserId:          userID.String(),
			StorageLocation: "s3://warehouse/features/movies",
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     "ICEBERG",
			CatalogProvider: "POLARIS",
		}}
		engine := NewLakehouseQueryEngineWithClient(client, &dataFusionQueryEngine{
			binaryPath: binaryPath,
			timeout:    time.Second,
			polaris: polarisExecutionConfig{
				BaseURL:     "http://polaris:8181",
				Catalog:     "bighill",
				Warehouse:   "s3://warehouse/",
				Credential:  "root:s3cr3t",
				Scope:       "PRINCIPAL_ROLE:ALL",
				S3Endpoint:  "http://polaris-object-store:9000",
				S3AccessKey: "polaris_root",
				S3SecretKey: "polaris_pass",
				S3Region:    "eu-west-1",
				S3PathStyle: true,
			},
		}, time.Second)

		_, err := engine.GetSchema(context.Background(), &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  lakehouseCommand(userID, datasetID, "SELECT * FROM dataset", ""),
		})

		Expect(err).To(MatchError(ContainSubstring("run datafusion iceberg query engine")))
		Expect(err).To(MatchError(ContainSubstring("iceberg unavailable")))
		Expect(client.calls).To(Equal(1))

		argsBytes, err := os.ReadFile(argsPath)
		Expect(err).NotTo(HaveOccurred())
		args := string(argsBytes)
		Expect(args).To(ContainSubstring("--source\niceberg"))
		Expect(args).To(ContainSubstring("--catalog\npolaris"))
		Expect(args).To(ContainSubstring("--catalog-uri\nhttp://polaris:8181"))
		Expect(args).To(ContainSubstring("--catalog-name\nbighill"))
		Expect(args).To(ContainSubstring("--warehouse\ns3://warehouse/"))
		Expect(args).To(ContainSubstring("--catalog-credential\nroot:s3cr3t"))
		Expect(args).To(ContainSubstring("--catalog-scope\nPRINCIPAL_ROLE:ALL"))
		Expect(args).To(ContainSubstring("--s3-endpoint\nhttp://polaris-object-store:9000"))
		Expect(args).To(ContainSubstring("--s3-access-key-id\npolaris_root"))
		Expect(args).To(ContainSubstring("--s3-secret-access-key\npolaris_pass"))
		Expect(args).To(ContainSubstring("--s3-region\neu-west-1"))
		Expect(args).To(ContainSubstring("--s3-path-style\ntrue"))
		Expect(args).To(ContainSubstring("--namespace\nfeatures"))
		Expect(args).To(ContainSubstring("--table\nmovies"))
	})

	It("fails closed for Polaris Iceberg queries without catalog credentials", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		client := &lakehouseRegistryClientStub{table: &dataregistrypb.ReadDatasetTableResponse{
			DatasetId:       datasetID.String(),
			UserId:          userID.String(),
			StorageLocation: "s3://warehouse/features/movies",
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     "ICEBERG",
			CatalogProvider: "POLARIS",
		}}
		engine := NewLakehouseQueryEngineWithClient(client, &dataFusionQueryEngine{
			binaryPath: "/should/not/run",
			timeout:    time.Second,
			polaris: polarisExecutionConfig{
				BaseURL:     "http://polaris:8181",
				Catalog:     "bighill",
				Warehouse:   "s3://warehouse/",
				S3Endpoint:  "http://polaris-object-store:9000",
				S3AccessKey: "polaris_root",
				S3SecretKey: "polaris_pass",
				S3Region:    "eu-west-1",
				S3PathStyle: true,
			},
		}, time.Second)

		_, err := engine.GetSchema(context.Background(), &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  lakehouseCommand(userID, datasetID, "SELECT * FROM dataset", ""),
		})

		Expect(err).To(MatchError(ContainSubstring("polaris credential or token is required")))
		Expect(client.calls).To(Equal(1))
	})

	It("rejects malformed commands before contacting data registry", func() {
		client := &lakehouseRegistryClientStub{err: fmt.Errorf("should not be called")}
		engine := NewLakehouseQueryEngineWithClient(client, &dataFusionQueryEngine{}, time.Second)

		_, err := engine.Execute(context.Background(), &flight.Ticket{Ticket: []byte(`{"userId":"","datasetId":"` + uuid.NewString() + `","sql":"SELECT 1"}`)})

		Expect(err).To(MatchError(ContainSubstring("lakehouse query command requires userId")))
		Expect(client.calls).To(Equal(0))
	})
})

func lakehouseCommand(userID, datasetID uuid.UUID, sql, snapshotID string) []byte {
	payload := map[string]any{
		"userId":    userID.String(),
		"datasetId": datasetID.String(),
		"sql":       sql,
	}
	if snapshotID != "" {
		payload["snapshotId"] = snapshotID
	}
	return []byte(sourceQueryJSON(payload))
}

func lakehouseArrowIPC() []byte {
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
	record := builder.NewRecord()
	defer record.Release()

	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
	Expect(writer.Write(record)).To(Succeed())
	Expect(writer.Close()).To(Succeed())
	return output.Bytes()
}
