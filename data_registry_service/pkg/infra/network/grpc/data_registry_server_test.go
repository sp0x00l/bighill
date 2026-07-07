package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	usecase "data_registry_service/pkg/app"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	dataregistrypb "lib/data_contracts_lib/data_registry"
	"lib/shared_lib/ctxutil"
	transport "lib/shared_lib/transport"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPC(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry gRPC unit test suite")
}

type datasetUsecaseStub struct {
	tableDataset   *model.Dataset
	tableErr       error
	tableDatasetID uuid.UUID
	tableUserID    uuid.UUID
	tableTenantID  uuid.UUID
	tableOrgID     uuid.UUID
	tableSnapshot  string
}

var _ usecase.DatasetUsecase = (*datasetUsecaseStub)(nil)

func (s *datasetUsecaseStub) CreateDataset(context.Context, *model.Dataset, uuid.UUID) error {
	return nil
}

func (s *datasetUsecaseStub) ReadDatasetsForUser(context.Context, uuid.UUID, transport.Pagination, []model.Filter) ([]*model.Dataset, int, error) {
	return nil, 0, nil
}

func (s *datasetUsecaseStub) ReadDatasetForUser(context.Context, uuid.UUID, uuid.UUID) (*model.Dataset, error) {
	return nil, nil
}

func (s *datasetUsecaseStub) DeleteDataset(context.Context, uuid.UUID, uuid.UUID) error {
	return nil
}

func (s *datasetUsecaseStub) PublishDataset(context.Context, uuid.UUID, uuid.UUID) error {
	return nil
}

func (s *datasetUsecaseStub) ReplaceDataset(context.Context, *model.Dataset) (*model.Dataset, error) {
	return nil, nil
}

func (s *datasetUsecaseStub) AdvanceDatasetProcessingState(context.Context, uuid.UUID, uuid.UUID, model.ProcessingState) (*model.Dataset, error) {
	return nil, nil
}

func (s *datasetUsecaseStub) RecordDatasetMaterialization(context.Context, *model.Dataset, model.ProcessingState) (*model.Dataset, error) {
	return nil, nil
}

func (s *datasetUsecaseStub) ReadDatasetTable(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID, snapshotID string) (*model.Dataset, error) {
	s.tableDatasetID = datasetID
	s.tableUserID = userID
	s.tableSnapshot = snapshotID
	if tenantID, ok := ctxutil.TenantID(ctx); ok {
		s.tableTenantID = tenantID
	}
	if orgID, ok := ctxutil.OrgID(ctx); ok {
		s.tableOrgID = orgID
	}
	return s.tableDataset, s.tableErr
}

type sourceUsecaseStub struct {
	connector       *model.SourceConnector
	err             error
	readConnectorID uuid.UUID
	readUserID      uuid.UUID
	readTenantID    uuid.UUID
	readOrgID       uuid.UUID
}

var _ usecase.SourceUsecase = (*sourceUsecaseStub)(nil)

func (s *sourceUsecaseStub) CreateSourceConnector(context.Context, *model.SourceConnector, uuid.UUID) error {
	return nil
}

func (s *sourceUsecaseStub) ReadSourceConnector(ctx context.Context, connectorID, userID uuid.UUID) (*model.SourceConnector, error) {
	s.readConnectorID = connectorID
	s.readUserID = userID
	if tenantID, ok := ctxutil.TenantID(ctx); ok {
		s.readTenantID = tenantID
	}
	if orgID, ok := ctxutil.OrgID(ctx); ok {
		s.readOrgID = orgID
	}
	return s.connector, s.err
}

func (s *sourceUsecaseStub) ReplaceSourceConnector(context.Context, *model.SourceConnector) error {
	return nil
}

func (s *sourceUsecaseStub) DeleteSourceConnector(context.Context, uuid.UUID, uuid.UUID) error {
	return nil
}

var _ = Describe("DataRegistryServer", func() {
	var (
		ctx         context.Context
		datasets    *datasetUsecaseStub
		sources     *sourceUsecaseStub
		server      *DataRegistryServer
		userID      uuid.UUID
		orgID       uuid.UUID
		datasetID   uuid.UUID
		connectorID uuid.UUID
	)

	BeforeEach(func() {
		ctx = context.Background()
		userID = uuid.New()
		orgID = uuid.New()
		datasetID = uuid.New()
		connectorID = uuid.New()
		datasets = &datasetUsecaseStub{}
		sources = &sourceUsecaseStub{}
		server = NewDataRegistryGrpcServer(datasets, sources)
	})

	It("reads source connectors and maps Postgres config to protobuf", func() {
		sources.connector = &model.SourceConnector{
			ID:        connectorID,
			UserID:    userID,
			OrgID:     orgID,
			CatalogID: uuid.New(),
			Config: &model.PostgresDBConnCfg{
				Hostname:           "localhost",
				Port:               5432,
				DatabaseName:       "mlops",
				Username:           "postgres",
				Password:           "password",
				AuthenticationType: model.Master,
			},
		}

		res, err := server.ReadSourceConnector(ctx, &dataregistrypb.ReadSourceConnectorRequest{
			ConnectorId: connectorID.String(),
			UserId:      userID.String(),
			OrgId:       orgID.String(),
			SourceType:  "POSTGRES",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(sources.readConnectorID).To(Equal(connectorID))
		Expect(sources.readUserID).To(Equal(userID))
		Expect(sources.readTenantID).To(Equal(userID))
		Expect(sources.readOrgID).To(Equal(orgID))
		Expect(res.GetConnector().GetId()).To(Equal(connectorID.String()))
		Expect(res.GetConnector().GetOrgId()).To(Equal(orgID.String()))
		Expect(res.GetConnector().GetSourceType()).To(Equal("POSTGRES"))
		Expect(res.GetConnector().GetPostgresConfig().GetDatabaseName()).To(Equal("mlops"))
		Expect(res.GetConnector().GetConfigJson()).NotTo(BeEmpty())
	})

	It("rejects invalid source connector requests", func() {
		_, err := server.ReadSourceConnector(ctx, &dataregistrypb.ReadSourceConnectorRequest{
			ConnectorId: "not-a-uuid",
			UserId:      userID.String(),
			OrgId:       orgID.String(),
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = server.ReadSourceConnector(ctx, &dataregistrypb.ReadSourceConnectorRequest{
			ConnectorId: connectorID.String(),
			UserId:      "",
			OrgId:       orgID.String(),
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = server.ReadSourceConnector(ctx, &dataregistrypb.ReadSourceConnectorRequest{
			ConnectorId: connectorID.String(),
			UserId:      userID.String(),
			OrgId:       "",
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("maps source connector domain errors to gRPC status codes", func() {
		sources.err = domainErrors.ErrResourceNotFound

		_, err := server.ReadSourceConnector(ctx, &dataregistrypb.ReadSourceConnectorRequest{
			ConnectorId: connectorID.String(),
			UserId:      userID.String(),
			OrgId:       orgID.String(),
		})

		Expect(status.Code(err)).To(Equal(codes.NotFound))
	})

	It("rejects requested source type mismatches", func() {
		sources.connector = &model.SourceConnector{
			ID:     connectorID,
			UserID: userID,
			OrgID:  orgID,
			Config: &model.PostgresDBConnCfg{
				Hostname: "localhost",
			},
		}

		_, err := server.ReadSourceConnector(ctx, &dataregistrypb.ReadSourceConnectorRequest{
			ConnectorId: connectorID.String(),
			UserId:      userID.String(),
			OrgId:       orgID.String(),
			SourceType:  "MYSQL",
		})

		Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))
	})

	It("maps empty source connectors to internal errors", func() {
		sources.connector = &model.SourceConnector{ID: connectorID, UserID: userID}

		_, err := server.ReadSourceConnector(ctx, &dataregistrypb.ReadSourceConnectorRequest{
			ConnectorId: connectorID.String(),
			UserId:      userID.String(),
			OrgId:       orgID.String(),
		})

		Expect(status.Code(err)).To(Equal(codes.Internal))
	})

	It("reads dataset table metadata", func() {
		datasets.tableDataset = &model.Dataset{
			ID:              datasetID,
			UserID:          userID,
			OrgID:           orgID,
			DatasetVersion:  3,
			ProcessingState: model.DatasetProcessingFeatureMaterialized,
			Location:        "s3://bucket/features/data.parquet",
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     model.Parquet,
			CatalogProvider: model.LocalCatalog,
			SchemaVersion:   2,
			SchemaMetadata:  `{"columns":["title"]}`,
		}

		res, err := server.ReadDatasetTable(ctx, &dataregistrypb.ReadDatasetTableRequest{
			DatasetId:  datasetID.String(),
			UserId:     userID.String(),
			OrgId:      orgID.String(),
			SnapshotId: "snapshot-1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(datasets.tableDatasetID).To(Equal(datasetID))
		Expect(datasets.tableUserID).To(Equal(userID))
		Expect(datasets.tableTenantID).To(Equal(userID))
		Expect(datasets.tableOrgID).To(Equal(orgID))
		Expect(datasets.tableSnapshot).To(Equal("snapshot-1"))
		Expect(res.GetDatasetId()).To(Equal(datasetID.String()))
		Expect(res.GetOrgId()).To(Equal(orgID.String()))
		Expect(res.GetStorageLocation()).To(Equal("s3://bucket/features/data.parquet"))
		Expect(res.GetTableFormat()).To(Equal("PARQUET"))
		Expect(res.GetSnapshotId()).To(Equal("snapshot-1"))
	})

	It("rejects invalid dataset table requests", func() {
		_, err := server.ReadDatasetTable(ctx, &dataregistrypb.ReadDatasetTableRequest{
			DatasetId: "not-a-uuid",
			UserId:    userID.String(),
			OrgId:     orgID.String(),
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = server.ReadDatasetTable(ctx, &dataregistrypb.ReadDatasetTableRequest{
			DatasetId: datasetID.String(),
			UserId:    "",
			OrgId:     orgID.String(),
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = server.ReadDatasetTable(ctx, &dataregistrypb.ReadDatasetTableRequest{
			DatasetId: datasetID.String(),
			UserId:    userID.String(),
			OrgId:     "",
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("maps dataset table domain errors to gRPC status codes", func() {
		datasets.tableErr = domainErrors.ErrValidationFailed.Extend("dataset table is not materialized")

		_, err := server.ReadDatasetTable(ctx, &dataregistrypb.ReadDatasetTableRequest{
			DatasetId: datasetID.String(),
			UserId:    userID.String(),
			OrgId:     orgID.String(),
		})
		Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))

		datasets.tableErr = domainErrors.ErrResourceNotFound
		_, err = server.ReadDatasetTable(ctx, &dataregistrypb.ReadDatasetTableRequest{
			DatasetId: datasetID.String(),
			UserId:    userID.String(),
			OrgId:     orgID.String(),
		})
		Expect(status.Code(err)).To(Equal(codes.NotFound))
	})

	It("maps unexpected domain errors to internal status codes", func() {
		sources.err = errors.New("database unavailable")

		_, err := server.ReadSourceConnector(ctx, &dataregistrypb.ReadSourceConnectorRequest{
			ConnectorId: connectorID.String(),
			UserId:      userID.String(),
			OrgId:       orgID.String(),
		})

		Expect(status.Code(err)).To(Equal(codes.Internal))
	})

	It("treats a stopped gRPC server as a clean shutdown", func() {
		lis := bufconn.Listen(1024)
		result := make(chan error, 1)

		go func() {
			result <- server.Serve(lis)
		}()
		time.Sleep(10 * time.Millisecond)
		server.Close()

		Eventually(result).Should(Receive(BeNil()))
	})
})
