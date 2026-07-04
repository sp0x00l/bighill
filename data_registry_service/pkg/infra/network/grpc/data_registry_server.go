package grpc

import (
	"context"
	"data_registry_service/pkg/domain/model"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	usecase "data_registry_service/pkg/app"
	domainErrors "data_registry_service/pkg/domain"
	dataregistrypb "lib/data_contracts_lib/data_registry"
	"lib/shared_lib/ctxutil"
	rpcLib "lib/shared_lib/rpc"

	log "github.com/sirupsen/logrus"
)

type DataRegistryServer struct {
	dataregistrypb.UnimplementedDataRegistryServiceServer
	datasetUsecase usecase.DatasetUsecase
	sourceUsecase  usecase.SourceUsecase
	grpcServer     *grpc.Server
}

func NewDataRegistryGrpcServer(datasetUsecase usecase.DatasetUsecase, sourceUsecase usecase.SourceUsecase) *DataRegistryServer {
	log.Trace("NewDataRegistryGrpcServer")

	if datasetUsecase == nil {
		log.Fatal("NewDataRegistryGrpcServer: datasetUsecase is required")
	}
	if sourceUsecase == nil {
		log.Fatal("NewDataRegistryGrpcServer: sourceUsecase is required")
	}
	return &DataRegistryServer{
		datasetUsecase: datasetUsecase,
		sourceUsecase:  sourceUsecase,
	}
}

func (s *DataRegistryServer) Connect(port int) error {
	log.Trace("DataRegistryServer Connect")

	s.grpcServer = rpcLib.NewServer(
		grpc.ChainUnaryInterceptor(rpcLib.MetricsUnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(rpcLib.MetricsStreamServerInterceptor()),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             60 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    2 * time.Minute,
			Timeout: 20 * time.Second,
		}),
	)
	dataregistrypb.RegisterDataRegistryServiceServer(s.grpcServer, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.WithError(err).WithField("port", port).Error("DataRegistryServer failed to listen")
		return fmt.Errorf("failed to open gRPC port %d: %w", port, err)
	}

	if err := s.grpcServer.Serve(lis); err != nil {
		log.WithError(err).Error("DataRegistryServer failed to serve")
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}
	return nil
}

func (s *DataRegistryServer) Close() {
	log.Trace("DataRegistryServer Close")

	if s.grpcServer == nil {
		return
	}
	s.grpcServer.Stop()
}

func (s *DataRegistryServer) ReadSourceConnector(ctx context.Context, req *dataregistrypb.ReadSourceConnectorRequest) (*dataregistrypb.ReadSourceConnectorResponse, error) {
	log.Trace("DataRegistryServer ReadSourceConnector")

	connectorID, err := uuid.Parse(req.GetConnectorId())
	if err != nil || connectorID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid connector_id")
	}
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil || userID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}
	ctx = ctxutil.WithTenantID(ctx, userID)

	connector, err := s.sourceUsecase.ReadSourceConnector(ctx, connectorID, userID)
	if err != nil {
		return nil, sourceConnectorStatusError(err)
	}
	if req.GetSourceType() != "" && !strings.EqualFold(req.GetSourceType(), connector.Config.GetStorageType().String()) {
		return nil, status.Errorf(codes.FailedPrecondition, "source connector type %s does not match requested type %s", connector.Config.GetStorageType().String(), req.GetSourceType())
	}

	pbConnector, err := sourceConnectorToPB(connector)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &dataregistrypb.ReadSourceConnectorResponse{Connector: pbConnector}, nil
}

func (s *DataRegistryServer) ReadDatasetTable(ctx context.Context, req *dataregistrypb.ReadDatasetTableRequest) (*dataregistrypb.ReadDatasetTableResponse, error) {
	log.Trace("DataRegistryServer ReadDatasetTable")

	datasetID, err := uuid.Parse(req.GetDatasetId())
	if err != nil || datasetID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid dataset_id")
	}
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil || userID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}
	ctx = ctxutil.WithTenantID(ctx, userID)

	dataset, err := s.datasetUsecase.ReadDatasetTable(ctx, datasetID, userID, req.GetSnapshotId())
	if err != nil {
		return nil, datasetTableStatusError(err)
	}

	return &dataregistrypb.ReadDatasetTableResponse{
		DatasetId:       dataset.ID.String(),
		UserId:          dataset.UserID.String(),
		DatasetVersion:  int32(dataset.DatasetVersion),
		ProcessingState: dataset.ProcessingState.String(),
		StorageLocation: dataset.Location,
		TableNamespace:  dataset.TableNamespace,
		TableName:       dataset.TableName,
		TableFormat:     dataset.TableFormat.String(),
		CatalogProvider: dataset.CatalogProvider.String(),
		SchemaVersion:   int32(dataset.SchemaVersion),
		SchemaMetadata:  dataset.SchemaMetadata,
		SnapshotId:      req.GetSnapshotId(),
	}, nil
}

func sourceConnectorToPB(connector *model.SourceConnector) (*dataregistrypb.SourceConnector, error) {
	log.Trace("sourceConnectorToPB")

	if connector == nil || connector.Config == nil {
		return nil, fmt.Errorf("source connector is empty")
	}

	configBytes, err := json.Marshal(connector.Config)
	if err != nil {
		return nil, fmt.Errorf("encode source connector config: %w", err)
	}

	pbConnector := &dataregistrypb.SourceConnector{
		Id:         connector.ID.String(),
		UserId:     connector.UserID.String(),
		CatalogId:  connector.CatalogID.String(),
		SourceType: connector.Config.GetStorageType().String(),
		ConfigJson: configBytes,
	}

	if pgConfig, ok := connector.Config.(*model.PostgresDBConnCfg); ok {
		pbConnector.PostgresConfig = &dataregistrypb.PostgresSourceConfig{
			Hostname:           pgConfig.Hostname,
			Port:               int32(pgConfig.Port),
			DatabaseName:       pgConfig.DatabaseName,
			Username:           pgConfig.Username,
			Password:           pgConfig.Password,
			SecretResourceUrl:  pgConfig.SecretResourceUrl,
			AuthenticationType: pgConfig.AuthenticationType.String(),
		}
	}
	if mysqlConfig, ok := connector.Config.(*model.MysqlDBConnCfg); ok {
		pbConnector.MysqlConfig = &dataregistrypb.MySQLSourceConfig{
			Hostname:           mysqlConfig.Hostname,
			Port:               int32(mysqlConfig.Port),
			DatabaseName:       mysqlConfig.DatabaseName,
			Username:           mysqlConfig.Username,
			Password:           mysqlConfig.Password,
			AuthenticationType: mysqlConfig.AuthenticationType.String(),
		}
	}
	if clickHouseConfig, ok := connector.Config.(*model.ClickHouseConnCfg); ok {
		pbConnector.ClickhouseConfig = &dataregistrypb.ClickHouseSourceConfig{
			Hostname:           clickHouseConfig.Hostname,
			Port:               int32(clickHouseConfig.Port),
			DatabaseName:       clickHouseConfig.DatabaseName,
			Username:           clickHouseConfig.Username,
			Password:           clickHouseConfig.Password,
			AuthenticationType: clickHouseConfig.AuthenticationType.String(),
		}
	}
	if mongoConfig, ok := connector.Config.(*model.MongoDBConnCfg); ok {
		hosts := make([]*dataregistrypb.MongoHost, len(mongoConfig.HostList))
		for i, host := range mongoConfig.HostList {
			hosts[i] = &dataregistrypb.MongoHost{
				Hostname: host.Hostname,
				Port:     int32(host.Port),
			}
		}
		pbConnector.MongoConfig = &dataregistrypb.MongoSourceConfig{
			Hosts:              hosts,
			AuthDatabase:       mongoConfig.AuthDatabase,
			Username:           mongoConfig.Username,
			Password:           mongoConfig.Password,
			AuthenticationType: mongoConfig.AuthenticationType.String(),
		}
	}
	if oracleConfig, ok := connector.Config.(*model.OracleDBConnCfg); ok {
		pbConnector.OracleConfig = &dataregistrypb.OracleSourceConfig{
			Hostname:           oracleConfig.Hostname,
			Port:               int32(oracleConfig.Port),
			Instance:           oracleConfig.Instance,
			Username:           oracleConfig.Username,
			Password:           oracleConfig.Password,
			SecretResourceUrl:  oracleConfig.SecretResourceUrl,
			AuthenticationType: oracleConfig.AuthenticationType.String(),
		}
	}

	return pbConnector, nil
}

func sourceConnectorStatusError(err error) error {
	log.Trace("sourceConnectorStatusError")

	if err == nil {
		return nil
	}
	code := rpcLib.MapToGRPCStatus(
		err,
		rpcLib.GRPCCode(codes.NotFound, domainErrors.ErrResourceNotFound),
		rpcLib.GRPCCode(codes.AlreadyExists, domainErrors.ErrResourceAlreadyExists),
		rpcLib.GRPCCode(codes.InvalidArgument, domainErrors.ErrValidationFailed),
	)
	return status.Error(code, err.Error())
}

func datasetTableStatusError(err error) error {
	log.Trace("datasetTableStatusError")

	if err == nil {
		return nil
	}
	code := rpcLib.MapToGRPCStatus(
		err,
		rpcLib.GRPCCode(codes.NotFound, domainErrors.ErrResourceNotFound),
		rpcLib.GRPCCode(codes.FailedPrecondition, domainErrors.ErrValidationFailed),
	)
	return status.Error(code, err.Error())
}
