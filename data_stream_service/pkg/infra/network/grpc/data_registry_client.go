package grpc

import (
	"context"
	"data_stream_service/pkg/infra"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	dataregistrypb "lib/data_contracts_lib/data_registry"
	rpcLib "lib/shared_lib/rpc"

	log "github.com/sirupsen/logrus"
)

type DataRegistryClient interface {
	ReadSourceConnector(ctx context.Context, connectorID, userID uuid.UUID, sourceType string) (*dataregistrypb.SourceConnector, error)
	Close() error
}

type dataRegistryClient struct {
	conn   *grpc.ClientConn
	client dataregistrypb.DataRegistryServiceClient
}

func NewDataRegistryClient(ctx context.Context, config infra.QueryEngineConfig, opts ...grpc.DialOption) (DataRegistryClient, error) {
	log.Trace("dataRegistryClient NewDataRegistryClient")

	address := config.RegistryAddress
	if address == "" {
		return nil, fmt.Errorf("data registry grpc address is required")
	}
	if config.RegistryDialMs <= 0 {
		return nil, fmt.Errorf("data registry grpc dial timeout must be greater than zero")
	}
	if config.RegistryCallMs <= 0 {
		return nil, fmt.Errorf("data registry grpc call timeout must be greater than zero")
	}
	if config.RegistryRetryCount <= 0 {
		return nil, fmt.Errorf("data registry grpc retry count must be greater than zero")
	}

	conn, err := rpcLib.NewClient(ctx, rpcLib.Config{
		Address:          address,
		Insecure:         true,
		DialTimeout:      time.Duration(config.RegistryDialMs) * time.Millisecond,
		PerCallTimeout:   time.Duration(config.RegistryCallMs) * time.Millisecond,
		MaxRetryAttempts: config.RegistryRetryCount,
	}, opts...)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("dataRegistryClient grpc connection instantiation failed")
		return nil, err
	}

	return &dataRegistryClient{
		conn:   conn,
		client: dataregistrypb.NewDataRegistryServiceClient(conn),
	}, nil
}

func (c *dataRegistryClient) Close() error {
	log.Trace("dataRegistryClient Close")

	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *dataRegistryClient) ReadSourceConnector(ctx context.Context, connectorID, userID uuid.UUID, sourceType string) (*dataregistrypb.SourceConnector, error) {
	log.Trace("dataRegistryClient ReadSourceConnector")

	resp, err := c.client.ReadSourceConnector(ctx, &dataregistrypb.ReadSourceConnectorRequest{
		ConnectorId: connectorID.String(),
		UserId:      userID.String(),
		SourceType:  sourceType,
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("dataRegistryClient read source connector failed")
		return nil, fmt.Errorf("read source connector: %w", rpcLib.ExtractGRPCErrMsg(err))
	}
	if resp.GetConnector() == nil {
		return nil, fmt.Errorf("read source connector returned empty connector")
	}
	return resp.GetConnector(), nil
}
