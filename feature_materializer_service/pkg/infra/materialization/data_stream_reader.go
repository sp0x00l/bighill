package materialization

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"feature_materializer_service/pkg/domain"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	flightCommandUserID            = "userId"
	flightCommandOrgID             = "orgId"
	flightCommandSourceType        = "sourceType"
	flightCommandSourceConnectorID = "sourceConnectorId"
	flightCommandSQL               = "sql"
	flightCommandDatabase          = "database"
	flightCommandCollection        = "collection"
	flightAuthTokenHeader          = "auth-token-bin"
)

const (
	dataStreamSourceFormatKey      = "source_format"
	dataStreamSourceFormatValue    = "data_stream"
	dataStreamSourceTypeKey        = "source_type"
	dataStreamSourceConnectorIDKey = "source_connector_id"
	dataStreamSourceDatabaseKey    = "source_database"
	dataStreamSourceCollectionKey  = "source_collection"
	dataStreamSourceQueryKey       = "source_query"
	dataStreamSourceReadAtKey      = "source_read_at"
)

type DataStreamReadRequest struct {
	UserID            string
	OrgID             string
	SourceType        string
	SourceConnectorID string
	SQL               string
	Database          string
	Collection        string
}

type DataStreamReader interface {
	ReadParquet(ctx context.Context, request DataStreamReadRequest) (*ParquetArtifact, error)
}

type FlightDataStreamReader struct {
	address   string
	timeout   time.Duration
	authToken string
	insecure  bool
	tlsConfig *tls.Config
	allocator memory.Allocator
}

type FlightDataStreamReaderConfig struct {
	Address        string
	Timeout        time.Duration
	AuthToken      string
	Insecure       bool
	ServerName     string
	CACertPath     string
	ClientCertPath string
	ClientKeyPath  string
}

func NewFlightDataStreamReader(address string, timeout time.Duration) *FlightDataStreamReader {
	log.Trace("NewFlightDataStreamReader")

	return NewFlightDataStreamReaderWithConfig(FlightDataStreamReaderConfig{
		Address: address,
		Timeout: timeout,
	})
}

func NewFlightDataStreamReaderWithConfig(config FlightDataStreamReaderConfig) *FlightDataStreamReader {
	log.Trace("NewFlightDataStreamReader")

	tlsConfig, err := flightTLSConfig(config)
	if err != nil {
		log.WithError(err).Fatal("unable to create data stream flight TLS config")
	}
	return &FlightDataStreamReader{
		address:   strings.TrimSpace(config.Address),
		timeout:   config.Timeout,
		authToken: strings.TrimSpace(config.AuthToken),
		insecure:  config.Insecure,
		tlsConfig: tlsConfig,
		allocator: memory.NewGoAllocator(),
	}
}

func (r *FlightDataStreamReader) ReadParquet(ctx context.Context, request DataStreamReadRequest) (*ParquetArtifact, error) {
	log.Trace("FlightDataStreamReader ReadParquet")

	if r.address == "" {
		return nil, domain.ErrValidationFailed.Extend("data stream flight address is required")
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	command, err := json.Marshal(map[string]any{
		flightCommandUserID:            strings.TrimSpace(request.UserID),
		flightCommandOrgID:             strings.TrimSpace(request.OrgID),
		flightCommandSourceType:        strings.ToLower(strings.TrimSpace(request.SourceType)),
		flightCommandSourceConnectorID: strings.TrimSpace(request.SourceConnectorID),
		flightCommandSQL:               strings.TrimSpace(request.SQL),
		flightCommandDatabase:          strings.TrimSpace(request.Database),
		flightCommandCollection:        strings.TrimSpace(request.Collection),
	})
	if err != nil {
		return nil, fmt.Errorf("%w: marshal data stream command: %w", domain.ErrRawSnapshotMaterialize, err)
	}

	client, err := flight.NewFlightClient(
		r.address,
		nil,
		grpc.WithTransportCredentials(r.transportCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: create data stream flight client: %w", domain.ErrRawSnapshotMaterialize, err)
	}
	defer client.Close()

	if r.authToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, flightAuthTokenHeader, r.authToken)
	}

	info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  command,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: get data stream flight info: %w", domain.ErrRawSnapshotMaterialize, err)
	}
	if len(info.GetEndpoint()) == 0 || info.GetEndpoint()[0].GetTicket() == nil {
		return nil, domain.ErrRawSnapshotMaterialize.Extend("data stream returned no flight endpoint")
	}

	stream, err := client.DoGet(ctx, info.GetEndpoint()[0].GetTicket())
	if err != nil {
		return nil, fmt.Errorf("%w: read data stream flight endpoint: %w", domain.ErrRawSnapshotMaterialize, err)
	}
	reader, err := flight.NewRecordReader(stream, ipc.WithAllocator(r.allocator))
	if err != nil {
		return nil, fmt.Errorf("%w: create data stream record reader: %w", domain.ErrRawSnapshotMaterialize, err)
	}
	defer reader.Release()

	var out bytes.Buffer
	writer, err := pqarrow.NewFileWriter(
		reader.Schema(),
		&out,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: create parquet writer: %w", domain.ErrRawSnapshotMaterialize, err)
	}

	var rows int64
	for reader.Next() {
		record := reader.Record()
		rows += record.NumRows()
		if err := writer.Write(record); err != nil {
			_ = writer.Close()
			return nil, fmt.Errorf("%w: write data stream record batch: %w", domain.ErrRawSnapshotMaterialize, err)
		}
	}
	if err := reader.Err(); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("%w: read data stream record batch: %w", domain.ErrRawSnapshotMaterialize, err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("%w: close parquet writer: %w", domain.ErrRawSnapshotMaterialize, err)
	}

	schemaMetadata, err := schemaMetadataJSON(reader.Schema(), rows, map[string]any{
		dataStreamSourceFormatKey:      dataStreamSourceFormatValue,
		dataStreamSourceTypeKey:        strings.ToLower(strings.TrimSpace(request.SourceType)),
		dataStreamSourceConnectorIDKey: strings.TrimSpace(request.SourceConnectorID),
		dataStreamSourceDatabaseKey:    strings.TrimSpace(request.Database),
		dataStreamSourceCollectionKey:  strings.TrimSpace(request.Collection),
		dataStreamSourceQueryKey:       strings.TrimSpace(request.SQL),
		dataStreamSourceReadAtKey:      time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, err
	}
	return &ParquetArtifact{
		Data:           out.Bytes(),
		SchemaVersion:  1,
		SchemaMetadata: schemaMetadata,
		RowCount:       rows,
	}, nil
}

func (r *FlightDataStreamReader) transportCredentials() credentials.TransportCredentials {
	log.Trace("FlightDataStreamReader transportCredentials")

	if r.insecure {
		return insecure.NewCredentials()
	}
	return credentials.NewTLS(r.tlsConfig)
}

func flightTLSConfig(config FlightDataStreamReaderConfig) (*tls.Config, error) {
	log.Trace("flightTLSConfig")

	if config.Insecure {
		return nil, nil
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: strings.TrimSpace(config.ServerName),
	}
	if strings.TrimSpace(config.CACertPath) != "" {
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load system cert pool: %w", err)
		}
		if certPool == nil {
			certPool = x509.NewCertPool()
		}
		ca, err := os.ReadFile(strings.TrimSpace(config.CACertPath))
		if err != nil {
			return nil, fmt.Errorf("read data stream ca cert: %w", err)
		}
		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			return nil, fmt.Errorf("data stream ca cert contains no PEM certificates")
		}
		tlsConfig.RootCAs = certPool
	}
	if strings.TrimSpace(config.ClientCertPath) != "" || strings.TrimSpace(config.ClientKeyPath) != "" {
		if strings.TrimSpace(config.ClientCertPath) == "" || strings.TrimSpace(config.ClientKeyPath) == "" {
			return nil, fmt.Errorf("data stream client cert and key must be configured together")
		}
		cert, err := tls.LoadX509KeyPair(strings.TrimSpace(config.ClientCertPath), strings.TrimSpace(config.ClientKeyPath))
		if err != nil {
			return nil, fmt.Errorf("load data stream client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}
