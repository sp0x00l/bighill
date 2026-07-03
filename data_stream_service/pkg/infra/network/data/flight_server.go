package data

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	domainErrors "data_stream_service/pkg/domain"
	"data_stream_service/pkg/infra"
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type flightServer struct {
	flight.BaseFlightServer
	flightServer flight.Server
	config       infra.DataConfig
	engine       QueryEngine
	allocator    memory.Allocator
}

type closeableQueryEngine interface {
	Close() error
}

func NewFlightServer(authHandler flight.ServerAuthHandler, config infra.DataConfig, engine QueryEngine) *flightServer {
	log.Trace("NewFlightServer")

	if engine == nil {
		engine = NewLocalQueryEngine()
	}
	fs := &flightServer{
		config:    config,
		engine:    engine,
		allocator: memory.NewGoAllocator(),
	}

	interceptor := flight.ServerMiddleware{
		Stream: fs.StreamServerInterceptor(),
	}
	mw := []flight.ServerMiddleware{
		interceptor,
	}
	fs.SetAuthHandler(authHandler)
	opts, err := serverOptions(config.Server)
	if err != nil {
		log.WithError(err).Fatal("unable to create data stream flight server options")
	}
	opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	server := flight.NewServerWithMiddleware(mw, opts...)
	fs.flightServer = server
	return fs
}

func (fs *flightServer) Connect() func() {
	log.Trace("flightServer Connect")

	dest := fmt.Sprintf("%s:%d", fs.config.Server.Hostname, fs.config.Server.Port)

	fs.flightServer.Init(dest)
	fs.flightServer.RegisterFlightService(fs)

	go fs.flightServer.Serve()
	log.Info("flight server listening for grpc connections on ", dest)
	return func() {
		fs.flightServer.Shutdown()
		if closer, ok := fs.engine.(closeableQueryEngine); ok {
			if err := closer.Close(); err != nil {
				log.WithError(err).Warn("failed to close query engine")
			}
		}
	}
}

func serverOptions(config infra.ServerConnectionConfig) ([]grpc.ServerOption, error) {
	log.Trace("serverOptions")

	certPath := strings.TrimSpace(config.TLSCertPath)
	keyPath := strings.TrimSpace(config.TLSKeyPath)
	if certPath == "" && keyPath == "" {
		return nil, nil
	}
	if certPath == "" || keyPath == "" {
		return nil, fmt.Errorf("data stream TLS cert and key must be configured together")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load data stream TLS certificate: %w", err)
	}
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if strings.TrimSpace(config.TLSClientCAPath) != "" || config.RequireClientCert {
		if strings.TrimSpace(config.TLSClientCAPath) == "" {
			return nil, fmt.Errorf("data stream client CA cert is required when client certificates are required")
		}
		clientCA, err := os.ReadFile(strings.TrimSpace(config.TLSClientCAPath))
		if err != nil {
			return nil, fmt.Errorf("read data stream client CA cert: %w", err)
		}
		clientCAPool := x509.NewCertPool()
		if ok := clientCAPool.AppendCertsFromPEM(clientCA); !ok {
			return nil, fmt.Errorf("data stream client CA cert contains no PEM certificates")
		}
		tlsConfig.ClientCAs = clientCAPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsConfig))}, nil
}

func (fs *flightServer) GetSchema(ctx context.Context, descriptor *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	log.Trace("flightServer GetSchema")

	schema, err := fs.engine.GetSchema(ctx, descriptor)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "get query schema: %v", err)
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, fs.allocator)}, nil
}

func (fs *flightServer) GetFlightInfo(ctx context.Context, descriptor *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	log.Trace("flightServer GetFlightInfo")

	schema, err := fs.engine.GetSchema(ctx, descriptor)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "get flight info schema: %v", err)
	}

	ticket := descriptor.GetCmd()
	if len(ticket) == 0 {
		ticket = []byte(descriptorCommand(descriptor))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, fs.allocator),
		FlightDescriptor: descriptor,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: ticket}},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (fs *flightServer) DoGet(ticket *flight.Ticket, outStream flight.FlightService_DoGetServer) error {
	log.Trace("flightServer DoGet")

	result, err := fs.engine.Execute(outStream.Context(), ticket)
	if err != nil {
		log.WithContext(outStream.Context()).WithError(err).Error("query execution failed")
		return status.Errorf(queryStatusCode(err), "query execution failed: %v", err)
	}
	if result == nil || result.Schema == nil {
		return status.Error(codes.Internal, "query execution returned no schema")
	}

	writer := flight.NewRecordWriter(outStream, ipc.WithSchema(result.Schema), ipc.WithAllocator(fs.allocator))
	defer writer.Close()

	for _, record := range result.Records {
		if record == nil {
			continue
		}
		if err := writer.Write(record); err != nil {
			record.Release()
			log.WithContext(outStream.Context()).WithError(err).Error("error sending record batch")
			return status.Errorf(codes.Internal, "send record batch: %v", err)
		}
		record.Release()
	}
	return nil
}

func queryStatusCode(err error) codes.Code {
	log.Trace("queryStatusCode")

	if domainErrors.IsServiceError(err, domainErrors.ErrValidationFailed) {
		return codes.InvalidArgument
	}
	return codes.Internal
}

func (fs *flightServer) StreamServerInterceptor() grpc.StreamServerInterceptor {
	log.Trace("flightServer StreamServerInterceptor")

	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		log.Trace("flightServer StreamServerInterceptor")

		err := handler(srv, stream)
		if err != nil {
			log.WithContext(stream.Context()).WithError(err).Errorf("error handling stream")
		}
		return err
	}
}
