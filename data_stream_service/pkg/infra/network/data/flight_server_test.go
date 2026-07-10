package data_test

import (
	"context"
	"io"

	"data_stream_service/pkg/infra"
	"data_stream_service/pkg/infra/network/data"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/metadata"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type doGetStream struct {
	flight.FlightService_DoGetServer
	ctx      context.Context
	messages []*flight.FlightData
}

func (s *doGetStream) Send(data *flight.FlightData) error {
	s.messages = append(s.messages, data)
	return nil
}

func (s *doGetStream) SetHeader(metadata.MD) error  { return nil }
func (s *doGetStream) SendHeader(metadata.MD) error { return nil }
func (s *doGetStream) SetTrailer(metadata.MD)       {}
func (s *doGetStream) Context() context.Context     { return s.ctx }
func (s *doGetStream) SendMsg(any) error            { return nil }
func (s *doGetStream) RecvMsg(any) error            { return io.EOF }

type streamingEngineStub struct {
	streamed bool
	executed bool
	schema   *arrow.Schema
}

func newStreamingEngineStub() *streamingEngineStub {
	return &streamingEngineStub{
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.BinaryTypes.String},
		}, nil),
	}
}

func (e *streamingEngineStub) GetSchema(context.Context, *flight.FlightDescriptor) (*arrow.Schema, error) {
	return e.schema, nil
}

func (e *streamingEngineStub) Execute(context.Context, *flight.Ticket) (*data.QueryResult, error) {
	e.executed = true
	return nil, nil
}

func (e *streamingEngineStub) Stream(_ context.Context, _ *flight.Ticket, outStream flight.FlightService_DoGetServer) error {
	e.streamed = true
	allocator := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(allocator, e.schema)
	defer builder.Release()
	builder.Field(0).(*array.StringBuilder).Append("streamed")
	record := builder.NewRecordBatch()
	defer record.Release()

	writer := flight.NewRecordWriter(outStream, ipc.WithSchema(e.schema), ipc.WithAllocator(allocator))
	defer writer.Close()
	return writer.Write(record)
}

var _ = Describe("Flight query gateway", func() {
	var (
		server  *data.FlightServerAuth
		gateway interface {
			GetSchema(context.Context, *flight.FlightDescriptor) (*flight.SchemaResult, error)
			GetFlightInfo(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error)
			DoGet(*flight.Ticket, flight.FlightService_DoGetServer) error
		}
		descriptor *flight.FlightDescriptor
	)

	BeforeEach(func() {
		server = data.NewFlightServerAuth("", true)
		var err error
		gateway, err = data.NewFlightServer(server, infra.DataConfig{
			Server: infra.ServerConnectionConfig{Hostname: "localhost", Port: 0},
			QueryEngine: infra.QueryEngineConfig{
				Mode:     "local",
				DataRoot: "tmp/local_s3_storage",
			},
		}, data.NewLocalQueryEngine())
		Expect(err).NotTo(HaveOccurred())
		descriptor = &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte("SELECT * FROM dataset LIMIT 10"),
		}
	})

	It("returns a schema from the query engine", func() {
		result, err := gateway.GetSchema(context.Background(), descriptor)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Schema).NotTo(BeEmpty())
	})

	It("returns local flight info with a redeemable ticket", func() {
		info, err := gateway.GetFlightInfo(context.Background(), descriptor)

		Expect(err).NotTo(HaveOccurred())
		Expect(info.Schema).NotTo(BeEmpty())
		Expect(info.Endpoint).To(HaveLen(1))
		Expect(string(info.Endpoint[0].Ticket.Ticket)).To(Equal("SELECT * FROM dataset LIMIT 10"))
	})

	It("streams Arrow records from the query engine", func() {
		stream := &doGetStream{ctx: context.Background()}
		ticket := &flight.Ticket{Ticket: []byte("SELECT * FROM dataset LIMIT 10")}

		err := gateway.DoGet(ticket, stream)

		Expect(err).NotTo(HaveOccurred())
		Expect(stream.messages).NotTo(BeEmpty())
		Expect(stream.messages[0].DataHeader).NotTo(BeEmpty())
	})

	It("uses a streaming query engine path when available", func() {
		streamingEngine := newStreamingEngineStub()
		var err error
		gateway, err = data.NewFlightServer(server, infra.DataConfig{
			Server: infra.ServerConnectionConfig{Hostname: "localhost", Port: 0},
		}, streamingEngine)
		Expect(err).NotTo(HaveOccurred())
		stream := &doGetStream{ctx: context.Background()}
		ticket := &flight.Ticket{Ticket: []byte("SELECT * FROM dataset LIMIT 10")}

		err = gateway.DoGet(ticket, stream)

		Expect(err).NotTo(HaveOccurred())
		Expect(streamingEngine.streamed).To(BeTrue())
		Expect(streamingEngine.executed).To(BeFalse())
		Expect(stream.messages).NotTo(BeEmpty())
	})
})

var _ = Describe("Flight auth", func() {
	It("allows anonymous auth only when explicitly enabled", func() {
		handler := data.NewFlightServerAuth("", true)

		identity, err := handler.IsValid("")

		Expect(err).NotTo(HaveOccurred())
		Expect(identity).To(Equal("anonymous-local"))
	})

	It("requires the configured token when anonymous auth is disabled", func() {
		handler := data.NewFlightServerAuth("secret-token", false)

		identity, err := handler.IsValid("secret-token")

		Expect(err).NotTo(HaveOccurred())
		Expect(identity).To(Equal("data-stream-client"))

		_, err = handler.IsValid("wrong-token")
		Expect(err).To(HaveOccurred())
	})
})
