package data_test

import (
	"context"
	"io"

	"data_stream_service/pkg/infra"
	"data_stream_service/pkg/infra/network/data"

	"github.com/apache/arrow-go/v18/arrow/flight"
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
		server = &data.FlightServerAuth{}
		gateway = data.NewFlightServer(server, infra.DataConfig{
			Server: infra.ServerConnectionConfig{Hostname: "localhost", Port: 0},
			QueryEngine: infra.QueryEngineConfig{
				Mode:     "local",
				DataRoot: "tmp/local_s3_storage",
			},
		}, data.NewLocalQueryEngine())
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
})
