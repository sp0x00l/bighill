package app_test

import (
	"context"
	"errors"
	"testing"

	streamapp "data_stream_service/pkg/app"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAppUsecases(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data stream app unit test suite")
}

type queryEngineStub struct {
	schema        *arrow.Schema
	schemaErr     error
	executeResult *streamapp.QueryResult
	executeErr    error
	closed        bool
}

func (s *queryEngineStub) GetSchema(context.Context, *flight.FlightDescriptor) (*arrow.Schema, error) {
	return s.schema, s.schemaErr
}

func (s *queryEngineStub) Execute(context.Context, *flight.Ticket) (*streamapp.QueryResult, error) {
	return s.executeResult, s.executeErr
}

func (s *queryEngineStub) Close() error {
	s.closed = true
	return nil
}

var _ = Describe("QueryUsecase", func() {
	var (
		ctx    context.Context
		engine *queryEngineStub
		uc     *streamapp.QueryUsecase
		schema *arrow.Schema
	)

	BeforeEach(func() {
		ctx = context.Background()
		schema = arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
		engine = &queryEngineStub{schema: schema, executeResult: &streamapp.QueryResult{Schema: schema}}
		uc = streamapp.NewQueryUsecase(engine)
	})

	It("delegates schema and query execution to the configured engine", func() {
		gotSchema, err := uc.GetSchema(ctx, &flight.FlightDescriptor{Cmd: []byte("select 1")})
		Expect(err).NotTo(HaveOccurred())
		Expect(gotSchema).To(Equal(schema))

		gotResult, err := uc.Execute(ctx, &flight.Ticket{Ticket: []byte("select 1")})
		Expect(err).NotTo(HaveOccurred())
		Expect(gotResult).To(Equal(engine.executeResult))
	})

	It("returns engine errors without replacing them", func() {
		expectedErr := errors.New("engine unavailable")
		engine.executeErr = expectedErr

		_, err := uc.Execute(ctx, &flight.Ticket{Ticket: []byte("select 1")})

		Expect(err).To(MatchError(expectedErr))
	})

	It("closes a closeable engine", func() {
		Expect(uc.Close()).To(Succeed())
		Expect(engine.closed).To(BeTrue())
	})
})
