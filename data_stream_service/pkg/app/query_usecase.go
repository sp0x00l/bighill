package app

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	log "github.com/sirupsen/logrus"
)

type QueryResult struct {
	Schema       *arrow.Schema
	Records      []arrow.Record
	TotalRecords int64
}

type QueryEngineAdapter interface {
	GetSchema(context.Context, *flight.FlightDescriptor) (*arrow.Schema, error)
	Execute(context.Context, *flight.Ticket) (*QueryResult, error)
}

type StreamingQueryEngineAdapter interface {
	Stream(context.Context, *flight.Ticket, flight.FlightService_DoGetServer) error
}

type CloseableQueryEngineAdapter interface {
	Close() error
}

type QueryUsecase struct {
	engine QueryEngineAdapter
}

func NewQueryUsecase(engine QueryEngineAdapter) *QueryUsecase {
	log.Trace("NewQueryUsecase")

	return &QueryUsecase{engine: engine}
}

func (u *QueryUsecase) GetSchema(ctx context.Context, descriptor *flight.FlightDescriptor) (*arrow.Schema, error) {
	log.Trace("QueryUsecase GetSchema")

	return u.engine.GetSchema(ctx, descriptor)
}

func (u *QueryUsecase) Execute(ctx context.Context, ticket *flight.Ticket) (*QueryResult, error) {
	log.Trace("QueryUsecase Execute")

	return u.engine.Execute(ctx, ticket)
}

func (u *QueryUsecase) Stream(ctx context.Context, ticket *flight.Ticket, outStream flight.FlightService_DoGetServer) error {
	log.Trace("QueryUsecase Stream")

	streamer, ok := u.engine.(StreamingQueryEngineAdapter)
	if !ok {
		result, err := u.engine.Execute(ctx, ticket)
		if err != nil {
			return err
		}
		return writeQueryResult(outStream, result)
	}
	return streamer.Stream(ctx, ticket, outStream)
}

func (u *QueryUsecase) Close() error {
	log.Trace("QueryUsecase Close")

	closer, ok := u.engine.(CloseableQueryEngineAdapter)
	if !ok {
		return nil
	}
	return closer.Close()
}

func writeQueryResult(outStream flight.FlightService_DoGetServer, result *QueryResult) error {
	log.Trace("writeQueryResult")

	writer := flight.NewRecordWriter(outStream, ipc.WithSchema(result.Schema))
	defer writer.Close()

	for _, record := range result.Records {
		if record == nil {
			continue
		}
		if err := writer.Write(record); err != nil {
			record.Release()
			return err
		}
		record.Release()
	}
	return nil
}
