package data

import (
	"context"
	"encoding/json"

	"data_stream_service/pkg/infra"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	log "github.com/sirupsen/logrus"
)

type hybridQueryEngine struct {
	registry  QueryEngine
	lakehouse QueryEngine
	allocator memory.Allocator
}

func NewHybridQueryEngine(config infra.QueryEngineConfig) (QueryEngine, error) {
	log.Trace("NewHybridQueryEngine")

	registry, err := NewRegistryQueryEngine(config)
	if err != nil {
		return nil, err
	}
	lakehouse, err := NewLakehouseQueryEngine(config)
	if err != nil {
		if closer, ok := registry.(closeableQueryEngine); ok {
			_ = closer.Close()
		}
		return nil, err
	}
	return newHybridQueryEngine(registry, lakehouse), nil
}

func newHybridQueryEngine(registry QueryEngine, lakehouse QueryEngine) *hybridQueryEngine {
	log.Trace("newHybridQueryEngine")

	return &hybridQueryEngine{
		registry:  registry,
		lakehouse: lakehouse,
		allocator: memory.NewGoAllocator(),
	}
}

func (e *hybridQueryEngine) Close() error {
	log.Trace("hybridQueryEngine Close")

	var firstErr error
	for _, engine := range []QueryEngine{e.registry, e.lakehouse} {
		if closer, ok := engine.(closeableQueryEngine); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (e *hybridQueryEngine) GetSchema(ctx context.Context, descriptor *flight.FlightDescriptor) (*arrow.Schema, error) {
	log.Trace("hybridQueryEngine GetSchema")

	return e.engineForCommand(descriptorCommand(descriptor)).GetSchema(ctx, descriptor)
}

func (e *hybridQueryEngine) Execute(ctx context.Context, ticket *flight.Ticket) (*QueryResult, error) {
	log.Trace("hybridQueryEngine Execute")

	return e.engineForCommand(ticketCommand(ticket)).Execute(ctx, ticket)
}

func (e *hybridQueryEngine) Stream(ctx context.Context, ticket *flight.Ticket, outStream flight.FlightService_DoGetServer) error {
	log.Trace("hybridQueryEngine Stream")

	engine := e.engineForCommand(ticketCommand(ticket))
	if streamer, ok := engine.(streamingQueryEngine); ok {
		return streamer.Stream(ctx, ticket, outStream)
	}

	result, err := engine.Execute(ctx, ticket)
	if err != nil {
		return err
	}
	return writeQueryResult(outStream, e.allocator, result)
}

func (e *hybridQueryEngine) engineForCommand(command string) QueryEngine {
	log.Trace("hybridQueryEngine engineForCommand")

	if isLakehouseCommand(command) {
		return e.lakehouse
	}
	return e.registry
}

func isLakehouseCommand(command string) bool {
	log.Trace("isLakehouseCommand")

	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(command), &fields); err != nil {
		return false
	}
	var datasetID string
	if err := json.Unmarshal(fields["datasetId"], &datasetID); err != nil {
		return false
	}
	return datasetID != ""
}

func writeQueryResult(outStream flight.FlightService_DoGetServer, allocator memory.Allocator, result *QueryResult) error {
	log.Trace("writeQueryResult")

	writer := flight.NewRecordWriter(outStream, ipc.WithSchema(result.Schema), ipc.WithAllocator(allocator))
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
