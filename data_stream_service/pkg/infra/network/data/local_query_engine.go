package data

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	log "github.com/sirupsen/logrus"
)

type localQueryEngine struct {
	allocator memory.Allocator
	schema    *arrow.Schema
}

func NewLocalQueryEngine() QueryEngine {
	log.Trace("NewLocalQueryEngine")

	return &localQueryEngine{
		allocator: memory.NewGoAllocator(),
		schema: arrow.NewSchema(
			[]arrow.Field{
				{Name: "query", Type: arrow.BinaryTypes.String},
				{Name: "row_number", Type: arrow.PrimitiveTypes.Int64},
				{Name: "value", Type: arrow.BinaryTypes.String},
			},
			nil,
		),
	}
}

func (e *localQueryEngine) GetSchema(_ context.Context, descriptor *flight.FlightDescriptor) (*arrow.Schema, error) {
	log.Trace("localQueryEngine GetSchema")

	if err := validateDescriptor(descriptor); err != nil {
		return nil, err
	}
	return e.schema, nil
}

func (e *localQueryEngine) Execute(_ context.Context, ticket *flight.Ticket) (*QueryResult, error) {
	log.Trace("localQueryEngine Execute")

	query := ticketCommand(ticket)
	if query == "" {
		query = "local"
	}

	builder := array.NewRecordBuilder(e.allocator, e.schema)
	defer builder.Release()

	builder.Field(0).(*array.StringBuilder).AppendValues([]string{query, query}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	builder.Field(2).(*array.StringBuilder).AppendValues([]string{"local-arrow-result", "datafusion-boundary-ready"}, nil)

	record := builder.NewRecord()
	return &QueryResult{
		Schema:       e.schema,
		Records:      []arrow.Record{record},
		TotalRecords: record.NumRows(),
	}, nil
}
