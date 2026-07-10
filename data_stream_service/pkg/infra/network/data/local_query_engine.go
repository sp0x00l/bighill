package data

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	log "github.com/sirupsen/logrus"
)

const (
	localQueryDefaultCommand        = "local"
	localQueryColumnQuery           = "query"
	localQueryColumnRowNumber       = "row_number"
	localQueryColumnValue           = "value"
	localQueryFirstResultValue      = "local-arrow-result"
	localQuerySecondResultValue     = "datafusion-boundary-ready"
	localQueryFirstResultRowNumber  = int64(1)
	localQuerySecondResultRowNumber = int64(2)
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
				{Name: localQueryColumnQuery, Type: arrow.BinaryTypes.String},
				{Name: localQueryColumnRowNumber, Type: arrow.PrimitiveTypes.Int64},
				{Name: localQueryColumnValue, Type: arrow.BinaryTypes.String},
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
		query = localQueryDefaultCommand
	}

	builder := array.NewRecordBuilder(e.allocator, e.schema)
	defer builder.Release()

	builder.Field(0).(*array.StringBuilder).AppendValues([]string{query, query}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{localQueryFirstResultRowNumber, localQuerySecondResultRowNumber}, nil)
	builder.Field(2).(*array.StringBuilder).AppendValues([]string{localQueryFirstResultValue, localQuerySecondResultValue}, nil)

	record := builder.NewRecordBatch()
	return &QueryResult{
		Schema:       e.schema,
		Records:      []arrow.Record{record},
		TotalRecords: record.NumRows(),
	}, nil
}
