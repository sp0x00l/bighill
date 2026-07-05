package data

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type trackingQueryEngine struct {
	name        string
	schemaCalls int
	execCalls   int
}

func (e *trackingQueryEngine) GetSchema(context.Context, *flight.FlightDescriptor) (*arrow.Schema, error) {
	e.schemaCalls++
	return arrow.NewSchema([]arrow.Field{{Name: e.name, Type: arrow.PrimitiveTypes.Int64}}, nil), nil
}

func (e *trackingQueryEngine) Execute(context.Context, *flight.Ticket) (*QueryResult, error) {
	e.execCalls++
	return nil, fmt.Errorf("%s execute called", e.name)
}

var _ = Describe("hybrid query engine", func() {
	It("routes dataset table commands to the lakehouse engine", func() {
		registry := &trackingQueryEngine{name: "registry"}
		lakehouse := &trackingQueryEngine{name: "lakehouse"}
		engine := newHybridQueryEngine(registry, lakehouse)

		schema, err := engine.GetSchema(context.Background(), &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(`{"userId":"user-1","datasetId":"dataset-1","sql":"SELECT * FROM dataset"}`),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(schema.Field(0).Name).To(Equal("lakehouse"))
		Expect(lakehouse.schemaCalls).To(Equal(1))
		Expect(registry.schemaCalls).To(Equal(0))
	})

	It("routes source connector commands to the registry engine", func() {
		registry := &trackingQueryEngine{name: "registry"}
		lakehouse := &trackingQueryEngine{name: "lakehouse"}
		engine := newHybridQueryEngine(registry, lakehouse)

		schema, err := engine.GetSchema(context.Background(), &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(`{"userId":"user-1","sourceConnectorId":"connector-1","sourceType":"POSTGRES","sql":"SELECT 1"}`),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(schema.Field(0).Name).To(Equal("registry"))
		Expect(registry.schemaCalls).To(Equal(1))
		Expect(lakehouse.schemaCalls).To(Equal(0))
	})

	It("keeps malformed commands on the registry path for source-query validation", func() {
		registry := &trackingQueryEngine{name: "registry"}
		lakehouse := &trackingQueryEngine{name: "lakehouse"}
		engine := newHybridQueryEngine(registry, lakehouse)

		_, err := engine.GetSchema(context.Background(), &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte("{"),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(registry.schemaCalls).To(Equal(1))
		Expect(lakehouse.schemaCalls).To(Equal(0))
	})
})
