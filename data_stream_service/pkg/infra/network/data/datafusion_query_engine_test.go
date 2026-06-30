package data_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"

	"data_stream_service/pkg/infra"
	"data_stream_service/pkg/infra/network/data"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DataFusion query engine adapter", func() {
	It("runs a subprocess and decodes Arrow IPC query results", func() {
		tmpDir := GinkgoT().TempDir()
		ipcPath := filepath.Join(tmpDir, "result.arrow")
		binaryPath := filepath.Join(tmpDir, "fake-datafusion")

		Expect(os.WriteFile(ipcPath, buildArrowIPC(), 0600)).To(Succeed())
		Expect(os.WriteFile(binaryPath, []byte("#!/usr/bin/env sh\ncat \"$FAKE_DATAFUSION_IPC\"\n"), 0700)).To(Succeed())
		Expect(os.Setenv("FAKE_DATAFUSION_IPC", ipcPath)).To(Succeed())
		DeferCleanup(os.Unsetenv, "FAKE_DATAFUSION_IPC")

		engine, err := data.NewDataFusionQueryEngine(infra.QueryEngineConfig{
			BinaryPath: binaryPath,
			DataRoot:   tmpDir,
			TimeoutSec: 5,
		})
		Expect(err).NotTo(HaveOccurred())

		schema, err := engine.GetSchema(context.Background(), &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte("SELECT * FROM dataset"),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(schema.Field(0).Name).To(Equal("feature"))

		result, err := engine.Execute(context.Background(), &flight.Ticket{Ticket: []byte("SELECT * FROM dataset")})
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			for _, record := range result.Records {
				record.Release()
			}
		}()
		Expect(result.TotalRecords).To(Equal(int64(2)))
		Expect(result.Records).To(HaveLen(1))
	})

	It("rejects unsupported query engine modes", func() {
		engine, err := data.NewQueryEngine(infra.QueryEngineConfig{Mode: "legacy"})

		Expect(engine).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("unsupported query engine mode")))
	})
})

func buildArrowIPC() []byte {
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "feature", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.StringBuilder).AppendValues([]string{"age", "score"}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{42, 9001}, nil)
	record := builder.NewRecord()
	defer record.Release()

	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
	Expect(writer.Write(record)).To(Succeed())
	Expect(writer.Close()).To(Succeed())
	return output.Bytes()
}
