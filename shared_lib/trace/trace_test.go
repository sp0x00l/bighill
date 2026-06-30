package trace_test

import (
	"context"
	"testing"

	"lib/shared_lib/trace"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

func TestTrace(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Trace unit test suite")
}

var _ = Describe("Trace", Ordered, func() {
	It("should write the trace to stdout", func() {

		// To capture the output in a buffer, we need to replace
		// the writer used by the trace package with our own writer.
		ctx := context.Background()
		writer := &trace.CustomWriter{}
		shutdown := trace.Init(ctx, "test", "1.0.0", trace.WithStdoutWriter(writer))
		Expect(shutdown).NotTo(BeNil())
		tracer := otel.Tracer("test-tracer")
		ctx, span := tracer.Start(context.Background(), "TestSpan")
		log.WithContext(ctx).Error("an error occurred")
		span.End()
		shutdown()

		Expect(writer.Buffer.String()).To(ContainSubstring("TestSpan"))
		Expect(writer.Buffer.String()).To(ContainSubstring("an error occurred"))
	})
})
