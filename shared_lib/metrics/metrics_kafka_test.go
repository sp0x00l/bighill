//go:build cgo

package metrics_test

import (
	"errors"

	metrics "lib/shared_lib/metrics"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Kafka classifier", func() {
	It("classifies Kafka errors", func() {
		Expect(metrics.ClassifyKafka(kafka.NewError(kafka.ErrTimedOut, "timeout", false))).To(Equal(metrics.ErrorClassTimeout))
		Expect(metrics.ClassifyKafka(kafka.NewError(kafka.ErrAllBrokersDown, "down", false))).To(Equal(metrics.ErrorClassUnavailable))
		Expect(metrics.ClassifyKafka(kafka.NewError(kafka.ErrTransport, "transport", false))).To(Equal(metrics.ErrorClassNetwork))
		Expect(metrics.ClassifyKafka(kafka.NewError(kafka.ErrInvalidArg, "invalid", false))).To(Equal(metrics.ErrorClassInternal))
		Expect(metrics.ClassifyKafka(errors.New("other"))).To(Equal(metrics.ErrorClassUnknown))
	})
})
