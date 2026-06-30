//go:build !cgo

package metrics_test

import (
	"errors"

	metrics "lib/shared_lib/metrics"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Kafka classifier", func() {
	It("defaults to unknown when kafka types are unavailable", func() {
		Expect(metrics.ClassifyKafka(errors.New("other"))).To(Equal(metrics.ErrorClassUnknown))
	})
})
