package modelstatus

import (
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModelStatus(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model status unit test suite")
}

var _ = Describe("ModelLoadStatus", func() {
	It("renders strings", func() {
		Expect(ModelLoadStatusNotLoaded.String()).To(Equal("NOT_LOADED"))
		Expect(ModelLoadStatusLoaded.String()).To(Equal("LOADED"))
		Expect(ModelLoadStatusFailed.String()).To(Equal("FAILED"))
		Expect(ModelLoadStatus(99).String()).To(Equal("UNKNOWN"))
	})

	It("parses known strings", func() {
		cases := map[string]ModelLoadStatus{
			"":           ModelLoadStatusNotLoaded,
			"not_loaded": ModelLoadStatusNotLoaded,
			"LOADED":     ModelLoadStatusLoaded,
			" failed ":   ModelLoadStatusFailed,
		}

		for input, want := range cases {
			got, err := ToModelLoadStatus(input)
			Expect(err).NotTo(HaveOccurred(), "input %q", input)
			Expect(got).To(Equal(want), "input %q", input)
		}
	})

	It("rejects unknown strings", func() {
		_, err := ToModelLoadStatus("READY")
		Expect(errors.Is(err, ErrUnknownModelLoadStatus)).To(BeTrue())
	})
})
