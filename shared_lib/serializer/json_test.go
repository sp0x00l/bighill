package serializer_test

import (
	"testing"

	"lib/shared_lib/serializer"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSerializer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Serializer Suite")
}

var _ = Describe("JSON serializer", func() {
	It("encodes without escaping HTML-sensitive query characters", func() {
		encoder := serializer.NewJSONSerializer()

		data, err := encoder.Serialize(map[string]string{"next": "/v1/data?limit=10&page=2"})

		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal(`{"next":"/v1/data?limit=10&page=2"}`))
	})

	It("round-trips data from strings", func() {
		encoder := serializer.NewJSONSerializer()
		var decoded struct {
			Name string `json:"name"`
		}

		Expect(encoder.DecodeStringToData(`{"name":"dataset"}`, &decoded)).To(Succeed())

		data, err := encoder.EncodeDataToString(decoded)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(`{"name":"dataset"}`))
	})
})
