package domain

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Shared domain unit test suite")
}

var _ = Describe("SourceType", func() {
	It("parses aliases", func() {
		tests := map[string]SourceType{
			"S3":                   SourceTypeS3,
			"azure_storage":        SourceTypeAzureStorage,
			"google-cloud-storage": SourceTypeGCS,
			"postgresql":           SourceTypePostgres,
			"mysql":                SourceTypeMySQL,
			"oracle":               SourceTypeOracle,
			"mongodb":              SourceTypeMongoDB,
			"clickhouse":           SourceTypeClickHouse,
		}

		for input, want := range tests {
			got, err := ToSourceType(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want), "input %q", input)
		}
	})

	It("uses the string contract for JSON", func() {
		var got SourceType
		Expect(json.Unmarshal([]byte(`"postgres"`), &got)).To(Succeed())
		Expect(got).To(Equal(SourceTypePostgres))

		data, err := json.Marshal(SourceTypePostgres)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal(`"POSTGRES"`))
	})
})

var _ = Describe("ModelKind", func() {
	It("parses exact enum values without mapping unknown values", func() {
		Expect(ToModelKind("BASE")).To(Equal(ModelKindBase))
		Expect(ToModelKind("fine_tuned")).To(Equal(ModelKindFineTuned))
		Expect(ToModelKind("unknown")).To(Equal(ModelKind("UNKNOWN")))
		Expect(ModelKind("").String()).To(Equal(""))
		Expect(IsKnownModelKind(ModelKindBase)).To(BeTrue())
		Expect(IsKnownModelKind(ModelKind("UNKNOWN"))).To(BeFalse())
	})
})

var _ = Describe("ModelSource", func() {
	It("parses exact enum values without mapping unknown values", func() {
		Expect(ToModelSource("UPLOAD")).To(Equal(ModelSourceUpload))
		Expect(ToModelSource("hugging_face")).To(Equal(ModelSourceHuggingFace))
		Expect(ToModelSource("training")).To(Equal(ModelSourceTraining))
		Expect(ToModelSource("unknown")).To(Equal(ModelSource("UNKNOWN")))
		Expect(ModelSource("").String()).To(Equal(""))
		Expect(IsKnownModelSource(ModelSourceUpload)).To(BeTrue())
		Expect(IsKnownModelSource(ModelSource("UNKNOWN"))).To(BeFalse())
	})
})
