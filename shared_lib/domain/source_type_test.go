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
