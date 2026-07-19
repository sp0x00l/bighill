package data

import (
	"encoding/json"

	streamdomain "data_stream_service/pkg/domain"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("source query command", func() {
	It("requires sourceType instead of defaulting it", func() {
		_, err := parseSourceQueryCommand(sourceQueryJSON(map[string]any{
			"userId":            "user-1",
			"orgId":             "org-1",
			"sourceConnectorId": "connector-1",
			"sql":               "select 1",
		}))

		Expect(err).To(MatchError(ContainSubstring("requires sourceType")))
	})

	It("parses all registered source types into the enum", func() {
		cases := []struct {
			input string
			want  streamdomain.SourceType
		}{
			{input: "S3", want: streamdomain.SourceTypeS3},
			{input: "azure_storage", want: streamdomain.SourceTypeAzureStorage},
			{input: "gcs", want: streamdomain.SourceTypeGCS},
			{input: "postgres", want: streamdomain.SourceTypePostgres},
			{input: "mysql", want: streamdomain.SourceTypeMySQL},
			{input: "oracle", want: streamdomain.SourceTypeOracle},
			{input: "mongo", want: streamdomain.SourceTypeMongoDB},
			{input: "clickhouse", want: streamdomain.SourceTypeClickHouse},
		}

		for _, tc := range cases {
			payload := map[string]any{
				"userId":            "user-1",
				"orgId":             "org-1",
				"sourceConnectorId": "connector-1",
				"sourceType":        tc.input,
				"sql":               "select 1",
			}
			if tc.want == streamdomain.SourceTypeMongoDB {
				delete(payload, "sql")
				payload["database"] = "db"
				payload["collection"] = "items"
			}
			query, err := parseSourceQueryCommand(sourceQueryJSON(payload))

			if sourceTypeSupportsRegistryQuery(tc.want) {
				Expect(err).NotTo(HaveOccurred())
				Expect(query.SourceType).To(Equal(tc.want))
			} else {
				Expect(err).To(MatchError(ContainSubstring("unsupported source type")))
			}
		}
	})

	It("allows mongo commands with database and collection instead of SQL", func() {
		query, err := parseSourceQueryCommand(sourceQueryJSON(map[string]any{
			"userId":            "user-1",
			"orgId":             "org-1",
			"sourceConnectorId": "connector-1",
			"sourceType":        "mongo",
			"database":          "sample",
			"collection":        "movies",
			"limit":             10,
		}))

		Expect(err).NotTo(HaveOccurred())
		Expect(query.SourceType).To(Equal(streamdomain.SourceTypeMongoDB))
		Expect(query.Database).To(Equal("sample"))
		Expect(query.Collection).To(Equal("movies"))
		Expect(query.Limit).To(Equal(int64(10)))
	})

	It("rejects invalid source types", func() {
		_, err := parseSourceQueryCommand(sourceQueryJSON(map[string]any{
			"userId":            "user-1",
			"orgId":             "org-1",
			"sourceConnectorId": "connector-1",
			"sourceType":        "sqlite",
			"sql":               "select 1",
		}))

		Expect(err).To(MatchError(ContainSubstring("invalid source type")))
	})

	It("requires SQL for SQL-backed sources", func() {
		_, err := parseSourceQueryCommand(sourceQueryJSON(map[string]any{
			"userId":            "user-1",
			"orgId":             "org-1",
			"sourceConnectorId": "connector-1",
			"sourceType":        "mysql",
		}))

		Expect(err).To(MatchError(ContainSubstring("requires sql")))
	})

	It("requires orgId", func() {
		_, err := parseSourceQueryCommand(sourceQueryJSON(map[string]any{
			"userId":            "user-1",
			"sourceConnectorId": "connector-1",
			"sql":               "select 1",
		}))

		Expect(err).To(MatchError(ContainSubstring("requires orgId")))
	})
})

func sourceQueryJSON(payload map[string]any) string {
	bytes, err := json.Marshal(payload)
	Expect(err).NotTo(HaveOccurred())
	return string(bytes)
}
