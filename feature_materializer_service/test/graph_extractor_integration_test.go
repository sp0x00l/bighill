package integration_test

import (
	"context"
	"strings"
	"time"

	"feature_materializer_service/pkg/domain/model"
	"feature_materializer_service/pkg/infra/materialization"
	env "lib/shared_lib/env"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var _ = Describe("Configured graph extraction model", Label("graph", "integration"), func() {
	It("extracts schema-valid graph data through the configured model endpoint", func() {
		endpoint := strings.TrimSpace(env.MustString("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_ENDPOINT"))
		modelName := strings.TrimSpace(env.MustString("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MODEL"))

		requestTimeout := time.Duration(env.MustInt("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_REQUEST_TIMEOUT_SECONDS")) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout+30*time.Second)
		defer cancel()

		extractor, err := materialization.NewModelServingGraphExtractor(materialization.ModelServingGraphExtractorConfig{
			Endpoint:         endpoint,
			AuthToken:        env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_AUTH_TOKEN", ""),
			Timeout:          requestTimeout,
			MaxResponseBytes: env.MustInt64("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MAX_RESPONSE_BYTES"),
			MaxOutputTokens:  env.MustInt("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MAX_OUTPUT_TOKENS"),
			MaxRetries:       env.MustInt("FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MAX_RETRIES"),
		})
		Expect(err).NotTo(HaveOccurred())

		extraction, err := extractor.ExtractGraph(ctx, []model.GraphChunk{{
			ChunkIndex: 0,
			SourceText: "Aurora Relay connects Beacon Hub.",
		}}, model.GraphExtractionStrategy{
			ExtractionModel:         modelName,
			ExtractionPromptVersion: model.DefaultGraphExtractionPromptVersion,
			ExtractionSchemaVersion: model.DefaultGraphExtractionSchemaVersion,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(extraction.Entities).NotTo(BeEmpty())
		Expect(extraction.Relations).NotTo(BeEmpty())
		Expect(graphEntityNamesContain(extraction, "Aurora Relay")).To(BeTrue(), "entity names: %v", graphEntityNames(extraction))
		Expect(graphEntityNamesContain(extraction, "Beacon Hub")).To(BeTrue(), "entity names: %v", graphEntityNames(extraction))
	})
})

func graphEntityNames(extraction *model.GraphExtraction) []string {
	log.Trace("graphEntityNames")

	names := make([]string, 0, len(extraction.Entities))
	for _, entity := range extraction.Entities {
		names = append(names, entity.Name)
	}
	return names
}

func graphEntityNamesContain(extraction *model.GraphExtraction, expected string) bool {
	log.Trace("graphEntityNamesContain")

	expected = normalizeGraphEntityName(expected)
	for _, name := range graphEntityNames(extraction) {
		if strings.Contains(normalizeGraphEntityName(name), expected) {
			return true
		}
	}
	return false
}

func normalizeGraphEntityName(value string) string {
	log.Trace("normalizeGraphEntityName")

	value = strings.ToLower(value)
	value = strings.NewReplacer("_", " ", "-", " ", ".", " ", "/", " ").Replace(value)
	return strings.Join(strings.Fields(value), " ")
}
