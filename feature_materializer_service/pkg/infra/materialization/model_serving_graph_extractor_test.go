package materialization_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"feature_materializer_service/pkg/infra/materialization"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var _ = Describe("ModelServingGraphExtractor", func() {
	It("extracts graph entities through an OpenAI-compatible model response", func() {
		client := &graphExtractionHTTPClientStub{
			responses: []graphExtractionHTTPResponse{{
				statusCode: http.StatusOK,
				body:       graphChatResponse(`{"entities":[{"id":"ada","name":"Ada","type":"person","description":"Ada is mentioned."}],"relations":[]}`),
			}},
		}
		extractor, err := materialization.NewModelServingGraphExtractorWithClient(validModelServingGraphExtractorConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		extraction, err := extractor.ExtractGraph(context.Background(), []model.GraphChunk{{
			ChunkIndex: 5,
			SourceText: "Ada founded BigHill.",
		}}, validGraphExtractionStrategy())

		Expect(err).NotTo(HaveOccurred())
		Expect(client.requests).To(HaveLen(1))
		Expect(client.requests[0]).To(ContainSubstring(`"model":"graph-model"`))
		Expect(client.requests[0]).To(ContainSubstring(`"stream":false`))
		Expect(client.requests[0]).To(ContainSubstring(`"max_tokens":512`))
		Expect(client.requests[0]).To(ContainSubstring(`"response_format":{"type":"json_schema"`))
		Expect(client.requests[0]).To(ContainSubstring(`"name":"graph_extraction_v1"`))
		Expect(client.requests[0]).To(ContainSubstring(`"strict":true`))
		Expect(client.requests[0]).To(ContainSubstring("chunk_index: 5"))
		Expect(client.requests[0]).To(ContainSubstring("source_text"))
		Expect(extraction.Entities).To(HaveLen(1))
		Expect(extraction.Entities[0].Name).To(Equal("Ada"))
		Expect(extraction.Entities[0].ChunkIndex).To(Equal(5))
	})

	It("rejects model output that does not match the graph extraction schema", func() {
		client := &graphExtractionHTTPClientStub{
			responses: []graphExtractionHTTPResponse{{
				statusCode: http.StatusOK,
				body:       graphChatResponse(`{"entities":[{"id":"ada","name":"Ada","type":"person","description":""}],"relations":[]}`),
			}},
		}
		extractor, err := materialization.NewModelServingGraphExtractorWithClient(validModelServingGraphExtractorConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		extraction, err := extractor.ExtractGraph(context.Background(), []model.GraphChunk{{ChunkIndex: 0, SourceText: "Ada"}}, validGraphExtractionStrategy())

		Expect(extraction).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphExtractionInvalid)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("graph extraction document does not match graph_extraction_v1")))
	})

	It("canonicalizes relation endpoints that use unambiguous entity names", func() {
		client := &graphExtractionHTTPClientStub{
			responses: []graphExtractionHTTPResponse{{
				statusCode: http.StatusOK,
				body: graphChatResponse(`{"entities":[` +
					`{"id":"aurora_relay","name":"Aurora Relay","type":"product","description":"Aurora Relay is mentioned."},` +
					`{"id":"beacon_hub","name":"Beacon Hub","type":"product","description":"Beacon Hub is mentioned."}` +
					`],"relations":[{"source":"Aurora Relay","target":"Beacon Hub","type":"CONNECTS","description":"Aurora Relay connects Beacon Hub.","weight":1}]}`),
			}},
		}
		extractor, err := materialization.NewModelServingGraphExtractorWithClient(validModelServingGraphExtractorConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		extraction, err := extractor.ExtractGraph(context.Background(), []model.GraphChunk{{ChunkIndex: 0, SourceText: "Aurora Relay connects Beacon Hub."}}, validGraphExtractionStrategy())

		Expect(err).NotTo(HaveOccurred())
		Expect(extraction.Relations).To(HaveLen(1))
		Expect(extraction.Relations[0].Source).To(Equal("aurora_relay"))
		Expect(extraction.Relations[0].Target).To(Equal("beacon_hub"))
	})

	It("canonicalizes relation endpoints that differ only by punctuation or separators", func() {
		client := &graphExtractionHTTPClientStub{
			responses: []graphExtractionHTTPResponse{{
				statusCode: http.StatusOK,
				body: graphChatResponse(`{"entities":[` +
					`{"id":"aurora_relay","name":"Aurora Relay","type":"product","description":"Aurora Relay is mentioned."},` +
					`{"id":"beacon_hub","name":"Beacon Hub","type":"product","description":"Beacon Hub is mentioned."}` +
					`],"relations":[{"source":"Aurora-Relay.","target":"Beacon Hub","type":"CONNECTS","description":"Aurora Relay connects Beacon Hub.","weight":1}]}`),
			}},
		}
		extractor, err := materialization.NewModelServingGraphExtractorWithClient(validModelServingGraphExtractorConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		extraction, err := extractor.ExtractGraph(context.Background(), []model.GraphChunk{{ChunkIndex: 0, SourceText: "Aurora Relay connects Beacon Hub."}}, validGraphExtractionStrategy())

		Expect(err).NotTo(HaveOccurred())
		Expect(extraction.Relations).To(HaveLen(1))
		Expect(extraction.Relations[0].Source).To(Equal("aurora_relay"))
		Expect(extraction.Relations[0].Target).To(Equal("beacon_hub"))
	})

	It("repairs relation endpoints into entities only when they are grounded in the source chunk", func() {
		client := &graphExtractionHTTPClientStub{
			responses: []graphExtractionHTTPResponse{{
				statusCode: http.StatusOK,
				body:       graphChatResponse(`{"entities":[],"relations":[{"source":"Aurora Relay","target":"Beacon Hub","type":"CONNECTS","description":"Aurora Relay connects Beacon Hub.","weight":1}]}`),
			}},
		}
		extractor, err := materialization.NewModelServingGraphExtractorWithClient(validModelServingGraphExtractorConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		extraction, err := extractor.ExtractGraph(context.Background(), []model.GraphChunk{{ChunkIndex: 0, SourceText: "Aurora Relay connects Beacon Hub."}}, validGraphExtractionStrategy())

		Expect(err).NotTo(HaveOccurred())
		Expect(extraction.Entities).To(HaveLen(2))
		Expect(extraction.Entities[0].ID).To(Equal("aurora_relay"))
		Expect(extraction.Entities[1].ID).To(Equal("beacon_hub"))
		Expect(extraction.Relations).To(HaveLen(1))
		Expect(extraction.Relations[0].Source).To(Equal("aurora_relay"))
		Expect(extraction.Relations[0].Target).To(Equal("beacon_hub"))
	})

	It("rejects relation endpoints that are not grounded in the source chunk", func() {
		client := &graphExtractionHTTPClientStub{
			responses: []graphExtractionHTTPResponse{{
				statusCode: http.StatusOK,
				body:       graphChatResponse(`{"entities":[],"relations":[{"source":"Ghost System","target":"Beacon Hub","type":"CONNECTS","description":"Ghost System connects Beacon Hub.","weight":1}]}`),
			}},
		}
		extractor, err := materialization.NewModelServingGraphExtractorWithClient(validModelServingGraphExtractorConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		extraction, err := extractor.ExtractGraph(context.Background(), []model.GraphChunk{{ChunkIndex: 0, SourceText: "Aurora Relay connects Beacon Hub."}}, validGraphExtractionStrategy())

		Expect(extraction).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphExtractionInvalid)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring(`relations[0].source must reference an entity id: "Ghost System"`)))
	})

	It("rejects relation endpoints that match ambiguous entity names", func() {
		client := &graphExtractionHTTPClientStub{
			responses: []graphExtractionHTTPResponse{{
				statusCode: http.StatusOK,
				body: graphChatResponse(`{"entities":[` +
					`{"id":"ada_person","name":"Ada","type":"person","description":"Ada is mentioned."},` +
					`{"id":"ada_product","name":"Ada","type":"product","description":"Ada is mentioned."},` +
					`{"id":"bighill","name":"BigHill","type":"organization","description":"BigHill is mentioned."}` +
					`],"relations":[{"source":"Ada","target":"BigHill","type":"FOUNDED","description":"Ada founded BigHill.","weight":1}]}`),
			}},
		}
		extractor, err := materialization.NewModelServingGraphExtractorWithClient(validModelServingGraphExtractorConfig(), client)
		Expect(err).NotTo(HaveOccurred())

		extraction, err := extractor.ExtractGraph(context.Background(), []model.GraphChunk{{ChunkIndex: 0, SourceText: "Ada founded BigHill."}}, validGraphExtractionStrategy())

		Expect(extraction).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphExtractionInvalid)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("relations[0].source must reference an entity id")))
	})

	It("rejects responses over the configured response cap", func() {
		client := &graphExtractionHTTPClientStub{
			responses: []graphExtractionHTTPResponse{{
				statusCode: http.StatusOK,
				body:       strings.Repeat("x", 64),
			}},
		}
		config := validModelServingGraphExtractorConfig()
		config.MaxResponseBytes = 8
		extractor, err := materialization.NewModelServingGraphExtractorWithClient(config, client)
		Expect(err).NotTo(HaveOccurred())

		extraction, err := extractor.ExtractGraph(context.Background(), []model.GraphChunk{{ChunkIndex: 0, SourceText: "Ada"}}, validGraphExtractionStrategy())

		Expect(extraction).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("graph extraction response exceeded max bytes")))
	})

	It("retries a failed parse before marking the chunk failed", func() {
		client := &graphExtractionHTTPClientStub{
			responses: []graphExtractionHTTPResponse{
				{statusCode: http.StatusOK, body: graphChatResponse(`not-json`)},
				{statusCode: http.StatusOK, body: graphChatResponse(`{"entities":[],"relations":[]}`)},
			},
		}
		config := validModelServingGraphExtractorConfig()
		config.MaxRetries = 1
		extractor, err := materialization.NewModelServingGraphExtractorWithClient(config, client)
		Expect(err).NotTo(HaveOccurred())

		extraction, err := extractor.ExtractGraph(context.Background(), []model.GraphChunk{{ChunkIndex: 0, SourceText: "plain text"}}, validGraphExtractionStrategy())

		Expect(err).NotTo(HaveOccurred())
		Expect(extraction.Entities).To(BeEmpty())
		Expect(client.requests).To(HaveLen(2))
	})

	It("passes a configured bearer token to the extraction endpoint", func() {
		client := &graphExtractionHTTPClientStub{}
		config := validModelServingGraphExtractorConfig()
		config.AuthToken = "secret-token"
		extractor, err := materialization.NewModelServingGraphExtractorWithClient(config, client)
		Expect(err).NotTo(HaveOccurred())

		_, err = extractor.ExtractGraph(context.Background(), []model.GraphChunk{{ChunkIndex: 0, SourceText: "Ada founded BigHill."}}, validGraphExtractionStrategy())

		Expect(err).NotTo(HaveOccurred())
		Expect(client.authorizationHeaders).To(Equal([]string{"Bearer secret-token"}))
	})
})

type graphExtractionHTTPResponse struct {
	statusCode int
	body       string
}

type graphExtractionHTTPClientStub struct {
	responses            []graphExtractionHTTPResponse
	requests             []string
	authorizationHeaders []string
}

func (c *graphExtractionHTTPClientStub) Do(req *http.Request) (*http.Response, error) {
	log.Trace("graphExtractionHTTPClientStub Do")

	body, err := io.ReadAll(req.Body)
	Expect(err).NotTo(HaveOccurred())
	c.requests = append(c.requests, string(body))
	c.authorizationHeaders = append(c.authorizationHeaders, req.Header.Get("Authorization"))

	response := graphExtractionHTTPResponse{statusCode: http.StatusOK, body: graphChatResponse(`{"entities":[],"relations":[]}`)}
	if len(c.responses) > 0 {
		response = c.responses[0]
		c.responses = c.responses[1:]
	}
	return &http.Response{
		StatusCode: response.statusCode,
		Body:       io.NopCloser(strings.NewReader(response.body)),
	}, nil
}

func validModelServingGraphExtractorConfig() materialization.ModelServingGraphExtractorConfig {
	log.Trace("validModelServingGraphExtractorConfig")

	return materialization.ModelServingGraphExtractorConfig{
		Endpoint:         "http://graph-model/v1/chat/completions",
		Timeout:          time.Second,
		MaxResponseBytes: 1024,
		MaxOutputTokens:  512,
		MaxRetries:       0,
	}
}

func validGraphExtractionStrategy() model.GraphExtractionStrategy {
	log.Trace("validGraphExtractionStrategy")

	return model.GraphExtractionStrategy{
		ExtractionModel:         "graph-model",
		ExtractionPromptVersion: model.DefaultGraphExtractionPromptVersion,
		ExtractionSchemaVersion: model.DefaultGraphExtractionSchemaVersion,
	}
}

func graphChatResponse(content string) string {
	log.Trace("graphChatResponse")

	return `{"choices":[{"message":{"role":"assistant","content":` + strconvQuote(content) + `}}]}`
}

func strconvQuote(value string) string {
	log.Trace("strconvQuote")

	return strconv.Quote(value)
}
