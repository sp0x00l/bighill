package retrieval_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"inference_service/pkg/domain/model"
	"inference_service/pkg/infra/retrieval"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRetrieval(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference retrieval unit test suite")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

var _ = Describe("TEIReranker", func() {
	It("posts query and texts to TEI and returns reranked contexts", func() {
		var requestBody []byte
		reranker, err := retrieval.NewTEIRerankerWithClient("http://tei.local", "bge-reranker", 0, &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				Expect(req.Method).To(Equal(http.MethodPost))
				Expect(req.URL.String()).To(Equal("http://tei.local/rerank"))
				var err error
				requestBody, err = io.ReadAll(req.Body)
				Expect(err).NotTo(HaveOccurred())
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(`[{"index":2,"score":0.99},{"index":0,"score":0.91},{"index":1,"score":0.42}]`)),
					Header:     make(http.Header),
				}, nil
			}),
		})
		Expect(err).NotTo(HaveOccurred())
		candidates := []model.RetrievedContext{
			{EmbeddingRecordID: uuid.New(), SourceText: "alpha", Similarity: 0.7},
			{EmbeddingRecordID: uuid.New(), SourceText: "beta", Similarity: 0.8},
			{EmbeddingRecordID: uuid.New(), SourceText: "gamma", Similarity: 0.6},
		}

		out, err := reranker.Rerank(context.Background(), "question", candidates, 2)

		Expect(err).NotTo(HaveOccurred())
		Expect(string(requestBody)).To(MatchJSON(`{"query":"question","texts":["alpha","beta","gamma"],"return_text":false}`))
		Expect(out).To(HaveLen(2))
		Expect(out[0].SourceText).To(Equal("gamma"))
		Expect(out[0].Similarity).To(Equal(0.6))
		Expect(out[0].RerankScore).To(Equal(0.99))
		Expect(out[1].SourceText).To(Equal("alpha"))
		Expect(out[1].RerankScore).To(Equal(0.91))
		Expect(reranker.Model()).To(Equal("bge-reranker"))
	})

	It("accepts wrapped TEI result responses", func() {
		reranker, err := retrieval.NewTEIRerankerWithClient("http://tei.local", "bge-reranker", 0, &http.Client{
			Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(`{"results":[{"index":1,"score":0.8}]}`)),
					Header:     make(http.Header),
				}, nil
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		out, err := reranker.Rerank(context.Background(), "question", []model.RetrievedContext{
			{SourceText: "first"},
			{SourceText: "second"},
		}, 1)

		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(HaveLen(1))
		Expect(out[0].SourceText).To(Equal("second"))
		Expect(out[0].RerankScore).To(Equal(0.8))
	})

	It("rejects invalid result indexes", func() {
		reranker, err := retrieval.NewTEIRerankerWithClient("http://tei.local", "bge-reranker", 0, &http.Client{
			Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(`[{"index":5,"score":0.9}]`)),
					Header:     make(http.Header),
				}, nil
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		out, err := reranker.Rerank(context.Background(), "question", []model.RetrievedContext{{SourceText: "only"}}, 1)

		Expect(out).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("out-of-range index 5")))
	})

	It("returns response errors", func() {
		reranker, err := retrieval.NewTEIRerankerWithClient("http://tei.local", "bge-reranker", 0, &http.Client{
			Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadGateway,
					Body:       io.NopCloser(bytes.NewBufferString("tei unavailable")),
					Header:     make(http.Header),
				}, nil
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		out, err := reranker.Rerank(context.Background(), "question", []model.RetrievedContext{{SourceText: "only"}}, 1)

		Expect(out).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("rerank failed with status 502: tei unavailable")))
	})
})
