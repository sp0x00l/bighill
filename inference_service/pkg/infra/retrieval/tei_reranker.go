package retrieval

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"inference_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

const DefaultTEIRerankerTimeout = 30 * time.Second

type TEIReranker struct {
	url    string
	model  string
	client *http.Client
}

func NewTEIReranker(url, modelName string, timeout time.Duration) (*TEIReranker, error) {
	log.Trace("NewTEIReranker")

	return NewTEIRerankerWithClient(url, modelName, timeout, nil)
}

func NewTEIRerankerWithClient(url, modelName string, timeout time.Duration, client *http.Client) (*TEIReranker, error) {
	log.Trace("NewTEIRerankerWithClient")

	url = strings.TrimRight(strings.TrimSpace(url), "/")
	modelName = strings.TrimSpace(modelName)
	if url == "" {
		return nil, fmt.Errorf("reranker url is required")
	}
	if modelName == "" {
		return nil, fmt.Errorf("reranker model is required")
	}
	if timeout <= 0 {
		timeout = DefaultTEIRerankerTimeout
	}
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	return &TEIReranker{
		url:    url,
		model:  modelName,
		client: client,
	}, nil
}

func (r *TEIReranker) Rerank(ctx context.Context, query string, candidates []model.RetrievedContext, topK int) ([]model.RetrievedContext, error) {
	log.Trace("TEIReranker Rerank")

	if len(candidates) == 0 {
		return candidates, nil
	}
	texts := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		texts = append(texts, candidate.SourceText)
	}
	results, err := r.postRerank(ctx, query, texts)
	if err != nil {
		return nil, err
	}
	reranked := make([]model.RetrievedContext, 0, len(results))
	seen := make(map[int]struct{}, len(results))
	for _, result := range results {
		if result.Index < 0 || result.Index >= len(candidates) {
			return nil, fmt.Errorf("reranker returned out-of-range index %d", result.Index)
		}
		if _, exists := seen[result.Index]; exists {
			continue
		}
		seen[result.Index] = struct{}{}
		next := candidates[result.Index]
		next.RerankScore = result.Score
		reranked = append(reranked, next)
	}
	if len(reranked) == 0 {
		return nil, fmt.Errorf("reranker returned no results")
	}
	if topK < len(reranked) {
		reranked = reranked[:topK]
	}
	return reranked, nil
}

func (r *TEIReranker) postRerank(ctx context.Context, query string, texts []string) ([]teiRerankResult, error) {
	log.Trace("TEIReranker postRerank")

	body, err := json.Marshal(teiRerankRequest{
		Query:      query,
		Texts:      texts,
		ReturnText: false,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal rerank request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url+"/rerank", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build rerank request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rerank request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("rerank failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read rerank response: %w", err)
	}
	results, err := decodeRerankResults(raw)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	return results, nil
}

func (r *TEIReranker) Model() string {
	log.Trace("TEIReranker Model")

	return r.model
}

func decodeRerankResults(raw []byte) ([]teiRerankResult, error) {
	log.Trace("decodeRerankResults")

	var direct []teiRerankResult
	if err := json.Unmarshal(raw, &direct); err == nil {
		return direct, nil
	}
	var wrapped teiRerankResponse
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		return nil, fmt.Errorf("decode rerank response: %w", err)
	}
	return wrapped.Results, nil
}

type teiRerankRequest struct {
	Query      string   `json:"query"`
	Texts      []string `json:"texts"`
	ReturnText bool     `json:"return_text"`
}

type teiRerankResponse struct {
	Results []teiRerankResult `json:"results"`
}

type teiRerankResult struct {
	Index int     `json:"index"`
	Score float64 `json:"score"`
}
