package materialization

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"feature_materializer_service/pkg/domain/model"

	tiktoken "github.com/pkoukk/tiktoken-go"
	tiktokenloader "github.com/pkoukk/tiktoken-go-loader"
	log "github.com/sirupsen/logrus"
)

const defaultTiktokenEncoding = "cl100k_base"

type TextChunk struct {
	ChunkIndex int
	Text       string
}

type Chunker interface {
	Chunk(ctx context.Context, rows []string) ([]TextChunk, error)
}

type TokenWindowChunker struct {
	strategy     model.EmbeddingStrategy
	encoding     tokenEncoding
	encodingErr  error
	encodingOnce sync.Once
}

type tokenEncoding interface {
	Encode(text string, allowedSpecial []string, disallowedSpecial []string) []int
	Decode(tokens []int) string
}

func NewTokenWindowChunker(strategy model.EmbeddingStrategy) *TokenWindowChunker {
	log.Trace("NewTokenWindowChunker")

	return &TokenWindowChunker{
		strategy: model.NormalizeEmbeddingStrategy(strategy),
	}
}

func (c *TokenWindowChunker) Chunk(_ context.Context, rows []string) ([]TextChunk, error) {
	log.Trace("TokenWindowChunker Chunk")

	strategy := model.NormalizeEmbeddingStrategy(c.strategy)
	encoding, err := c.getEncoding()
	if err != nil {
		return nil, err
	}

	chunks := make([]TextChunk, 0, len(rows))
	for _, row := range rows {
		row = strings.TrimSpace(row)
		if row == "" {
			continue
		}
		tokens := encoding.Encode(row, nil, nil)
		if len(tokens) == 0 {
			continue
		}
		step := strategy.ChunkSize - strategy.ChunkOverlap
		if step <= 0 {
			step = strategy.ChunkSize
		}
		for start := 0; start < len(tokens); start += step {
			end := start + strategy.ChunkSize
			if end > len(tokens) {
				end = len(tokens)
			}
			text := strings.TrimSpace(encoding.Decode(tokens[start:end]))
			if text == "" {
				if end == len(tokens) {
					break
				}
				continue
			}
			chunks = append(chunks, TextChunk{
				ChunkIndex: len(chunks),
				Text:       text,
			})
			if end == len(tokens) {
				break
			}
		}
	}
	return chunks, nil
}

func (c *TokenWindowChunker) getEncoding() (tokenEncoding, error) {
	log.Trace("TokenWindowChunker getEncoding")

	c.encodingOnce.Do(func() {
		tiktoken.SetBpeLoader(tiktokenloader.NewOfflineLoader())
		c.encoding, c.encodingErr = tiktoken.GetEncoding(defaultTiktokenEncoding)
		if c.encodingErr != nil {
			c.encodingErr = fmt.Errorf("get tiktoken encoding %s: %w", defaultTiktokenEncoding, c.encodingErr)
		}
	})
	return c.encoding, c.encodingErr
}
