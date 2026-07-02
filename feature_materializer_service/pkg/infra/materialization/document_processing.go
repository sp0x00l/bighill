package materialization

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"unicode"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	pdfextractor "lib/pdf_extractor_lib/pkg"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/html"
)

const sourceTextField = "source_text"

const (
	htmlExtractorName    = "go-html-text-extractor"
	htmlExtractorVersion = "v1"
)

type DocumentExtraction struct {
	Text      string
	PageCount int
}

type DocumentExtractor interface {
	Name() string
	Version() string
	ExtractText(context.Context, []byte) (*DocumentExtraction, error)
}

type PDFDocumentExtractor struct {
	extractor *pdfextractor.Extractor
}

func NewPDFDocumentExtractor() *PDFDocumentExtractor {
	log.Trace("NewPDFDocumentExtractor")

	return &PDFDocumentExtractor{extractor: pdfextractor.NewExtractor()}
}

func (e *PDFDocumentExtractor) Name() string {
	log.Trace("PDFDocumentExtractor Name")

	return e.extractor.Name()
}

func (e *PDFDocumentExtractor) Version() string {
	log.Trace("PDFDocumentExtractor Version")

	return e.extractor.Version()
}

func (e *PDFDocumentExtractor) ExtractText(ctx context.Context, data []byte) (*DocumentExtraction, error) {
	log.Trace("PDFDocumentExtractor ExtractText")

	extraction, err := e.extractor.ExtractText(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("%w: extract pdf text: %w", domain.ErrArtifactRead, err)
	}
	return &DocumentExtraction{
		Text:      extraction.Text,
		PageCount: extraction.PageCount,
	}, nil
}

type HTMLDocumentExtractor struct{}

func NewHTMLDocumentExtractor() *HTMLDocumentExtractor {
	log.Trace("NewHTMLDocumentExtractor")

	return &HTMLDocumentExtractor{}
}

func (e *HTMLDocumentExtractor) Name() string {
	log.Trace("HTMLDocumentExtractor Name")

	return htmlExtractorName
}

func (e *HTMLDocumentExtractor) Version() string {
	log.Trace("HTMLDocumentExtractor Version")

	return htmlExtractorVersion
}

func (e *HTMLDocumentExtractor) ExtractText(_ context.Context, data []byte) (*DocumentExtraction, error) {
	log.Trace("HTMLDocumentExtractor ExtractText")

	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: parse html: %w", domain.ErrArtifactRead, err)
	}

	var parts []string
	var walk func(*html.Node, bool)
	walk = func(n *html.Node, skip bool) {
		if n == nil {
			return
		}
		if n.Type == html.ElementNode {
			switch strings.ToLower(n.Data) {
			case "script", "style", "noscript", "template":
				skip = true
			}
		}
		if !skip && n.Type == html.TextNode {
			text := strings.TrimSpace(n.Data)
			if text != "" {
				parts = append(parts, text)
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child, skip)
		}
	}
	walk(doc, false)

	return &DocumentExtraction{
		Text:      strings.Join(parts, " "),
		PageCount: 0,
	}, nil
}

type TextCleaner interface {
	Name() string
	Version() string
	Clean(context.Context, string) (string, error)
}

type BasicTextCleaner struct{}

func NewBasicTextCleaner() *BasicTextCleaner {
	log.Trace("NewBasicTextCleaner")

	return &BasicTextCleaner{}
}

func (c *BasicTextCleaner) Name() string {
	log.Trace("BasicTextCleaner Name")

	return model.DefaultCleanerName
}

func (c *BasicTextCleaner) Version() string {
	log.Trace("BasicTextCleaner Version")

	return model.DefaultCleanerVersion
}

func (c *BasicTextCleaner) Clean(_ context.Context, text string) (string, error) {
	log.Trace("BasicTextCleaner Clean")

	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, text)
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return "", nil
	}
	return strings.Join(fields, " "), nil
}

func cleanTextRows(ctx context.Context, cleaner TextCleaner, rows []string) ([]string, error) {
	log.Trace("cleanTextRows")

	if cleaner == nil {
		cleaner = NewBasicTextCleaner()
	}
	cleaned := make([]string, 0, len(rows))
	for _, row := range rows {
		text, err := cleaner.Clean(ctx, row)
		if err != nil {
			return nil, err
		}
		if text != "" {
			cleaned = append(cleaned, text)
		}
	}
	return cleaned, nil
}
