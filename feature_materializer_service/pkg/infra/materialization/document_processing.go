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

const (
	sourceTextField = "source_text"

	htmlExtractorName    = "go-html-text-extractor"
	htmlExtractorVersion = "v1"

	htmlTagScript     = "script"
	htmlTagStyle      = "style"
	htmlTagNoScript   = "noscript"
	htmlTagTemplate   = "template"
	htmlTagH1         = "h1"
	htmlTagH2         = "h2"
	htmlTagH3         = "h3"
	htmlTagH4         = "h4"
	htmlTagH5         = "h5"
	htmlTagH6         = "h6"
	htmlTagParagraph  = "p"
	htmlTagListItem   = "li"
	htmlTagBlockquote = "blockquote"
	htmlTagPre        = "pre"
	htmlTagCode       = "code"
	htmlTagTableData  = "td"
	htmlTagTableHead  = "th"

	documentSectionKindHeading   = "heading"
	documentSectionKindParagraph = "paragraph"
)

type DocumentExtraction struct {
	Text      string
	PageCount int
	Sections  []DocumentSection
}

type DocumentSection struct {
	Text       string
	Kind       string
	Level      int
	PageNumber int
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
		Sections:  plainDocumentSections(extraction.Text),
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
	var sections []DocumentSection
	var walk func(*html.Node, bool)
	walk = func(n *html.Node, skip bool) {
		if n == nil {
			return
		}
		if n.Type == html.ElementNode {
			switch strings.ToLower(n.Data) {
			case htmlTagScript, htmlTagStyle, htmlTagNoScript, htmlTagTemplate:
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
	collectHTMLSections(doc, false, &sections)

	return &DocumentExtraction{
		Text:      strings.Join(parts, " "),
		PageCount: 0,
		Sections:  sections,
	}, nil
}

func collectHTMLSections(n *html.Node, skip bool, sections *[]DocumentSection) {
	log.Trace("collectHTMLSections")

	if n == nil {
		return
	}
	if n.Type == html.ElementNode {
		switch strings.ToLower(n.Data) {
		case htmlTagScript, htmlTagStyle, htmlTagNoScript, htmlTagTemplate:
			skip = true
		}
	}
	if !skip && n.Type == html.ElementNode {
		if kind, level, ok := htmlSectionKind(n.Data); ok {
			text := strings.TrimSpace(nodeText(n))
			if text != "" {
				*sections = append(*sections, DocumentSection{Text: text, Kind: kind, Level: level})
				return
			}
		}
	}
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		collectHTMLSections(child, skip, sections)
	}
}

func htmlSectionKind(tag string) (string, int, bool) {
	log.Trace("htmlSectionKind")

	switch strings.ToLower(tag) {
	case htmlTagH1:
		return documentSectionKindHeading, 1, true
	case htmlTagH2:
		return documentSectionKindHeading, 2, true
	case htmlTagH3:
		return documentSectionKindHeading, 3, true
	case htmlTagH4:
		return documentSectionKindHeading, 4, true
	case htmlTagH5:
		return documentSectionKindHeading, 5, true
	case htmlTagH6:
		return documentSectionKindHeading, 6, true
	case htmlTagParagraph, htmlTagListItem, htmlTagBlockquote, htmlTagPre, htmlTagCode, htmlTagTableData, htmlTagTableHead:
		return strings.ToLower(tag), 0, true
	default:
		return "", 0, false
	}
}

func nodeText(n *html.Node) string {
	log.Trace("nodeText")

	var parts []string
	var walk func(*html.Node, bool)
	walk = func(current *html.Node, skip bool) {
		if current == nil {
			return
		}
		if current.Type == html.ElementNode {
			switch strings.ToLower(current.Data) {
			case htmlTagScript, htmlTagStyle, htmlTagNoScript, htmlTagTemplate:
				skip = true
			}
		}
		if !skip && current.Type == html.TextNode {
			text := strings.TrimSpace(current.Data)
			if text != "" {
				parts = append(parts, text)
			}
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			walk(child, skip)
		}
	}
	walk(n, false)
	return strings.Join(parts, " ")
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

func sectionTexts(extraction *DocumentExtraction) []string {
	log.Trace("sectionTexts")

	if extraction == nil {
		return nil
	}
	if len(extraction.Sections) == 0 {
		return plainTextSections(extraction.Text)
	}
	texts := make([]string, 0, len(extraction.Sections))
	for _, section := range extraction.Sections {
		text := strings.TrimSpace(section.Text)
		if text != "" {
			texts = append(texts, text)
		}
	}
	if len(texts) == 0 {
		return plainTextSections(extraction.Text)
	}
	return texts
}

func plainTextSections(text string) []string {
	log.Trace("plainTextSections")

	text = strings.ReplaceAll(text, "\r\n", "\n")
	paragraphs := strings.Split(text, "\n\n")
	sections := make([]string, 0, len(paragraphs))
	for _, paragraph := range paragraphs {
		paragraph = strings.TrimSpace(paragraph)
		if paragraph != "" {
			sections = append(sections, paragraph)
		}
	}
	if len(sections) == 0 && strings.TrimSpace(text) != "" {
		return []string{strings.TrimSpace(text)}
	}
	return sections
}

func plainDocumentSections(text string) []DocumentSection {
	log.Trace("plainDocumentSections")

	texts := plainTextSections(text)
	sections := make([]DocumentSection, 0, len(texts))
	for _, text := range texts {
		sections = append(sections, DocumentSection{Text: text, Kind: documentSectionKindParagraph})
	}
	return sections
}

func markdownSections(text string) []string {
	log.Trace("markdownSections")

	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	var sections []string
	var current []string
	flush := func() {
		if len(current) == 0 {
			return
		}
		section := strings.TrimSpace(strings.Join(current, "\n"))
		if section != "" {
			sections = append(sections, section)
		}
		current = nil
	}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || trimmed == "" {
			flush()
			if trimmed != "" {
				current = append(current, trimmed)
			}
			continue
		}
		current = append(current, trimmed)
	}
	flush()
	if len(sections) == 0 {
		return plainTextSections(text)
	}
	return sections
}
