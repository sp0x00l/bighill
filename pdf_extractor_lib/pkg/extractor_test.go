package pdfextractor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	pdfextractor "lib/pdf_extractor_lib/pkg"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPDFExtractor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PDF extractor lib unit test suite")
}

var _ = Describe("Extractor", func() {
	It("exposes a stable extractor identity", func() {
		extractor := pdfextractor.NewExtractor()

		Expect(extractor.Name()).To(Equal(pdfextractor.ExtractorName))
		Expect(extractor.Version()).To(Equal(pdfextractor.ExtractorVersion))
	})

	It("rejects empty PDF data at the Go boundary", func() {
		extractor := pdfextractor.NewExtractor()

		_, err := extractor.ExtractText(context.Background(), nil)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("pdf data is required"))
	})

	It("extracts text from an in-memory PDF through the C++ boundary", func() {
		extractor := pdfextractor.NewExtractor()

		result, err := extractor.ExtractText(context.Background(), minimalPDF("Hello PDF"))

		Expect(err).NotTo(HaveOccurred())
		Expect(result.PageCount).To(Equal(1))
		Expect(result.Text).To(ContainSubstring("Hello PDF"))
	})
})

func minimalPDF(text string) []byte {
	escaped := strings.NewReplacer(`\`, `\\`, `(`, `\(`, `)`, `\)`).Replace(text)
	stream := fmt.Sprintf("BT /F1 24 Tf 72 720 Td (%s) Tj ET", escaped)
	objects := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
		"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
		fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(stream), stream),
	}

	var builder strings.Builder
	builder.WriteString("%PDF-1.4\n")
	offsets := []int{0}
	for i, object := range objects {
		offsets = append(offsets, builder.Len())
		builder.WriteString(fmt.Sprintf("%d 0 obj\n%s\nendobj\n", i+1, object))
	}
	xrefOffset := builder.Len()
	builder.WriteString(fmt.Sprintf("xref\n0 %d\n", len(objects)+1))
	builder.WriteString("0000000000 65535 f \n")
	for _, offset := range offsets[1:] {
		builder.WriteString(fmt.Sprintf("%010d 00000 n \n", offset))
	}
	builder.WriteString(fmt.Sprintf("trailer\n<< /Root 1 0 R /Size %d >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xrefOffset))
	return []byte(builder.String())
}
