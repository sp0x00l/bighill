package rest_test

import (
	"bytes"
	"context"
	"io"

	serviceRest "data_ingestion_service/pkg/infra/network/rest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Detector", func() {
	It("detects PDF before CSV when the caller provides fallback order", func() {
		content := []byte("%PDF-1.7\n1 0 obj\n<<>>\nendobj\n")
		detector := serviceRest.NewDetector(map[string]serviceRest.FormatValidatorFunc{
			serviceRest.FileTypeCSV:      serviceRest.IsCSV,
			serviceRest.FileTypeJSON:     serviceRest.IsJSON,
			serviceRest.FileTypeParquet:  serviceRest.IsParquet,
			serviceRest.FileTypePDF:      serviceRest.IsPDF,
			serviceRest.FileTypeHTML:     serviceRest.IsHTML,
			serviceRest.FileTypeMarkdown: serviceRest.IsMarkdown,
			serviceRest.FileTypeText:     serviceRest.IsText,
		})

		fileType := detector.DetectFileFormat(context.Background(), bytes.NewReader(content), len(content), []string{
			serviceRest.FileTypeParquet,
			serviceRest.FileTypeJSON,
			serviceRest.FileTypePDF,
			serviceRest.FileTypeCSV,
		})

		Expect(fileType).To(Equal(serviceRest.FileTypePDF))
		Expect(detector.GetContentType(fileType)).To(Equal("application/pdf"))
	})

	It("detects HTML before generic text", func() {
		content := []byte("<!doctype html><html><body><p>hello</p></body></html>")
		detector := serviceRest.NewDetector(map[string]serviceRest.FormatValidatorFunc{
			serviceRest.FileTypeHTML: serviceRest.IsHTML,
			serviceRest.FileTypeText: serviceRest.IsText,
		})

		fileType := detector.DetectFileFormat(context.Background(), bytes.NewReader(content), len(content), []string{
			serviceRest.FileTypeHTML,
			serviceRest.FileTypeText,
		})

		Expect(fileType).To(Equal(serviceRest.FileTypeHTML))
		Expect(detector.GetContentType(fileType)).To(Equal("text/html"))
	})

	It("detects plain text when no structured format matches", func() {
		content := []byte("plain document text")
		detector := serviceRest.NewDetector(map[string]serviceRest.FormatValidatorFunc{
			serviceRest.FileTypeJSON: serviceRest.IsJSON,
			serviceRest.FileTypeHTML: serviceRest.IsHTML,
			serviceRest.FileTypeText: serviceRest.IsText,
		})

		fileType := detector.DetectFileFormat(context.Background(), bytes.NewReader(content), len(content), []string{
			serviceRest.FileTypeJSON,
			serviceRest.FileTypeHTML,
			serviceRest.FileTypeText,
		})

		Expect(fileType).To(Equal(serviceRest.FileTypeText))
		Expect(detector.GetContentType(fileType)).To(Equal("text/plain"))
	})

	It("rejects non-PDF content as PDF", func() {
		content := []byte("title,views\nIntro,10\n")

		Expect(serviceRest.IsPDF(context.Background(), bytes.NewReader(content), len(content))).To(BeFalse())
	})

	It("detects text when the 1MB sample ends on a partial UTF-8 rune", func() {
		content := append(bytes.Repeat([]byte("a"), 999999), []byte("€")...)

		Expect(serviceRest.IsText(context.Background(), bytes.NewReader(content), len(content))).To(BeTrue())
	})

	It("uses full reads for magic-byte validators", func() {
		pdf := []byte("%PDF-1.7\n")
		parquet := []byte("PAR1payloadPAR1")
		json := []byte(`{"ok":true}`)

		Expect(serviceRest.IsPDF(context.Background(), newSlowReadSeeker(pdf), len(pdf))).To(BeTrue())
		Expect(serviceRest.IsParquet(context.Background(), newSlowReadSeeker(parquet), len(parquet))).To(BeTrue())
		Expect(serviceRest.IsJSON(context.Background(), newSlowReadSeeker(json), len(json))).To(BeTrue())
	})
})

type slowReadSeeker struct {
	reader *bytes.Reader
}

func newSlowReadSeeker(content []byte) *slowReadSeeker {
	return &slowReadSeeker{reader: bytes.NewReader(content)}
}

func (r *slowReadSeeker) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.reader.Read(p)
}

func (r *slowReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.reader.Seek(offset, whence)
}

var _ io.ReadSeeker = (*slowReadSeeker)(nil)
