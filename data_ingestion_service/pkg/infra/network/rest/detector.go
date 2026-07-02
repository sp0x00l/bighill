package rest

import (
	"context"
	"io"
)

const (
	FileTypeCSV         = "csv"
	FileTypeJSON        = "json"
	FileTypeParquet     = "parquet"
	FileTypePDF         = "pdf"
	FileTypeHTML        = "html"
	FileTypeMarkdown    = "markdown"
	FileTypeText        = "text"
	FileTypeUnsupported = "unsupported"
	DefaultContentType  = "application/octet-stream"
)

type FormatValidatorFunc func(context.Context, io.ReadSeeker, int) bool

type Detector struct {
	validators map[string]FormatValidatorFunc
}

func NewDetector(validators map[string]FormatValidatorFunc) *Detector {
	return &Detector{validators: validators}
}

func (d *Detector) DetectFileFormat(ctx context.Context, file io.ReadSeeker, fileSize int, validFormats []string) string {
	for _, format := range validFormats {
		validator := d.validators[format]
		if validator == nil {
			continue
		}
		_, _ = file.Seek(0, io.SeekStart)
		if validator(ctx, file, fileSize) {
			_, _ = file.Seek(0, io.SeekStart)
			return format
		}
	}
	_, _ = file.Seek(0, io.SeekStart)
	return FileTypeUnsupported
}

func (d *Detector) GetContentType(fileType string) string {
	switch fileType {
	case FileTypeCSV:
		return "text/csv"
	case FileTypeJSON:
		return "application/json"
	case FileTypeParquet:
		return "application/vnd.apache.parquet"
	case FileTypePDF:
		return "application/pdf"
	case FileTypeHTML:
		return "text/html"
	case FileTypeMarkdown:
		return "text/markdown"
	case FileTypeText:
		return "text/plain"
	default:
		return DefaultContentType
	}
}
