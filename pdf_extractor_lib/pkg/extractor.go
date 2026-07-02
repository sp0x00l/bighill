package pdfextractor

/*
#cgo CFLAGS: -I${SRCDIR}/../cpp/src
#cgo LDFLAGS: -L${SRCDIR}/../cpp/build/bin -L${SRCDIR}/../cpp/prebuilt/lib -lgo_pdf_extractor_lib -lstdc++
#cgo pkg-config: poppler-cpp

#include <stdlib.h>
#include "bridge/go/cgo_pdf_extractor.h"
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

const (
	ExtractorName    = "poppler-cpp-pdf-extractor"
	ExtractorVersion = "v1"
)

type Extraction struct {
	Text      string
	PageCount int
}

type Extractor struct{}

func NewExtractor() *Extractor {
	log.Trace("NewExtractor")

	return &Extractor{}
}

func (e *Extractor) Name() string {
	log.Trace("Extractor Name")

	return ExtractorName
}

func (e *Extractor) Version() string {
	log.Trace("Extractor Version")

	return ExtractorVersion
}

func (e *Extractor) ExtractText(_ context.Context, data []byte) (*Extraction, error) {
	log.Trace("Extractor ExtractText")

	if len(data) == 0 {
		return nil, fmt.Errorf("pdf data is required")
	}

	result := C.pdf_extract_text((*C.char)(unsafe.Pointer(&data[0])), C.int(len(data)))
	defer C.pdf_free_result(result)

	if result == nil {
		return nil, fmt.Errorf("pdf extraction returned nil result")
	}
	if result.ok == 0 {
		return nil, fmt.Errorf("%s", C.GoString(result.error_message))
	}

	return &Extraction{
		Text:      C.GoString(result.text),
		PageCount: int(result.page_count),
	}, nil
}
