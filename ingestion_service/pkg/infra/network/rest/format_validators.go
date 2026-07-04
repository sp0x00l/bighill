package rest

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	log "github.com/sirupsen/logrus"
)

func IsPDF(ctx context.Context, file io.ReadSeeker, fileSize int) bool {
	log.Trace("rest IsPDF")

	if fileSize < 5 {
		return false
	}
	defer file.Seek(0, io.SeekStart)

	header := make([]byte, 5)
	if _, err := io.ReadFull(file, header); err != nil {
		log.WithContext(ctx).Warnf("failed to read PDF header: %v", err)
		return false
	}
	return bytes.Equal(header, []byte("%PDF-"))
}

func IsHTML(ctx context.Context, file io.ReadSeeker, fileSize int) bool {
	log.Trace("rest IsHTML")

	sample, ok := readTextSample(ctx, file, fileSize)
	if !ok {
		return false
	}
	lower := strings.ToLower(string(sample))
	return strings.Contains(lower, "<!doctype html") ||
		strings.Contains(lower, "<html") ||
		strings.Contains(lower, "<body") ||
		strings.Contains(lower, "<article") ||
		strings.Contains(lower, "<main") ||
		strings.Contains(lower, "<p>")
}

func IsMarkdown(ctx context.Context, file io.ReadSeeker, fileSize int) bool {
	log.Trace("rest IsMarkdown")

	return IsText(ctx, file, fileSize)
}

func IsText(ctx context.Context, file io.ReadSeeker, fileSize int) bool {
	log.Trace("rest IsText")

	sample, ok := readTextSample(ctx, file, fileSize)
	if !ok {
		return false
	}
	return strings.TrimSpace(string(sample)) != ""
}

// https://parquet.apache.org/docs/file-format/
// IsParquet checks if the given file is a valid Parquet file by checking
// the magic number PAR1 at the beginning and end of the file.
func IsParquet(ctx context.Context, file io.ReadSeeker, fileSize int) bool {
	log.Trace("rest IsParquet")

	// Parquet files are at least 8 bytes long
	if fileSize < 8 {
		return false
	}

	// Reset the file pointer to the beginning
	defer file.Seek(0, io.SeekStart)

	parquetMarker := []byte("PAR1")

	// Check the 4-byte magic number at the beginning of the file
	header := make([]byte, 4)
	if _, err := io.ReadFull(file, header); err != nil {
		log.WithContext(ctx).Warnf("failed to read file start: %v", err)
		return false
	}

	if !bytes.Equal(header, parquetMarker) {
		return false
	}

	// Check the 4-byte magic number at the end of the file
	if _, err := file.Seek(-4, io.SeekEnd); err != nil {
		log.WithContext(ctx).Warnf("failed to seek file end, %v", err)
		return false
	}

	footer := make([]byte, 4)
	if _, err := io.ReadFull(file, footer); err != nil {
		log.WithContext(ctx).Warnf("failed to read file footer, %v", err)
		return false
	}

	return bytes.Equal(footer, parquetMarker)
}

// IsJSON checks if the given file starts and ends with valid JSON characters
// It reads the first and last 512 non-space characters to determine if the file is JSON.
func IsJSON(ctx context.Context, file io.ReadSeeker, fileSize int) bool {
	log.Trace("rest IsJSON")

	// JSON files must be at least 2 bytes long
	if fileSize < 2 {
		log.WithContext(ctx).Warn("file too small to be valid JSON")
		return false
	}

	// Reset the file pointer to the beginning
	defer file.Seek(0, io.SeekStart)

	readSizeBytes := 512
	buffer := make([]byte, min(fileSize, readSizeBytes)) // Read at most 512 bytes

	// Read the first portion of the file
	if _, err := io.ReadFull(file, buffer); err != nil {
		log.WithContext(ctx).Warnf("failed to read file start, %v", err)
		return false
	}

	opening := firstNonSpaceChar(buffer)
	if opening == "" {
		log.WithContext(ctx).Warn("no non-space characters found in the first 512 bytes")
		return false
	}
	if opening != "{" && opening != "[" {
		log.WithContext(ctx).Warn("invalid opening character, expected '{' or '['")
		return false
	}

	expectedClosing := "}"
	if opening == "[" {
		expectedClosing = "]"
	}

	// If the file is larger than 512 bytes, read the last portion of the file
	if fileSize > readSizeBytes {
		if _, err := file.Seek(int64(-readSizeBytes), io.SeekEnd); err != nil {
			log.WithContext(ctx).Warnf("failed to seek to the end of the file, %v", err)
			return false
		}

		if _, err := io.ReadFull(file, buffer); err != nil {
			log.WithContext(ctx).Warnf("failed to read file end, %v", err)
			return false
		}
	}

	closing := lastNonSpaceChar(buffer)
	if closing == "" {
		log.WithContext(ctx).Warn("no non-space characters found in the last 512 bytes")
		return false
	}
	return closing == expectedClosing
}

// IsCSV checks if the given file is a valid CSV file by reading the header and the first record
func IsCSV(ctx context.Context, file io.ReadSeeker, fileSize int) bool {
	log.Trace("rest IsCSV")

	if fileSize == 0 {
		return false
	}

	// Reset the file pointer to the beginning
	defer file.Seek(0, io.SeekStart)

	// Limit the reader to at most 1MB
	readerLimitBytes := min(fileSize, 1*1000*1000)
	reader, headerFields := getReaderAndHeader(ctx, file, int64(readerLimitBytes))
	if reader == nil {
		return false
	}

	// Detect UTF-8 BOM and remove it if present
	hasUTF8BOM := false
	hasUTF8BOM, headerFields[0] = detectUTF8BOM([]byte(headerFields[0]))

	if !validateHeaderFields(ctx, headerFields, hasUTF8BOM) {
		return false
	}

	// Read the first record from the CSV file
	recordFields, err := reader.Read()
	if err != nil {
		log.WithContext(ctx).Warnf("failed to read first CSV record, %v", err)
		return false
	}

	if !validateRecordFields(ctx, recordFields, hasUTF8BOM) {
		return false
	}

	return true
}

func readTextSample(ctx context.Context, file io.ReadSeeker, fileSize int) ([]byte, bool) {
	log.Trace("rest readTextSample")

	if fileSize <= 0 {
		return nil, false
	}
	defer file.Seek(0, io.SeekStart)

	readSizeBytes := min(fileSize, 1*1000*1000)
	buffer := make([]byte, readSizeBytes)
	n, err := io.ReadFull(file, buffer)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		log.WithContext(ctx).Warnf("failed to read text sample, %v", err)
		return nil, false
	}
	buffer = buffer[:n]
	if len(buffer) == 0 || !utf8.Valid(buffer) {
		buffer = trimPartialUTF8Suffix(buffer)
	}
	if len(buffer) == 0 || !utf8.Valid(buffer) {
		return nil, false
	}
	return buffer, true
}

func trimPartialUTF8Suffix(buffer []byte) []byte {
	log.Trace("rest trimPartialUTF8Suffix")

	for len(buffer) > 0 {
		r, size := utf8.DecodeLastRune(buffer)
		if r != utf8.RuneError || size != 1 {
			return buffer
		}
		buffer = buffer[:len(buffer)-1]
	}
	return buffer
}

// firstNonSpaceChar returns the first non-space character in the byte slice.
func firstNonSpaceChar(buffer []byte) string {
	log.Trace("rest firstNonSpaceChar")
	for _, b := range buffer {
		// Check if the byte is not a space character
		// https://golang.org/pkg/unicode/#IsSpace
		if !unicode.IsSpace(rune(b)) {
			return string(b)
		}
	}
	return ""
}

// lastNonSpaceChar returns the last non-space character in the byte slice.
func lastNonSpaceChar(buffer []byte) string {
	log.Trace("rest lastNonSpaceChar")
	for i := len(buffer) - 1; i >= 0; i-- {
		if !unicode.IsSpace(rune(buffer[i])) {
			return string(buffer[i])
		}
	}
	return ""
}

// getReaderAndHeader reads the first 1MB of the file to determine the CSV reader and header
// If the header has only one field, it will try to read the header with the next delimiter until it finds a valid delimiter.
// If no delimiter was found it returns the last delimiter tried as in this case there is only one field in the header.
func getReaderAndHeader(ctx context.Context, file io.ReadSeeker, readerLimit int64) (*csv.Reader, []string) {
	log.Trace("rest getReaderAndHeader")
	var (
		headerFields []string
		err          error
		reader       *csv.Reader
	)

	supportedDelimiters := []rune{',', ';', '\t', '|'}
	for _, delim := range supportedDelimiters {
		// Reset the file pointer to the beginning as this is a new reader
		file.Seek(0, io.SeekStart)
		reader = csv.NewReader(io.LimitReader(file, readerLimit))
		reader.Comma = delim
		reader.ReuseRecord = true // Reuse the same slice for each record to reduce allocations
		reader.TrimLeadingSpace = true

		// Read the header which is the first row
		headerFields, err = reader.Read()
		if err != nil {
			log.WithContext(ctx).Warnf("failed to read CSV header with delimiter '%c': %v", delim, err)
			return nil, nil
		}

		if len(headerFields) == 1 {
			if len(headerFields[0]) == 0 {
				log.WithContext(ctx).Warnf("empty CSV header with delimiter '%c'", delim)
				return nil, nil
			}
			// If the returned header has only one field there is a chance that the current delimiter is incorrect
			// In this case, we should try the next delimiter.
		} else {
			break // Found a valid delimiter
		}
	}

	return reader, headerFields
}

// detectUTF8BOM checks if the given data starts with a UTF-8 Byte Order Mark (BOM) and removes it if present
func detectUTF8BOM(data []byte) (bool, string) {
	log.Trace("rest detectUTF8BOM")
	// https://en.wikipedia.org/wiki/Byte_order_mark
	bomPrefix := []byte{0xEF, 0xBB, 0xBF}

	if bytes.HasPrefix(data, bomPrefix) {
		res := bytes.TrimPrefix(data, bomPrefix)
		return true, string(res)
	}

	return false, string(data)
}

// validateHeaderFields checks if the header fields are valid
func validateHeaderFields(ctx context.Context, fields []string, hasUTF8BOM bool) bool {
	log.Trace("rest validateHeaderFields")
	for _, field := range fields {
		if field == "" {
			log.WithContext(ctx).Warn("header contains empty fields")
			return false
		}

		if !hasUTF8BOM && !utf8.ValidString(field) {
			log.WithContext(ctx).Warnf("header contains non-UTF-8 value: %s", field)
			return false
		}
	}

	return true
}

// validateRecordFields checks if the record fields are valid
func validateRecordFields(ctx context.Context, fields []string, hasUTF8BOM bool) bool {
	log.Trace("rest validateRecordFields")
	allEmpty := true
	for _, field := range fields {
		if field != "" {
			if !hasUTF8BOM && !utf8.ValidString(field) {
				log.WithContext(ctx).Warnf("record contains non-UTF-8 value: %s", field)
				return false
			}
			allEmpty = false
		}
	}
	if allEmpty {
		log.WithContext(ctx).Warn("all values in the first record are empty")
		return false
	}

	return true
}
