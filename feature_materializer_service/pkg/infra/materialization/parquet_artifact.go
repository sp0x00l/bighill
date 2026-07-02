package materialization

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"feature_materializer_service/pkg/domain"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	log "github.com/sirupsen/logrus"
)

const parquetContentType = "application/vnd.apache.parquet"

type ParquetArtifact struct {
	Data           []byte
	SchemaVersion  int
	SchemaMetadata string
	RowCount       int64
}

func NormalizeArtifactToParquet(ctx context.Context, data []byte, contentType, extension string) (*ParquetArtifact, error) {
	log.Trace("NormalizeArtifactToParquet")

	return NormalizeArtifactToParquetWithProcessors(ctx, data, contentType, extension, NewPDFDocumentExtractor(), NewBasicTextCleaner())
}

func NormalizeArtifactToParquetWithProcessors(ctx context.Context, data []byte, contentType, extension string, pdfExtractor DocumentExtractor, cleaner TextCleaner) (*ParquetArtifact, error) {
	log.Trace("NormalizeArtifactToParquetWithProcessors")

	if isParquet(data, contentType, extension) {
		schemaMetadata, rows, err := parquetSchemaMetadata(ctx, data)
		if err != nil {
			return nil, err
		}
		return &ParquetArtifact{
			Data:           data,
			SchemaVersion:  1,
			SchemaMetadata: schemaMetadata,
			RowCount:       rows,
		}, nil
	}

	rows, fields, metadata, err := artifactRows(ctx, data, contentType, extension, pdfExtractor, cleaner)
	if err != nil {
		return nil, err
	}
	parquetBytes, schemaMetadata, err := writeStringTableParquetWithMetadata(fields, rows, metadata)
	if err != nil {
		return nil, err
	}
	return &ParquetArtifact{
		Data:           parquetBytes,
		SchemaVersion:  1,
		SchemaMetadata: schemaMetadata,
		RowCount:       int64(len(rows)),
	}, nil
}

func ExtractTextRowsFromParquet(ctx context.Context, data []byte, maxRows int) ([]string, error) {
	log.Trace("ExtractTextRowsFromParquet")

	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("%w: read parquet table: %w", domain.ErrArtifactRead, err)
	}
	defer table.Release()

	reader := array.NewTableReader(table, 1024)
	defer reader.Release()

	var output []string
	for reader.Next() {
		record := reader.Record()
		for row := 0; row < int(record.NumRows()); row++ {
			values := make([]string, 0, record.NumCols())
			for col := 0; col < int(record.NumCols()); col++ {
				arr := record.Column(col)
				if arr.IsNull(row) {
					continue
				}
				value := strings.TrimSpace(arr.ValueStr(row))
				if value != "" {
					values = append(values, value)
				}
			}
			text := strings.TrimSpace(strings.Join(values, " "))
			if text != "" {
				output = append(output, text)
			}
			if maxRows > 0 && len(output) >= maxRows {
				return output, nil
			}
		}
	}
	return output, reader.Err()
}

func isParquet(data []byte, contentType, extension string) bool {
	extension = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(extension)), ".")
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	return extension == "parquet" ||
		contentType == parquetContentType ||
		(len(data) >= 8 && string(data[:4]) == "PAR1" && string(data[len(data)-4:]) == "PAR1")
}

func artifactRows(ctx context.Context, data []byte, contentType, extension string, pdfExtractor DocumentExtractor, cleaner TextCleaner) ([]map[string]string, []string, map[string]any, error) {
	log.Trace("artifactRows")

	extension = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(extension)), ".")
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	switch {
	case extension == "csv" || strings.Contains(contentType, "csv"):
		rows, fields, err := csvRows(data)
		return rows, fields, map[string]any{"source_format": "csv"}, err
	case extension == "json" || strings.Contains(contentType, "json"):
		rows, fields, err := jsonRows(data)
		return rows, fields, map[string]any{"source_format": "json"}, err
	case extension == "pdf" || contentType == "application/pdf":
		return pdfRows(ctx, data, pdfExtractor, cleaner)
	case strings.HasPrefix(contentType, "text/html") || extension == "html" || extension == "htm":
		return htmlRows(ctx, data, NewHTMLDocumentExtractor(), cleaner)
	case strings.HasPrefix(contentType, "text/markdown") || extension == "md" || extension == "markdown":
		return textRows(ctx, data, cleaner, "markdown")
	case strings.HasPrefix(contentType, "text/plain") || extension == "txt" || extension == "text":
		return textRows(ctx, data, cleaner, "text")
	default:
		return nil, nil, nil, domain.ErrValidationFailed.Extend("unsupported raw artifact format")
	}
}

func pdfRows(ctx context.Context, data []byte, extractor DocumentExtractor, cleaner TextCleaner) ([]map[string]string, []string, map[string]any, error) {
	log.Trace("pdfRows")

	if extractor == nil {
		extractor = NewPDFDocumentExtractor()
	}
	if cleaner == nil {
		cleaner = NewBasicTextCleaner()
	}
	extraction, err := extractor.ExtractText(ctx, data)
	if err != nil {
		return nil, nil, nil, err
	}
	cleaned, err := cleaner.Clean(ctx, extraction.Text)
	if err != nil {
		return nil, nil, nil, err
	}
	if cleaned == "" {
		return nil, nil, nil, domain.ErrValidationFailed.Extend("pdf text is empty")
	}
	metadata := map[string]any{
		"source_format":     "pdf",
		"source_page_count": extraction.PageCount,
		"extractor_name":    extractor.Name(),
		"extractor_version": extractor.Version(),
		"cleaner_name":      cleaner.Name(),
		"cleaner_version":   cleaner.Version(),
	}
	return []map[string]string{{sourceTextField: cleaned}}, []string{sourceTextField}, metadata, nil
}

func htmlRows(ctx context.Context, data []byte, extractor DocumentExtractor, cleaner TextCleaner) ([]map[string]string, []string, map[string]any, error) {
	log.Trace("htmlRows")

	if extractor == nil {
		extractor = NewHTMLDocumentExtractor()
	}
	if cleaner == nil {
		cleaner = NewBasicTextCleaner()
	}
	extraction, err := extractor.ExtractText(ctx, data)
	if err != nil {
		return nil, nil, nil, err
	}
	cleaned, err := cleaner.Clean(ctx, extraction.Text)
	if err != nil {
		return nil, nil, nil, err
	}
	if cleaned == "" {
		return nil, nil, nil, domain.ErrValidationFailed.Extend("html text is empty")
	}
	metadata := map[string]any{
		"source_format":     "html",
		"extractor_name":    extractor.Name(),
		"extractor_version": extractor.Version(),
		"cleaner_name":      cleaner.Name(),
		"cleaner_version":   cleaner.Version(),
	}
	return []map[string]string{{sourceTextField: cleaned}}, []string{sourceTextField}, metadata, nil
}

func textRows(ctx context.Context, data []byte, cleaner TextCleaner, sourceFormat string) ([]map[string]string, []string, map[string]any, error) {
	log.Trace("textRows")

	if cleaner == nil {
		cleaner = NewBasicTextCleaner()
	}
	cleaned, err := cleaner.Clean(ctx, string(data))
	if err != nil {
		return nil, nil, nil, err
	}
	if cleaned == "" {
		return nil, nil, nil, domain.ErrValidationFailed.Extend("text artifact is empty")
	}
	metadata := map[string]any{
		"source_format":   sourceFormat,
		"cleaner_name":    cleaner.Name(),
		"cleaner_version": cleaner.Version(),
	}
	return []map[string]string{{sourceTextField: cleaned}}, []string{sourceTextField}, metadata, nil
}

func csvRows(data []byte) ([]map[string]string, []string, error) {
	log.Trace("csvRows")

	reader := csv.NewReader(bytes.NewReader(data))
	reader.TrimLeadingSpace = true
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: read csv: %w", domain.ErrArtifactRead, err)
	}
	if len(records) < 1 {
		return nil, nil, domain.ErrValidationFailed.Extend("csv header is required")
	}

	fields := normalizeFields(records[0])
	rows := make([]map[string]string, 0, len(records)-1)
	for _, record := range records[1:] {
		row := make(map[string]string, len(fields))
		for i, field := range fields {
			if i < len(record) {
				row[field] = record[i]
			}
		}
		rows = append(rows, row)
	}
	return rows, fields, nil
}

func jsonRows(data []byte) ([]map[string]string, []string, error) {
	log.Trace("jsonRows")

	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		rows := make([]map[string]string, 0, len(lines))
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var item map[string]any
			if err := json.Unmarshal([]byte(line), &item); err != nil {
				return nil, nil, fmt.Errorf("%w: read json line: %w", domain.ErrArtifactRead, err)
			}
			rows = append(rows, stringifyJSONRow(item))
		}
		rows, fields := rowsWithSortedFields(rows)
		return rows, fields, nil
	}

	var rows []map[string]string
	switch value := decoded.(type) {
	case []any:
		for _, item := range value {
			obj, ok := item.(map[string]any)
			if !ok {
				return nil, nil, domain.ErrValidationFailed.Extend("json array must contain objects")
			}
			rows = append(rows, stringifyJSONRow(obj))
		}
	case map[string]any:
		rows = append(rows, stringifyJSONRow(value))
	default:
		return nil, nil, domain.ErrValidationFailed.Extend("json artifact must be an object, object array, or jsonl")
	}
	rows, fields := rowsWithSortedFields(rows)
	return rows, fields, nil
}

func rowsWithSortedFields(rows []map[string]string) ([]map[string]string, []string) {
	fieldSet := map[string]struct{}{}
	for _, row := range rows {
		for field := range row {
			fieldSet[field] = struct{}{}
		}
	}
	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return rows, fields
}

func stringifyJSONRow(row map[string]any) map[string]string {
	out := make(map[string]string, len(row))
	for key, value := range row {
		field := sanitizeFieldName(key, len(out))
		switch v := value.(type) {
		case nil:
			out[field] = ""
		case string:
			out[field] = v
		case float64, bool:
			out[field] = fmt.Sprint(v)
		default:
			encoded, _ := json.Marshal(v)
			out[field] = string(encoded)
		}
	}
	return out
}

func writeStringTableParquet(fields []string, rows []map[string]string) ([]byte, string, error) {
	log.Trace("writeStringTableParquet")

	return writeStringTableParquetWithMetadata(fields, rows, nil)
}

func writeStringTableParquetWithMetadata(fields []string, rows []map[string]string, extraMetadata map[string]any) ([]byte, string, error) {
	log.Trace("writeStringTableParquetWithMetadata")

	if len(fields) == 0 {
		return nil, "", domain.ErrValidationFailed.Extend("at least one column is required")
	}

	arrowFields := make([]arrow.Field, len(fields))
	for i, field := range fields {
		arrowFields[i] = arrow.Field{Name: field, Type: arrow.BinaryTypes.String, Nullable: true}
	}
	schema := arrow.NewSchema(arrowFields, nil)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for _, row := range rows {
		for col, field := range fields {
			builder.Field(col).(*array.StringBuilder).Append(row[field])
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()

	var out bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	writer, err := pqarrow.NewFileWriter(schema, &out, writerProps, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		return nil, "", fmt.Errorf("%w: create parquet writer: %w", domain.ErrArtifactWrite, err)
	}
	if err := writer.Write(record); err != nil {
		return nil, "", fmt.Errorf("%w: write parquet record: %w", domain.ErrArtifactWrite, err)
	}
	if err := writer.Close(); err != nil {
		return nil, "", fmt.Errorf("%w: close parquet writer: %w", domain.ErrArtifactWrite, err)
	}

	schemaMetadata, err := schemaMetadataJSON(schema, int64(len(rows)), extraMetadata)
	if err != nil {
		return nil, "", err
	}
	return out.Bytes(), schemaMetadata, nil
}

func parquetSchemaMetadata(ctx context.Context, data []byte) (string, int64, error) {
	log.Trace("parquetSchemaMetadata")

	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return "", 0, fmt.Errorf("%w: read parquet schema: %w", domain.ErrArtifactRead, err)
	}
	defer table.Release()
	schemaMetadata, err := schemaMetadataJSON(table.Schema(), table.NumRows(), nil)
	return schemaMetadata, table.NumRows(), err
}

func schemaMetadataJSON(schema *arrow.Schema, rows int64, extraMetadata map[string]any) (string, error) {
	log.Trace("schemaMetadataJSON")

	fields := make([]map[string]string, schema.NumFields())
	for i, field := range schema.Fields() {
		fields[i] = map[string]string{
			"name": field.Name,
			"type": field.Type.String(),
		}
	}
	metadata := map[string]any{
		"format": "arrow",
		"rows":   rows,
		"fields": fields,
	}
	for key, value := range extraMetadata {
		metadata[key] = value
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("encode schema metadata: %w", err)
	}
	return string(encoded), nil
}

func MergeSourceSchemaMetadata(schemaMetadata, sourceMetadata string) (string, error) {
	log.Trace("MergeSourceSchemaMetadata")

	sourceMetadata = strings.TrimSpace(sourceMetadata)
	if sourceMetadata == "" || sourceMetadata == "{}" {
		return schemaMetadata, nil
	}

	var target map[string]any
	if err := json.Unmarshal([]byte(schemaMetadata), &target); err != nil {
		return "", fmt.Errorf("%w: decode schema metadata: %w", domain.ErrArtifactRead, err)
	}
	var source map[string]any
	if err := json.Unmarshal([]byte(sourceMetadata), &source); err != nil {
		return "", fmt.Errorf("%w: decode source schema metadata: %w", domain.ErrArtifactRead, err)
	}

	for _, key := range []string{
		"source_format",
		"source_page_count",
		"extractor_name",
		"extractor_version",
		"cleaner_name",
		"cleaner_version",
	} {
		if value, ok := source[key]; ok {
			target[key] = value
		}
	}

	encoded, err := json.Marshal(target)
	if err != nil {
		return "", fmt.Errorf("encode schema metadata: %w", err)
	}
	return string(encoded), nil
}

func normalizeFields(fields []string) []string {
	out := make([]string, len(fields))
	seen := map[string]int{}
	for i, field := range fields {
		normalized := sanitizeFieldName(field, i)
		if count := seen[normalized]; count > 0 {
			seen[normalized] = count + 1
			normalized = fmt.Sprintf("%s_%d", normalized, count+1)
		} else {
			seen[normalized] = 1
		}
		out[i] = normalized
	}
	return out
}

func sanitizeFieldName(value string, index int) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		out = fmt.Sprintf("column_%d", index+1)
	}
	if out[0] >= '0' && out[0] <= '9' {
		out = "column_" + out
	}
	return out
}

func normalizeVector(vector []float32) []float32 {
	var sum float64
	for _, value := range vector {
		sum += float64(value * value)
	}
	if sum == 0 {
		return vector
	}
	norm := float32(math.Sqrt(sum))
	out := make([]float32, len(vector))
	for i, value := range vector {
		out[i] = value / norm
	}
	return out
}
