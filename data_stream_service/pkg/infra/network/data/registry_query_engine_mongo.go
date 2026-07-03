package data

import (
	"context"
	domainErrors "data_stream_service/pkg/domain"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	dataregistrypb "lib/data_contracts_lib/data_registry"

	log "github.com/sirupsen/logrus"
)

type mongoFindSpec struct {
	Database   string         `json:"database"`
	Collection string         `json:"collection"`
	Filter     map[string]any `json:"filter"`
	Limit      int64          `json:"limit"`
}

func (e *registryQueryEngine) executeMongo(ctx context.Context, cfg *dataregistrypb.MongoSourceConfig, query *sourceQueryCommand) (*QueryResult, error) {
	log.Trace("registryQueryEngine executeMongo")

	if cfg == nil {
		return nil, domainErrors.ErrValidationFailed.Extend("source connector does not include mongo config")
	}

	spec, err := mongoSpecFromCommand(query)
	if err != nil {
		return nil, err
	}

	clientOptions := options.Client().ApplyURI(mongoConnectionURI(cfg))
	if cfg.GetUsername() != "" {
		clientOptions.SetAuth(options.Credential{
			AuthSource: cfg.GetAuthDatabase(),
			Username:   cfg.GetUsername(),
			Password:   cfg.GetPassword(),
		})
	}

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("connect mongo source: %w", err)
	}
	defer client.Disconnect(ctx)

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("ping mongo source: %w", err)
	}

	filter := bson.M{}
	if len(spec.Filter) > 0 {
		filter = bson.M(spec.Filter)
	}
	findOptions := options.Find()
	if spec.Limit > 0 {
		findOptions.SetLimit(spec.Limit)
	}

	cursor, err := client.Database(spec.Database).Collection(spec.Collection).Find(ctx, filter, findOptions)
	if err != nil {
		return nil, fmt.Errorf("query mongo source: %w", err)
	}
	defer cursor.Close(ctx)

	documents := make([]bson.M, 0)
	for cursor.Next(ctx) {
		var document bson.M
		if err := cursor.Decode(&document); err != nil {
			return nil, fmt.Errorf("read mongo source document: %w", err)
		}
		documents = append(documents, document)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("read mongo source cursor: %w", err)
	}

	return e.mongoDocumentsToArrow(documents)
}

func mongoSpecFromCommand(query *sourceQueryCommand) (*mongoFindSpec, error) {
	log.Trace("mongoSpecFromCommand")

	spec := &mongoFindSpec{
		Database:   query.Database,
		Collection: query.Collection,
		Limit:      query.Limit,
	}

	raw := strings.TrimSpace(query.SQL)
	if raw != "" {
		if strings.HasPrefix(raw, "{") {
			if err := json.Unmarshal([]byte(raw), spec); err != nil {
				return nil, domainErrors.ErrValidationFailed.Extend(fmt.Sprintf("mongo query command must be JSON: %v", err))
			}
		} else {
			parts := strings.Split(raw, ".")
			if len(parts) != 2 {
				return nil, domainErrors.ErrValidationFailed.Extend("mongo query command must be database.collection or JSON")
			}
			spec.Database = strings.TrimSpace(parts[0])
			spec.Collection = strings.TrimSpace(parts[1])
		}
	}

	spec.Database = strings.TrimSpace(spec.Database)
	spec.Collection = strings.TrimSpace(spec.Collection)
	if spec.Database == "" || spec.Collection == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("mongo query command requires database and collection")
	}
	return spec, nil
}

func mongoConnectionURI(cfg *dataregistrypb.MongoSourceConfig) string {
	log.Trace("mongoConnectionURI")

	hosts := make([]string, 0, len(cfg.GetHosts()))
	for _, host := range cfg.GetHosts() {
		hostname := strings.TrimSpace(host.GetHostname())
		if hostname == "" {
			hostname = "localhost"
		}
		port := int(host.GetPort())
		if port == 0 {
			port = 27017
		}
		hosts = append(hosts, fmt.Sprintf("%s:%d", hostname, port))
	}
	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:27017")
	}
	return "mongodb://" + strings.Join(hosts, ",")
}

func (e *registryQueryEngine) mongoDocumentsToArrow(documents []bson.M) (*QueryResult, error) {
	log.Trace("registryQueryEngine mongoDocumentsToArrow")

	if len(documents) == 0 {
		return nil, fmt.Errorf("mongo source query returned no documents")
	}

	columns := mongoDocumentColumns(documents)
	if len(columns) == 0 {
		return nil, fmt.Errorf("mongo source query returned no document fields")
	}

	fields := make([]arrow.Field, len(columns))
	for i, column := range columns {
		fields[i] = arrow.Field{
			Name:     column,
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		}
	}

	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(e.allocator, schema)
	defer builder.Release()

	for _, document := range documents {
		for i, column := range columns {
			value, ok := document[column]
			if !ok || value == nil {
				builder.Field(i).AppendNull()
				continue
			}
			builder.Field(i).(*array.StringBuilder).Append(mongoValueString(value))
		}
	}

	record := builder.NewRecord()
	return &QueryResult{
		Schema:       schema,
		Records:      []arrow.Record{record},
		TotalRecords: int64(len(documents)),
	}, nil
}

func mongoDocumentColumns(documents []bson.M) []string {
	log.Trace("mongoDocumentColumns")

	columnSet := map[string]struct{}{}
	for _, document := range documents {
		for key := range document {
			if key == "_id" {
				continue
			}
			columnSet[key] = struct{}{}
		}
	}

	columns := make([]string, 0, len(columnSet))
	for column := range columnSet {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func mongoValueString(value any) string {
	log.Trace("mongoValueString")

	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		bytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(bytes)
	}
}
