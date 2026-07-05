package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	defaultDataStreamFlightAddress = "localhost:7070"
	postgresDataSourceHost         = "127.0.0.1"
	postgresDataSourcePort         = 5435
	postgresDataSourceDatabase     = "pagila"
	postgresDataSourceUsername     = "postgres"
	postgresDataSourcePassword     = "mypassword"
	mysqlDataSourceHost            = "127.0.0.1"
	mysqlDataSourcePort            = 3306
	mysqlDataSourceDatabase        = "sakila"
	mysqlDataSourceUsername        = "user"
	mysqlDataSourcePassword        = "password"
	clickHouseDataSourceHost       = "127.0.0.1"
	clickHouseDataSourcePort       = 19000
	clickHouseDataSourceDatabase   = "mlops"
	clickHouseDataSourceUsername   = "user"
	clickHouseDataSourcePassword   = "password"
	mongoDataSourceHost            = "127.0.0.1"
	mongoDataSourcePort            = 27017
	mongoDataSourceDatabase        = "sample_db"
	mongoDataSourceCollection      = "movies"
	mongoDataSourceUsername        = "root"
	mongoDataSourcePassword        = "example"
	mongoDataSourceAuthDatabase    = "admin"
)

var _ = Describe("Data Stream datasource query", Ordered, func() {
	var (
		user profileTestUser
	)

	BeforeAll(func() {
		requireDatasourceFixtures()
		user = createVerifiedProfileAndLogin()
	})

	It("streams Arrow records from a registered Postgres datasource", func() {
		connectorID := createPostgresSourceConnector(user)

		command := map[string]string{
			"userId":            user.ID.String(),
			"sourceType":        "postgres",
			"sourceConnectorId": connectorID,
			"sql":               "SELECT actor_id, first_name, last_name FROM public.actor ORDER BY actor_id LIMIT 3",
		}
		commandBytes, err := json.Marshal(command)
		Expect(err).NotTo(HaveOccurred())

		result := queryFlight(commandBytes)
		Expect(result.RowCount).To(Equal(int64(3)))
		Expect(result.Columns).To(Equal([]string{"actor_id", "first_name", "last_name"}))
		Expect(result.FirstRow["actor_id"]).To(BeNumerically("==", 1))
		Expect(result.FirstRow["first_name"]).To(Equal("PENELOPE"))
		Expect(result.FirstRow["last_name"]).NotTo(BeEmpty())
	})

	It("streams Arrow records from a registered MySQL datasource", func() {
		connectorID := createMySQLSourceConnector(user)

		command := map[string]string{
			"userId":            user.ID.String(),
			"sourceType":        "mysql",
			"sourceConnectorId": connectorID,
			"sql":               "SELECT actor_id, first_name, last_name FROM actor ORDER BY actor_id LIMIT 3",
		}
		commandBytes, err := json.Marshal(command)
		Expect(err).NotTo(HaveOccurred())

		result := queryFlight(commandBytes)
		Expect(result.RowCount).To(Equal(int64(3)))
		Expect(result.Columns).To(Equal([]string{"actor_id", "first_name", "last_name"}))
		Expect(result.FirstRow["actor_id"]).To(BeNumerically("==", 1))
		Expect(result.FirstRow["first_name"]).To(Equal("PENELOPE"))
		Expect(result.FirstRow["last_name"]).NotTo(BeEmpty())
	})

	It("streams Arrow records from a registered ClickHouse datasource", func() {
		connectorID := createClickHouseSourceConnector(user)

		command := map[string]string{
			"userId":            user.ID.String(),
			"sourceType":        "clickhouse",
			"sourceConnectorId": connectorID,
			"sql":               "SELECT title, release_year FROM movies WHERE has(genres, 'Silent') ORDER BY release_year, title LIMIT 3",
		}
		commandBytes, err := json.Marshal(command)
		Expect(err).NotTo(HaveOccurred())

		result := queryFlight(commandBytes)
		Expect(result.RowCount).To(Equal(int64(3)))
		Expect(result.Columns).To(Equal([]string{"title", "release_year"}))
		Expect(result.FirstRow["title"]).To(Equal("Capture of Boer Battery by British"))
		Expect(result.FirstRow["release_year"]).To(BeNumerically("==", 1900))
	})

	It("streams Arrow records from a registered Mongo datasource", func() {
		connectorID := createMongoSourceConnector(user)

		command := map[string]any{
			"userId":            user.ID.String(),
			"sourceType":        "mongo",
			"sourceConnectorId": connectorID,
			"database":          mongoDataSourceDatabase,
			"collection":        mongoDataSourceCollection,
			"limit":             3,
		}
		commandBytes, err := json.Marshal(command)
		Expect(err).NotTo(HaveOccurred())

		result := queryFlight(commandBytes)
		Expect(result.RowCount).To(Equal(int64(3)))
		Expect(result.Columns).To(ContainElement("title"))
		Expect(result.FirstRow["title"]).NotTo(BeEmpty())
	})

	It("rejects invalid datasource connector payloads", func() {
		status, body := doJSON(http.MethodPost, "/v1/data/registry/connector/mysql", map[string]any{
			"config": map[string]any{
				"hostname":           mysqlDataSourceHost,
				"port":               mysqlDataSourcePort,
				"username":           mysqlDataSourceUsername,
				"password":           mysqlDataSourcePassword,
				"authenticationType": "MASTER",
			},
		}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))

		status, body = doJSON(http.MethodPost, "/v1/data/registry/connector/clickhouse", map[string]any{
			"config": map[string]any{
				"hostname":           clickHouseDataSourceHost,
				"port":               clickHouseDataSourcePort,
				"databaseName":       clickHouseDataSourceDatabase,
				"username":           clickHouseDataSourceUsername,
				"authenticationType": "MASTER",
			},
		}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))

		status, body = doJSON(http.MethodPost, "/v1/data/registry/connector/mongo", map[string]any{
			"config": map[string]any{
				"hostList":           []map[string]any{},
				"username":           mongoDataSourceUsername,
				"password":           mongoDataSourcePassword,
				"authDatabase":       mongoDataSourceAuthDatabase,
				"authenticationType": "MASTER",
			},
		}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
	})

	It("rejects malformed Flight datasource commands", func() {
		expectFlightInfoError([]byte("{"), codes.InvalidArgument, "registry query command must be JSON")

		expectFlightInfoError(mustMarshalCommand(map[string]any{
			"sourceType":        "postgres",
			"sourceConnectorId": uuid.NewString(),
			"sql":               "SELECT 1",
		}), codes.InvalidArgument, "registry query command requires userId")

		expectFlightInfoError(mustMarshalCommand(map[string]any{
			"userId":            user.ID.String(),
			"sourceType":        "postgres",
			"sourceConnectorId": "not-a-uuid",
			"sql":               "SELECT 1",
		}), codes.InvalidArgument, "registry query command has invalid sourceConnectorId")

		expectFlightInfoError(mustMarshalCommand(map[string]any{
			"userId":            user.ID.String(),
			"sourceType":        "mongo",
			"sourceConnectorId": uuid.NewString(),
			"database":          mongoDataSourceDatabase,
		}), codes.InvalidArgument, "mongo registry query command requires database and collection")
	})

	It("rejects datasource query execution failures", func() {
		postgresConnectorID := createPostgresSourceConnector(user)
		expectFlightInfoError(mustMarshalCommand(map[string]any{
			"userId":            user.ID.String(),
			"sourceType":        "postgres",
			"sourceConnectorId": postgresConnectorID,
			"sql":               "SELECT missing_column FROM public.actor LIMIT 1",
		}), codes.InvalidArgument, "query postgres source")

		mysqlConnectorID := createMySQLSourceConnector(user)
		expectFlightInfoError(mustMarshalCommand(map[string]any{
			"userId":            user.ID.String(),
			"sourceType":        "mysql",
			"sourceConnectorId": mysqlConnectorID,
			"sql":               "SELECT missing_column FROM actor LIMIT 1",
		}), codes.InvalidArgument, "query mysql source")

		clickHouseConnectorID := createClickHouseSourceConnector(user)
		expectFlightInfoError(mustMarshalCommand(map[string]any{
			"userId":            user.ID.String(),
			"sourceType":        "clickhouse",
			"sourceConnectorId": clickHouseConnectorID,
			"sql":               "SELECT missing_column FROM movies LIMIT 1",
		}), codes.InvalidArgument, "query clickhouse source")

		mongoConnectorID := createMongoSourceConnector(user)
		expectFlightInfoError(mustMarshalCommand(map[string]any{
			"userId":            user.ID.String(),
			"sourceType":        "mongo",
			"sourceConnectorId": mongoConnectorID,
			"database":          mongoDataSourceDatabase,
			"collection":        "collection_that_does_not_exist",
			"limit":             1,
		}), codes.InvalidArgument, "mongo source query returned no documents")
	})
})

func requireDatasourceFixtures() {
	if datasourcePortOpen(postgresDataSourceHost, postgresDataSourcePort) &&
		datasourcePortOpen(mysqlDataSourceHost, mysqlDataSourcePort) &&
		datasourcePortOpen(clickHouseDataSourceHost, clickHouseDataSourcePort) &&
		datasourcePortOpen(mongoDataSourceHost, mongoDataSourcePort) {
		return
	}
	Skip("external datasource fixtures are not running")
}

func datasourcePortOpen(host string, port int) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 250*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

type flightQueryResult struct {
	Columns  []string       `json:"columns"`
	RowCount int64          `json:"rowCount"`
	FirstRow map[string]any `json:"firstRow"`
}

func createPostgresSourceConnector(user profileTestUser) string {
	payload := map[string]any{
		"config": map[string]any{
			"hostname":           postgresDataSourceHost,
			"port":               postgresDataSourcePort,
			"databaseName":       postgresDataSourceDatabase,
			"username":           postgresDataSourceUsername,
			"password":           postgresDataSourcePassword,
			"authenticationType": "MASTER",
		},
	}

	status, body := doJSON(http.MethodPost, "/v1/data/registry/connector/postgres", payload, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

	created := decodeObject(body)
	return stringField(created, "id")
}

func createMySQLSourceConnector(user profileTestUser) string {
	payload := map[string]any{
		"config": map[string]any{
			"hostname":           mysqlDataSourceHost,
			"port":               mysqlDataSourcePort,
			"databaseName":       mysqlDataSourceDatabase,
			"username":           mysqlDataSourceUsername,
			"password":           mysqlDataSourcePassword,
			"authenticationType": "MASTER",
		},
	}

	status, body := doJSON(http.MethodPost, "/v1/data/registry/connector/mysql", payload, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

	created := decodeObject(body)
	return stringField(created, "id")
}

func createClickHouseSourceConnector(user profileTestUser) string {
	payload := map[string]any{
		"config": map[string]any{
			"hostname":           clickHouseDataSourceHost,
			"port":               clickHouseDataSourcePort,
			"databaseName":       clickHouseDataSourceDatabase,
			"username":           clickHouseDataSourceUsername,
			"password":           clickHouseDataSourcePassword,
			"authenticationType": "MASTER",
		},
	}

	status, body := doJSON(http.MethodPost, "/v1/data/registry/connector/clickhouse", payload, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

	created := decodeObject(body)
	return stringField(created, "id")
}

func createMongoSourceConnector(user profileTestUser) string {
	payload := map[string]any{
		"config": map[string]any{
			"hostList": []map[string]any{
				{
					"hostname": mongoDataSourceHost,
					"port":     mongoDataSourcePort,
				},
			},
			"username":           mongoDataSourceUsername,
			"password":           mongoDataSourcePassword,
			"authDatabase":       mongoDataSourceAuthDatabase,
			"authenticationType": "MASTER",
		},
	}

	status, body := doJSON(http.MethodPost, "/v1/data/registry/connector/mongo", payload, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

	created := decodeObject(body)
	return stringField(created, "id")
}

func dataStreamFlightAddress() string {
	host := strings.TrimSpace(os.Getenv("DATA_STREAM_SERVICE_API_GRPC_HOST"))
	if host == "" {
		host = "localhost"
	}
	port := strings.TrimSpace(os.Getenv("DATA_STREAM_SERVICE_API_GRPC_PORT"))
	if port == "" {
		return defaultDataStreamFlightAddress
	}
	if _, err := strconv.Atoi(port); err != nil {
		return defaultDataStreamFlightAddress
	}
	return fmt.Sprintf("%s:%s", host, port)
}

func queryFlight(command []byte) flightQueryResult {
	client, err := flight.NewFlightClient(dataStreamFlightAddress(), nil, grpc.WithInsecure())
	Expect(err).NotTo(HaveOccurred())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  command,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(info.GetEndpoint()).NotTo(BeEmpty())
	Expect(info.GetEndpoint()[0].GetTicket()).NotTo(BeNil())

	stream, err := client.DoGet(ctx, info.GetEndpoint()[0].GetTicket())
	Expect(err).NotTo(HaveOccurred())

	reader, err := flight.NewRecordReader(stream, ipc.WithAllocator(memory.NewGoAllocator()))
	Expect(err).NotTo(HaveOccurred())
	defer reader.Release()

	result := flightQueryResult{
		Columns: make([]string, len(reader.Schema().Fields())),
	}
	for i, field := range reader.Schema().Fields() {
		result.Columns[i] = field.Name
	}

	for reader.Next() {
		record := reader.Record()
		if result.RowCount == 0 && record.NumRows() > 0 {
			result.FirstRow = firstRecordRow(record)
		}
		result.RowCount += record.NumRows()
	}
	Expect(reader.Err()).NotTo(HaveOccurred())

	return result
}

func mustMarshalCommand(command any) []byte {
	bytes, err := json.Marshal(command)
	Expect(err).NotTo(HaveOccurred())
	return bytes
}

func expectFlightInfoError(command []byte, expectedCode codes.Code, expectedMessages ...string) {
	client, err := flight.NewFlightClient(dataStreamFlightAddress(), nil, grpc.WithInsecure())
	Expect(err).NotTo(HaveOccurred())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err = client.GetFlightInfo(ctx, &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  command,
	})
	Expect(err).To(HaveOccurred())
	Expect(grpcstatus.Code(err)).To(Equal(expectedCode), "error: %v", err)

	message := err.Error()
	for _, expectedMessage := range expectedMessages {
		Expect(message).To(ContainSubstring(expectedMessage))
	}
}

func firstRecordRow(record arrow.Record) map[string]any {
	row := make(map[string]any, record.NumCols())
	for i, field := range record.Schema().Fields() {
		row[field.Name] = valueAt(record.Column(i), 0)
	}
	return row
}

func valueAt(column arrow.Array, row int) any {
	switch col := column.(type) {
	case *array.Boolean:
		return col.Value(row)
	case *array.Int16:
		return int64(col.Value(row))
	case *array.Int32:
		return int64(col.Value(row))
	case *array.Int64:
		return col.Value(row)
	case *array.Float32:
		return float64(col.Value(row))
	case *array.Float64:
		return col.Value(row)
	case *array.String:
		return col.Value(row)
	default:
		return fmt.Sprint(column)
	}
}
