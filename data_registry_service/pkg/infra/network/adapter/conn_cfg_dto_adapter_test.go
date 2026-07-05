package adapter

import (
	"context"
	"data_registry_service/pkg/domain/model"
	"lib/shared_lib/serializer"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry network adapter unit test suite")
}

var _ = Describe("Connector config DTO adapters", func() {
	var (
		ctx     context.Context
		encoder *serializer.Encoder
	)

	BeforeEach(func() {
		ctx = context.Background()
		encoder = serializer.NewJSONSerializer()
	})

	It("round-trips each source connector config type", func() {
		tests := []struct {
			name    string
			config  model.ConnectorConfig
			toDTO   ToDTOFunc
			fromDTO FromDTOFunc
			want    model.ConnectorConfig
		}{
			{
				name: "aws s3",
				config: &model.AwsS3StorageConnCfg{
					AccessKey:          "AKIAIOSFODNN7EXAMPLE",
					AccessSecret:       "0123456789012345678901234567890123456789",
					AssumedRoleARN:     "arn:aws:iam::123456789012:role/example",
					RootPath:           "s3://bucket/root",
					Secure:             true,
					DefaultCtasFormat:  model.Parquet,
					WhitelistedBuckets: []string{"bucket"},
				},
				toDTO:   ToAwsS3ConnCfgDTO,
				fromDTO: FromAwsS3ConnCfgDTO,
				want: &model.AwsS3StorageConnCfg{
					AccessKey:          "AKIAIOSFODNN7EXAMPLE",
					AccessSecret:       "0123456789012345678901234567890123456789",
					AssumedRoleARN:     "arn:aws:iam::123456789012:role/example",
					RootPath:           "s3://bucket/root",
					Secure:             true,
					DefaultCtasFormat:  model.Parquet,
					WhitelistedBuckets: []string{"bucket"},
				},
			},
			{
				name: "azure access key",
				config: &model.AzureStorageConnCfg{
					CredentialsType:   model.AccessKey,
					AccountKind:       model.AzureV2,
					DefaultCtasFormat: model.Iceberg,
					AccountName:       "mlopsacct",
					AccessKey:         "azure-access-key",
					RootPath:          "container/root",
				},
				toDTO:   ToAzureStorageConnCfgDTO,
				fromDTO: FromAzureStorageConnCfgDTO,
				want: &model.AzureStorageConnCfg{
					CredentialsType:   model.AccessKey,
					AccountKind:       model.AzureV2,
					DefaultCtasFormat: model.Iceberg,
					AccountName:       "mlopsacct",
					AccessKey:         "azure-access-key",
					RootPath:          "container/root",
				},
			},
			{
				name: "google cloud storage",
				config: &model.GoogleCloudStorageConnCfg{
					ProjectID:         "mlops-project",
					AuthMode:          model.ServiceAccountKeys,
					RootPath:          "bucket/root",
					PrivateKeyId:      "1234567890123456789012345678901234567890",
					PrivateKey:        "private-key",
					ClientEmail:       "svc@mlops-project.iam.gserviceaccount.com",
					ClientId:          "client-id",
					DefaultCtasFormat: model.Parquet,
				},
				toDTO:   ToGoogleCloudStorageConnCfgDTO,
				fromDTO: FromGoogleCloudStorageConnCfgDTO,
				want: &model.GoogleCloudStorageConnCfg{
					ProjectID:         "mlops-project",
					AuthMode:          model.ServiceAccountKeys,
					RootPath:          "bucket/root",
					PrivateKeyId:      "1234567890123456789012345678901234567890",
					PrivateKey:        "private-key",
					ClientEmail:       "svc@mlops-project.iam.gserviceaccount.com",
					ClientId:          "client-id",
					DefaultCtasFormat: model.Parquet,
				},
			},
			{
				name: "mongo",
				config: &model.MongoDBConnCfg{
					HostList:           []model.Host{{Hostname: "localhost", Port: 27017}},
					AuthenticationType: model.Master,
					Username:           "root",
					Password:           "example",
					AuthDatabase:       "admin",
				},
				toDTO:   ToMongoDBConnCfgDTO,
				fromDTO: FromMongoDBConnCfgDTO,
				want: &model.MongoDBConnCfg{
					HostList:           []model.Host{{Hostname: "localhost", Port: 27017}},
					AuthenticationType: model.Master,
					Username:           "root",
					Password:           "example",
					AuthDatabase:       "admin",
				},
			},
			{
				name: "mysql",
				config: &model.MysqlDBConnCfg{
					Hostname:           "localhost",
					Port:               3306,
					DatabaseName:       "sakila",
					Username:           "user",
					Password:           "password",
					AuthenticationType: model.Master,
				},
				toDTO:   ToMySqlDBConnCfgDTO,
				fromDTO: FromMySqlDBConnCfgDTO,
				want: &model.MysqlDBConnCfg{
					Hostname:           "localhost",
					Port:               3306,
					DatabaseName:       "sakila",
					Username:           "user",
					Password:           "password",
					AuthenticationType: model.Master,
				},
			},
			{
				name: "clickhouse",
				config: &model.ClickHouseConnCfg{
					Hostname:           "localhost",
					Port:               19000,
					DatabaseName:       "mlops",
					Username:           "user",
					Password:           "password",
					AuthenticationType: model.Master,
				},
				toDTO:   ToClickHouseConnCfgDTO,
				fromDTO: FromClickHouseConnCfgDTO,
				want: &model.ClickHouseConnCfg{
					Hostname:           "localhost",
					Port:               19000,
					DatabaseName:       "mlops",
					Username:           "user",
					Password:           "password",
					AuthenticationType: model.Master,
				},
			},
			{
				name: "oracle",
				config: &model.OracleDBConnCfg{
					Hostname:           "localhost",
					Port:               1521,
					Instance:           "FREEPDB1",
					Username:           "oracle_user",
					Password:           "password",
					AuthenticationType: model.Master,
				},
				toDTO:   ToOracleDBConnCfgDTO,
				fromDTO: FromOracleDBConnCfgDTO,
				want: &model.OracleDBConnCfg{
					Hostname:           "localhost",
					Port:               1521,
					Instance:           "FREEPDB1",
					Username:           "oracle_user",
					Password:           "password",
					AuthenticationType: model.Master,
				},
			},
			{
				name: "postgres",
				config: &model.PostgresDBConnCfg{
					Hostname:           "localhost",
					Port:               5432,
					DatabaseName:       "pagila",
					Username:           "postgres",
					Password:           "mypassword",
					AuthenticationType: model.Master,
				},
				toDTO:   ToPostgresDBConnCfgDTO,
				fromDTO: FromPostgresDBConnCfgDTO,
				want: &model.PostgresDBConnCfg{
					Hostname:           "localhost",
					Port:               5432,
					DatabaseName:       "pagila",
					Username:           "postgres",
					Password:           "mypassword",
					AuthenticationType: model.Master,
				},
			},
		}

		for _, tc := range tests {
			tc := tc
			By(tc.name)
			dto, err := tc.toDTO(ctx, tc.config, nil, encoder.Serialize)
			Expect(err).NotTo(HaveOccurred())

			got, err := tc.fromDTO(ctx, dto, encoder.Deserialize)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(tc.want))
		}
	})

	It("rejects invalid required fields for each config type", func() {
		tests := []struct {
			name    string
			payload []byte
			fromDTO FromDTOFunc
		}{
			{name: "aws s3", payload: []byte(`{"accessKey":"","accessSecret":""}`), fromDTO: FromAwsS3ConnCfgDTO},
			{name: "azure", payload: []byte(`{"accountName":"ml","accountKind":"STORAGE_V2","credentialsType":"ACCESS_KEY"}`), fromDTO: FromAzureStorageConnCfgDTO},
			{name: "gcs", payload: []byte(`{"projectId":"ml","authMode":"SERVICE_ACCOUNT_KEYS"}`), fromDTO: FromGoogleCloudStorageConnCfgDTO},
			{name: "mongo", payload: []byte(`{"hostList":[],"authenticationType":"MASTER","authDatabase":"admin"}`), fromDTO: FromMongoDBConnCfgDTO},
			{name: "mysql", payload: []byte(`{"hostname":"localhost","port":3306,"databaseName":"sakila","authenticationType":"MASTER","username":"user"}`), fromDTO: FromMySqlDBConnCfgDTO},
			{name: "clickhouse", payload: []byte(`{"hostname":"localhost","port":19000,"databaseName":"mlops","authenticationType":"MASTER","username":"user"}`), fromDTO: FromClickHouseConnCfgDTO},
			{name: "oracle", payload: []byte(`{"hostname":"localhost","port":1521,"instance":"FREEPDB1","authenticationType":"MASTER","username":"user"}`), fromDTO: FromOracleDBConnCfgDTO},
			{name: "postgres", payload: []byte(`{"hostname":"localhost","port":5432,"databaseName":"pagila","authenticationType":"MASTER","username":"postgres"}`), fromDTO: FromPostgresDBConnCfgDTO},
		}

		for _, tc := range tests {
			tc := tc
			By(tc.name)
			_, err := tc.fromDTO(ctx, tc.payload, encoder.Deserialize)
			Expect(err).To(HaveOccurred())
		}
	})
})
