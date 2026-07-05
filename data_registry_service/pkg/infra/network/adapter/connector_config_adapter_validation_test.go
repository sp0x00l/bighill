package adapter

import (
	"context"
	"errors"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"lib/shared_lib/serializer"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connector config DTO adapter validation", func() {
	var (
		ctx     context.Context
		encoder *serializer.Encoder
	)

	BeforeEach(func() {
		ctx = context.Background()
		encoder = serializer.NewJSONSerializer()
	})

	DescribeTable("decodes valid connector config payloads",
		func(fromDTO FromDTOFunc, payload []byte, expectedType model.StorageType) {
			cfg, err := fromDTO(ctx, payload, encoder.Deserialize)

			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.GetStorageType()).To(Equal(expectedType))
		},
		Entry("aws s3", FromAwsS3ConnCfgDTO, []byte(`{
			"accessKey":"AKIAIOSFODNN7EX",
			"accessSecret":"0123456789012345678901234567890123456789",
			"assumedRoleARN":"arn:aws:iam::123456789012:role/example",
			"rootPath":"s3://bucket/root",
			"secure":true,
			"defaultCtasFormat":"PARQUET",
			"whitelistedBuckets":["bucket"]
		}`), model.S3),
		Entry("azure access key", FromAzureStorageConnCfgDTO, []byte(`{
			"accountName":"mlopsacct",
			"accessKey":"azure-access-key",
			"rootPath":"container/root",
			"accountKind":"STORAGE_V2",
			"defaultCtasFormat":"ICEBERG",
			"credentialsType":"ACCESS_KEY"
		}`), model.AzureStorage),
		Entry("google cloud storage", FromGoogleCloudStorageConnCfgDTO, []byte(`{
			"projectId":"mlops-project",
			"authMode":"SERVICE_ACCOUNT_KEYS",
			"rootPath":"bucket/root",
			"privateKeyId":"1234567890123456789012345678901234567890",
			"privateKey":"private-key",
			"clientEmail":"svc@mlops-project.iam.gserviceaccount.com",
			"clientId":"client-id",
			"defaultCtasFormat":"PARQUET"
		}`), model.GoogleCloudStorage),
		Entry("mongo", FromMongoDBConnCfgDTO, []byte(`{
			"hostList":[{"hostname":"localhost","port":27017}],
			"authenticationType":"MASTER",
			"username":"root",
			"password":"example",
			"authDatabase":"admin"
		}`), model.MongoDB),
		Entry("mysql", FromMySqlDBConnCfgDTO, []byte(`{
			"hostname":"localhost",
			"port":3306,
			"databaseName":"sakila",
			"authenticationType":"MASTER",
			"username":"user",
			"password":"password"
		}`), model.MySQL),
		Entry("clickhouse", FromClickHouseConnCfgDTO, []byte(`{
			"hostname":"localhost",
			"port":19000,
			"databaseName":"mlops",
			"authenticationType":"MASTER",
			"username":"user",
			"password":"password"
		}`), model.ClickHouse),
		Entry("oracle", FromOracleDBConnCfgDTO, []byte(`{
			"hostname":"localhost",
			"port":1521,
			"instance":"FREEPDB1",
			"authenticationType":"MASTER",
			"username":"oracle_user",
			"password":"password"
		}`), model.Oracle),
		Entry("postgres", FromPostgresDBConnCfgDTO, []byte(`{
			"hostname":"localhost",
			"port":5432,
			"databaseName":"pagila",
			"authenticationType":"MASTER",
			"username":"postgres",
			"password":"mypassword"
		}`), model.Postgres),
	)

	DescribeTable("rejects invalid connector config payloads",
		func(fromDTO FromDTOFunc, payload []byte) {
			_, err := fromDTO(ctx, payload, encoder.Deserialize)

			Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
		},
		Entry("aws s3 missing secret", FromAwsS3ConnCfgDTO, []byte(`{"accessKey":"AKIAIOSFODNN7EX"}`)),
		Entry("azure access key missing access key", FromAzureStorageConnCfgDTO, []byte(`{
			"accountName":"mlopsacct",
			"accountKind":"STORAGE_V2",
			"credentialsType":"ACCESS_KEY"
		}`)),
		Entry("google cloud storage invalid auth mode", FromGoogleCloudStorageConnCfgDTO, []byte(`{
			"projectId":"mlops-project",
			"authMode":"NOT_REAL",
			"rootPath":"bucket/root",
			"privateKeyId":"1234567890123456789012345678901234567890",
			"privateKey":"private-key",
			"clientEmail":"svc@mlops-project.iam.gserviceaccount.com",
			"clientId":"client-id"
		}`)),
		Entry("mongo empty hosts", FromMongoDBConnCfgDTO, []byte(`{
			"hostList":[],
			"authenticationType":"MASTER",
			"username":"root",
			"password":"example",
			"authDatabase":"admin"
		}`)),
		Entry("mysql missing password", FromMySqlDBConnCfgDTO, []byte(`{
			"hostname":"localhost",
			"port":3306,
			"databaseName":"sakila",
			"authenticationType":"MASTER",
			"username":"user"
		}`)),
		Entry("clickhouse invalid authentication type", FromClickHouseConnCfgDTO, []byte(`{
			"hostname":"localhost",
			"port":19000,
			"databaseName":"mlops",
			"authenticationType":"NOPE",
			"username":"user",
			"password":"password"
		}`)),
		Entry("oracle missing credential", FromOracleDBConnCfgDTO, []byte(`{
			"hostname":"localhost",
			"port":1521,
			"instance":"FREEPDB1",
			"authenticationType":"MASTER",
			"username":"oracle_user"
		}`)),
		Entry("postgres malformed json", FromPostgresDBConnCfgDTO, []byte(`{"hostname":`)),
	)

	DescribeTable("encodes valid connector configs",
		func(toDTO ToDTOFunc, cfg model.ConnectorConfig, expectedFragment string) {
			payload, err := toDTO(ctx, cfg, func(secret string) string { return "secret:" + secret }, encoder.Serialize)

			Expect(err).NotTo(HaveOccurred())
			Expect(string(payload)).To(ContainSubstring(expectedFragment))
		},
		Entry("aws s3", ToAwsS3ConnCfgDTO, &model.AwsS3StorageConnCfg{
			AccessKey:    "AKIAIOSFODNN7EX",
			AccessSecret: "secret-value",
		}, `"accessKey":"AKIAIOSFODNN7EX"`),
		Entry("azure", ToAzureStorageConnCfgDTO, &model.AzureStorageConnCfg{
			AccountName:     "mlopsacct",
			AccountKind:     model.AzureV2,
			CredentialsType: model.AccessKey,
			AccessKey:       "secret-value",
		}, `"accountName":"mlopsacct"`),
		Entry("google cloud storage", ToGoogleCloudStorageConnCfgDTO, &model.GoogleCloudStorageConnCfg{
			ProjectID:    "mlops-project",
			AuthMode:     model.ServiceAccountKeys,
			RootPath:     "bucket/root",
			PrivateKeyId: "1234567890123456789012345678901234567890",
			PrivateKey:   "private-key",
			ClientEmail:  "svc@mlops-project.iam.gserviceaccount.com",
			ClientId:     "client-id",
		}, `"projectId":"mlops-project"`),
		Entry("mongo", ToMongoDBConnCfgDTO, &model.MongoDBConnCfg{
			HostList:           []model.Host{{Hostname: "localhost", Port: 27017}},
			AuthenticationType: model.Master,
			Username:           "root",
			Password:           "example",
			AuthDatabase:       "admin",
		}, `"authDatabase":"admin"`),
		Entry("mysql", ToMySqlDBConnCfgDTO, &model.MysqlDBConnCfg{
			Hostname:           "localhost",
			Port:               3306,
			DatabaseName:       "sakila",
			AuthenticationType: model.Master,
			Username:           "user",
			Password:           "password",
		}, `"databaseName":"sakila"`),
		Entry("clickhouse", ToClickHouseConnCfgDTO, &model.ClickHouseConnCfg{
			Hostname:           "localhost",
			Port:               19000,
			DatabaseName:       "mlops",
			AuthenticationType: model.Master,
			Username:           "user",
			Password:           "password",
		}, `"databaseName":"mlops"`),
		Entry("oracle", ToOracleDBConnCfgDTO, &model.OracleDBConnCfg{
			Hostname:           "localhost",
			Port:               1521,
			Instance:           "FREEPDB1",
			AuthenticationType: model.Master,
			Username:           "oracle_user",
			Password:           "password",
		}, `"instance":"FREEPDB1"`),
		Entry("postgres", ToPostgresDBConnCfgDTO, &model.PostgresDBConnCfg{
			Hostname:           "localhost",
			Port:               5432,
			DatabaseName:       "pagila",
			AuthenticationType: model.Master,
			Username:           "postgres",
			Password:           "mypassword",
		}, `"databaseName":"pagila"`),
	)

	DescribeTable("rejects wrong connector config domain types",
		func(toDTO ToDTOFunc, wrong model.ConnectorConfig) {
			_, err := toDTO(ctx, wrong, nil, encoder.Serialize)

			Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
		},
		Entry("aws s3", ToAwsS3ConnCfgDTO, &model.PostgresDBConnCfg{}),
		Entry("azure", ToAzureStorageConnCfgDTO, &model.PostgresDBConnCfg{}),
		Entry("google cloud storage", ToGoogleCloudStorageConnCfgDTO, &model.PostgresDBConnCfg{}),
		Entry("mongo", ToMongoDBConnCfgDTO, &model.PostgresDBConnCfg{}),
		Entry("mysql", ToMySqlDBConnCfgDTO, &model.PostgresDBConnCfg{}),
		Entry("clickhouse", ToClickHouseConnCfgDTO, &model.PostgresDBConnCfg{}),
		Entry("oracle", ToOracleDBConnCfgDTO, &model.PostgresDBConnCfg{}),
		Entry("postgres", ToPostgresDBConnCfgDTO, &model.AwsS3StorageConnCfg{}),
	)

	It("selects connector config adapter functions by storage type", func() {
		Expect(GetConnCfgToDTOFunc(ctx, model.Postgres)).NotTo(BeNil())
		fromDTO, err := GetConnCfgFromDTOFunc(ctx, model.Postgres)
		Expect(err).NotTo(HaveOccurred())
		Expect(fromDTO).NotTo(BeNil())
	})

	It("rejects unknown connector config adapter factories", func() {
		Expect(GetConnCfgToDTOFunc(ctx, model.UnknownStorageType)).To(BeNil())
		_, err := GetConnCfgFromDTOFunc(ctx, model.UnknownStorageType)
		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})
})
