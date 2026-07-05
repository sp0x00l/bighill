package adapter

import (
	"context"
	"encoding/json"
	"errors"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("RestSourceConnDTOAdapter", func() {
	var (
		ctx     context.Context
		encoder *serializers.Encoder
		adapter *RestSourceConnDTOAdapter
	)

	BeforeEach(func() {
		ctx = context.Background()
		encoder = serializers.NewJSONSerializer()
		adapter = NewRestSourceConnDTOAdapter(GetConnCfgToDTOFunc, GetConnCfgFromDTOFunc, encoder)
	})

	It("decodes a connector DTO into a domain source connector", func() {
		connectorID := uuid.New()
		body := []byte(`{
			"id":"` + connectorID.String() + `",
			"config":{
				"hostname":"localhost",
				"port":5432,
				"databaseName":"mlops",
				"username":"postgres",
				"password":"password",
					"authenticationType":"MASTER"
				}
		}`)

		connector, err := adapter.FromDTO(ctx, model.Postgres, body)

		Expect(err).NotTo(HaveOccurred())
		Expect(connector.ID).To(Equal(connectorID))
		cfg, ok := connector.Config.(*model.PostgresDBConnCfg)
		Expect(ok).To(BeTrue())
		Expect(cfg.DatabaseName).To(Equal("mlops"))
	})

	It("encodes a domain source connector into a DTO", func() {
		connectorID := uuid.New()
		payload, err := adapter.ToDTO(ctx, &model.SourceConnector{
			ID: connectorID,
			Config: &model.PostgresDBConnCfg{
				Hostname:           "localhost",
				Port:               5432,
				DatabaseName:       "mlops",
				Username:           "postgres",
				Password:           "password",
				AuthenticationType: model.Master,
			},
		})

		Expect(err).NotTo(HaveOccurred())
		var dto RestSourceConnDTO
		Expect(json.Unmarshal(payload, &dto)).To(Succeed())
		Expect(dto.ID).To(Equal(connectorID.String()))
		Expect(dto.Config).NotTo(BeEmpty())
	})

	It("rejects domain connectors with unsupported config types", func() {
		unsupportedAdapter := NewRestSourceConnDTOAdapter(
			func(context.Context, model.StorageType) ToDTOFunc { return nil },
			GetConnCfgFromDTOFunc,
			encoder,
		)

		_, err := unsupportedAdapter.ToDTO(ctx, &model.SourceConnector{
			ID:     uuid.New(),
			Config: &model.PostgresDBConnCfg{},
		})

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects malformed connector payloads", func() {
		_, err := adapter.FromDTO(ctx, model.Postgres, []byte(`{"config":`))

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects missing connector config", func() {
		_, err := adapter.FromDTO(ctx, model.Postgres, []byte(`{}`))

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects invalid connector ids", func() {
		_, err := adapter.FromDTO(ctx, model.Postgres, []byte(`{
			"id":"not-a-uuid",
			"config":{
				"hostname":"localhost",
				"port":5432,
				"databaseName":"mlops",
				"username":"postgres",
				"password":"password",
					"authenticationType":"MASTER"
				}
		}`))

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects unsupported storage types", func() {
		_, err := adapter.FromDTO(ctx, model.UnknownStorageType, []byte(`{"config":{"hostname":"localhost"}}`))

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})
})
