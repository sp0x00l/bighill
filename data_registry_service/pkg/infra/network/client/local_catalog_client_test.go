package client

import (
	"context"
	"errors"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("LocalCatalogClient", func() {
	var (
		ctx    context.Context
		client *LocalCatalogClient
		config model.ConnectorConfig
	)

	BeforeEach(func() {
		ctx = context.Background()
		client = NewLocalCatalogClient()
		config = &model.PostgresDBConnCfg{
			Hostname:           "localhost",
			Port:               5432,
			DatabaseName:       "mlops",
			Username:           "postgres",
			Password:           "password",
			AuthenticationType: model.Master,
		}
	})

	It("uses the source connector id as the deterministic local catalog id", func() {
		resourceID := uuid.New()

		catalogID, err := client.CreateResource(ctx, resourceID.String(), config)

		Expect(err).NotTo(HaveOccurred())
		Expect(catalogID).To(Equal(resourceID))
	})

	It("rejects non-uuid resource names", func() {
		_, err := client.CreateResource(ctx, "warehouse-source", config)

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("replaces resources only when the local catalog id matches the resource name", func() {
		resourceID := uuid.New()

		Expect(client.ReplaceResource(ctx, resourceID.String(), resourceID, config)).To(Succeed())

		err := client.ReplaceResource(ctx, resourceID.String(), uuid.New(), config)
		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects deleting the nil catalog id", func() {
		err := client.DeleteResource(ctx, uuid.Nil)

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects Polaris Iceberg table validation", func() {
		err := client.ValidateDatasetTable(ctx, &model.Dataset{
			TableFormat:     model.Iceberg,
			CatalogProvider: model.PolarisCatalog,
		})

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})
})
