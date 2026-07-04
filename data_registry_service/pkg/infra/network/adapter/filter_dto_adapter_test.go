package adapter

import (
	"context"
	"errors"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	pagination "lib/shared_lib/transport"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("FilterDTOAdapter", func() {
	var adapter FilterDTOAdapter

	BeforeEach(func() {
		adapter = NewFilterDTOAdapter()
	})

	It("maps category query filters to domain filters", func() {
		filters, err := adapter.QueryFiltersToDatasetsFilters(context.Background(), []pagination.Filter{
			{Field: "category", Operation: pagination.FilterOpIn, Values: []string{"movies", "books"}},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(filters).To(HaveLen(1))
		categoryFilter, ok := filters[0].(*model.CategoryFilter)
		Expect(ok).To(BeTrue())
		Expect(categoryFilter.Values).To(Equal([]string{"movies", "books"}))
	})

	It("rejects unsupported filter fields", func() {
		_, err := adapter.QueryFiltersToDatasetsFilters(context.Background(), []pagination.Filter{
			{Field: "status", Operation: pagination.FilterOpIn, Values: []string{"published"}},
		})

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects unsupported category operations", func() {
		_, err := adapter.QueryFiltersToDatasetsFilters(context.Background(), []pagination.Filter{
			{Field: "category", Operation: pagination.FilterOpIs, Values: []string{"movies"}},
		})

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects empty category filters", func() {
		_, err := adapter.QueryFiltersToDatasetsFilters(context.Background(), []pagination.Filter{
			{Field: "category", Operation: pagination.FilterOpIn},
		})

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})
})
