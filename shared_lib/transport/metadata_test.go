package transport_test

import (
	"context"
	"lib/shared_lib/transport"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metadata unit tests", func() {
	var (
		pagination transport.Pagination
		filter     transport.Filter
		ctx        context.Context
	)

	BeforeEach(func() {
		pagination = transport.Pagination{
			Page:  1,
			Limit: 10,
		}
		ctx = context.Background()
	})

	Describe("NewMetadata", func() {
		When("total count supports next page", func() {
			It("should generate the next URL correctly", func() {
				originalURL := "http://test.com/res?page=1&limit=10&filter=field1:is:(value1)"
				filter = transport.Filter{
					Field:     "field1",
					Operation: "is",
					Values:    []string{"value1"},
				}

				res, err := transport.NewMetadata(ctx, 100, pagination, []transport.Filter{filter}, originalURL)

				Expect(err).ToNot(HaveOccurred())
				Expect(res.TotalCount).To(Equal(100))
				Expect(res.Page).To(Equal(1))
				Expect(res.Limit).To(Equal(10))
				Expect(res.Filter[0].Field).To(Equal("field1"))
				Expect(res.Filter[0].Operation).To(Equal(transport.FilterOpIs))
				Expect(res.Filter[0].Values).To(Equal([]string{"value1"}))
				Expect(res.NextURL).To(Equal("/res?page=2&limit=10&filter=field1:is:(value1)"))
			})

			It("should return an error if it fails to generate next url", func() {
				originalURL := "http://foo\x7f.com/"

				_, err := transport.NewMetadata(ctx, 100, pagination, []transport.Filter{filter}, originalURL)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("parse original URL failed"))
			})

			It("should keep all filters in next URL", func() {
				originalURL := "http://test.com/res?page=1&limit=10&filter=field1:is:value1&filter=field2:in:value2,value3"
				filters := []transport.Filter{
					{
						Field:     "field1",
						Operation: transport.FilterOpIs,
						Values:    []string{"value1"},
					},
					{
						Field:     "field2",
						Operation: transport.FilterOpIn,
						Values:    []string{"value2", "value3"},
					},
				}

				res, err := transport.NewMetadata(ctx, 100, pagination, filters, originalURL)

				Expect(err).ToNot(HaveOccurred())
				Expect(res.NextURL).To(ContainSubstring("filter=field1:is:(value1)"))
				Expect(res.NextURL).To(ContainSubstring("filter=field2:in:(value2%2Cvalue3)"))
			})

			It("should preserve non-pagination query params in next URL", func() {
				originalURL := "http://test.com/res?page=1&limit=10&dataset=training&start=2026-01-01T00:00:00Z"

				res, err := transport.NewMetadata(ctx, 100, pagination, nil, originalURL)

				Expect(err).ToNot(HaveOccurred())
				Expect(res.NextURL).To(Equal("/res?page=2&limit=10&dataset=training&start=2026-01-01T00:00:00Z"))
			})
		})
		When("total count does not support next page", func() {
			It("should not generate the next URL", func() {
				originalURL := "http://test.com/res?page=1&limit=10"

				res, err := transport.NewMetadata(ctx, 9, pagination, nil, originalURL)

				Expect(err).ToNot(HaveOccurred())
				Expect(res.TotalCount).To(Equal(9))
				Expect(res.Page).To(Equal(1))
				Expect(res.Limit).To(Equal(10))
				Expect(res.Filter).To(BeNil())
				Expect(res.NextURL).To(Equal(""))
			})
		})
	})
})
