package transport_test

import (
	"net/url"
	"shared_lib/transport"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("HTTP query filter", func() {
	It("should return FilterOpIn for input 'in'", func() {
		op := transport.StringToFilterOperation("in")

		Expect(op).To(Equal(transport.FilterOpIn))
	})

	It("should return FilterOpNotIn for input 'notin'", func() {
		op := transport.StringToFilterOperation("notin")

		Expect(op).To(Equal(transport.FilterOpNotIn))
	})

	It("should return FilterOpIs for input 'is'", func() {
		op := transport.StringToFilterOperation("is")

		Expect(op).To(Equal(transport.FilterOpIs))
	})

	It("should return FilterOpIsNot for input 'isnot'", func() {
		op := transport.StringToFilterOperation("isnot")

		Expect(op).To(Equal(transport.FilterOpIsNot))
	})

	It("should return FilterOpInvalid for unknown input", func() {
		op := transport.StringToFilterOperation("unknown")

		Expect(op).To(Equal(transport.FilterOpInvalid))
	})

	Describe("ParseFilterParams", func() {
		It("should parse query filters correctly", func() {

			filterParams := []string{"field1:is:value1", "field2:in:value1,value2"}
			expectedFilters := []transport.Filter{
				{
					Field:     "field1",
					Operation: transport.FilterOpIs,
					Values:    []string{"value1"},
				},
				{
					Field:     "field2",
					Operation: transport.FilterOpIn,
					Values:    []string{"value1", "value2"},
				},
			}

			filters, err := transport.ParseFilterParams(filterParams)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(filters)).To(Equal(2))
			Expect(filters).To(Equal(expectedFilters))
		})

		It("should return an error for invalid filter format", func() {
			filterParams := []string{"invalid_filter_format"}

			res, err := transport.ParseFilterParams(filterParams)
			Expect(err).To(HaveOccurred())
			Expect(res).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("invalid filter format: `invalid_filter_format`"))
		})

		It("should return an error for empty field, op or value", func() {
			filterParams := []string{":is:value", "field::value", "field:is:"}

			res, err := transport.ParseFilterParams(filterParams)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("field, operation and value must not be empty: `field::value`"))
			Expect(err.Error()).To(ContainSubstring("field, operation and value must not be empty: `:is:value`"))
			Expect(err.Error()).To(ContainSubstring("field, operation and value must not be empty: `field:is:`"))
			Expect(res).To(BeNil())
		})

		It("should return an error for empty space field, op or value", func() {
			filterParams := []string{" :is:value", "field: :value", "field:is: "}

			res, err := transport.ParseFilterParams(filterParams)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("field, operation and value must not be empty: `field: :value`"))
			Expect(err.Error()).To(ContainSubstring("field, operation and value must not be empty: ` :is:value`"))
			Expect(err.Error()).To(ContainSubstring("field, operation and value must not be empty: `field:is: `"))
			Expect(res).To(BeNil())
		})

		It("should return an error for invalid op", func() {
			filterParams := []string{"field:invalid:value"}

			res, err := transport.ParseFilterParams(filterParams)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid filter operation: `field:invalid:value`"))
			Expect(res).To(BeNil())
		})

		It("should return an empty map for no filters", func() {
			filterParams := []string{}

			res, err := transport.ParseFilterParams(filterParams)
			Expect(err).ToNot(HaveOccurred())
			Expect(res).ToNot(BeNil())
			Expect(res).To(BeEmpty())
		})
	})

	Describe("FiltersFromURL", func() {
		It("should return the correct query filter parameters", func() {
			sutURL := &url.URL{
				RawQuery: "filter=type:in:Type0,Type1&filter=status:is:read",
			}

			res, err := transport.FiltersFromURL(sutURL)

			Expect(err).ToNot(HaveOccurred())
			Expect(res).To(Equal([]string{"type:in:Type0,Type1", "status:is:read"}))
		})

		It("should return an error for decoding query parameters", func() {
			sutURL := &url.URL{
				RawQuery: "invalid-param=value",
			}

			res, err := transport.FiltersFromURL(sutURL)

			Expect(res).To(BeNil())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("decode filter parameters failed: schema: invalid path \"invalid-param\""))
		})
	})
})
