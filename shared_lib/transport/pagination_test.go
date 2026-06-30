package transport_test

import (
	"lib/shared_lib/transport"
	"net/url"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pagination", func() {
	var sut transport.Pagination

	Describe("PaginationFromURL", func() {
		It("uses documented defaults when pagination params are omitted", func() {
			parsed, err := url.Parse("/resources")
			Expect(err).NotTo(HaveOccurred())

			got, err := transport.PaginationFromURL(parsed)

			Expect(err).NotTo(HaveOccurred())
			Expect(got.Page).To(Equal(transport.DefaultPage))
			Expect(got.Limit).To(Equal(transport.DefaultPageSize))
		})

		It("rejects explicit invalid limits instead of defaulting", func() {
			parsed, err := url.Parse("/resources?offset=0&limit=0")
			Expect(err).NotTo(HaveOccurred())

			_, err = transport.PaginationFromURL(parsed)

			Expect(err).To(MatchError(ContainSubstring("limit must be greater than zero")))
		})

		It("parses explicit offset pagination", func() {
			parsed, err := url.Parse("/resources?offset=20&limit=10")
			Expect(err).NotTo(HaveOccurred())

			got, err := transport.PaginationFromURL(parsed)

			Expect(err).NotTo(HaveOccurred())
			Expect(got.Page).To(Equal(3))
			Expect(got.Limit).To(Equal(10))
		})

		It("defaults explicit page zero to the first page", func() {
			parsed, err := url.Parse("/resources?page=0&limit=10")
			Expect(err).NotTo(HaveOccurred())

			got, err := transport.PaginationFromURL(parsed)

			Expect(err).NotTo(HaveOccurred())
			Expect(got.Page).To(Equal(transport.DefaultPage))
			Expect(got.Limit).To(Equal(10))
		})
	})

	Describe("NewPagination ", func() {
		It("should create a new pagination object with default values when page and limit are zero", func() {
			sut := transport.NewPagination(0, 0)

			Expect(sut.Page).To(Equal(transport.DefaultPage))
			Expect(sut.Limit).To(Equal(transport.DefaultPageSize))
		})

		It("should create a new pagination object with the provided page and default limit when limit is zero", func() {
			page := 3
			sut := transport.NewPagination(page, 0)

			Expect(sut.Page).To(Equal(page))
			Expect(sut.Limit).To(Equal(transport.DefaultPageSize))
		})

		It("should create a new pagination object with the provided limit and default page when page is zero", func() {
			limit := 50
			sut := transport.NewPagination(0, limit)

			Expect(sut.Page).To(Equal(transport.DefaultPage))
			Expect(sut.Limit).To(Equal(limit))
		})

		It("should create a new pagination object with the provided page and limit when both are non-zero", func() {
			page := 2
			limit := 10
			sut := transport.NewPagination(page, limit)

			Expect(sut.Page).To(Equal(page))
			Expect(sut.Limit).To(Equal(limit))
		})
	})

	Describe("GetOffset", func() {
		BeforeEach(func() {
			sut = transport.Pagination{
				Limit: 5,
			}
		})
		It("should return the correct offset", func() {
			sut.Page = 2
			offset := sut.GetOffset()

			Expect(offset).To(Equal(5))
		})

		It("should return 0 for page 0", func() {
			sut.Page = 0
			offset := sut.GetOffset()

			Expect(offset).To(Equal(0))
		})

		It("should return 0 for page 1", func() {
			sut.Page = 1

			offset := sut.GetOffset()
			Expect(offset).To(Equal(0))
		})
	})
	Context("IsValidForCount", func() {
		BeforeEach(func() {
			sut = transport.Pagination{
				Page:  2,
				Limit: 5,
			}
		})

		It("should return false when count is equal to offset", func() {
			result := sut.IsValidForCount(5)

			Expect(result).To(BeFalse())
		})

		It("should return false when count is less than offset", func() {
			result := sut.IsValidForCount(4)

			Expect(result).To(BeFalse())
		})

		It("should return true when count is greater than offset", func() {
			result := sut.IsValidForCount(6)

			Expect(result).To(BeTrue())
		})
	})
})
