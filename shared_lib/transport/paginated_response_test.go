package transport_test

import (
	"lib/shared_lib/transport"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PaginatedResponse", func() {
	Describe("ToBytes", func() {
		It("should encode the paginated response to a byte slice", func() {
			testResource1 := struct {
				Name  string                  `json:"name"`
				Age   int                     `json:"age"`
				Links transport.ResourceLinks `json:"links"`
			}{Name: "resource1", Age: 10, Links: transport.ResourceLinks{Self: transport.Self{Href: "/test/1"}}}

			testResource2 := struct {
				Location string                  `json:"location"`
				Number   int                     `json:"number"`
				Links    transport.ResourceLinks `json:"links"`
			}{Location: "loc1", Number: 5, Links: transport.ResourceLinks{Self: transport.Self{Href: "/test/2"}}}

			response := &transport.PaginatedResponse{
				Resources: []any{testResource1, testResource2},
				Metadata: transport.Metadata{
					TotalCount: 2,
					Limit:      10,
					Page:       1,
					NextURL:    "/test/limit=10&page=2",
				},
			}

			resBytes, err := response.ToBytes()
			Expect(err).To(BeNil())
			Expect(resBytes).To(Not(BeNil()))
			toString := string(resBytes)

			Expect(err).To(BeNil())
			Expect(toString).To(ContainSubstring("resources"))
			Expect(toString).To(ContainSubstring("{\"name\":\"resource1\",\"age\":10,\"links\":{\"self\":{\"href\":\"/test/1\"}}}"))
			Expect(toString).To(ContainSubstring("{\"location\":\"loc1\",\"number\":5,\"links\":{\"self\":{\"href\":\"/test/2\"}}}]"))
			Expect(toString).To(ContainSubstring("metadata\":{\"total\":2,\"page\":1,\"limit\":10,\"next\":\"/test/limit=10&page=2"))
		})

		It("should not have resources attribute when resources are empty", func() {
			response := &transport.PaginatedResponse{
				Metadata: transport.Metadata{
					TotalCount: 2,
					Limit:      10,
					Page:       1,
					NextURL:    "",
				},
			}

			resBytes, err := response.ToBytes()
			Expect(err).To(BeNil())
			Expect(resBytes).To(Not(BeNil()))

			toString := string(resBytes)
			Expect(toString).To(Equal("{\"metadata\":{\"total\":2,\"page\":1,\"limit\":10}}\n"))
		})
	})
})
