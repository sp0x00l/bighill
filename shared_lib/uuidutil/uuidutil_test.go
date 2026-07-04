package uuidutil_test

import (
	"testing"

	"lib/shared_lib/uuidutil"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUUIDUtil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "UUID utility suite")
}

var _ = Describe("ParseOptional", func() {
	It("returns nil UUID for blank values", func() {
		got, err := uuidutil.ParseOptional("dataset_id", " ")

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(uuid.Nil))
	})

	It("parses non-empty values", func() {
		id := uuid.New()

		got, err := uuidutil.ParseOptional("dataset_id", id.String())

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(id))
	})

	It("rejects malformed values", func() {
		_, err := uuidutil.ParseOptional("dataset_id", "not-a-uuid")

		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("StringOrEmpty", func() {
	It("returns an empty string for nil UUID", func() {
		Expect(uuidutil.StringOrEmpty(uuid.Nil)).To(Equal(""))
	})

	It("returns the UUID string for a real UUID", func() {
		id := uuid.New()

		Expect(uuidutil.StringOrEmpty(id)).To(Equal(id.String()))
	})
})
