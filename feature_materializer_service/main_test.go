package main

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFeatureMaterializerMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer main unit test suite")
}

var _ = Describe("postgresConnectionString", func() {
	It("escapes credentials and includes connection options", func() {
		connection := postgresConnectionString("feature user", "pa:ss/word", "localhost", "5432", "bighill_feature_materializer_db", "disable", 7)

		Expect(connection).To(ContainSubstring("postgres://feature+user:pa%3Ass%2Fword@localhost:5432/bighill_feature_materializer_db?"))
		Expect(connection).To(ContainSubstring("pool_max_conns=7"))
		Expect(connection).To(ContainSubstring("sslmode=disable"))
	})
})
