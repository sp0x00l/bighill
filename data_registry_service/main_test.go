package main

import (
	"os"
	"testing"

	env "lib/shared_lib/env"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDataRegistryMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry main unit test suite")
}

var _ = Describe("readRegistryConfig", func() {
	BeforeEach(func() {
		env.ResetEnvironmentCache()
		Expect(os.Unsetenv("DATA_REGISTRY_SERVICE_PROFILE_SUBSCRIBER_TOPIC")).To(Succeed())
	})

	It("uses the profile service topic for tenant projections by default", func() {
		cfg := readRegistryConfig()

		Expect(cfg.ProfileTopic).To(Equal("profile"))
	})
})
