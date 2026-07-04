package main

import (
	"os"
	"testing"

	env "lib/shared_lib/env"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFeatureMaterializerMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer main unit test suite")
}

var _ = Describe("readMaterializerConfig", func() {
	BeforeEach(func() {
		env.ResetEnvironmentCache()
		Expect(os.Unsetenv("FEATURE_MATERIALIZER_SERVICE_PROFILE_SUBSCRIBER_TOPIC")).To(Succeed())
	})

	It("uses the profile service topic for tenant projections by default", func() {
		cfg := readMaterializerConfig()

		Expect(cfg.ProfileTopic).To(Equal("profile"))
	})
})
