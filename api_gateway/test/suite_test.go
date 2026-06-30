package test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAPIGatewayE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API Gateway E2E Suite")
}
