package rest_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRestHandlers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion REST handler test suite")
}
