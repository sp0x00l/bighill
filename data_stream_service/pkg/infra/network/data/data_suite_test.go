package data_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDataInfra(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data stream infra data test suite")
}
