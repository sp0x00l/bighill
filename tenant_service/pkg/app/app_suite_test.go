package usecase_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAppUseCases(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Profile app unit test suite")
}
