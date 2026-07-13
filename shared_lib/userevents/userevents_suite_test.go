package userevents_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUserEvents(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "User events test suite")
}
