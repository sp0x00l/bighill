package heuristic_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModelServingHeuristicContracts(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model serving heuristic contract suite")
}
