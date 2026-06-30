package domain_test

import (
	"errors"
	"testing"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer domain unit test suite")
}

var _ = Describe("Idempotency errors", func() {
	It("extracts raw snapshot replay records", func() {
		record := &model.RawSnapshot{}
		extracted, ok := domain.IsRawSnapshotAlreadyMaterialized(errors.Join(errors.New("wrapped"), &domain.RawSnapshotAlreadyMaterializedError{Record: record}))
		Expect(ok).To(BeTrue())
		Expect(extracted).To(Equal(record))
	})

	It("extracts feature snapshot replay records", func() {
		record := &model.FeatureSnapshot{}
		extracted, ok := domain.IsFeatureSnapshotAlreadyBuilt(errors.Join(errors.New("wrapped"), &domain.FeatureSnapshotAlreadyBuiltError{Record: record}))
		Expect(ok).To(BeTrue())
		Expect(extracted).To(Equal(record))
	})

	It("extracts embedding snapshot replay records", func() {
		record := &model.EmbeddingSnapshot{}
		extracted, ok := domain.IsEmbeddingsAlreadyMaterialized(errors.Join(errors.New("wrapped"), &domain.EmbeddingsAlreadyMaterializedError{Record: record}))
		Expect(ok).To(BeTrue())
		Expect(extracted).To(Equal(record))
	})
})
