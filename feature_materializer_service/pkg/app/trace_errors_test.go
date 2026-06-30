package app_test

import (
	"context"
	"errors"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Trace error classification", func() {
	It("classifies expected idempotency and lookup errors", func() {
		Expect(usecase.IsExpectedTraceError(&domain.RawSnapshotAlreadyMaterializedError{})).To(BeTrue())
		Expect(usecase.IsExpectedTraceError(&domain.FeatureSnapshotAlreadyBuiltError{})).To(BeTrue())
		Expect(usecase.IsExpectedTraceError(&domain.EmbeddingsAlreadyMaterializedError{})).To(BeTrue())
		Expect(usecase.IsExpectedTraceError(domain.ErrRawSnapshotNotFound)).To(BeTrue())
		Expect(usecase.IsExpectedTraceError(domain.ErrFeatureSnapshotNotFound)).To(BeTrue())
		Expect(usecase.IsExpectedTraceError(domain.ErrEmbeddingSnapshotNotFound)).To(BeTrue())
		Expect(usecase.IsExpectedTraceError(context.Canceled)).To(BeTrue())
		Expect(usecase.IsExpectedTraceError(errors.New("boom"))).To(BeFalse())
	})
})
