package messaging_test

import (
	"context"
	"errors"

	"feature_materializer_service/pkg/domain"
	featuremessaging "feature_materializer_service/pkg/infra/network/messaging"
	msgConn "lib/shared_lib/messaging"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type errorPolicySubscriberStub struct {
	policy msgConn.ErrorPolicy
}

func (s *errorPolicySubscriberStub) Subscribe(context.Context, []string) error {
	return nil
}

func (s *errorPolicySubscriberStub) RegisterListener(msgConn.MsgType, func(context.Context, msgConn.Message) error) {
}

func (s *errorPolicySubscriberStub) AddTopics(context.Context, []string) error {
	return nil
}

func (s *errorPolicySubscriberStub) ConfigureErrorPolicy(policy msgConn.ErrorPolicy) {
	s.policy = policy
}

var _ = Describe("subscriber error policy", func() {
	var policy msgConn.ErrorPolicy

	BeforeEach(func() {
		subscriber := &errorPolicySubscriberStub{}
		featuremessaging.NewDatasetFileUploadedSubscriber(subscriber, &recordingRawSnapshotUsecase{}, []string{"dataset"})
		policy = subscriber.policy
	})

	It("is configured on the subscriber", func() {
		Expect(policy).NotTo(BeNil())
	})

	DescribeTable("classifies deterministic errors",
		func(err error, expected bool) {
			Expect(policy.IsNonRetryableError(err)).To(Equal(expected))
		},
		Entry("raw snapshot already materialized", &domain.RawSnapshotAlreadyMaterializedError{}, true),
		Entry("feature snapshot already built", &domain.FeatureSnapshotAlreadyBuiltError{}, true),
		Entry("embeddings already materialized", &domain.EmbeddingsAlreadyMaterializedError{}, true),
		Entry("raw snapshot not found", domain.ErrRawSnapshotNotFound, true),
		Entry("feature snapshot not found", domain.ErrFeatureSnapshotNotFound, true),
		Entry("embedding snapshot not found", domain.ErrEmbeddingSnapshotNotFound, true),
		Entry("shared non retryable", msgConn.NonRetryable(errors.New("invalid")), true),
		Entry("transient", errors.New("transient"), false),
	)
})
