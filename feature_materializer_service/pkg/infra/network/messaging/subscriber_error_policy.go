package messaging

import (
	"errors"

	"feature_materializer_service/pkg/domain"
	msgConn "lib/shared_lib/messaging"

	log "github.com/sirupsen/logrus"
)

type errorPolicy struct{}

func configureErrorPolicy(subscriber msgConn.Subscriber) {
	if err := msgConn.ConfigureErrorPolicy(subscriber, errorPolicy{}); err != nil {
		log.WithError(err).Trace("subscriber does not support error policy")
	}
}

func (errorPolicy) IsNonRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := domain.IsRawSnapshotAlreadyMaterialized(err); ok {
		return true
	}
	if _, ok := domain.IsFeatureSnapshotAlreadyBuilt(err); ok {
		return true
	}
	if _, ok := domain.IsEmbeddingsAlreadyMaterialized(err); ok {
		return true
	}
	return msgConn.IsNonRetryable(err) ||
		msgConn.IsAlreadyProcessed(err) ||
		errors.Is(err, domain.ErrRawSnapshotNotFound) ||
		errors.Is(err, domain.ErrFeatureSnapshotNotFound) ||
		errors.Is(err, domain.ErrEmbeddingSnapshotNotFound)
}
