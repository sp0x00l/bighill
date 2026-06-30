package messaging

import "github.com/google/uuid"

func featureSnapshotIdempotencyKey(rawSnapshotID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("feature_snapshot:"+rawSnapshotID.String()))
}

func embeddingSnapshotIdempotencyKey(featureSnapshotID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("embedding_snapshot:"+featureSnapshotID.String()))
}
