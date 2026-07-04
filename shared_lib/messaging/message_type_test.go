package messaging

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("MsgType", func() {
	It("matches the ML contract ordinals", func() {
		expect := map[MsgType]int{
			MsgTypeUnknown:                    0,
			MsgTypeUserCreated:                1,
			MsgTypeUserUpdated:                2,
			MsgTypeUserDeleted:                3,
			MsgTypeEmailVerificationRequested: 4,
			MsgTypeDatasetFileUploaded:        5,
			MsgTypeRawSnapshotReady:           6,
			MsgTypeFeatureSnapshotReady:       8,
			MsgTypeEmbeddingSnapshotReady:     10,
			MsgTypeDatasetCreated:             11,
			MsgTypeDatasetDeleted:             12,
			MsgTypeDatasetUpdated:             13,
			MsgTypeModelTrainingCompleted:     14,
			MsgTypeModelTrainingFailed:        15,
			MsgTypeModelUpdated:               16,
			MsgTypePreferenceDatasetReady:     17,
			MsgTypeModelArtifactIngested:      18,
		}

		for msgType, ordinal := range expect {
			Expect(int(msgType)).To(Equal(ordinal), "%s ordinal changed", msgType.String())
		}
	})

	It("round-trips string mappings", func() {
		for _, msgType := range []MsgType{
			MsgTypeUserCreated,
			MsgTypeUserUpdated,
			MsgTypeUserDeleted,
			MsgTypeEmailVerificationRequested,
			MsgTypeDatasetFileUploaded,
			MsgTypeRawSnapshotReady,
			MsgTypeFeatureSnapshotReady,
			MsgTypeEmbeddingSnapshotReady,
			MsgTypeDatasetCreated,
			MsgTypeDatasetDeleted,
			MsgTypeDatasetUpdated,
			MsgTypeModelTrainingCompleted,
			MsgTypeModelTrainingFailed,
			MsgTypeModelUpdated,
			MsgTypePreferenceDatasetReady,
			MsgTypeModelArtifactIngested,
		} {
			Expect(msgType.String()).NotTo(BeEmpty(), "missing string mapping for msg type ordinal %d", msgType)
			Expect(MsgTypeFromString(msgType.String())).To(Equal(msgType))
		}
	})
})
