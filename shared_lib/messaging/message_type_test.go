package messaging

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("MsgType", func() {
	It("matches the ML contract ordinals", func() {
		expect := map[MsgType]int{
			MsgTypeUnknown:                0,
			MsgTypeUserCreated:            1,
			MsgTypeUserUpdated:            2,
			MsgTypeUserDeleted:            3,
			MsgTypeDatasetFileUploaded:    5,
			MsgTypeRawSnapshotReady:       6,
			MsgTypeFeatureSnapshotReady:   8,
			MsgTypeEmbeddingSnapshotReady: 10,
			MsgTypeDatasetCreated:         11,
			MsgTypeDatasetDeleted:         12,
			MsgTypeDatasetUpdated:         13,
			MsgTypeModelTrainingCompleted: 14,
			MsgTypeModelTrainingFailed:    15,
			MsgTypeModelUpdated:           16,
			MsgTypeModelArtifactIngested:  18,
			MsgTypePromotionRequested:     19,
			MsgTypePromotionReportReady:   20,
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
			MsgTypeModelArtifactIngested,
			MsgTypePromotionRequested,
			MsgTypePromotionReportReady,
		} {
			Expect(msgType.String()).NotTo(BeEmpty(), "missing string mapping for msg type ordinal %d", msgType)
			Expect(MsgTypeFromString(msgType.String())).To(Equal(msgType))
		}
	})
})
