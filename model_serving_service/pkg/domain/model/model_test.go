package model

import (
	"testing"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model serving model unit test suite")
}

var _ = Describe("ModelLoadStatus", func() {
	It("converts known load status strings", func() {
		status, err := ToModelLoadStatus("LOADED")

		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(ModelLoadStatusLoaded))
		Expect(ModelLoadStatusNotLoaded.String()).To(Equal("NOT_LOADED"))
	})

	It("rejects unknown load status strings", func() {
		_, err := ToModelLoadStatus("WARMING")

		Expect(err).To(MatchError(ErrUnknownModelLoadStatus))
	})
})

var _ = Describe("ServedModel", func() {
	It("carries desired model spec and observed status", func() {
		modelID := uuid.New()
		status := &ServedModelStatus{ServingLoadStatus: ModelLoadStatusLoaded, ServingTarget: "http://runtime"}
		served := ServedModel{
			ResourceName:  "served-model",
			Namespace:     "default",
			ModelID:       modelID,
			Name:          "llama",
			ModelVersion:  1,
			BaseModel:     "meta-llama/Llama",
			ServingTarget: "http://runtime",
			Status:        status,
		}

		Expect(served.ModelID).To(Equal(modelID))
		Expect(served.Status.ServingLoadStatus).To(Equal(ModelLoadStatusLoaded))
		Expect(served.BaseModel).NotTo(BeEmpty())
	})

	It("distinguishes ready and failed runtime state", func() {
		ready := ServingRuntimeState{Ready: true, ServingTarget: "http://runtime", ReadyReplicas: 1}
		failed := ServingRuntimeState{Failed: true, FailureReason: "image pull failed"}

		Expect(ready.Ready).To(BeTrue())
		Expect(ready.Failed).To(BeFalse())
		Expect(failed.Failed).To(BeTrue())
		Expect(failed.FailureReason).NotTo(BeEmpty())
	})
})
