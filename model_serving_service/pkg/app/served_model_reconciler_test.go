package app_test

import (
	"context"
	"errors"
	"testing"

	"model_serving_service/pkg/app"
	"model_serving_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model serving app unit test suite")
}

var _ = Describe("ServedModelReconciler", func() {
	It("marks a model loaded when the runtime is ready", func() {
		runtime := &servingRuntimeStub{state: &model.ServingRuntimeState{
			Ready:         true,
			ServingTarget: "http://served-model.default.svc.cluster.local:8000",
			ServingModel:  "ranker-v1",
			ReadyReplicas: 1,
		}}
		writer := &statusWriterStub{}
		reconciler := app.NewServedModelReconciler(runtime, writer)
		servedModel := validServedModel()

		status, err := reconciler.Reconcile(context.Background(), servedModel)

		Expect(err).NotTo(HaveOccurred())
		Expect(status.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
		Expect(runtime.received).To(Equal(servedModel))
		Expect(writer.resourceName).To(Equal(servedModel.ResourceName))
		Expect(writer.status.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
		Expect(writer.status.ServingTarget).To(Equal("http://served-model.default.svc.cluster.local:8000"))
		Expect(writer.status.ServingModel).To(Equal("ranker-v1"))
		Expect(writer.status.ObservedGeneration).To(Equal(servedModel.Generation))
	})

	It("marks a model not loaded while the runtime is still converging", func() {
		runtime := &servingRuntimeStub{state: &model.ServingRuntimeState{
			Ready:         false,
			ServingTarget: "http://served-model.default.svc.cluster.local:8000",
			ServingModel:  "ranker-v1",
		}}
		writer := &statusWriterStub{}
		reconciler := app.NewServedModelReconciler(runtime, writer)

		status, err := reconciler.Reconcile(context.Background(), validServedModel())

		Expect(err).NotTo(HaveOccurred())
		Expect(status.ServingLoadStatus).To(Equal(model.ModelLoadStatusNotLoaded))
		Expect(writer.status.ServingLoadStatus).To(Equal(model.ModelLoadStatusNotLoaded))
	})

	It("marks a model failed when the runtime reports a terminal failed state", func() {
		runtime := &servingRuntimeStub{state: &model.ServingRuntimeState{
			Failed:        true,
			ServingTarget: "http://served-model.default.svc.cluster.local:8000",
			ServingModel:  "ranker-v1",
			FailureReason: "deployment exceeded progress deadline",
		}}
		writer := &statusWriterStub{}
		reconciler := app.NewServedModelReconciler(runtime, writer)

		status, err := reconciler.Reconcile(context.Background(), validServedModel())

		Expect(err).NotTo(HaveOccurred())
		Expect(status.ServingLoadStatus).To(Equal(model.ModelLoadStatusFailed))
		Expect(writer.status.ServingLoadStatus).To(Equal(model.ModelLoadStatusFailed))
		Expect(writer.status.FailureReason).To(ContainSubstring("progress deadline"))
	})

	It("records a failed status when serving reconciliation fails", func() {
		expectedErr := errors.New("deployment invalid")
		runtime := &servingRuntimeStub{err: expectedErr}
		writer := &statusWriterStub{}
		reconciler := app.NewServedModelReconciler(runtime, writer)

		status, err := reconciler.Reconcile(context.Background(), validServedModel())

		Expect(err).To(HaveOccurred())
		Expect(status.ServingLoadStatus).To(Equal(model.ModelLoadStatusFailed))
		Expect(writer.status.ServingLoadStatus).To(Equal(model.ModelLoadStatusFailed))
		Expect(writer.status.FailureReason).To(ContainSubstring("deployment invalid"))
	})
})

type servingRuntimeStub struct {
	received *model.ServedModel
	state    *model.ServingRuntimeState
	err      error
}

func (s *servingRuntimeStub) EnsureServedModel(_ context.Context, servedModel *model.ServedModel) (*model.ServingRuntimeState, error) {
	s.received = servedModel
	return s.state, s.err
}

type statusWriterStub struct {
	resourceName string
	status       *model.ServedModelStatus
	err          error
}

func (s *statusWriterStub) UpdateStatus(_ context.Context, resourceName string, status *model.ServedModelStatus) error {
	s.resourceName = resourceName
	s.status = status
	return s.err
}

func validServedModel() *model.ServedModel {
	return &model.ServedModel{
		ResourceName:  "served-model-1",
		Namespace:     "default",
		Generation:    3,
		ModelID:       uuid.New(),
		TrainingRunID: uuid.New(),
		DatasetID:     uuid.New(),
		Name:          "ranker",
		ModelVersion:  1,
		BaseModel:     "mistral-7b",
		AdapterURI:    "s3://models/run-1",
		ServingModel:  "ranker-v1",
	}
}
