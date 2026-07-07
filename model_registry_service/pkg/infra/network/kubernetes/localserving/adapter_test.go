package localserving

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	localstore "lib/shared_lib/servedmodel"
	"model_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLocalServing(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry local serving unit test suite")
}

type statusRecorderStub struct {
	statuses []*model.ServedModelStatus
	keys     []uuid.UUID
	err      error
}

func (s *statusRecorderStub) RecordModelServingStatus(_ context.Context, status *model.ServedModelStatus, key uuid.UUID) (*model.Model, error) {
	s.statuses = append(s.statuses, status)
	s.keys = append(s.keys, key)
	return &model.Model{ModelID: status.ModelID, ServingLoadStatus: status.ServingLoadStatus}, s.err
}

var _ = Describe("Local serving adapter", func() {
	It("writes served model specs into the shared local store", func() {
		path := filepath.Join(GinkgoT().TempDir(), "served_models.json")
		adapter, err := NewAdapter("default", path)
		Expect(err).NotTo(HaveOccurred())
		modelID := uuid.New()
		trainingRunID := uuid.New()
		datasetID := uuid.New()

		err = adapter.EnsureServedModel(context.Background(), &model.Model{
			ModelID:           modelID,
			TrainingRunID:     trainingRunID,
			DatasetID:         datasetID,
			ModelKind:         model.ModelKindBase,
			Name:              "llama",
			ModelVersion:      3,
			BaseModel:         "meta-llama/Llama",
			ArtifactLocation:  "s3://bucket/model",
			ArtifactFormat:    "HF_MODEL",
			ArtifactChecksum:  "sha256",
			AdapterURI:        "s3://bucket/adapter",
			ServingTarget:     "http://runtime",
			ServingModel:      "llama",
			ServingLoadStatus: model.ModelLoadStatusNotLoaded,
		})

		Expect(err).NotTo(HaveOccurred())
		record, ok, err := adapter.store.Read(ServedModelName(modelID, 3))
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(record.Spec.ModelID).To(Equal(modelID.String()))
		Expect(record.Spec.TrainingRunID).To(Equal(trainingRunID.String()))
		Expect(record.Spec.DatasetID).To(Equal(datasetID.String()))
		Expect(record.Spec.ModelKind).To(Equal(model.ModelKindBase.String()))
		Expect(record.Spec.BaseModel).To(Equal("meta-llama/Llama"))
	})

	It("rejects missing observer dependencies", func() {
		_, err := NewStatusObserver(nil, &statusRecorderStub{}, time.Second)
		Expect(err).To(MatchError(ContainSubstring("adapter is required")))

		adapter, err := NewAdapter("default", filepath.Join(GinkgoT().TempDir(), "served_models.json"))
		Expect(err).NotTo(HaveOccurred())
		_, err = NewStatusObserver(adapter, nil, time.Second)
		Expect(err).To(MatchError(ContainSubstring("recorder is required")))
	})

	It("records each new observed status once", func() {
		path := filepath.Join(GinkgoT().TempDir(), "served_models.json")
		adapter, err := NewAdapter("default", path)
		Expect(err).NotTo(HaveOccurred())
		modelID := uuid.New()
		Expect(adapter.EnsureServedModel(context.Background(), &model.Model{
			ModelID:      modelID,
			ModelKind:    model.ModelKindBase,
			Name:         "llama",
			ModelVersion: 1,
			BaseModel:    "meta-llama/Llama",
		})).To(Succeed())
		name := ServedModelName(modelID, 1)
		Expect(adapter.store.UpdateStatus(name, localstore.Status{
			ServingLoadStatus:  model.ModelLoadStatusLoaded.String(),
			ServingTarget:      "http://runtime",
			ServingModel:       "llama",
			ObservedGeneration: 1,
		})).To(Succeed())
		recorder := &statusRecorderStub{}
		observer, err := NewStatusObserver(adapter, recorder, time.Second)
		Expect(err).NotTo(HaveOccurred())

		resourceVersion, err := observer.ProcessSnapshot(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(resourceVersion).NotTo(BeEmpty())
		Expect(recorder.statuses).To(HaveLen(1))
		Expect(recorder.statuses[0].ModelID).To(Equal(modelID))
		Expect(recorder.statuses[0].ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))

		_, err = observer.ProcessSnapshot(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(recorder.statuses).To(HaveLen(1))
	})
})
