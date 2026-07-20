package kubernetes_test

import (
	"context"
	"sync"
	"time"

	"model_serving_service/pkg/domain/model"
	servingkubernetes "model_serving_service/pkg/infra/network/k8s"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

var _ = Describe("ServedModelController serialization", func() {
	It("re-reads the latest ServedModel before reconciling a listed resource", func() {
		resourceName := "served-model-latest"
		listed := controllerServedModel(resourceName, "s3://models/old")
		latest := controllerServedModel(resourceName, "s3://models/new")
		store := &controllerStoreStub{
			namespace: "default",
			listed:    []*model.ServedModel{listed},
			latest:    map[string]*model.ServedModel{resourceName: latest},
		}
		reconciler := &controllerReconcilerStub{}
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)

		_, err := controller.ProcessSnapshot(context.Background())

		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.adapterURIs).To(Equal([]string{"s3://models/new"}))
	})

	It("serializes concurrent reconciles for the same resource key", func() {
		resourceName := "served-model-serialized"
		servedModel := controllerServedModel(resourceName, "s3://models/current")
		store := &controllerStoreStub{
			namespace: "default",
			listed:    []*model.ServedModel{servedModel},
			latest:    map[string]*model.ServedModel{resourceName: servedModel},
		}
		reconciler := &controllerReconcilerStub{delay: 20 * time.Millisecond}
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := controller.ProcessSnapshot(context.Background())
			Expect(err).NotTo(HaveOccurred())
		}()
		go func() {
			defer wg.Done()
			_, err := controller.ProcessSnapshot(context.Background())
			Expect(err).NotTo(HaveOccurred())
		}()
		wg.Wait()

		Expect(reconciler.maxActive).To(Equal(1))
		Expect(reconciler.calls).To(Equal(2))
	})

	It("serializes concurrent reconciles for different resources on the same shared runtime", func() {
		first := controllerServedModel("served-model-first", "s3://models/first")
		second := controllerServedModel("served-model-second", "s3://models/second")
		second.ModelID = uuid.New()
		second.ModelVersion = first.ModelVersion + 1
		second.BaseModel = first.BaseModel
		store := &controllerStoreStub{
			namespace: "default",
			listed:    []*model.ServedModel{first, second},
			latest: map[string]*model.ServedModel{
				first.ResourceName:  first,
				second.ResourceName: second,
			},
		}
		reconciler := &controllerReconcilerStub{delay: 20 * time.Millisecond}
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			err := controller.ProcessWatchEvent(context.Background(), watch.Event{Type: watch.Modified, Object: controllerServedModelObject(first)})
			Expect(err).NotTo(HaveOccurred())
		}()
		go func() {
			defer wg.Done()
			err := controller.ProcessWatchEvent(context.Background(), watch.Event{Type: watch.Modified, Object: controllerServedModelObject(second)})
			Expect(err).NotTo(HaveOccurred())
		}()
		wg.Wait()

		Expect(reconciler.maxActive).To(Equal(1))
		Expect(reconciler.calls).To(Equal(2))
	})

	It("does not reconcile terminal failed resources from the current snapshot", func() {
		resourceName := "served-model-terminal-failed"
		servedModel := controllerServedModel(resourceName, "s3://models/failed")
		servedModel.Status = &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusFailed,
			ObservedGeneration: servedModel.Generation,
			FailureReason:      "validation failed",
		}
		store := &controllerStoreStub{
			namespace: "default",
			listed:    []*model.ServedModel{servedModel},
			latest:    map[string]*model.ServedModel{resourceName: servedModel},
		}
		reconciler := &controllerReconcilerStub{}
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)

		_, err := controller.ProcessSnapshot(context.Background())

		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.calls).To(Equal(0))
	})

	It("does not reconcile terminal failed resources from status-only watch events", func() {
		resourceName := "served-model-terminal-failed-watch"
		servedModel := controllerServedModel(resourceName, "s3://models/failed")
		servedModel.Status = &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusFailed,
			ObservedGeneration: servedModel.Generation,
			FailureReason:      "validation failed",
		}
		store := &controllerStoreStub{
			namespace: "default",
			latest:    map[string]*model.ServedModel{resourceName: servedModel},
		}
		reconciler := &controllerReconcilerStub{}
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)

		err := controller.ProcessWatchEvent(context.Background(), watch.Event{Type: watch.Modified, Object: controllerServedModelObject(servedModel)})

		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.calls).To(Equal(0))
	})

	It("does not requeue a failed reconciliation after a terminal failed status is recorded", func() {
		resourceName := "served-model-failed-once"
		servedModel := controllerServedModel(resourceName, "s3://models/failed")
		store := &controllerStoreStub{
			namespace: "default",
			listed:    []*model.ServedModel{servedModel},
			latest:    map[string]*model.ServedModel{resourceName: servedModel},
		}
		reconciler := &controllerReconcilerStub{
			status: &model.ServedModelStatus{
				ServingLoadStatus:  model.ModelLoadStatusFailed,
				ObservedGeneration: servedModel.Generation,
				FailureReason:      "validation failed",
			},
			err: context.DeadlineExceeded,
		}
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)

		_, err := controller.ProcessSnapshot(context.Background())

		Expect(err).NotTo(HaveOccurred())
		Consistently(func() int {
			reconciler.mu.Lock()
			defer reconciler.mu.Unlock()
			return reconciler.calls
		}, 300*time.Millisecond, 25*time.Millisecond).Should(Equal(1))
	})
})

type controllerStoreStub struct {
	namespace string
	listed    []*model.ServedModel
	latest    map[string]*model.ServedModel
}

func (s *controllerStoreStub) Namespace() string {
	return s.namespace
}

func (s *controllerStoreStub) ListWithResourceVersion(context.Context) ([]*model.ServedModel, string, error) {
	return s.listed, "1", nil
}

func (s *controllerStoreStub) Read(_ context.Context, resourceName string) (*model.ServedModel, error) {
	return s.latest[resourceName], nil
}

func (s *controllerStoreStub) Watch(context.Context, string) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}

func (s *controllerStoreStub) UpdateStatus(context.Context, string, *model.ServedModelStatus) error {
	return nil
}

type controllerReconcilerStub struct {
	mu          sync.Mutex
	active      int
	maxActive   int
	calls       int
	delay       time.Duration
	status      *model.ServedModelStatus
	err         error
	adapterURIs []string
}

func (r *controllerReconcilerStub) Reconcile(_ context.Context, servedModel *model.ServedModel) (*model.ServedModelStatus, error) {
	r.mu.Lock()
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	r.calls++
	r.adapterURIs = append(r.adapterURIs, servedModel.AdapterURI)
	r.mu.Unlock()

	if r.delay > 0 {
		time.Sleep(r.delay)
	}

	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	if r.status != nil || r.err != nil {
		return r.status, r.err
	}
	return &model.ServedModelStatus{ServingLoadStatus: model.ModelLoadStatusLoaded}, nil
}

func (r *controllerReconcilerStub) Delete(context.Context, *model.ServedModel) error {
	return nil
}

func controllerServedModel(resourceName string, adapterURI string) *model.ServedModel {
	return &model.ServedModel{
		ResourceName: resourceName,
		Namespace:    "default",
		Generation:   1,
		ModelID:      uuid.MustParse("4f4b8258-f9af-49f8-b5a8-f84d75891f3b"),
		OrgID:        uuid.MustParse("6629d88a-05af-411e-8439-7497620e41df"),
		ModelKind:    "FINE_TUNED",
		Name:         "ranker",
		ModelVersion: 1,
		BaseModel:    "mistral",
		AdapterURI:   adapterURI,
		AdapterRank:  16,
	}
}

func controllerServedModelObject(servedModel *model.ServedModel) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "serving.bighill.io/v1alpha1",
		"kind":       "ServedModel",
		"metadata": map[string]any{
			"name":       servedModel.ResourceName,
			"namespace":  servedModel.Namespace,
			"generation": int64(servedModel.Generation),
		},
		"spec": map[string]any{
			"modelID":      servedModel.ModelID.String(),
			"orgID":        servedModel.OrgID.String(),
			"modelKind":    servedModel.ModelKind,
			"modelVersion": int64(servedModel.ModelVersion),
			"name":         servedModel.Name,
			"baseModel":    servedModel.BaseModel,
			"adapterURI":   servedModel.AdapterURI,
			"adapterRank":  int64(servedModel.AdapterRank),
		},
	}}
	if servedModel.Status != nil {
		obj.Object["status"] = map[string]any{
			"servingLoadStatus":  servedModel.Status.ServingLoadStatus.String(),
			"servingTarget":      servedModel.Status.ServingTarget,
			"servingModel":       servedModel.Status.ServingModel,
			"servingProtocol":    servedModel.Status.ServingProtocol.String(),
			"failureReason":      servedModel.Status.FailureReason,
			"observedGeneration": servedModel.Status.ObservedGeneration,
			"readyReplicas":      int64(servedModel.Status.ReadyReplicas),
		}
	}
	obj.SetGeneration(servedModel.Generation)
	return obj
}
