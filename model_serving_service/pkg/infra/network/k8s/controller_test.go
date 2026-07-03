package k8s_test

import (
	"context"
	"sync"
	"time"

	"model_serving_service/pkg/domain/model"
	servingk8s "model_serving_service/pkg/infra/network/k8s"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
		controller := servingk8s.NewServedModelController(store, reconciler, time.Millisecond)

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
		controller := servingk8s.NewServedModelController(store, reconciler, time.Millisecond)

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
	return &model.ServedModelStatus{ServingLoadStatus: model.ModelLoadStatusLoaded}, nil
}

func controllerServedModel(resourceName string, adapterURI string) *model.ServedModel {
	return &model.ServedModel{
		ResourceName: resourceName,
		Namespace:    "default",
		Generation:   1,
		ModelID:      uuid.MustParse("4f4b8258-f9af-49f8-b5a8-f84d75891f3b"),
		Name:         "ranker",
		ModelVersion: 1,
		BaseModel:    "mistral",
		AdapterURI:   adapterURI,
	}
}
