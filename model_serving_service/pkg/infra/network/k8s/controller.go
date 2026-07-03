package k8s

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"model_serving_service/pkg/app"
	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

type ServedModelRepository interface {
	Namespace() string
	ListWithResourceVersion(ctx context.Context) ([]*model.ServedModel, string, error)
	Read(ctx context.Context, resourceName string) (*model.ServedModel, error)
	Watch(ctx context.Context, resourceVersion string) (watch.Interface, error)
	UpdateStatus(ctx context.Context, resourceName string, status *model.ServedModelStatus) error
}

type ServedModelController struct {
	store        ServedModelRepository
	reconciler   app.ServedModelReconciler
	pollInterval time.Duration
	mu           sync.Mutex
	pending      map[string]struct{}
	retries      map[string]int
	locks        sync.Map
}

func NewServedModelController(store ServedModelRepository, reconciler app.ServedModelReconciler, pollInterval time.Duration) *ServedModelController {
	log.Trace("NewServedModelController")

	return &ServedModelController{
		store:        store,
		reconciler:   reconciler,
		pollInterval: pollInterval,
		pending:      map[string]struct{}{},
		retries:      map[string]int{},
	}
}

func (c *ServedModelController) Start(ctx context.Context) error {
	log.Trace("ServedModelController Start")

	reconnectAttempts := 0
	for {
		resourceVersion, err := c.processSnapshot(ctx, true)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			log.WithContext(ctx).WithError(err).Error("served model reconciliation snapshot failed")
			reconnectAttempts++
			if err := sleepContext(ctx, backoffDelay(c.pollInterval, reconnectAttempts)); err != nil {
				return err
			}
			continue
		}
		if err := c.Watch(ctx, resourceVersion); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			log.WithContext(ctx).WithError(err).Error("served model reconciliation watch failed")
			reconnectAttempts++
		} else {
			reconnectAttempts = 0
		}
		if err := sleepContext(ctx, reconnectDelay(c.pollInterval)); err != nil {
			return err
		}
	}
}

func (c *ServedModelController) Watch(ctx context.Context, resourceVersion string) error {
	log.Trace("ServedModelController Watch")

	watcher, err := c.store.Watch(ctx, resourceVersion)
	if err != nil {
		return err
	}
	defer watcher.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return nil
			}
			if err := c.processWatchEvent(ctx, event, true); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				log.WithContext(ctx).WithError(err).Error("served model reconciliation event failed")
			}
		}
	}
}

func (c *ServedModelController) ProcessOnce(ctx context.Context) error {
	log.Trace("ServedModelController ProcessOnce")

	_, err := c.ProcessSnapshot(ctx)
	return err
}

func (c *ServedModelController) ProcessSnapshot(ctx context.Context) (string, error) {
	log.Trace("ServedModelController ProcessSnapshot")

	return c.processSnapshot(ctx, false)
}

func (c *ServedModelController) processSnapshot(ctx context.Context, requeuePending bool) (string, error) {
	log.Trace("ServedModelController processSnapshot")

	servedModels, resourceVersion, err := c.store.ListWithResourceVersion(ctx)
	if err != nil {
		return "", err
	}
	for _, servedModel := range servedModels {
		c.processResource(ctx, servedModel.ResourceName, requeuePending)
	}
	return resourceVersion, nil
}

func (c *ServedModelController) ProcessWatchEvent(ctx context.Context, event watch.Event) error {
	log.Trace("ServedModelController ProcessWatchEvent")

	return c.processWatchEvent(ctx, event, false)
}

func (c *ServedModelController) processWatchEvent(ctx context.Context, event watch.Event, requeuePending bool) error {
	log.Trace("ServedModelController processWatchEvent")

	switch event.Type {
	case watch.Added, watch.Modified:
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			return domain.ErrModelServe.Extend("served model watch event object is not unstructured")
		}
		servedModel, err := servedModelFromObject(obj, c.store.Namespace())
		if err != nil {
			log.WithContext(ctx).WithError(err).WithField("served_model", obj.GetName()).Error("served model spec ignored")
			return nil
		}
		c.processResource(ctx, servedModel.ResourceName, requeuePending)
	case watch.Deleted, watch.Bookmark:
		return nil
	case watch.Error:
		return domain.ErrModelServe.Extend("served model watch returned an error event")
	}
	return nil
}

func (c *ServedModelController) processResource(ctx context.Context, resourceName string, requeuePending bool) {
	log.Trace("ServedModelController processResource")

	lock := c.resourceLock(resourceName)
	lock.Lock()
	defer lock.Unlock()

	servedModel, err := c.store.Read(ctx, resourceName)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("served_model", resourceName).Error("served model read failed")
		c.scheduleRequeue(ctx, resourceName)
		return
	}
	status, err := c.reconciler.Reconcile(ctx, servedModel)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("served_model", servedModel.ResourceName).Error("served model reconcile failed")
		c.scheduleRequeue(ctx, resourceName)
		return
	}
	if status == nil || status.ServingLoadStatus == model.ModelLoadStatusLoaded {
		c.clearRetry(resourceName)
		return
	}
	if requeuePending && (status.ServingLoadStatus == model.ModelLoadStatusNotLoaded || status.ServingLoadStatus == model.ModelLoadStatusFailed) {
		c.scheduleRequeue(ctx, resourceName)
	}
}

func (c *ServedModelController) scheduleRequeue(ctx context.Context, resourceName string) {
	log.Trace("ServedModelController scheduleRequeue")

	c.mu.Lock()
	if _, exists := c.pending[resourceName]; exists {
		c.mu.Unlock()
		return
	}
	c.pending[resourceName] = struct{}{}
	c.retries[resourceName]++
	attempt := c.retries[resourceName]
	c.mu.Unlock()

	go func() {
		if err := sleepContext(ctx, backoffDelay(c.pollInterval, attempt)); err != nil {
			c.clearPending(resourceName)
			return
		}
		c.clearPending(resourceName)
		c.processResource(ctx, resourceName, true)
	}()
}

func (c *ServedModelController) clearPending(key string) {
	log.Trace("ServedModelController clearPending")

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, key)
}

func (c *ServedModelController) clearRetry(key string) {
	log.Trace("ServedModelController clearRetry")

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.retries, key)
}

func (c *ServedModelController) resourceLock(key string) *sync.Mutex {
	log.Trace("ServedModelController resourceLock")

	value, _ := c.locks.LoadOrStore(key, &sync.Mutex{})
	return value.(*sync.Mutex)
}

func reconnectDelay(base time.Duration) time.Duration {
	log.Trace("reconnectDelay")

	if base <= 0 {
		base = time.Second
	}
	jitterMax := int64(base / 5)
	if jitterMax <= 0 {
		return base
	}
	return base + time.Duration(rand.Int63n(jitterMax))
}

func backoffDelay(base time.Duration, attempt int) time.Duration {
	log.Trace("backoffDelay")

	if base < 250*time.Millisecond {
		base = 250 * time.Millisecond
	}
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for i := 1; i < attempt && delay < 30*time.Second; i++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	return reconnectDelay(delay)
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	log.Trace("sleepContext")

	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
