package kubernetes

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

type BaseRuntimeRepository interface {
	ListWithResourceVersion(ctx context.Context) ([]*model.BaseRuntime, string, error)
	Watch(ctx context.Context, resourceVersion string) (watch.Interface, error)
}

type ServedModelController struct {
	store                  ServedModelRepository
	baseRuntimeStore       BaseRuntimeRepository
	reconciler             app.ServedModelReconciler
	pollInterval           time.Duration
	mu                     sync.Mutex
	pending                map[string]struct{}
	retries                map[string]int
	resources              map[string]bool
	baseRuntimeGenerations map[string]int64
	locks                  sync.Map
	healthMu               sync.RWMutex
	health                 ControllerHealth
}

type ControllerHealth struct {
	Started                   bool
	WatchActive               bool
	LastActivityAt            time.Time
	LastSuccessfulReconcileAt time.Time
	FirstKnownServedModelAt   time.Time
	KnownServedModels         int
	OutstandingServedModels   int
	LastErrorAt               time.Time
	LastError                 string
}

type ServedModelControllerOption func(*ServedModelController)

func WithBaseRuntimeStore(store BaseRuntimeRepository) ServedModelControllerOption {
	log.Trace("WithBaseRuntimeStore")

	return func(c *ServedModelController) {
		c.baseRuntimeStore = store
	}
}

func NewServedModelController(store ServedModelRepository, reconciler app.ServedModelReconciler, pollInterval time.Duration, options ...ServedModelControllerOption) *ServedModelController {
	log.Trace("NewServedModelController")

	controller := &ServedModelController{
		store:                  store,
		reconciler:             reconciler,
		pollInterval:           pollInterval,
		pending:                map[string]struct{}{},
		retries:                map[string]int{},
		resources:              map[string]bool{},
		baseRuntimeGenerations: map[string]int64{},
	}
	for _, option := range options {
		option(controller)
	}
	return controller
}

func (c *ServedModelController) Start(ctx context.Context) error {
	log.Trace("ServedModelController Start")

	c.markStarted()
	if c.baseRuntimeStore != nil {
		go c.watchBaseRuntimes(ctx)
	}
	reconnectAttempts := 0
	for {
		resourceVersion, err := c.processSnapshot(ctx, true)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			c.markError(err)
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
			c.markError(err)
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

func (c *ServedModelController) watchBaseRuntimes(ctx context.Context) {
	log.Trace("ServedModelController watchBaseRuntimes")

	reconnectAttempts := 0
	for {
		resourceVersion, err := c.processBaseRuntimeSnapshot(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			c.markError(err)
			log.WithContext(ctx).WithError(err).Error("base runtime snapshot failed")
			reconnectAttempts++
			if err := sleepContext(ctx, backoffDelay(c.pollInterval, reconnectAttempts)); err != nil {
				return
			}
			continue
		}
		if err := c.watchBaseRuntimeEvents(ctx, resourceVersion); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			c.markError(err)
			log.WithContext(ctx).WithError(err).Error("base runtime watch failed")
			reconnectAttempts++
		} else {
			reconnectAttempts = 0
		}
		if err := sleepContext(ctx, reconnectDelay(c.pollInterval)); err != nil {
			return
		}
	}
}

func (c *ServedModelController) processBaseRuntimeSnapshot(ctx context.Context) (string, error) {
	log.Trace("ServedModelController processBaseRuntimeSnapshot")

	baseRuntimes, resourceVersion, err := c.baseRuntimeStore.ListWithResourceVersion(ctx)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, baseRuntime := range baseRuntimes {
		c.baseRuntimeGenerations[baseRuntime.ResourceName] = baseRuntime.Generation
	}
	return resourceVersion, nil
}

func (c *ServedModelController) watchBaseRuntimeEvents(ctx context.Context, resourceVersion string) error {
	log.Trace("ServedModelController watchBaseRuntimeEvents")

	watcher, err := c.baseRuntimeStore.Watch(ctx, resourceVersion)
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
			if err := c.ProcessBaseRuntimeWatchEvent(ctx, event); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				log.WithContext(ctx).WithError(err).Error("base runtime event failed")
			}
		}
	}
}

func (c *ServedModelController) ProcessBaseRuntimeWatchEvent(ctx context.Context, event watch.Event) error {
	log.Trace("ServedModelController ProcessBaseRuntimeWatchEvent")

	switch event.Type {
	case watch.Added, watch.Modified:
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			return domain.ErrModelServe.Extend("base runtime watch event object is not unstructured")
		}
		if !c.rememberBaseRuntimeGeneration(obj.GetName(), obj.GetGeneration()) {
			return nil
		}
		_, err := c.processSnapshot(ctx, true)
		return err
	case watch.Deleted:
		if obj, ok := event.Object.(*unstructured.Unstructured); ok {
			c.mu.Lock()
			delete(c.baseRuntimeGenerations, obj.GetName())
			c.mu.Unlock()
		}
		_, err := c.processSnapshot(ctx, true)
		return err
	case watch.Bookmark:
		return nil
	case watch.Error:
		err := domain.ErrModelServe.Extend("base runtime watch returned an error event")
		c.markError(err)
		return err
	}
	return nil
}

func (c *ServedModelController) rememberBaseRuntimeGeneration(resourceName string, generation int64) bool {
	log.Trace("ServedModelController rememberBaseRuntimeGeneration")

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.baseRuntimeGenerations[resourceName]; ok && existing == generation {
		return false
	}
	c.baseRuntimeGenerations[resourceName] = generation
	return true
}

func (c *ServedModelController) Watch(ctx context.Context, resourceVersion string) error {
	log.Trace("ServedModelController Watch")

	watcher, err := c.store.Watch(ctx, resourceVersion)
	if err != nil {
		return err
	}
	defer watcher.Stop()
	c.markWatchActive(true)
	defer c.markWatchActive(false)
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
	c.setKnownResources(servedModels)
	c.markActivity()
	for _, servedModel := range servedModels {
		if !servedModelNeedsReconcile(servedModel) {
			continue
		}
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
		c.markActivity()
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			return domain.ErrModelServe.Extend("served model watch event object is not unstructured")
		}
		servedModel, err := servedModelDTOAdapter{namespace: c.store.Namespace()}.FromObject(obj)
		if err != nil {
			log.WithContext(ctx).WithError(err).WithField("served_model", obj.GetName()).Error("served model spec ignored")
			return nil
		}
		c.markResourceKnown(servedModel)
		if !servedModelNeedsReconcile(servedModel) {
			return nil
		}
		c.processResource(ctx, servedModel.ResourceName, requeuePending)
	case watch.Deleted, watch.Bookmark:
		c.markActivity()
		if event.Type == watch.Deleted {
			if obj, ok := event.Object.(*unstructured.Unstructured); ok {
				servedModel, err := servedModelDTOAdapter{namespace: c.store.Namespace()}.FromObject(obj)
				if err == nil {
					if deleteErr := c.reconciler.Delete(ctx, servedModel); deleteErr != nil {
						c.markError(deleteErr)
						log.WithContext(ctx).WithError(deleteErr).WithField("served_model", obj.GetName()).Error("served model delete reconciliation failed")
					}
				}
				c.markResourceDeleted(obj.GetName())
			}
		}
		return nil
	case watch.Error:
		err := domain.ErrModelServe.Extend("served model watch returned an error event")
		c.markError(err)
		return err
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
		if errors.Is(err, domain.ErrServedModelNotFound) {
			c.markResourceDeleted(resourceName)
			c.clearRetry(resourceName)
			return
		}
		c.markError(err)
		log.WithContext(ctx).WithError(err).WithField("served_model", resourceName).Error("served model read failed")
		c.scheduleRequeue(ctx, resourceName)
		return
	}
	var runtimeLock *sync.Mutex
	if servedModel.IsAdapter() {
		runtimeLock = c.resourceLock(sharedRuntimeLockKey(servedModel))
		runtimeLock.Lock()
		defer runtimeLock.Unlock()
	}

	status, err := c.reconciler.Reconcile(ctx, servedModel)
	if err != nil {
		c.markError(err)
		log.WithContext(ctx).WithError(err).WithField("served_model", servedModel.ResourceName).Error("served model reconcile failed")
		c.markResourceReconcileStatus(resourceName, status)
		if status == nil || statusNeedsReconcile(status) {
			c.scheduleRequeue(ctx, resourceName)
		}
		return
	}
	c.markSuccessfulReconcile()
	c.markResourceReconcileStatus(resourceName, status)
	if status == nil || status.ServingLoadStatus == model.ModelLoadStatusLoaded {
		c.clearRetry(resourceName)
		return
	}
	if requeuePending && statusNeedsReconcile(status) {
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

func (c *ServedModelController) setKnownResources(servedModels []*model.ServedModel) {
	log.Trace("ServedModelController setKnownResources")

	c.mu.Lock()
	c.resources = make(map[string]bool, len(servedModels))
	for _, servedModel := range servedModels {
		if servedModel == nil || servedModel.ResourceName == "" {
			continue
		}
		c.resources[servedModel.ResourceName] = servedModelNeedsReconcile(servedModel)
	}
	count := len(c.resources)
	outstanding := countOutstandingResources(c.resources)
	c.mu.Unlock()

	c.setKnownResourceCounts(count, outstanding)
}

func (c *ServedModelController) markResourceKnown(servedModel *model.ServedModel) {
	log.Trace("ServedModelController markResourceKnown")

	if servedModel == nil || servedModel.ResourceName == "" {
		return
	}
	c.mu.Lock()
	c.resources[servedModel.ResourceName] = servedModelNeedsReconcile(servedModel)
	count := len(c.resources)
	outstanding := countOutstandingResources(c.resources)
	c.mu.Unlock()

	c.setKnownResourceCounts(count, outstanding)
}

func (c *ServedModelController) markResourceReconcileStatus(resourceName string, status *model.ServedModelStatus) {
	log.Trace("ServedModelController markResourceReconcileStatus")

	if resourceName == "" {
		return
	}
	c.mu.Lock()
	if _, ok := c.resources[resourceName]; ok {
		c.resources[resourceName] = statusNeedsReconcile(status)
	}
	count := len(c.resources)
	outstanding := countOutstandingResources(c.resources)
	c.mu.Unlock()

	c.setKnownResourceCounts(count, outstanding)
}

func (c *ServedModelController) markResourceDeleted(resourceName string) {
	log.Trace("ServedModelController markResourceDeleted")

	if resourceName == "" {
		return
	}
	c.mu.Lock()
	delete(c.resources, resourceName)
	count := len(c.resources)
	outstanding := countOutstandingResources(c.resources)
	c.mu.Unlock()

	c.setKnownResourceCounts(count, outstanding)
}

func (c *ServedModelController) resourceLock(key string) *sync.Mutex {
	log.Trace("ServedModelController resourceLock")

	value, _ := c.locks.LoadOrStore(key, &sync.Mutex{})
	return value.(*sync.Mutex)
}

func servedModelNeedsReconcile(servedModel *model.ServedModel) bool {
	log.Trace("servedModelNeedsReconcile")

	if servedModel == nil {
		return false
	}
	if servedModel.Status == nil {
		return true
	}
	if servedModel.Status.ObservedGeneration != servedModel.Generation {
		return true
	}
	return statusNeedsReconcile(servedModel.Status)
}

func statusNeedsReconcile(status *model.ServedModelStatus) bool {
	log.Trace("statusNeedsReconcile")

	if status == nil {
		return true
	}
	if status.ServingLoadStatus == model.ModelLoadStatusNotLoaded && status.FailureReason == model.NotLoadedReasonCapacityEvicted {
		return false
	}
	return status.ServingLoadStatus == model.ModelLoadStatusNotLoaded
}

func countOutstandingResources(resources map[string]bool) int {
	log.Trace("countOutstandingResources")

	count := 0
	for _, outstanding := range resources {
		if outstanding {
			count++
		}
	}
	return count
}

func (c *ServedModelController) Health() ControllerHealth {
	log.Trace("ServedModelController Health")

	c.healthMu.RLock()
	defer c.healthMu.RUnlock()
	return c.health
}

func (c *ServedModelController) setKnownResourceCounts(count int, outstanding int) {
	log.Trace("ServedModelController setKnownResourceCounts")

	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	if count == 0 {
		c.health.FirstKnownServedModelAt = time.Time{}
	} else if c.health.KnownServedModels == 0 || c.health.FirstKnownServedModelAt.IsZero() {
		c.health.FirstKnownServedModelAt = time.Now()
	}
	c.health.KnownServedModels = count
	c.health.OutstandingServedModels = outstanding
}

func (c *ServedModelController) markStarted() {
	log.Trace("ServedModelController markStarted")

	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	c.health.Started = true
	c.health.LastActivityAt = time.Now()
}

func (c *ServedModelController) markWatchActive(active bool) {
	log.Trace("ServedModelController markWatchActive")

	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	c.health.WatchActive = active
	c.health.LastActivityAt = time.Now()
}

func (c *ServedModelController) markActivity() {
	log.Trace("ServedModelController markActivity")

	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	c.health.LastActivityAt = time.Now()
}

func (c *ServedModelController) markSuccessfulReconcile() {
	log.Trace("ServedModelController markSuccessfulReconcile")

	now := time.Now()
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	c.health.LastActivityAt = now
	c.health.LastSuccessfulReconcileAt = now
}

func (c *ServedModelController) markError(err error) {
	log.Trace("ServedModelController markError")

	if err == nil {
		return
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	c.health.LastErrorAt = time.Now()
	c.health.LastError = err.Error()
}

func sharedRuntimeLockKey(servedModel *model.ServedModel) string {
	log.Trace("sharedRuntimeLockKey")

	return "shared-runtime:" + SharedRuntimeWorkloadName(servedModel)
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
