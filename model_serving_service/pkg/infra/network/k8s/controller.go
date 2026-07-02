package k8s

import (
	"context"
	"errors"
	"time"

	"model_serving_service/pkg/app"

	log "github.com/sirupsen/logrus"
)

type ServedModelController struct {
	store        *ServedModelStore
	reconciler   app.ServedModelReconciler
	pollInterval time.Duration
}

func NewServedModelController(store *ServedModelStore, reconciler app.ServedModelReconciler, pollInterval time.Duration) *ServedModelController {
	log.Trace("NewServedModelController")

	return &ServedModelController{
		store:        store,
		reconciler:   reconciler,
		pollInterval: pollInterval,
	}
}

func (c *ServedModelController) Start(ctx context.Context) error {
	log.Trace("ServedModelController Start")

	if err := c.ProcessOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.WithContext(ctx).WithError(err).Error("served model reconciliation poll failed")
	}
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.ProcessOnce(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				log.WithContext(ctx).WithError(err).Error("served model reconciliation poll failed")
			}
		}
	}
}

func (c *ServedModelController) ProcessOnce(ctx context.Context) error {
	log.Trace("ServedModelController ProcessOnce")

	servedModels, err := c.store.List(ctx)
	if err != nil {
		return err
	}
	for _, servedModel := range servedModels {
		if err := c.reconciler.Reconcile(ctx, servedModel); err != nil {
			log.WithContext(ctx).WithError(err).WithField("served_model", servedModel.ResourceName).Error("served model reconcile failed")
			continue
		}
	}
	return nil
}
