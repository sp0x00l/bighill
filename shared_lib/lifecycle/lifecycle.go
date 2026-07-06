package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type State string

const (
	StateStarting State = "starting"
	StateReady    State = "ready"
	StateDraining State = "draining"
	StateStopped  State = "stopped"
)

type Health struct {
	Name  string
	State State
	Ready bool
	Error error
}

type Component interface {
	Start(context.Context) error
	Drain(context.Context) error
	Close() error
	Health() Health
}

type Config struct {
	ReadinessTimeout time.Duration
	DrainTimeout     time.Duration
	CloseTimeout     time.Duration
}

type ComponentConfig struct {
	Name   string
	Start  func(context.Context) error
	Drain  func(context.Context) error
	Close  func() error
	Health func() Health
}

type FuncComponent struct {
	config ComponentConfig
	state  atomic.Value
}

func NewFuncComponent(config ComponentConfig) *FuncComponent {
	log.Trace("lifecycle NewFuncComponent")

	c := &FuncComponent{config: config}
	c.state.Store(StateStarting)
	return c
}

func (c *FuncComponent) Start(ctx context.Context) error {
	log.Trace("lifecycle FuncComponent Start")

	c.state.Store(StateReady)
	if c.config.Start == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	return c.config.Start(ctx)
}

func (c *FuncComponent) Drain(ctx context.Context) error {
	log.Trace("lifecycle FuncComponent Drain")

	c.state.Store(StateDraining)
	if c.config.Drain == nil {
		return nil
	}
	return c.config.Drain(ctx)
}

func (c *FuncComponent) Close() error {
	log.Trace("lifecycle FuncComponent Close")

	c.state.Store(StateStopped)
	if c.config.Close == nil {
		return nil
	}
	return c.config.Close()
}

func (c *FuncComponent) Health() Health {
	log.Trace("lifecycle FuncComponent Health")

	if c.config.Health != nil {
		return c.config.Health()
	}
	state, _ := c.state.Load().(State)
	return Health{Name: c.config.Name, State: state, Ready: state == StateReady}
}

type ReadinessGate struct {
	state atomic.Value
}

func NewReadinessGate() *ReadinessGate {
	log.Trace("lifecycle NewReadinessGate")

	gate := &ReadinessGate{}
	gate.state.Store(StateStarting)
	return gate
}

func (g *ReadinessGate) MarkReady() {
	log.Trace("lifecycle ReadinessGate MarkReady")

	g.state.Store(StateReady)
}

func (g *ReadinessGate) MarkDraining() {
	log.Trace("lifecycle ReadinessGate MarkDraining")

	g.state.Store(StateDraining)
}

func (g *ReadinessGate) MarkStopped() {
	log.Trace("lifecycle ReadinessGate MarkStopped")

	g.state.Store(StateStopped)
}

func (g *ReadinessGate) State() State {
	log.Trace("lifecycle ReadinessGate State")

	state, _ := g.state.Load().(State)
	return state
}

func (g *ReadinessGate) Ready() bool {
	log.Trace("lifecycle ReadinessGate Ready")

	return g.State() == StateReady
}

type Supervisor struct {
	components []Component
	readiness  *ReadinessGate
	config     Config
}

func NewSupervisor(components ...Component) *Supervisor {
	log.Trace("lifecycle NewSupervisor")

	return NewSupervisorWithConfig(Config{}, components...)
}

func NewSupervisorWithConfig(config Config, components ...Component) *Supervisor {
	log.Trace("lifecycle NewSupervisorWithConfig")

	return &Supervisor{
		components: append([]Component{}, components...),
		readiness:  NewReadinessGate(),
		config:     config,
	}
}

func (s *Supervisor) Readiness() *ReadinessGate {
	log.Trace("lifecycle Supervisor Readiness")

	return s.readiness
}

func (s *Supervisor) RunWithSignals(ctx context.Context, signals ...os.Signal) error {
	log.Trace("lifecycle Supervisor RunWithSignals")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, signals...)
	defer signal.Stop(quit)
	return s.Run(ctx, quit)
}

func (s *Supervisor) Run(ctx context.Context, signals <-chan os.Signal) error {
	log.Trace("lifecycle Supervisor Run")

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(s.components))
	var startWG sync.WaitGroup
	for _, component := range s.components {
		component := component
		startWG.Add(1)
		go func() {
			defer startWG.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					errCh <- fmt.Errorf("lifecycle component panic: %v", recovered)
				}
			}()
			if err := component.Start(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}

	var runErr error
	if err := s.waitUntilReady(runCtx, errCh); err != nil {
		runErr = err
	} else {
		s.readiness.MarkReady()
		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				runErr = ctx.Err()
			}
		case signal := <-signals:
			log.WithField("signal", signal).Info("lifecycle supervisor received shutdown signal")
		case err := <-errCh:
			runErr = err
		}
	}

	s.readiness.MarkDraining()
	drainCtx, cancelDrain := timeoutContext(s.config.DrainTimeout)
	drainErr := s.Drain(drainCtx)
	cancelDrain()
	cancel()
	waitCtx, cancelWait := timeoutContext(s.config.CloseTimeout)
	startWaitErr := waitForStartGoroutines(waitCtx, &startWG)
	cancelWait()
	closeCtx, cancelClose := timeoutContext(s.config.CloseTimeout)
	closeErr := s.CloseContext(closeCtx)
	cancelClose()
	s.readiness.MarkStopped()

	if drainErr != nil {
		return drainErr
	}
	if closeErr != nil {
		return closeErr
	}
	if startWaitErr != nil {
		return startWaitErr
	}
	return runErr
}

func (s *Supervisor) Drain(ctx context.Context) error {
	log.Trace("lifecycle Supervisor Drain")

	var out error
	for i := len(s.components) - 1; i >= 0; i-- {
		if ctx.Err() != nil {
			return errors.Join(out, ctx.Err())
		}
		if err := s.components[i].Drain(ctx); err != nil {
			out = errors.Join(out, shutdownError(err))
		}
	}
	return out
}

func (s *Supervisor) Close() error {
	log.Trace("lifecycle Supervisor Close")

	return s.CloseContext(context.Background())
}

func (s *Supervisor) CloseContext(ctx context.Context) error {
	log.Trace("lifecycle Supervisor CloseContext")

	var out error
	for i := len(s.components) - 1; i >= 0; i-- {
		if ctx.Err() != nil {
			return errors.Join(out, ctx.Err())
		}
		if err := closeComponent(ctx, s.components[i]); err != nil {
			out = errors.Join(out, shutdownError(err))
		}
	}
	return out
}

func (s *Supervisor) waitUntilReady(ctx context.Context, errCh <-chan error) error {
	log.Trace("lifecycle Supervisor waitUntilReady")

	readyCtx, cancel := childTimeoutContext(ctx, s.config.ReadinessTimeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.allComponentsReady() {
			return nil
		}
		select {
		case <-readyCtx.Done():
			return readyCtx.Err()
		case err := <-errCh:
			return err
		case <-ticker.C:
		}
	}
}

func (s *Supervisor) allComponentsReady() bool {
	log.Trace("lifecycle Supervisor allComponentsReady")

	for _, component := range s.components {
		if !component.Health().Ready {
			return false
		}
	}
	return true
}

func closeComponent(ctx context.Context, component Component) error {
	log.Trace("lifecycle closeComponent")

	done := make(chan error, 1)
	go func() {
		done <- component.Close()
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func shutdownError(err error) error {
	log.Trace("lifecycle shutdownError")

	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var out error
		for _, child := range joined.Unwrap() {
			out = errors.Join(out, shutdownError(child))
		}
		return out
	}
	if errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

func waitForStartGoroutines(ctx context.Context, wg *sync.WaitGroup) error {
	log.Trace("lifecycle waitForStartGoroutines")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func timeoutContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	log.Trace("lifecycle timeoutContext")

	if timeout <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), timeout)
}

func childTimeoutContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	log.Trace("lifecycle childTimeoutContext")

	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}
