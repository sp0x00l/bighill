package lifecycle_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"lib/shared_lib/lifecycle"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLifecycle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lifecycle unit test suite")
}

var _ = Describe("Supervisor", func() {
	It("marks readiness and drains before canceling component context", func() {
		var mu sync.Mutex
		order := []string{}
		ctxLiveDuringDrain := false
		signals := make(chan os.Signal, 1)
		component := lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "server",
			Start: func(ctx context.Context) error {
				recordOrder(&mu, &order, "start")
				<-ctx.Done()
				recordOrder(&mu, &order, "start-done")
				return ctx.Err()
			},
			Drain: func(ctx context.Context) error {
				recordOrder(&mu, &order, "drain")
				select {
				case <-ctx.Done():
				default:
					ctxLiveDuringDrain = true
				}
				return nil
			},
			Close: func() error {
				recordOrder(&mu, &order, "close")
				return nil
			},
		})
		supervisor := lifecycle.NewSupervisorWithConfig(testLifecycleConfig(), component)
		done := make(chan error, 1)

		go func() {
			done <- supervisor.Run(context.Background(), signals)
		}()

		Eventually(supervisor.Readiness().Ready).Should(BeTrue())
		Eventually(func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string{}, order...)
		}).Should(Equal([]string{"start"}))
		signals <- syscall.SIGTERM

		Eventually(done).Should(Receive(Succeed()))
		Expect(ctxLiveDuringDrain).To(BeTrue())
		Expect(order).To(Equal([]string{"start", "drain", "start-done", "close"}))
		Expect(supervisor.Readiness().State()).To(Equal(lifecycle.StateStopped))
	})

	It("drains and closes components in reverse start order", func() {
		var mu sync.Mutex
		order := []string{}
		signals := make(chan os.Signal, 1)
		first := orderedComponent("first", &mu, &order)
		second := orderedComponent("second", &mu, &order)
		supervisor := lifecycle.NewSupervisorWithConfig(testLifecycleConfig(), first, second)
		done := make(chan error, 1)

		go func() {
			done <- supervisor.Run(context.Background(), signals)
		}()

		Eventually(supervisor.Readiness().Ready).Should(BeTrue())
		Eventually(func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string{}, order...)
		}).Should(ConsistOf("first:start", "second:start"))
		signals <- syscall.SIGTERM

		Eventually(done).Should(Receive(Succeed()))
		Expect(order).To(ContainElements("first:start", "second:start", "first:drain", "second:drain", "first:close", "second:close"))
		Expect(orderIndex(order, "second:drain")).To(BeNumerically("<", orderIndex(order, "first:drain")))
		Expect(orderIndex(order, "second:close")).To(BeNumerically("<", orderIndex(order, "first:close")))
	})

	It("returns component start errors after cleanup", func() {
		expectedErr := errors.New("listen failed")
		component := lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "server",
			Start: func(context.Context) error {
				return expectedErr
			},
		})
		supervisor := lifecycle.NewSupervisorWithConfig(testLifecycleConfig(), component)

		err := supervisor.Run(context.Background(), make(chan os.Signal))

		Expect(err).To(MatchError(expectedErr))
		Expect(supervisor.Readiness().State()).To(Equal(lifecycle.StateStopped))
	})

	It("bounds drain with the configured timeout and still closes components", func() {
		signals := make(chan os.Signal, 1)
		closed := make(chan struct{})
		component := lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "stuck-drain",
			Start: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			Drain: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			Close: func() error {
				close(closed)
				return nil
			},
		})
		supervisor := lifecycle.NewSupervisorWithConfig(lifecycle.Config{
			ReadinessTimeout: time.Second,
			DrainTimeout:     10 * time.Millisecond,
			CloseTimeout:     time.Second,
		}, component)
		done := make(chan error, 1)

		go func() {
			done <- supervisor.Run(context.Background(), signals)
		}()

		Eventually(supervisor.Readiness().Ready).Should(BeTrue())
		signals <- syscall.SIGTERM

		Eventually(done).Should(Receive(MatchError(context.DeadlineExceeded)))
		Eventually(closed).Should(BeClosed())
	})

	It("bounds close with the configured timeout", func() {
		signals := make(chan os.Signal, 1)
		component := lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "stuck-close",
			Start: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			Close: func() error {
				select {}
			},
		})
		supervisor := lifecycle.NewSupervisorWithConfig(lifecycle.Config{
			ReadinessTimeout: time.Second,
			DrainTimeout:     time.Second,
			CloseTimeout:     10 * time.Millisecond,
		}, component)
		done := make(chan error, 1)

		go func() {
			done <- supervisor.Run(context.Background(), signals)
		}()

		Eventually(supervisor.Readiness().Ready).Should(BeTrue())
		signals <- syscall.SIGTERM

		Eventually(done).Should(Receive(MatchError(context.DeadlineExceeded)))
	})

	It("treats component context cancellation during drain and close as clean shutdown", func() {
		signals := make(chan os.Signal, 1)
		component := lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "canceling-component",
			Start: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			Drain: func(context.Context) error {
				return context.Canceled
			},
			Close: func() error {
				return context.Canceled
			},
		})
		supervisor := lifecycle.NewSupervisorWithConfig(testLifecycleConfig(), component)
		done := make(chan error, 1)

		go func() {
			done <- supervisor.Run(context.Background(), signals)
		}()

		Eventually(supervisor.Readiness().Ready).Should(BeTrue())
		signals <- syscall.SIGTERM

		Eventually(done).Should(Receive(Succeed()))
	})

	It("waits for component health before marking readiness", func() {
		signals := make(chan os.Signal, 1)
		ready := make(chan struct{})
		component := &healthControlledComponent{ready: ready}
		supervisor := lifecycle.NewSupervisorWithConfig(lifecycle.Config{
			ReadinessTimeout: time.Second,
			DrainTimeout:     time.Second,
			CloseTimeout:     time.Second,
		}, component)
		done := make(chan error, 1)

		go func() {
			done <- supervisor.Run(context.Background(), signals)
		}()

		Consistently(supervisor.Readiness().Ready, 30*time.Millisecond, 5*time.Millisecond).Should(BeFalse())
		close(ready)
		Eventually(supervisor.Readiness().Ready).Should(BeTrue())
		signals <- syscall.SIGTERM
		Eventually(done).Should(Receive(Succeed()))
	})

	It("routes component panics through the supervisor cleanup path", func() {
		closed := make(chan struct{})
		component := lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
			Name: "panic",
			Start: func(context.Context) error {
				values := []string{}
				_ = values[0]
				return nil
			},
			Close: func() error {
				close(closed)
				return nil
			},
		})
		supervisor := lifecycle.NewSupervisorWithConfig(testLifecycleConfig(), component)

		err := supervisor.Run(context.Background(), make(chan os.Signal))

		Expect(err).To(MatchError(ContainSubstring("lifecycle component panic: runtime error: index out of range")))
		Eventually(closed).Should(BeClosed())
	})
})

func orderedComponent(name string, mu *sync.Mutex, order *[]string) *lifecycle.FuncComponent {
	return lifecycle.NewFuncComponent(lifecycle.ComponentConfig{
		Name: name,
		Start: func(ctx context.Context) error {
			recordOrder(mu, order, name+":start")
			<-ctx.Done()
			return ctx.Err()
		},
		Drain: func(context.Context) error {
			recordOrder(mu, order, name+":drain")
			return nil
		},
		Close: func() error {
			recordOrder(mu, order, name+":close")
			return nil
		},
	})
}

func recordOrder(mu *sync.Mutex, order *[]string, item string) {
	mu.Lock()
	defer mu.Unlock()
	*order = append(*order, item)
}

func orderIndex(order []string, item string) int {
	for idx, got := range order {
		if got == item {
			return idx
		}
	}
	return -1
}

func testLifecycleConfig() lifecycle.Config {
	return lifecycle.Config{
		ReadinessTimeout: time.Second,
		DrainTimeout:     time.Second,
		CloseTimeout:     time.Second,
	}
}

type healthControlledComponent struct {
	ready <-chan struct{}
}

func (c *healthControlledComponent) Start(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (c *healthControlledComponent) Drain(context.Context) error {
	return nil
}

func (c *healthControlledComponent) Close() error {
	return nil
}

func (c *healthControlledComponent) Health() lifecycle.Health {
	select {
	case <-c.ready:
		return lifecycle.Health{Name: "health-controlled", State: lifecycle.StateReady, Ready: true}
	default:
		return lifecycle.Health{Name: "health-controlled", State: lifecycle.StateStarting, Ready: false}
	}
}
