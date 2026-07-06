package lifecycle

import (
	"context"
	"errors"
	"net/http"

	log "github.com/sirupsen/logrus"
)

type ConnectServer interface {
	Connect() error
}

type ShutdownServer interface {
	Shutdown(context.Context) error
}

type ReadyServer interface {
	Ready() bool
}

type CloseFunc func() error

type HealthCheckServer interface {
	Connect(context.Context) error
	Shutdown(context.Context) error
}

type StartStopWorker interface {
	Start() error
	Stop()
}

func ServerComponent(name string, server interface {
	ConnectServer
	ShutdownServer
}) Component {
	log.Trace("lifecycle ServerComponent")

	return NewFuncComponent(ComponentConfig{
		Name: name,
		Start: func(context.Context) error {
			err := server.Connect()
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		},
		Drain:  server.Shutdown,
		Health: readyHealth(name, server),
	})
}

func WorkerComponent(name string, start func(context.Context) error) Component {
	log.Trace("lifecycle WorkerComponent")

	return NewFuncComponent(ComponentConfig{
		Name:  name,
		Start: start,
	})
}

func CloserComponent(name string, close CloseFunc) Component {
	log.Trace("lifecycle CloserComponent")

	return NewFuncComponent(ComponentConfig{
		Name: name,
		Close: func() error {
			return close()
		},
	})
}

func HealthCheckComponent(name string, server HealthCheckServer) Component {
	log.Trace("lifecycle HealthCheckComponent")

	return NewFuncComponent(ComponentConfig{
		Name: name,
		Start: func(ctx context.Context) error {
			err := server.Connect(ctx)
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		},
		Drain: func(ctx context.Context) error {
			return server.Shutdown(ctx)
		},
		Health: readyHealth(name, server),
	})
}

func TemporalWorkerComponent(name string, worker StartStopWorker) Component {
	log.Trace("lifecycle TemporalWorkerComponent")

	return NewFuncComponent(ComponentConfig{
		Name: name,
		Start: func(ctx context.Context) error {
			if err := worker.Start(); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		},
		Drain: func(context.Context) error {
			worker.Stop()
			return nil
		},
	})
}

func readyHealth(name string, server any) func() Health {
	log.Trace("lifecycle readyHealth")

	readyServer, ok := server.(ReadyServer)
	if !ok {
		return nil
	}
	return func() Health {
		if readyServer.Ready() {
			return Health{Name: name, State: StateReady, Ready: true}
		}
		return Health{Name: name, State: StateStarting, Ready: false}
	}
}
