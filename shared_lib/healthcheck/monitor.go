package healthcheck

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	log "github.com/sirupsen/logrus"
)

type HealthCheckConfig struct {
	CpuThresholdPercentage                       int
	MemFreeThresholdPercentage                   int
	HealthCheckPort                              int
	DBConnectionString                           string
	MessageBrokerConnectionString                string
	DbLatencyThresholdSec                        time.Duration
	MessageBrokerLatencyThresholdSec             time.Duration
	ServiceLatencyThresholdSec                   time.Duration
	HttpCheckTargets                             map[string]string
	HttpCheckTimeoutSec                          time.Duration
	MessageBrokerSubscriberMaxPollSilenceSec     time.Duration
	MessageBrokerSubscriberMaxProgressSilenceSec time.Duration
	MessageBrokerSubscriberMaxLag                int64
}

type healthCheck func(ctx context.Context, config HealthCheckConfig) error

type Monitor struct {
	httpServer *http.Server
	router     *mux.Router
	config     HealthCheckConfig
	checks     map[string]healthCheck
	results    map[string]error
	mutex      sync.Mutex
	ready      atomic.Bool
}

func NewMonitor(config HealthCheckConfig) *Monitor {
	log.Trace("NewMonitor Healthcheck")

	if config.MemFreeThresholdPercentage < 0 || config.MemFreeThresholdPercentage > 100 {
		log.Fatalf("invalid memory threshold: %d", config.MemFreeThresholdPercentage)
	}
	if config.CpuThresholdPercentage < 0 || config.CpuThresholdPercentage > 100 {
		log.Fatalf("invalid cpu threshold: %d", config.CpuThresholdPercentage)
	}

	m := &Monitor{
		router:  mux.NewRouter().StrictSlash(true),
		checks:  make(map[string]healthCheck),
		results: make(map[string]error),
		mutex:   sync.Mutex{},
		config:  config,
	}
	m.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", config.HealthCheckPort),
		Handler: m, // interface w/ ServeHTTP method
	}
	return m
}

func (m *Monitor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Trace("HealthCheck ServeHTTP")

	resultsErr := m.runChecks(r.Context())

	if len(resultsErr) > 0 {
		w.WriteHeader(http.StatusInternalServerError)
		for _, msg := range resultsErr {
			fmt.Fprint(w, msg)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (m *Monitor) Check(ctx context.Context) error {
	log.Trace("Monitor Check")

	resultsErr := m.runChecks(ctx)
	if len(resultsErr) > 0 {
		return errors.New(strings.Join(resultsErr, "; "))
	}
	return nil
}

func (m *Monitor) Connect(ctx context.Context) error {
	log.Trace("Monitor Connect")

	log.WithContext(ctx).Infof("health check monitor starting http listener on %s", m.httpServer.Addr)
	listener, err := net.Listen("tcp", m.httpServer.Addr)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("HealthCheck HTTP server failed to listen")
		return err
	}
	m.ready.Store(true)
	defer m.ready.Store(false)
	err = m.httpServer.Serve(listener)
	if err != nil && err != http.ErrServerClosed {
		log.WithContext(ctx).WithError(err).Error("HealthCheck HTTP server failed")
	}
	return err
}

func (m *Monitor) Close() {
	log.Trace("Monitor Close")

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := m.Shutdown(ctx); err != nil {
		log.WithContext(ctx).Errorf("Monitor http server shutdown error: %v", err)
	}
}

func (m *Monitor) Shutdown(ctx context.Context) error {
	log.Trace("Monitor Shutdown")

	m.ready.Store(false)
	if err := m.httpServer.Shutdown(ctx); err != nil {
		log.WithContext(ctx).Errorf("Monitor http server shutdown error: %v", err)
		return err
	}
	return nil
}

func (m *Monitor) Ready() bool {
	log.Trace("Monitor Ready")

	return m.ready.Load()
}

func (m *Monitor) WithDatabaseCheck() *Monitor {
	return m.Register("postgres", dbCheck)
}

func (m *Monitor) WithMessageBrokerCheck() *Monitor {
	return m.Register("message_broker", messageBrokerCheck)
}

func (m *Monitor) WithHttpCheck() *Monitor {
	return m.Register("http", httpCheck)
}

func (m *Monitor) WithMemoryCheck() *Monitor {
	return m.Register("memory", memoryHealthcheck)
}

func (m *Monitor) WithCpuCheck() *Monitor {
	return m.Register("cpu", cpuHealthcheck)
}

// adds a new health check
func (m *Monitor) Register(name string, check healthCheck) *Monitor {
	log.Trace("Monitor Register")

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.checks[name] = check

	return m
}

func (m *Monitor) Unregister(name string) *Monitor {
	log.Trace("Monitor Unregister")

	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.checks, name)

	return m
}

func (m *Monitor) runChecks(ctx context.Context) []string {
	// if the healthcheck doesn't complete within the timeout, an error is returned within
	// the context passed to the check, so the check will fail.
	timeout := m.config.ServiceLatencyThresholdSec
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup
	results := make(map[string]error)
	var resultsMu sync.Mutex

	m.mutex.Lock()
	checks := make(map[string]healthCheck, len(m.checks))
	for name, check := range m.checks {
		checks[name] = check
	}
	m.mutex.Unlock()

	for name, check := range checks {
		wg.Add(1)
		go func(name string, check healthCheck) {
			defer wg.Done()
			if err := check(ctx, m.config); err != nil {
				resultsMu.Lock()
				results[name] = err
				resultsMu.Unlock()
			}
		}(name, check)
	}
	wg.Wait()

	resultsErr := []string{}
	for name, err := range results {
		if err != nil {
			msg := fmt.Sprintf("%s check failed: %v\n", name, err.Error())
			resultsErr = append(resultsErr, msg)
		}
	}
	return resultsErr
}

// Two approaches were considered.
// 1. To maintain an open connection and poll the health of the connection.
// 2. To open a connection, check the health, and close the connection.
// The first approach is complex, stale connections can be a problem
// (reconnections are a normal occurance with networked services) so they can
// give false positives. The second approach is simpler and more reliable.

func dbCheck(ctx context.Context, config HealthCheckConfig) error {
	log.Trace("Monitor Postgres Healthcheck")

	start := time.Now()
	dbpool, err := pgxpool.New(ctx, config.DBConnectionString)
	if err != nil {
		// not logging errors, as they are health checks
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	defer dbpool.Close()

	err = dbpool.Ping(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	duration := time.Since(start)
	if duration.Seconds() > config.DbLatencyThresholdSec.Seconds() {
		log.Printf("Postgres response time warning: %v", duration)
	}

	return nil
}

func memoryHealthcheck(ctx context.Context, config HealthCheckConfig) error {
	log.Trace("Monitor Memory Healthcheck")

	v, err := mem.VirtualMemory()
	if err != nil {
		log.WithContext(ctx).Error("failed to get memory info in Healthcheck")
		return fmt.Errorf("failed to get memory info: %w", err)
	}
	if v.UsedPercent > float64(100-config.MemFreeThresholdPercentage) {
		log.WithContext(ctx).Warnf("Memory usage: %f", v.UsedPercent)
		return fmt.Errorf("memory usage above threshold: %v%%", v.UsedPercent)
	}

	return nil
}

func cpuHealthcheck(ctx context.Context, config HealthCheckConfig) error {
	log.Trace("Monitor CPU Healthcheck")

	// current cpu times against the last call, all cpu cores combined
	percentages, err := cpu.Percent(0, false)
	if err != nil {
		return fmt.Errorf("failed to get CPU info: %w", err)
	}

	for _, p := range percentages {
		if p > float64(config.CpuThresholdPercentage) {
			log.WithContext(ctx).Warnf("CPU usage above threshold: %v%%", p)
			return fmt.Errorf("CPU usage above threshold: %v%%", p)
		}
	}
	return nil
}
