package healthcheck_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"lib/shared_lib/healthcheck"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHealthCheck(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "HealthCheck Suite")
}

func getFreePort() (int, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()

	addr := l.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

var _ = Describe("HealthCheck", func() {
	var (
		monitor *healthcheck.Monitor
		ctx     context.Context
		port    int
		addr    string
		url     string
	)
	BeforeEach(func() {
		var err error
		port, err = getFreePort()
		Expect(err).To(BeNil())
		addr = fmt.Sprintf("localhost:%d", port)
		url = fmt.Sprintf("http://%s/health/", addr)

		cfg := healthcheck.HealthCheckConfig{
			CpuThresholdPercentage:           60,
			MemFreeThresholdPercentage:       20,
			HealthCheckPort:                  port,
			DBConnectionString:               "postgres://user:password@localhost:5432/dbName",
			MessageBrokerConnectionString:    "localhost:9092",
			DbLatencyThresholdSec:            5 * time.Second,
			MessageBrokerLatencyThresholdSec: 5 * time.Second,
			ServiceLatencyThresholdSec:       5 * time.Second,
		}

		monitor = healthcheck.NewMonitor(cfg)
		ctx = context.Background()
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			wg.Done()
			err = monitor.Connect(ctx)
		}()

		wg.Wait()
		Expect(err).To(BeNil())
		Eventually(func() error {
			conn, dialErr := net.DialTimeout("tcp", addr, 200*time.Millisecond)
			if dialErr == nil {
				conn.Close()
			}
			return dialErr
		}, "2s", "100ms").Should(BeNil())
	})

	AfterEach(func() {
		monitor.Close()
	})

	Context("Health check endpoint", func() {
		It("should return 200 OK when all checks report no errors", func() {
			monitor.Register("database", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			}).Register("message_broker", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			}).Register("memory", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			}).Register("cpu", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			})

			resp, err := http.Get(url)
			Expect(err).Should(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(body).To(Equal([]byte("ok")))
		})

		It("should return 500 when a database is unhealthy", func() {

			monitor.Register("database", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return errors.New("Postgres health check failed")
			}).Register("message_broker", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			}).Register("memory", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			}).Register("cpu", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			})

			resp, err := http.Get(url)
			Expect(err).Should(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))

			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("Postgres health check failed"))
		})

		It("should return 500 when a message broker is unhealthy", func() {

			monitor.Register("database", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			}).Register("message_broker", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return errors.New("Kafka health check failed")
			}).Register("memory", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			}).Register("cpu", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			})

			resp, err := http.Get(url)
			Expect(err).Should(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))

			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("Kafka health check failed"))
		})

		It("should return 500 when a memory is unhealthy", func() {

			monitor.Register("database", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			}).Register("message_broker", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			}).Register("memory", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return nil
			}).Register("memory", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return errors.New("Memory health check failed")
			})

			resp, err := http.Get(url)
			Expect(err).Should(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("Memory health check failed"))
		})

		It("should return 500 when a cpu is unhealthy", func() {

			monitor.WithMemoryCheck()
			monitor.Register("cpu", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return errors.New("CPU health check failed")
			})

			resp, err := http.Get(url)
			Expect(err).Should(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))

			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("CPU health check failed"))
		})

		It("should return 500 when all checks report errors", func() {

			monitor.Register("database", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return errors.New("Postgres health check failed")
			}).Register("message_broker", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return errors.New("Kafka health check failed")
			}).Register("memory", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return errors.New("Memory health check failed")
			})
			monitor.Register("cpu", func(_ context.Context, _ healthcheck.HealthCheckConfig) error {
				return errors.New("CPU health check failed")
			})

			resp, err := http.Get(url)
			Expect(err).Should(BeNil())
			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))

			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("Postgres health check failed"))
			Expect(string(body)).To(ContainSubstring("Kafka health check failed"))
			Expect(string(body)).To(ContainSubstring("Memory health check failed"))
			Expect(string(body)).To(ContainSubstring("CPU health check failed"))
		})
	})
})
