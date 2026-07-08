package transport_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"lib/shared_lib/transport"
	"net"
	"net/http"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("test-trace")

func TestHTTPServer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "http tansport unit test suite")
}

func freeTCPPort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).ShouldNot(HaveOccurred())
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	Expect(ok).To(BeTrue())
	return addr.Port
}

var _ = Describe("http server", func() {
	var (
		routes []transport.Route
		server *transport.HttpServer
		port   int
	)

	BeforeEach(func() {
		routes = []transport.Route{
			{
				Handler: func(ctx context.Context, r *http.Request) (int, []byte, error) {
					return http.StatusOK, []byte("get reply"), nil
				},
				Path:   "/test-get-success",
				Method: "GET",
			},
			{
				Handler: func(ctx context.Context, r *http.Request) (int, []byte, error) {
					return http.StatusInternalServerError, nil, errors.New("test-get error")
				},
				Path:   "/test-get-with-error",
				Method: "GET",
			},
		}
		port = freeTCPPort()
		server = transport.NewHttpServer(tracer, routes, port, "test")
		Expect(server).NotTo(BeNil())

		go func() {
			defer GinkgoRecover()
			err := server.Connect()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				Fail(fmt.Sprintf("server connect failed: %v", err))
			}
		}()
		// Wait for server to start listening
		Eventually(func() error {
			_, err := http.Get(fmt.Sprintf("http://localhost:%d/test-get-success", port))
			return err
		}).Should(Succeed())
	})

	AfterEach(func() {
		server.Close()
	})

	Context("Postive tests", func() {
		When("the server is connected and listening", func() {

			It("should return a 200 status code and body", func() {
				request, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/test-get-success", port), nil)
				response, err := http.DefaultClient.Do(request)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(response.StatusCode).To(Equal(http.StatusOK))
				Expect(response.Body).NotTo(BeNil())

				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(string(body)).To(Equal("get reply"))
			})
		})
	})

	Context("Negative tests", func() {
		When("the server is connected and listening", func() {

			It("should return a 400 status code and writes error for a GET request", func() {
				request, _ := http.NewRequest("GET", fmt.Sprintf("http://localhost:%d/test-get-with-error", port), nil)
				response, err := http.DefaultClient.Do(request)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
				Expect(response.Body).NotTo(BeNil())

				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				Expect(err).ShouldNot(HaveOccurred())

				errJson := transport.ErrorMessage{}
				err = json.Unmarshal(body, &errJson)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(errJson.Message).To(ContainSubstring("test-get error"))
			})
		})

		It("applies a configured write timeout", func() {
			slowPort := freeTCPPort()
			slowServer := transport.NewHttpServer(
				tracer,
				[]transport.Route{
					{
						Handler: func(ctx context.Context, r *http.Request) (int, []byte, error) {
							time.Sleep(150 * time.Millisecond)
							return http.StatusOK, []byte("slow reply"), nil
						},
						Path:   "/slow",
						Method: http.MethodGet,
					},
				},
				slowPort,
				"slow-test",
				transport.WithServerTimeouts(time.Second, 25*time.Millisecond, time.Second),
			)
			go func() {
				defer GinkgoRecover()
				err := slowServer.Connect()
				if err != nil && !errors.Is(err, http.ErrServerClosed) {
					Fail(fmt.Sprintf("slow server connect failed: %v", err))
				}
			}()
			defer slowServer.Close()

			Eventually(func() error {
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", slowPort), 50*time.Millisecond)
				if err != nil {
					return err
				}
				return conn.Close()
			}).Should(Succeed())

			client := &http.Client{Timeout: time.Second}
			response, err := client.Get(fmt.Sprintf("http://localhost:%d/slow", slowPort))
			if response != nil && response.Body != nil {
				response.Body.Close()
			}
			Expect(err).To(HaveOccurred())
		})
	})
})
