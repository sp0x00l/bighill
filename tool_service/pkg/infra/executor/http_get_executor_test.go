package executor

import (
	"context"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"tool_service/pkg/domain"
	"tool_service/pkg/domain/model"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHTTPGetExecutor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool Service Executor Suite")
}

var _ = Describe("HTTPGetExecutor", func() {
	var adapter *HTTPGetArgumentsDTOAdapter

	BeforeEach(func() {
		adapter = NewHTTPGetArgumentsDTOAdapter(validator.New())
	})

	It("fetches an allowlisted URL and wraps the response as JSON", func() {
		client := &httpClientStub{
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("tool response")),
			},
		}
		targetURL := "https://allowed.example/resource"
		host := mustHost(targetURL)
		executor := NewHTTPGetExecutor(client, adapter, time.Second, 1024)

		result, err := executor.Execute(context.Background(), toolWithHosts(host), model.InvokeToolCommand{
			InvocationID:  uuid.New(),
			ToolName:      "http_get",
			ArgumentsJSON: []byte(`{"url":"` + targetURL + `"}`),
			OrgID:         uuid.New(),
			UserID:        uuid.New(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(client.request.Method).To(Equal(http.MethodGet))
		Expect(client.request.URL.String()).To(Equal(targetURL))
		Expect(result.IsError).To(BeFalse())
		Expect(result.ResultJSON).To(MatchJSON(`{"status":200,"body":"tool response"}`))
		Expect(result.ImplementationVersion).To(Equal("http_get:v1"))
	})

	It("rejects malformed arguments at the executor boundary", func() {
		executor := NewHTTPGetExecutor(http.DefaultClient, adapter, time.Second, 1024)

		_, err := executor.Execute(context.Background(), toolWithHosts("example.com"), model.InvokeToolCommand{
			ArgumentsJSON: []byte(`{"url":""}`),
		})

		Expect(err).To(MatchError(ContainSubstring("validation failed")))
	})

	It("blocks metadata service SSRF targets even when the host is allowlisted", func() {
		executor := NewHTTPGetExecutor(http.DefaultClient, adapter, time.Second, 1024)

		_, err := executor.Execute(context.Background(), toolWithHosts("169.254.169.254"), model.InvokeToolCommand{
			ArgumentsJSON: []byte(`{"url":"http://169.254.169.254/latest/meta-data"}`),
		})

		Expect(err).To(MatchError(ContainSubstring("http tool url host is blocked")))
		Expect(err).To(MatchError(MatchRegexp(domain.ErrToolPolicy.Error() + ".*")))
	})

	It("blocks loopback and private IP targets even when the host is allowlisted", func() {
		Expect(blockedAddr(netip.MustParseAddr("127.0.0.1"))).To(BeTrue())
		Expect(blockedAddr(netip.MustParseAddr("10.0.0.1"))).To(BeTrue())
		Expect(blockedAddr(netip.MustParseAddr("192.168.1.10"))).To(BeTrue())
		Expect(blockedAddr(netip.MustParseAddr("100.64.0.1"))).To(BeTrue())
		Expect(blockedAddr(netip.MustParseAddr("64:ff9b::c000:0201"))).To(BeTrue())
		Expect(blockedAddr(netip.MustParseAddr("::1"))).To(BeTrue())
	})

	It("does not follow redirects or use environment proxies in the hardened production client", func() {
		client := NewHardenedHTTPClient(time.Second)

		err := client.CheckRedirect(&http.Request{}, []*http.Request{{}})

		Expect(err).To(Equal(http.ErrUseLastResponse))
		transport, ok := client.Transport.(*http.Transport)
		Expect(ok).To(BeTrue())
		Expect(transport.Proxy).To(BeNil())
	})

	It("propagates hardened client policy blocks as executor errors", func() {
		executor := NewHTTPGetExecutor(&httpClientStub{err: domain.ErrToolPolicy.Extend("http tool resolved host is blocked")}, adapter, time.Second, 1024)

		_, err := executor.Execute(context.Background(), toolWithHosts("allowed.example"), model.InvokeToolCommand{
			ArgumentsJSON: []byte(`{"url":"https://allowed.example/path"}`),
		})

		Expect(err).To(MatchError(ContainSubstring("http tool resolved host is blocked")))
		Expect(err).To(MatchError(MatchRegexp(domain.ErrToolPolicy.Error() + ".*")))
	})

	It("denies hosts outside the tool allowlist", func() {
		executor := NewHTTPGetExecutor(http.DefaultClient, adapter, time.Second, 1024)

		_, err := executor.Execute(context.Background(), toolWithHosts("allowed.example"), model.InvokeToolCommand{
			ArgumentsJSON: []byte(`{"url":"https://denied.example/path"}`),
		})

		Expect(err).To(MatchError(ContainSubstring("host denied.example is not allowlisted")))
		Expect(err).To(MatchError(MatchRegexp(domain.ErrToolDenied.Error() + ".*")))
	})
})

func toolWithHosts(hosts ...string) *model.ToolDefinition {
	return &model.ToolDefinition{
		Name:                  "http_get",
		ImplementationVersion: "http_get:v1",
		ExecutorKind:          model.ToolExecutorKindHTTPGet,
		EgressHosts:           hosts,
		Enabled:               true,
	}
}

func mustHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	Expect(err).NotTo(HaveOccurred())
	return parsed.Hostname()
}

type httpClientStub struct {
	request  *http.Request
	response *http.Response
	err      error
}

func (s *httpClientStub) Do(req *http.Request) (*http.Response, error) {
	s.request = req
	return s.response, s.err
}
