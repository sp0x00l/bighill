package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"tool_service/pkg/domain"
	"tool_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

var blockedNetworkPrefixes = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("64:ff9b::/96"),
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type HTTPGetExecutor struct {
	client           HTTPClient
	adapter          *HTTPGetArgumentsDTOAdapter
	timeout          time.Duration
	maxResponseBytes int64
}

func NewHTTPGetExecutor(client HTTPClient, adapter *HTTPGetArgumentsDTOAdapter, timeout time.Duration, maxResponseBytes int64) *HTTPGetExecutor {
	log.Trace("NewHTTPGetExecutor")

	if adapter == nil {
		log.Fatal("http tool arguments adapter is required")
	}
	if timeout <= 0 {
		log.Fatal("http tool timeout must be positive")
	}
	if maxResponseBytes <= 0 {
		log.Fatal("http tool max response bytes must be positive")
	}
	if client == nil {
		client = NewHardenedHTTPClient(timeout)
	}
	return &HTTPGetExecutor{
		client:           client,
		adapter:          adapter,
		timeout:          timeout,
		maxResponseBytes: maxResponseBytes,
	}
}

func (e *HTTPGetExecutor) Execute(ctx context.Context, tool *model.ToolDefinition, command model.InvokeToolCommand) (*model.ToolInvocationResult, error) {
	log.Trace("HTTPGetExecutor Execute")

	args, err := e.adapter.FromDTO(command.ArgumentsJSON)
	if err != nil {
		return nil, err
	}
	target, err := url.Parse(args.URL)
	if err != nil {
		return nil, domain.ErrValidationFailed.Extend("url is invalid")
	}
	if err := validateHTTPGetTarget(target, tool.EgressHosts); err != nil {
		return nil, err
	}

	requestCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, domain.ErrToolExecution.Extend(err.Error())
	}
	startedAt := time.Now()
	resp, err := e.client.Do(req)
	latency := time.Since(startedAt).Milliseconds()
	if err != nil {
		if errors.Is(err, domain.ErrToolPolicy) || errors.Is(err, domain.ErrToolDenied) {
			return nil, err
		}
		errorType := classifyHTTPToolError(err)
		return &model.ToolInvocationResult{
			ResultJSON:            []byte(`{}`),
			IsError:               true,
			ErrorCode:             httpToolErrorCode(errorType),
			ErrorMessage:          "http tool request failed",
			ErrorType:             errorType,
			ImplementationVersion: tool.ImplementationVersion,
			LatencyMs:             latency,
		}, nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, e.maxResponseBytes+1))
	if err != nil {
		return nil, domain.ErrToolExecution.Extend("read http tool response")
	}
	if int64(len(body)) > e.maxResponseBytes {
		return nil, domain.ErrToolPolicy.Extend("http tool response exceeds max response bytes")
	}
	payload, err := json.Marshal(map[string]any{
		"status": resp.StatusCode,
		"body":   string(body),
	})
	if err != nil {
		return nil, domain.ErrToolExecution.Extend("marshal http tool response")
	}
	return &model.ToolInvocationResult{
		ResultJSON:            payload,
		IsError:               resp.StatusCode >= http.StatusBadRequest,
		ErrorCode:             httpErrorCode(resp.StatusCode),
		ErrorMessage:          httpErrorMessage(resp.StatusCode),
		ErrorType:             httpErrorType(resp.StatusCode),
		ImplementationVersion: tool.ImplementationVersion,
		LatencyMs:             latency,
	}, nil
}

func validateHTTPGetTarget(target *url.URL, allowedHosts []string) error {
	log.Trace("validateHTTPGetTarget")

	if target.Scheme != "http" && target.Scheme != "https" {
		return domain.ErrToolPolicy.Extend("http tool url scheme is not allowed")
	}
	host := strings.ToLower(target.Hostname())
	if host == "" {
		return domain.ErrToolPolicy.Extend("http tool url host is required")
	}
	if !hostAllowed(host, allowedHosts) {
		return domain.ErrToolDenied.Extend(fmt.Sprintf("host %s is not allowlisted", host))
	}
	if blockedHost(host) {
		return domain.ErrToolPolicy.Extend("http tool url host is blocked")
	}
	return nil
}

func blockedHost(host string) bool {
	log.Trace("blockedHost")

	if strings.EqualFold(host, "metadata.google.internal") {
		return true
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		ips, lookupErr := net.LookupIP(host)
		if lookupErr != nil {
			return false
		}
		for _, ip := range ips {
			if blockedIP(ip) {
				return true
			}
		}
		return false
	}
	return blockedAddr(addr)
}

func blockedIP(ip net.IP) bool {
	log.Trace("blockedIP")

	addr, ok := netip.AddrFromSlice(ip)
	return ok && blockedAddr(addr)
}

func blockedAddr(addr netip.Addr) bool {
	log.Trace("blockedAddr")

	addr = addr.Unmap()
	for _, prefix := range blockedNetworkPrefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return addr.IsLoopback() ||
		addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsUnspecified() ||
		addr.IsMulticast() ||
		!addr.IsGlobalUnicast()
}

func hostAllowed(host string, allowedHosts []string) bool {
	log.Trace("hostAllowed")

	for _, allowed := range allowedHosts {
		if strings.EqualFold(strings.TrimSpace(allowed), host) {
			return true
		}
	}
	return false
}

func httpErrorCode(status int) string {
	log.Trace("httpErrorCode")

	if status >= http.StatusBadRequest {
		return "http_tool_request_failed"
	}
	return ""
}

func httpErrorMessage(status int) string {
	log.Trace("httpErrorMessage")

	if status >= http.StatusBadRequest {
		return fmt.Sprintf("http tool returned status %d", status)
	}
	return ""
}

func httpErrorType(status int) model.ToolErrorType {
	log.Trace("httpErrorType")

	if status >= http.StatusInternalServerError {
		return model.ToolErrorTypeTransient
	}
	if status >= http.StatusBadRequest {
		return model.ToolErrorTypePermanent
	}
	return model.ToolErrorTypeUnknown
}

func NewHardenedHTTPClient(timeout time.Duration) *http.Client {
	log.Trace("NewHardenedHTTPClient")

	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, domain.ErrToolPolicy.Extend("http tool dial target is invalid")
			}
			resolved, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			for _, candidate := range resolved {
				addr, ok := netip.AddrFromSlice(candidate.IP)
				if !ok || blockedAddr(addr) {
					continue
				}
				return dialer.DialContext(ctx, network, net.JoinHostPort(addr.Unmap().String(), port))
			}
			return nil, domain.ErrToolPolicy.Extend("http tool resolved host is blocked")
		},
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func classifyHTTPToolError(err error) model.ToolErrorType {
	log.Trace("classifyHTTPToolError")

	if errors.Is(err, domain.ErrToolPolicy) || errors.Is(err, domain.ErrToolDenied) {
		return model.ToolErrorTypePolicyDenied
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return model.ToolErrorTypeTransient
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return model.ToolErrorTypeTransient
	}
	return model.ToolErrorTypeTransient
}

func httpToolErrorCode(errorType model.ToolErrorType) string {
	log.Trace("httpToolErrorCode")

	if errorType == model.ToolErrorTypePolicyDenied {
		return domain.ErrToolPolicy.Code
	}
	return domain.ErrToolExecution.Code
}
