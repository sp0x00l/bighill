package router

import (
	"api/pkg/adapter"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"lib/shared_lib/authz"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-lambda-go/events"
	log "github.com/sirupsen/logrus"
)

const (
	publicEndpoint    = "public"
	privateEndpoint   = "private"
	userHeader        = "X-User-ID"
	sessionHeader     = "X-Session-ID"
	orgHeader         = "X-Org-ID"
	rolesHeader       = "X-Roles"
	permissionsHeader = "X-Permissions"
	denyRoutePolicy   = "__deny_private_route__"

	corsAllowOrigin  = "*"
	corsAllowMethods = "GET,POST,PUT,DELETE,PATCH,OPTIONS"
	corsAllowHeaders = "Content-Type,Authorization,X-Request-ID,X-Amz-Date,X-Api-Key,X-Amz-Security-Token"
)

type AuthorizerContext struct {
	UserID      string   `json:"userId"`
	SessionID   string   `json:"sid"`
	OrgID       string   `json:"orgId"`
	Roles       []string `json:"roles"`
	Permissions []string `json:"permissions"`
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type newRequestFunc func(method, url string, body io.Reader) (*http.Request, error)

type Config struct {
	DataRegistryServiceRoute  string
	IngestionServiceRoute     string
	ModelRegistryServiceRoute string
	TenantServiceRoute        string
	TrainingServiceRoute      string
	InferenceServiceRoute     string
	SocketServiceRoute        string
}

type routeResolver struct {
	dataRegistryServiceRoute  string
	ingestionServiceRoute     string
	modelRegistryServiceRoute string
	tenantServiceRoute        string
	trainingServiceRoute      string
	inferenceServiceRoute     string
	socketServiceRoute        string
}

type routeContext struct {
	method        string
	path          string
	version       string
	scope         string
	resource      string
	resourceIndex int
	segments      []string
}

type routeStatusError struct {
	status int
	body   string
}

func (e *routeStatusError) Error() string {
	return e.body
}

func (cfg Config) resolver() (routeResolver, error) {
	if cfg.DataRegistryServiceRoute == "" {
		return routeResolver{}, fmt.Errorf("missing data registry service route")
	}
	if cfg.IngestionServiceRoute == "" {
		return routeResolver{}, fmt.Errorf("missing ingestion service route")
	}
	if cfg.ModelRegistryServiceRoute == "" {
		return routeResolver{}, fmt.Errorf("missing model registry service route")
	}
	if cfg.TenantServiceRoute == "" {
		return routeResolver{}, fmt.Errorf("missing tenant service route")
	}
	if cfg.TrainingServiceRoute == "" {
		return routeResolver{}, fmt.Errorf("missing training service route")
	}
	if cfg.InferenceServiceRoute == "" {
		return routeResolver{}, fmt.Errorf("missing inference service route")
	}
	if cfg.SocketServiceRoute == "" {
		return routeResolver{}, fmt.Errorf("missing socket service route")
	}
	return routeResolver{
		dataRegistryServiceRoute:  cfg.DataRegistryServiceRoute,
		ingestionServiceRoute:     cfg.IngestionServiceRoute,
		modelRegistryServiceRoute: cfg.ModelRegistryServiceRoute,
		tenantServiceRoute:        cfg.TenantServiceRoute,
		trainingServiceRoute:      cfg.TrainingServiceRoute,
		inferenceServiceRoute:     cfg.InferenceServiceRoute,
		socketServiceRoute:        cfg.SocketServiceRoute,
	}, nil
}

func getHeaderValue(headers map[string]string, key string) string {
	for k, v := range headers {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

func spoofedTrustedHeader(headers map[string]string) string {
	log.Trace("spoofedTrustedHeader")

	for _, trusted := range []string{userHeader, sessionHeader, orgHeader, rolesHeader, permissionsHeader} {
		if value := getHeaderValue(headers, trusted); value != "" {
			return trusted
		}
	}
	return ""
}

func requiredRoutePermission(request events.APIGatewayProxyRequest) string {
	log.Trace("requiredRoutePermission")

	routeCtx, err := parseRouteContext(request)
	if err != nil || routeCtx.scope != privateEndpoint {
		return ""
	}
	afterResource := routeCtx.segments[routeCtx.resourceIndex+1:]
	switch routeCtx.resource {
	case "data":
		if routeCtx.method == http.MethodPost || routeCtx.method == http.MethodPut || routeCtx.method == http.MethodDelete {
			return authz.PermissionDataWrite
		}
		return authz.PermissionDataRead
	case "models":
		if routeCtx.method == http.MethodPost || routeCtx.method == http.MethodPut || routeCtx.method == http.MethodDelete {
			return authz.PermissionModelWrite
		}
		return authz.PermissionModelRead
	case "training-runs":
		if routeCtx.method == http.MethodPost {
			return authz.PermissionTrainingStart
		}
		return authz.PermissionTrainingRead
	case "inference":
		if len(afterResource) == 1 && afterResource[0] == "endpoints" && routeCtx.method == http.MethodGet {
			return authz.PermissionInferenceEndpointsRead
		}
		if len(afterResource) == 1 && afterResource[0] == "endpoints" && routeCtx.method == http.MethodPost {
			return authz.PermissionModelWrite
		}
		if len(afterResource) == 1 && afterResource[0] == "agent-specs" && routeCtx.method == http.MethodPost {
			return authz.PermissionModelWrite
		}
		if len(afterResource) >= 3 && afterResource[0] == "endpoints" && (afterResource[2] == "datasets" || afterResource[2] == "merge-strategy") && routeCtx.method == http.MethodPut {
			return authz.PermissionModelWrite
		}
		if len(afterResource) >= 3 && afterResource[0] == "endpoints" && afterResource[2] == "generations" && routeCtx.method == http.MethodPost {
			return authz.PermissionInferenceInvoke
		}
		if len(afterResource) >= 3 && afterResource[0] == "endpoints" && afterResource[2] == "preference-datasets" && routeCtx.method == http.MethodPost {
			return authz.PermissionModelWrite
		}
		if len(afterResource) >= 1 && afterResource[0] == "preference-datasets" && routeCtx.method == http.MethodGet {
			return authz.PermissionInferenceEndpointsRead
		}
		if len(afterResource) >= 2 && afterResource[0] == "agent-runs" && routeCtx.method == http.MethodGet {
			return authz.PermissionInferenceAgentRunsRead
		}
		if len(afterResource) == 1 && afterResource[0] == "feedback" && routeCtx.method == http.MethodPost {
			return authz.PermissionInferenceFeedback
		}
	case "socket-token":
		if routeCtx.method == http.MethodPost {
			return authz.PermissionInferenceEndpointsRead
		}
	case "orgs":
		if len(afterResource) == 1 && afterResource[0] == "current" && routeCtx.method == http.MethodGet {
			return ""
		}
		if routeCtx.method == http.MethodGet {
			return authz.PermissionOrgMembersRead
		}
		if routeCtx.method == http.MethodPost || routeCtx.method == http.MethodPut || routeCtx.method == http.MethodDelete {
			return authz.PermissionOrgMembersWrite
		}
	case "profiles":
		return ""
	}
	return denyRoutePolicy
}

func withCORSHeaders(resp events.APIGatewayProxyResponse, origin string) events.APIGatewayProxyResponse {
	if origin == "" {
		return resp
	}
	if resp.Headers == nil {
		resp.Headers = map[string]string{}
	}
	if _, ok := resp.Headers["Access-Control-Allow-Origin"]; !ok {
		resp.Headers["Access-Control-Allow-Origin"] = corsAllowOrigin
	}
	if _, ok := resp.Headers["Access-Control-Allow-Methods"]; !ok {
		resp.Headers["Access-Control-Allow-Methods"] = corsAllowMethods
	}
	if _, ok := resp.Headers["Access-Control-Allow-Headers"]; !ok {
		resp.Headers["Access-Control-Allow-Headers"] = corsAllowHeaders
	}
	return resp
}

func gatewayResponse(status int, body string) events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       body,
	}
}

// NewRouter returns a HandlerFunc that routes the request based on the path.
func NewRouter(client HttpClient, newReqFunc newRequestFunc, cfg Config) adapter.HandlerFunc {
	resolver, resolverErr := cfg.resolver()
	return func(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		log.Trace("API Gateway Router ", request.HTTPMethod, " ", request.Path)
		origin := getHeaderValue(request.Headers, "Origin")

		if request.HTTPMethod == http.MethodOptions {
			return withCORSHeaders(gatewayResponse(http.StatusNoContent, ""), origin), nil
		}

		if spoofedHeader := spoofedTrustedHeader(request.Headers); spoofedHeader != "" {
			err := fmt.Errorf("%s header found on request", spoofedHeader)
			log.WithContext(ctx).WithError(err).Errorf("API Gateway request with spoofed auth header and IP: `%s`", request.RequestContext.Identity.SourceIP)
			return withCORSHeaders(events.APIGatewayProxyResponse{
				StatusCode: http.StatusBadRequest,
				Body:       "Invalid request. Please check your headers",
			}, origin), nil
		}

		var userID string
		var sessionID string
		var orgID string
		var roles []string
		var permissions []string
		authorizerContext := request.RequestContext.Authorizer
		if authorizerContext != nil {
			if v, ok := authorizerContext["userId"]; ok {
				if s, ok := v.(string); ok {
					userID = s
				}
			}
			if v, ok := authorizerContext["sid"]; ok {
				if s, ok := v.(string); ok {
					sessionID = s
				}
			}
			if v, ok := authorizerContext["orgId"]; ok {
				if s, ok := v.(string); ok {
					orgID = s
				}
			}
			if v, ok := authorizerContext["permissions"]; ok {
				if s, ok := v.(string); ok {
					permissions, _ = authz.DecodeStringSlice(s)
				}
			}
			if v, ok := authorizerContext["roles"]; ok {
				if s, ok := v.(string); ok {
					roles, _ = authz.DecodeStringSlice(s)
				}
			}
		}

		if resolverErr != nil {
			log.WithContext(ctx).WithError(resolverErr).Error("API Gateway Router invalid route config")
			return withCORSHeaders(gatewayResponse(http.StatusInternalServerError, "bighill gateway route error"), origin), nil
		}

		serviceRoute, err := serviceRoute(request, resolver)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("API Gateway Router service route error")
			var routeErr *routeStatusError
			if errors.As(err, &routeErr) {
				return withCORSHeaders(gatewayResponse(routeErr.status, routeErr.body), origin), nil
			}
			return withCORSHeaders(gatewayResponse(http.StatusInternalServerError, "bighill gateway route error"), origin), nil
		}
		if requiredPermission := requiredRoutePermission(request); requiredPermission == denyRoutePolicy || (requiredPermission != "" && !authz.HasPermission(permissions, requiredPermission)) {
			return withCORSHeaders(gatewayResponse(http.StatusForbidden, "forbidden"), origin), nil
		}

		req, errorResponse := newProxyRequest(ctx, request, serviceRoute, newReqFunc)
		if errorResponse != nil {
			return withCORSHeaders(*errorResponse, origin), nil
		}

		if userID != "" {
			req.Header.Set(userHeader, userID)
		}
		if orgID != "" {
			req.Header.Set(orgHeader, orgID)
		}
		if len(roles) > 0 {
			req.Header.Set(rolesHeader, authz.EncodeStringSlice(roles))
		}
		if len(permissions) > 0 {
			req.Header.Set(permissionsHeader, authz.EncodeStringSlice(permissions))
		}
		if sessionID != "" {
			req.Header.Set(sessionHeader, sessionID)
		}

		response, err := requestWithRetry(ctx, req, client)
		if err != nil {
			log.WithContext(ctx).WithError(err).Errorf("API Gateway request error for %s %s", request.HTTPMethod, request.Path)
			return withCORSHeaders(newProxyResponse(ctx, response), origin), nil
		}

		defer response.Body.Close()
		bodyBytes, err := io.ReadAll(response.Body)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("API Gateway response read error")
			return withCORSHeaders(gatewayResponse(http.StatusInternalServerError, "bighill gateway read response error"), origin), nil
		}

		headers := make(map[string]string)
		for k, v := range response.Header {
			if len(v) > 0 {
				headers[k] = v[0]
			}
		}

		return withCORSHeaders(events.APIGatewayProxyResponse{
			StatusCode: response.StatusCode,
			Body:       string(bodyBytes),
			Headers:    headers,
		}, origin), nil
	}
}

func serviceRoute(request events.APIGatewayProxyRequest, resolver routeResolver) (string, error) {
	log.Trace("API Gateway Router serviceRoute")

	routeCtx, err := parseRouteContext(request)
	if err != nil {
		return "", err
	}
	path := backendPath(routeCtx)
	switch routeCtx.resource {
	case "profiles":
		return fmt.Sprintf("%s%s", resolver.tenantServiceRoute, profileBackendPath(routeCtx)), nil
	case "orgs":
		return fmt.Sprintf("%s%s", resolver.tenantServiceRoute, profileBackendPath(routeCtx)), nil
	case "data":
		if len(routeCtx.segments) > routeCtx.resourceIndex+1 &&
			(routeCtx.segments[routeCtx.resourceIndex+1] == "store" || routeCtx.segments[routeCtx.resourceIndex+1] == "uploads") {
			return fmt.Sprintf("%s%s", resolver.ingestionServiceRoute, path), nil
		}
		return fmt.Sprintf("%s%s", resolver.dataRegistryServiceRoute, path), nil
	case "models":
		if len(routeCtx.segments) > routeCtx.resourceIndex+1 {
			switch routeCtx.segments[routeCtx.resourceIndex+1] {
			case "uploads", "onboard":
				return fmt.Sprintf("%s%s", resolver.ingestionServiceRoute, path), nil
			}
		}
		return fmt.Sprintf("%s%s", resolver.modelRegistryServiceRoute, path), nil
	case "training-runs":
		return fmt.Sprintf("%s%s", resolver.trainingServiceRoute, path), nil
	case "inference":
		return fmt.Sprintf("%s%s", resolver.inferenceServiceRoute, path), nil
	case "socket-token":
		return fmt.Sprintf("%s%s", resolver.socketServiceRoute, path), nil
	default:
		return "", fmt.Errorf("invalid resource: %s", routeCtx.resource)
	}
}

func parseRouteContext(request events.APIGatewayProxyRequest) (routeContext, error) {
	trimmed := strings.TrimPrefix(request.Path, "/")
	segments := strings.Split(trimmed, "/")
	if len(segments) < 2 {
		return routeContext{}, fmt.Errorf("invalid url path: %s", request.Path)
	}

	scope := ""
	resourceIndex := 1
	version := segments[0]
	if segments[0] == publicEndpoint || segments[0] == privateEndpoint {
		if len(segments) < 3 {
			return routeContext{}, fmt.Errorf("invalid url path: %s", request.Path)
		}
		scope = segments[0]
		version = segments[1]
		resourceIndex = 2
	} else if segments[1] == publicEndpoint || segments[1] == privateEndpoint {
		scope = segments[1]
		resourceIndex = 2
	}
	if len(segments) <= resourceIndex {
		return routeContext{}, fmt.Errorf("invalid url path: %s", request.Path)
	}

	return routeContext{
		method:        request.HTTPMethod,
		path:          request.Path,
		version:       version,
		scope:         scope,
		resource:      segments[resourceIndex],
		resourceIndex: resourceIndex,
		segments:      segments,
	}, nil
}

func backendPath(routeCtx routeContext) string {
	log.Trace("API Gateway Router backendPath")

	if routeCtx.scope == privateEndpoint && routeCtx.resourceIndex == 2 && len(routeCtx.segments) > 2 {
		return "/" + routeCtx.version + "/" + strings.Join(routeCtx.segments[2:], "/")
	}
	return routeCtx.path
}

func profileBackendPath(routeCtx routeContext) string {
	log.Trace("API Gateway Router profileBackendPath")

	if routeCtx.scope == privateEndpoint && routeCtx.resourceIndex == 2 && len(routeCtx.segments) > 2 {
		return "/" + privateEndpoint + "/" + routeCtx.version + "/" + strings.Join(routeCtx.segments[2:], "/")
	}
	return routeCtx.path
}

func newRequestWithBody(ctx context.Context, request events.APIGatewayProxyRequest, serviceRoute, verb string, genProxyRequest newRequestFunc) (*http.Request, error) {
	log.Trace("API Gateway Router newRequestWithBody")

	body, err := requestBodyReader(request)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("API Gateway Router - decode request body error")
		return nil, err
	}

	req, err := genProxyRequest(verb, serviceRoute, body)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("API Gateway Router - create request error")
		return nil, err
	}
	return req, nil
}

func requestBodyReader(request events.APIGatewayProxyRequest) (io.Reader, error) {
	if request.IsBase64Encoded {
		bodyBytes, err := base64.StdEncoding.DecodeString(request.Body)
		if err != nil {
			return nil, fmt.Errorf("decode base64 request body: %w", err)
		}
		return bytes.NewReader(bodyBytes), nil
	}
	return bytes.NewBufferString(request.Body), nil
}

func newProxyRequest(ctx context.Context, request events.APIGatewayProxyRequest, serviceRoute string, genProxyRequest newRequestFunc) (*http.Request, *events.APIGatewayProxyResponse) {
	log.Trace("API Gateway Router newProxyRequest")

	var (
		req *http.Request
		err error
	)
	switch request.HTTPMethod {
	case http.MethodGet:
		req, err = genProxyRequest(http.MethodGet, serviceRoute, nil)
	case http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch:
		req, err = newRequestWithBody(ctx, request, serviceRoute, request.HTTPMethod, genProxyRequest)
	default:
		resp := gatewayResponse(http.StatusMethodNotAllowed, "bighill gateway method not allowed")
		return nil, &resp
	}

	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("create %s request error", request.HTTPMethod)
		resp := gatewayResponse(http.StatusInternalServerError, fmt.Sprintf("bighill gateway %s error", request.HTTPMethod))
		return nil, &resp
	}

	if request.QueryStringParameters != nil {
		query := req.URL.Query()
		for key, value := range request.QueryStringParameters {
			query.Add(key, value)
		}
		req.URL.RawQuery = query.Encode()
	}

	for key, value := range request.Headers {
		if strings.EqualFold(key, "Content-Length") {
			continue
		}
		req.Header.Set(key, value)
	}

	return req, nil
}

func newProxyResponse(ctx context.Context, response *http.Response) events.APIGatewayProxyResponse {
	log.Trace("API Gateway Router newProxyResponse")

	if response != nil && response.Body != nil {
		defer response.Body.Close()
		bodyBytes, err := io.ReadAll(response.Body)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("API Gateway response read error")
			return gatewayResponse(http.StatusBadGateway, "bighill gateway routing read error")
		}
		return gatewayResponse(response.StatusCode, string(bodyBytes))
	}
	return gatewayResponse(http.StatusBadGateway, "no response. Bighill gateway routing error")
}

func requestWithRetry(ctx context.Context, req *http.Request, client HttpClient) (*http.Response, error) {
	log.Trace("API Gateway Router requestWithRetry")

	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("failed to buffer request body for retries")
			return nil, err
		}
		resetBody := func() io.ReadCloser { return io.NopCloser(bytes.NewReader(bodyBytes)) }
		req.Body = resetBody()
		req.GetBody = func() (io.ReadCloser, error) { return resetBody(), nil }
		req.ContentLength = int64(len(bodyBytes))
	}

	cloneReq := func() *http.Request {
		cloned := req.Clone(ctx)
		if bodyBytes != nil {
			resetBody := func() io.ReadCloser { return io.NopCloser(bytes.NewReader(bodyBytes)) }
			cloned.Body = resetBody()
			cloned.GetBody = func() (io.ReadCloser, error) { return resetBody(), nil }
			cloned.ContentLength = int64(len(bodyBytes))
		}
		return cloned
	}

	var lastErr error
	var lastResp *http.Response
	const maxAttempts = 6
	backoff := 200 * time.Millisecond
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attemptReq := cloneReq()
		resp, err := client.Do(attemptReq)
		if err == nil && resp.StatusCode < 500 {
			return resp, nil
		}

		if err != nil {
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			log.Warnf("Request failed with error: %v (type: %T)", err, err)
			var ne net.Error
			if !(errors.Is(err, syscall.ECONNREFUSED) || errors.As(err, &ne) && (ne.Timeout() || ne.Temporary())) {
				log.Error("Error is not retryable, returning immediately")
				return resp, err
			}
			lastErr = err
			lastResp = nil
		} else {
			if resp.StatusCode >= 500 && isRetryableStatus(resp.StatusCode) {
				lastResp = resp
				if attempt != maxAttempts && resp.Body != nil {
					resp.Body.Close()
				}
				lastErr = fmt.Errorf("downstream %s", resp.Status)
			} else {
				log.Warnf("Got non-retryable 5xx status %d, returning downstream response", resp.StatusCode)
				return resp, nil
			}
		}

		if attempt == maxAttempts {
			break
		}

		select {
		case <-time.After(backoff):
			log.Warnf("API Gateway Router retrying request after error: %v", lastErr)
			if backoff < 2*time.Second {
				backoff *= 2
			}
		case <-ctx.Done():
			log.Errorf("API Gateway Router request context done: %v", ctx.Err())
			return nil, ctx.Err()
		}
	}

	log.Errorf("All retries exhausted, last error: %v", lastErr)
	if lastResp != nil {
		return lastResp, lastErr
	}
	return nil, lastErr
}

func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
