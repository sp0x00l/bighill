package main

import (
	"auth/pkg/provider"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"lib/shared_lib/authz"
	"lib/shared_lib/logs"
	"lib/shared_lib/observability"
	"net"
	"os"
	"strings"

	auth "lib/shared_lib/auth"
	env "lib/shared_lib/env"

	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/otel"
	otelTracer "go.opentelemetry.io/otel/trace"
)

// Version set at compile time
var Version string

var tracer otelTracer.Tracer
var isDevEnv bool
var authStore auth.RevocationStore

func init() {
	logs.Init()

	ctx := context.Background()
	traceName := "api-gateway"
	// gracefull shutdown is not possible in an AWS lambda
	_ = observability.Init(ctx, traceName, Version)
	tracer = otel.Tracer(traceName)

	redisClient, err := newRedisClientFromEnv()
	if err != nil {
		log.WithContext(ctx).WithError(err).Fatal("failed to initialize redis client")
	}
	authStore = auth.NewRevocationStore(redisClient, auth.WithKeyPrefix("auth:"))

	if err := provider.InitAuthProvider(ctx); err != nil {
		log.WithContext(ctx).WithError(err).Fatal("failed to initialize auth provider")
	}

	isDevEnv = env.IsDevEnv()
	log.WithField("isDevEnv", isDevEnv).Info("environment initialized")
}

func newRedisClientFromEnv() (rueidis.Client, error) {
	log.Trace("newRedisClientFromEnv")

	addr := os.Getenv("REDIS_ADDRESS")
	if addr == "" {
		return nil, fmt.Errorf("missing REDIS_ADDRESS")
	}

	opt := rueidis.ClientOption{
		InitAddress: []string{addr},
		Username:    os.Getenv("REDIS_USERNAME"),
		Password:    os.Getenv("REDIS_PASSWORD"),
	}

	if strings.EqualFold(os.Getenv("REDIS_TLS"), "true") {
		serverName := addr
		if host, _, err := net.SplitHostPort(addr); err == nil {
			serverName = host
		}
		opt.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: serverName,
		}
	}

	return rueidis.NewClient(opt)
}

// implements lambda.Handler interface.
type AuthHandler struct{}

func (h AuthHandler) Invoke(ctx context.Context, req events.APIGatewayCustomAuthorizerRequest) (events.APIGatewayCustomAuthorizerResponse, error) {
	log.Trace("AuthHandler Invoke")

	ctx, span := tracer.Start(ctx, "API Gateway Auth")
	defer span.End()

	token := req.AuthorizationToken
	claims, err := provider.AuthProvider(ctx, token)
	if err != nil {
		switch {
		case errors.Is(err, auth.ErrMissingToken),
			errors.Is(err, auth.ErrInvalidTokenFormat),
			errors.Is(err, auth.ErrInvalidAlg),
			errors.Is(err, auth.ErrInvalidKeyID),
			errors.Is(err, auth.ErrInvalidJWT),
			errors.Is(err, auth.ErrInvalidClaims),
			errors.Is(err, auth.ErrInvalidUserID),
			errors.Is(err, auth.ErrInvalidType),
			errors.Is(err, auth.ErrExpired):
			log.WithContext(ctx).Warnf("authentication failed: %v -> 401", err)
			if isDevEnv {
				return generateDenyPolicy(req.MethodArn), nil
			}
			return events.APIGatewayCustomAuthorizerResponse{}, errors.New("Unauthorized")
		case errors.Is(err, auth.ErrAccessDenied):
			log.WithContext(ctx).Warn("authorization denied -> 403")
			return generateDenyPolicy(req.MethodArn), nil
		default:
			log.WithContext(ctx).WithError(err).Error("authorizer internal error -> 5xx")
			return events.APIGatewayCustomAuthorizerResponse{}, fmt.Errorf("authorizer failure")
		}
	}

	userID, ok := claims["userId"].(string)
	if !ok || userID == "" {
		log.WithContext(ctx).Warn("missing or invalid userId in claims")
		return generateDenyPolicy(req.MethodArn), nil
	}

	sid, ok := claims["sid"].(string)
	if !ok || sid == "" {
		log.WithContext(ctx).Warn("missing or invalid sid in claims")
		return generateDenyPolicy(req.MethodArn), nil
	}
	orgID, ok := claims["orgId"].(string)
	if !ok || orgID == "" {
		log.WithContext(ctx).Warn("missing or invalid orgId in claims")
		return generateDenyPolicy(req.MethodArn), nil
	}
	roles, _ := claims["roles"].([]string)
	if len(roles) == 0 {
		log.WithContext(ctx).Warn("missing roles in claims")
		return generateDenyPolicy(req.MethodArn), nil
	}
	permissions, _ := claims["permissions"].([]string)
	if len(permissions) == 0 {
		log.WithContext(ctx).Warn("missing permissions in claims")
		return generateDenyPolicy(req.MethodArn), nil
	}

	sessionExists, err := authStore.SessionExists(ctx, sid)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to check session existence")
		return events.APIGatewayCustomAuthorizerResponse{}, fmt.Errorf("authorizer failure")
	}
	if !sessionExists {
		log.WithContext(ctx).Warn("session does not exist or has been revoked")
		return generateDenyPolicy(req.MethodArn), nil
	}

	revokedAfter, err := authStore.GetUserRevokedAfter(ctx, userID)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to check user revocation status")
		return events.APIGatewayCustomAuthorizerResponse{}, fmt.Errorf("authorizer failure")
	}
	if revokedAfter > 0 {
		_, ok := auth.ClaimUnixSeconds(claims, "iat")
		if !ok {
			log.WithContext(ctx).Warn("missing or invalid iat in claims")
			return generateDenyPolicy(req.MethodArn), nil
		}
		if auth.ClaimIssuedBefore(claims, "iat", revokedAfter) {
			log.WithContext(ctx).Warn("token issued before user sessions were revoked")
			return generateDenyPolicy(req.MethodArn), nil
		}
	}

	return generatePolicy(userID, sid, orgID, roles, permissions, "Allow", req.MethodArn), nil
}

func generateDenyPolicy(resource string) events.APIGatewayCustomAuthorizerResponse {
	log.Trace("API Gateway auth generateDenyPolicy")

	return generatePolicy("unauthorized", "unauthorized", "", nil, nil, "Deny", resource)
}

func generatePolicy(userID, sessionID, orgID string, roles []string, permissions []string, effect, resource string) events.APIGatewayCustomAuthorizerResponse {
	log.Trace("API Gateway auth generatePolicy")

	wildcardResource := resource
	if parts := strings.Split(resource, "/"); len(parts) >= 3 {
		wildcardResource = strings.Join(parts[:2], "/") + "/*"
	}

	authResponse := events.APIGatewayCustomAuthorizerResponse{
		PrincipalID: userID,
		Context: map[string]any{
			"userId":      userID,
			"sid":         sessionID,
			"orgId":       orgID,
			"roles":       authz.EncodeStringSlice(roles),
			"permissions": authz.EncodeStringSlice(permissions),
		},
	}
	if effect != "" && wildcardResource != "" {
		policyDocument := events.APIGatewayCustomAuthorizerPolicy{
			Version: "2012-10-17",
			Statement: []events.IAMPolicyStatement{
				{
					Action:   []string{"execute-api:Invoke"},
					Effect:   effect,
					Resource: []string{wildcardResource},
				},
			},
		}
		authResponse.PolicyDocument = policyDocument
	}
	return authResponse
}

func main() {
	log.Trace("API Gateway auth main")

	handler := AuthHandler{}
	typed := lambda.NewHandler(handler.Invoke)
	wrappedHandler := otellambda.WrapHandler(typed, otellambda.WithTracerProvider(otel.GetTracerProvider()))

	lambda.Start(wrappedHandler)
}
