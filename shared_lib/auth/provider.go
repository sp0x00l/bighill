package provider

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"lib/shared_lib/authz"
	"strings"
	"time"

	kms "lib/shared_lib/key_management"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var (
	ErrMissingToken       = errors.New("missing authentication token")
	ErrInvalidTokenFormat = errors.New("invalid authentication token format")
	ErrInvalidAlg         = errors.New("invalid token signature algorithm")
	ErrInvalidKeyID       = errors.New("invalid token key id")
	ErrInvalidJWT         = errors.New("invalid JWT token")
	ErrInvalidClaims      = errors.New("invalid JWT token claims")
	// userId is the JWT claim name; Go identifiers keep the UserID spelling.
	ErrInvalidUserID = errors.New("invalid token userId claim")
	ErrInvalidType   = errors.New("invalid token type claim")
	ErrExpired       = errors.New("token expired")
	ErrAccessDenied  = errors.New("access denied")
)

type AuthProvider interface {
	CreateToken(ctx context.Context, userID uuid.UUID, authExpirationInMinutes int) (string, string, int64, error)
	CreateAccessToken(ctx context.Context, claims authz.TokenClaims, authExpirationInMinutes int) (string, string, int64, error)
	Validate(ctx context.Context, authorizationToken string) (map[string]any, error)
}

type authProvider struct {
	kmsClient kms.KMSClient
	publicKey *rsa.PublicKey
}

func NewAuthProvider(ctx context.Context, kmsClient kms.KMSClient) (AuthProvider, error) {
	log.Trace("NewAuthProvider")

	pubKey, err := kmsClient.PublicKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch KMS public key: %w", err)
	}

	return &authProvider{
		kmsClient: kmsClient,
		publicKey: pubKey,
	}, nil
}

func (a *authProvider) CreateToken(ctx context.Context, userID uuid.UUID, authExpirationInMinutes int) (string, string, int64, error) {
	log.Trace("authProvider CreateToken")

	return a.CreateAccessToken(ctx, authz.TokenClaims{
		UserID:    userID.String(),
		TokenType: "access",
	}, authExpirationInMinutes)
}

func (a *authProvider) CreateAccessToken(ctx context.Context, tokenClaims authz.TokenClaims, authExpirationInMinutes int) (string, string, int64, error) {
	log.Trace("authProvider CreateAccessToken")

	now := time.Now()
	expiresAt := now.Add(time.Duration(authExpirationInMinutes) * time.Minute).Unix()

	// JWT identifiers are token claims, not database row IDs. They must be
	// unique at issuance so revocation/audit logic can distinguish tokens.
	jti := uuid.NewString()
	sid := uuid.NewString()
	if strings.TrimSpace(tokenClaims.SessionID) != "" {
		sid = strings.TrimSpace(tokenClaims.SessionID)
	}
	tokenType := strings.TrimSpace(tokenClaims.TokenType)
	if tokenType == "" {
		tokenType = "access"
	}

	claims := jwt.MapClaims{
		"userId":    strings.TrimSpace(tokenClaims.UserID),
		"tokenType": tokenType,
		"jti":       jti,
		"sid":       sid,
		"exp":       expiresAt,
		"iat":       now.Unix(),
		"expiresAt": expiresAt,
	}
	if strings.TrimSpace(tokenClaims.OrgID) != "" {
		claims["orgId"] = strings.TrimSpace(tokenClaims.OrgID)
	}
	if len(tokenClaims.Roles) > 0 {
		claims["roles"] = tokenClaims.Roles
	}
	if len(tokenClaims.Permissions) > 0 {
		claims["permissions"] = tokenClaims.Permissions
	}

	header := map[string]any{
		"alg": "RS256",
		"typ": "JWT",
		"kid": normalizeKMSKeyID(a.kmsClient.KeyID()),
	}

	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", "", 0, fmt.Errorf("failed to marshal JWT header: %w", err)
	}
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		return "", "", 0, fmt.Errorf("failed to marshal JWT claims: %w", err)
	}

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)
	signingString := headerB64 + "." + payloadB64

	sigBytes, err := a.kmsClient.SignJWT(ctx, signingString)
	if err != nil {
		return "", "", 0, fmt.Errorf("failed to sign token with KMS: %w", err)
	}
	signatureB64 := base64.RawURLEncoding.EncodeToString(sigBytes)

	return signingString + "." + signatureB64, sid, expiresAt, nil
}

func (a *authProvider) Validate(ctx context.Context, authorizationToken string) (map[string]any, error) {
	log.Trace("authProvider Validate")

	authorizationToken = strings.TrimSpace(authorizationToken)
	if authorizationToken == "" {
		log.WithContext(ctx).Error("missing authorization token")
		return map[string]any{}, ErrMissingToken
	}

	parts := strings.SplitN(authorizationToken, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" || strings.TrimSpace(parts[1]) == "" {
		log.WithContext(ctx).Error("invalid authorization request format")
		return map[string]any{}, ErrInvalidTokenFormat
	}
	jwtToken := parts[1]

	type claims struct {
		jwt.RegisteredClaims
		UserIdField string   `json:"userId"`
		OrgID       string   `json:"orgId"`
		Roles       []string `json:"roles"`
		Permissions []string `json:"permissions"`
		TokenType   string   `json:"tokenType"`
		SID         string   `json:"sid"`
	}

	wantKID := normalizeKMSKeyID(a.kmsClient.KeyID())

	token, err := jwt.ParseWithClaims(jwtToken, &claims{}, func(t *jwt.Token) (any, error) {
		if t.Method.Alg() != jwt.SigningMethodRS256.Alg() {
			log.WithContext(ctx).Error("invalid signing algorithm")
			return nil, ErrInvalidAlg
		}
		gotKID := normalizeKMSKeyID(stringFromAny(t.Header["kid"]))
		if gotKID != "" && wantKID != "" && gotKID != wantKID {
			log.WithContext(ctx).Warnf("token kid mismatch (token=%q, configured=%q)", gotKID, wantKID)
			return nil, ErrInvalidKeyID
		}
		return a.publicKey, nil
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("invalid JWT token")
		return map[string]any{}, ErrInvalidJWT
	}

	tc, ok := token.Claims.(*claims)
	if !ok || !token.Valid {
		log.WithContext(ctx).Error("invalid JWT token claims")
		return map[string]any{}, ErrInvalidClaims
	}

	if tc.UserIdField == "" {
		log.WithContext(ctx).Error("invalid token userId claim")
		return map[string]any{}, ErrInvalidUserID
	}
	if strings.ToLower(tc.TokenType) != "access" {
		log.WithContext(ctx).Error("invalid token type claim")
		return map[string]any{}, ErrInvalidType
	}

	exp := tc.ExpiresAt.Time.Unix()
	if exp == 0 {
		log.WithContext(ctx).Error("missing expiration claim")
		return map[string]any{}, ErrInvalidClaims
	}
	if exp-time.Now().Unix() <= 0 {
		log.WithContext(ctx).Error("token expired")
		return map[string]any{}, ErrExpired
	}

	if tc.SID == "" {
		log.WithContext(ctx).Error("invalid token sid claim")
		return map[string]any{}, ErrInvalidClaims
	}

	iat := tc.IssuedAt.Time.Unix()
	if iat == 0 {
		log.WithContext(ctx).Error("missing issued at claim")
		return map[string]any{}, ErrInvalidClaims
	}

	return map[string]any{
		"userId":      tc.UserIdField,
		"orgId":       tc.OrgID,
		"roles":       tc.Roles,
		"permissions": tc.Permissions,
		"jti":         tc.ID,
		"sid":         tc.SID,
		"expiresAt":   exp,
		"iat":         iat,
	}, nil
}

func stringFromAny(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func normalizeKMSKeyID(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Accept raw key-id (xxxxxxxx-xxxx-...), alias name (alias/foo), or ARNs.
	// For ARNs, keep only the last path segment (after the final '/').
	if strings.HasPrefix(s, "arn:") {
		if i := strings.LastIndexByte(s, '/'); i >= 0 && i+1 < len(s) {
			return s[i+1:]
		}
	}
	return s
}
