package kms

import (
	"context"
	"crypto/rsa"
	env "lib/shared_lib/env"

	log "github.com/sirupsen/logrus"
)

const (
	kmsKeyIDEnv   = "AUTH_KMS_KEY_ID"
	localDevKeyID = "local-dev-kms-key"
)

type KMSClient interface {
	KeyID() string
	SignJWT(ctx context.Context, signingString string) ([]byte, error)
	PublicKey(ctx context.Context) (*rsa.PublicKey, error)
}

func NewKMSClient(ctx context.Context) (KMSClient, error) {
	log.Trace("NewKMSClient")

	keyID := env.WithDefaultString(kmsKeyIDEnv, "")

	if env.IsDevEnv() {
		return NewLocalKMS(ctx, localDevKeyID)
	}

	// For staging/production, require a KMS key ID
	if keyID == "" {
		log.WithContext(ctx).Warn("AUTH_KMS_KEY_ID not set, falling back to local KMS")
		return NewLocalKMS(ctx, localDevKeyID)
	}

	return NewAWSKMSClient(ctx, keyID)
}
