package kms

import (
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	log "github.com/sirupsen/logrus"
)

type AWSKMSClient struct {
	client *awskms.Client
	keyID  string
}

func NewAWSKMSClient(ctx context.Context, keyID string) (KMSClient, error) {
	log.Trace("NewAWSKMSClient")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to load AWS config")
		wrappedErr := fmt.Errorf("failed to load AWS config: %w", err)
		return nil, wrappedErr
	}

	client := awskms.NewFromConfig(cfg)
	return &AWSKMSClient{
		client: client,
		keyID:  keyID,
	}, nil
}

func (k *AWSKMSClient) KeyID() string {
	return k.keyID
}

// hash the signingString with SHA256, then call KMS.Sign with
// RSASSA_PKCS1_V1_5_SHA_256, which corresponds to JWT's "RS256".
func (k *AWSKMSClient) SignJWT(ctx context.Context, signingString string) ([]byte, error) {
	log.Trace("AWSKMSClient SignJWT")

	// JWT RS256 = SHA256(signingString) -> RSA PKCS#1 v1.5 sign
	digest := sha256.Sum256([]byte(signingString))

	input := &awskms.SignInput{
		KeyId:            aws.String(k.keyID),
		Message:          digest[:],
		MessageType:      kmstypes.MessageTypeDigest, // because we pre-hashed
		SigningAlgorithm: kmstypes.SigningAlgorithmSpecRsassaPkcs1V15Sha256,
	}

	result, err := k.client.Sign(ctx, input)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to sign JWT with KMS")
		return nil, fmt.Errorf("failed to sign JWT with KMS: %w", err)
	}

	return result.Signature, nil
}

// PublicKey fetches and parses the RSA public key for this KMS key.
// We'll hand this to jwt.ParseWithClaims() so it can verify RS256 signatures
// locally without needing KMS.
func (k *AWSKMSClient) PublicKey(ctx context.Context) (*rsa.PublicKey, error) {
	log.Trace("AWSKMSClient PublicKey")

	input := &awskms.GetPublicKeyInput{
		KeyId: aws.String(k.keyID),
	}

	result, err := k.client.GetPublicKey(ctx, input)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to get KMS public key")
		return nil, fmt.Errorf("failed to get KMS public key: %w", err)
	}

	parsedAny, err := x509.ParsePKIXPublicKey(result.PublicKey)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to parse KMS public key")
		return nil, fmt.Errorf("failed to parse KMS public key: %w", err)
	}

	pubKey, ok := parsedAny.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("KMS public key is not RSA")
	}

	return pubKey, nil
}
