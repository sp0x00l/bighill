package provider

import (
	"context"

	auth "lib/shared_lib/auth"
	kms "lib/shared_lib/key_management"

	log "github.com/sirupsen/logrus"
)

var authProvider auth.AuthProvider

func InitAuthProvider(ctx context.Context) error {
	log.Trace("InitAuthProvider")

	kmsClient, err := kms.NewKMSClient(ctx)
	if err != nil {
		return err
	}

	authProvider, err = auth.NewAuthProvider(ctx, kmsClient)
	if err != nil {
		return err
	}

	return nil
}

func AuthProvider(ctx context.Context, authorizationToken string) (map[string]any, error) {
	log.Trace("API Gateway AuthProvider")

	if authProvider == nil {
		log.WithContext(ctx).Error("auth provider not initialized")
		return map[string]any{}, nil
	}

	return authProvider.Validate(ctx, authorizationToken)
}
