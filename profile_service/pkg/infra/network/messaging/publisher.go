package messaging

import (
	"context"
	"fmt"

	profileeventpb "lib/data_contracts_lib/profile"
	shared "lib/shared_lib/messaging"
	"profile_service/pkg/domain"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type UserEventPublisher interface {
	PublishUserCreatedEvent(ctx context.Context, profileAccount *domain.ProfileAccount) error
	PublishEmailVerificationRequestedEvent(ctx context.Context, profileAccount *domain.ProfileAccount) error
	PublishUserUpdatedEvent(ctx context.Context, profile *domain.Profile) error
	PublishUserDeletedEvent(ctx context.Context, userID uuid.UUID) error
}

type userEventPublisher struct {
	publisher    shared.Publisher
	profileTopic string
}

func NewUserEventPublisher(msgPublisher shared.Publisher, profileTopic string) UserEventPublisher {
	log.Trace("NewUserEventPublisher")

	return &userEventPublisher{
		publisher:    msgPublisher,
		profileTopic: profileTopic,
	}
}

func (b *userEventPublisher) PublishUserCreatedEvent(ctx context.Context, profileAccount *domain.ProfileAccount) error {
	log.Trace("UserEventPublisher PublishUserCreatedEvent")

	if profileAccount == nil || profileAccount.ID == uuid.Nil {
		log.WithContext(ctx).Error("Invalid arguments for publishing user created message")
		return fmt.Errorf("invalid arguments to publish user event")
	}

	userCreatedEvent := &profileeventpb.UserCreatedEvent{
		UserId:                     profileAccount.ID.String(),
		Email:                      profileAccount.Email,
		PhoneNumber:                profileAccount.PhoneNumber,
		CountryCode:                profileAccount.CountryCode,
		EmailVerificationStatus:    shared.EmailVerificationStatusToProfileEventProto(profileAccount.EmailVerified),
		HuggingfaceTokenCiphertext: profileAccount.HuggingFaceTokenCiphertext,
	}

	message := shared.Message{
		ResourceKey: profileAccount.ID,
		MsgType:     shared.MsgTypeUserCreated,
	}

	if err := b.publisher.Publish(ctx, b.profileTopic, message, userCreatedEvent); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("Failed to publish event")
		return fmt.Errorf("publish event failed")
	}

	return nil
}

func (b *userEventPublisher) PublishEmailVerificationRequestedEvent(ctx context.Context, profileAccount *domain.ProfileAccount) error {
	log.Trace("UserEventPublisher PublishEmailVerificationRequestedEvent")

	if profileAccount == nil || profileAccount.ID == uuid.Nil || profileAccount.Email == "" || profileAccount.EmailVerifyToken == "" {
		log.WithContext(ctx).Error("Invalid arguments for publishing email verification requested message")
		return fmt.Errorf("invalid arguments to publish email verification event")
	}

	emailVerificationEvent := &profileeventpb.EmailVerificationRequestedEvent{
		UserId:           profileAccount.ID.String(),
		Email:            profileAccount.Email,
		EmailVerifyToken: profileAccount.EmailVerifyToken,
	}

	message := shared.Message{
		ResourceKey: profileAccount.ID,
		MsgType:     shared.MsgTypeEmailVerificationRequested,
	}

	if err := b.publisher.Publish(ctx, b.profileTopic, message, emailVerificationEvent); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("Failed to publish email verification event")
		return fmt.Errorf("publish email verification event failed")
	}

	return nil
}

func (b *userEventPublisher) PublishUserUpdatedEvent(ctx context.Context, profile *domain.Profile) error {
	log.Trace("UserEventPublisher PublishUserUpdatedEvent")

	if profile == nil || profile.ID == uuid.Nil {
		log.WithContext(ctx).Error("Invalid arguments for publishing user updated message")
		return fmt.Errorf("invalid arguments to publish user event")
	}

	userUpdatedEvent := &profileeventpb.UserUpdatedEvent{
		UserId:                     profile.ID.String(),
		Email:                      profile.Email,
		FirstName:                  profile.FirstName,
		LastName:                   profile.LastName,
		PhoneNumber:                profile.PhoneNumber,
		CountryCode:                profile.CountryCode,
		EmailVerificationStatus:    shared.EmailVerificationStatusToProfileEventProto(profile.EmailVerified),
		HuggingfaceTokenCiphertext: profile.HuggingFaceTokenCiphertext,
	}

	message := shared.Message{
		ResourceKey: profile.ID,
		MsgType:     shared.MsgTypeUserUpdated,
	}

	if err := b.publisher.Publish(ctx, b.profileTopic, message, userUpdatedEvent); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("Failed to publish event")
		return fmt.Errorf("publish event failed")
	}

	return nil
}

func (b *userEventPublisher) PublishUserDeletedEvent(ctx context.Context, userID uuid.UUID) error {
	log.Trace("UserEventPublisher PublishUserDeletedEvent")

	if userID == uuid.Nil {
		log.WithContext(ctx).Errorf("Invalid arguments for publishing message: userID is nil")
		return fmt.Errorf("invalid arguments to publish user event")
	}

	userDeletedEvent := &profileeventpb.UserDeletedEvent{
		UserId: userID.String(),
	}

	message := shared.Message{
		ResourceKey: userID,
		MsgType:     shared.MsgTypeUserDeleted,
	}

	if err := b.publisher.Publish(ctx, b.profileTopic, message, userDeletedEvent); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("Failed to publish event")
		return fmt.Errorf("publish event failed")
	}

	return nil
}
