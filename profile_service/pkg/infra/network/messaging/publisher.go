package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	profileeventpb "lib/data_contracts_lib/profile"
	shared "lib/shared_lib/messaging"
	"profile_service/pkg/domain"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type UserEventPublisher interface {
	PublishUserCreatedEvent(ctx context.Context, profileAccount *domain.ProfileAccount) error
	PublishUserUpdatedEvent(ctx context.Context, profile *domain.Profile) error
	PublishUserDeletedEvent(ctx context.Context, userID uuid.UUID) error
}

type userEventPublisher struct {
	publisher    shared.Publisher
	profileTopic string
}

type UserEventBuilder struct {
	profileTopic string
}

func NewUserEventBuilder(profileTopic string) *UserEventBuilder {
	log.Trace("NewUserEventBuilder")

	return &UserEventBuilder{profileTopic: profileTopic}
}

func NewUserEventPublisher(msgPublisher shared.Publisher, profileTopic string) UserEventPublisher {
	log.Trace("NewUserEventPublisher")

	return &userEventPublisher{
		publisher:    msgPublisher,
		profileTopic: profileTopic,
	}
}

func (b *UserEventBuilder) UserCreatedMessage(profileAccount *domain.ProfileAccount) shared.OutboundMessage {
	log.Trace("UserEventBuilder UserCreatedMessage")

	if profileAccount == nil || profileAccount.ID == uuid.Nil {
		panic("invalid arguments to build user created event")
	}
	payload := mustMarshalProfileEvent(&profileeventpb.UserCreatedEvent{
		UserId:                     profileAccount.ID.String(),
		Email:                      profileAccount.Email,
		PhoneNumber:                profileAccount.PhoneNumber,
		CountryCode:                profileAccount.CountryCode,
		EmailVerificationStatus:    shared.EmailVerificationStatusToProfileEventProto(profileAccount.EmailVerified),
		HuggingfaceTokenCiphertext: profileAccount.HuggingFaceTokenCiphertext,
	})
	return shared.OutboundMessage{
		Topic: b.profileTopic,
		Message: shared.Message{
			ResourceKey: profileAccount.ID,
			MsgType:     shared.MsgTypeUserCreated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("user_created:%s", profileAccount.ID),
	}
}

func (b *UserEventBuilder) UserUpdatedMessage(profile *domain.Profile) shared.OutboundMessage {
	log.Trace("UserEventBuilder UserUpdatedMessage")

	if profile == nil || profile.ID == uuid.Nil {
		panic("invalid arguments to build user updated event")
	}
	payload := mustMarshalProfileEvent(&profileeventpb.UserUpdatedEvent{
		UserId:                     profile.ID.String(),
		Email:                      profile.Email,
		FirstName:                  profile.FirstName,
		LastName:                   profile.LastName,
		PhoneNumber:                profile.PhoneNumber,
		CountryCode:                profile.CountryCode,
		EmailVerificationStatus:    shared.EmailVerificationStatusToProfileEventProto(profile.EmailVerified),
		HuggingfaceTokenCiphertext: profile.HuggingFaceTokenCiphertext,
	})
	return shared.OutboundMessage{
		Topic: b.profileTopic,
		Message: shared.Message{
			ResourceKey: profile.ID,
			MsgType:     shared.MsgTypeUserUpdated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("user_updated:%s:%s", profile.ID, profilePayloadHash(payload)),
	}
}

func (b *UserEventBuilder) UserDeletedMessage(userID uuid.UUID) shared.OutboundMessage {
	log.Trace("UserEventBuilder UserDeletedMessage")

	if userID == uuid.Nil {
		panic("invalid arguments to build user deleted event")
	}
	payload := mustMarshalProfileEvent(&profileeventpb.UserDeletedEvent{
		UserId: userID.String(),
	})
	return shared.OutboundMessage{
		Topic: b.profileTopic,
		Message: shared.Message{
			ResourceKey: userID,
			MsgType:     shared.MsgTypeUserDeleted,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("user_deleted:%s", userID),
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

func mustMarshalProfileEvent(payload proto.Message) []byte {
	log.Trace("mustMarshalProfileEvent")

	out, err := proto.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return out
}

func profilePayloadHash(payload []byte) string {
	log.Trace("profilePayloadHash")

	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:8])
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
