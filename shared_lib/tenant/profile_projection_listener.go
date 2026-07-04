package tenant

import (
	"context"
	"fmt"
	"strings"

	profilepb "lib/data_contracts_lib/profile"
	sharedDomain "lib/shared_lib/domain"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type ProfileProjectionStore interface {
	Upsert(ctx context.Context, tenant *sharedDomain.Tenant) error
	Delete(ctx context.Context, tenantID uuid.UUID) error
}

func ConfigureProfileProjectionErrorPolicy(subscriber msgConn.Subscriber) {
	log.Trace("ConfigureProfileProjectionErrorPolicy")

	_ = msgConn.ConfigureErrorPolicy(subscriber, msgConn.ErrorPolicyFunc(func(err error) bool {
		return msgConn.IsNonRetryable(err)
	}))
}

type userCreatedProjectionListener struct {
	store ProfileProjectionStore
}

func NewUserCreatedProjectionListener(store ProfileProjectionStore) *userCreatedProjectionListener {
	log.Trace("NewUserCreatedProjectionListener")

	return &userCreatedProjectionListener{store: store}
}

func (l *userCreatedProjectionListener) MsgType() msgConn.MsgType {
	log.Trace("userCreatedProjectionListener MsgType")

	return msgConn.MsgTypeUserCreated
}

func (l *userCreatedProjectionListener) NewMessage() *profilepb.UserCreatedEvent {
	log.Trace("userCreatedProjectionListener NewMessage")

	return &profilepb.UserCreatedEvent{}
}

func (l *userCreatedProjectionListener) Handle(ctx context.Context, resourceKey uuid.UUID, event *profilepb.UserCreatedEvent) error {
	log.Trace("userCreatedProjectionListener Handle")

	tenant, err := TenantFromUserCreatedEvent(resourceKey, event)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	return l.store.Upsert(ctx, tenant)
}

type userUpdatedProjectionListener struct {
	store ProfileProjectionStore
}

func NewUserUpdatedProjectionListener(store ProfileProjectionStore) *userUpdatedProjectionListener {
	log.Trace("NewUserUpdatedProjectionListener")

	return &userUpdatedProjectionListener{store: store}
}

func (l *userUpdatedProjectionListener) MsgType() msgConn.MsgType {
	log.Trace("userUpdatedProjectionListener MsgType")

	return msgConn.MsgTypeUserUpdated
}

func (l *userUpdatedProjectionListener) NewMessage() *profilepb.UserUpdatedEvent {
	log.Trace("userUpdatedProjectionListener NewMessage")

	return &profilepb.UserUpdatedEvent{}
}

func (l *userUpdatedProjectionListener) Handle(ctx context.Context, resourceKey uuid.UUID, event *profilepb.UserUpdatedEvent) error {
	log.Trace("userUpdatedProjectionListener Handle")

	tenant, err := TenantFromUserUpdatedEvent(resourceKey, event)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	return l.store.Upsert(ctx, tenant)
}

type userDeletedProjectionListener struct {
	store ProfileProjectionStore
}

func NewUserDeletedProjectionListener(store ProfileProjectionStore) *userDeletedProjectionListener {
	log.Trace("NewUserDeletedProjectionListener")

	return &userDeletedProjectionListener{store: store}
}

func (l *userDeletedProjectionListener) MsgType() msgConn.MsgType {
	log.Trace("userDeletedProjectionListener MsgType")

	return msgConn.MsgTypeUserDeleted
}

func (l *userDeletedProjectionListener) NewMessage() *profilepb.UserDeletedEvent {
	log.Trace("userDeletedProjectionListener NewMessage")

	return &profilepb.UserDeletedEvent{}
}

func (l *userDeletedProjectionListener) Handle(ctx context.Context, resourceKey uuid.UUID, event *profilepb.UserDeletedEvent) error {
	log.Trace("userDeletedProjectionListener Handle")

	tenantID, err := TenantIDFromEvent(resourceKey, event.GetUserId())
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	return l.store.Delete(ctx, tenantID)
}

func TenantFromUserCreatedEvent(resourceKey uuid.UUID, event *profilepb.UserCreatedEvent) (*sharedDomain.Tenant, error) {
	log.Trace("TenantFromUserCreatedEvent")

	if event == nil {
		return nil, fmt.Errorf("user created payload is required")
	}
	tenantID, err := TenantIDFromEvent(resourceKey, event.GetUserId())
	if err != nil {
		return nil, err
	}
	return &sharedDomain.Tenant{
		TenantID:                   tenantID,
		Email:                      strings.TrimSpace(event.GetEmail()),
		HuggingFaceTokenCiphertext: strings.TrimSpace(event.GetHuggingfaceTokenCiphertext()),
	}, nil
}

func TenantFromUserUpdatedEvent(resourceKey uuid.UUID, event *profilepb.UserUpdatedEvent) (*sharedDomain.Tenant, error) {
	log.Trace("TenantFromUserUpdatedEvent")

	if event == nil {
		return nil, fmt.Errorf("user updated payload is required")
	}
	tenantID, err := TenantIDFromEvent(resourceKey, event.GetUserId())
	if err != nil {
		return nil, err
	}
	return &sharedDomain.Tenant{
		TenantID:                   tenantID,
		Email:                      strings.TrimSpace(event.GetEmail()),
		HuggingFaceTokenCiphertext: strings.TrimSpace(event.GetHuggingfaceTokenCiphertext()),
	}, nil
}

func TenantIDFromEvent(resourceKey uuid.UUID, userID string) (uuid.UUID, error) {
	log.Trace("TenantIDFromEvent")

	userID = strings.TrimSpace(userID)
	if userID != "" {
		parsed, err := uuid.Parse(userID)
		if err != nil {
			return uuid.Nil, fmt.Errorf("profile event user_id is invalid: %w", err)
		}
		return parsed, nil
	}
	if resourceKey == uuid.Nil {
		return uuid.Nil, fmt.Errorf("profile event user_id is required")
	}
	return resourceKey, nil
}
