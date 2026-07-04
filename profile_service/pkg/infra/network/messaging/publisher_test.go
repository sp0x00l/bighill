package messaging_test

import (
	"context"
	"errors"
	profileeventpb "lib/data_contracts_lib/profile"
	shared "lib/shared_lib/messaging"
	"profile_service/pkg/domain"
	"profile_service/pkg/infra/network/messaging"
	"testing"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

type publishClientMock struct {
	PublishCalled bool

	LastTopic   string
	LastMessage shared.Message
	LastPayload proto.Message

	NextError error
}

func (m *publishClientMock) Publish(_ context.Context, topic string, message shared.Message, payload proto.Message) error {
	m.PublishCalled = true
	m.LastTopic = topic
	m.LastPayload = payload

	if payload != nil {
		if b, err := proto.Marshal(payload); err == nil {
			message.Payload = b
		}
	}

	m.LastMessage = message
	return m.NextError
}

func (m *publishClientMock) Close() {}

func TestPublisher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "User publisher test suite")
}

var _ = Describe("UserEventPublisher", func() {
	var (
		clientMock *publishClientMock
		ctx        context.Context
		publisher  messaging.UserEventPublisher
		userID     uuid.UUID
	)

	BeforeEach(func() {
		ctx = context.Background()
		clientMock = &publishClientMock{}
		publisher = messaging.NewUserEventPublisher(clientMock, "profile")
		userID = uuid.New()
	})

	Describe("PublishUserCreatedEvent", func() {
		When("the arguments are valid", func() {
			It("publishes a user created event", func() {
				err := publisher.PublishUserCreatedEvent(ctx, &domain.ProfileAccount{
					ID:                         userID,
					Email:                      "user@example.com",
					PhoneNumber:                "+447700900123",
					CountryCode:                "GB",
					EmailVerifyToken:           "token-1",
					HuggingFaceTokenCiphertext: "ciphertext-1",
				})

				Expect(err).To(BeNil())
				Expect(clientMock.PublishCalled).To(BeTrue())
				Expect(clientMock.LastTopic).To(Equal("profile"))
				Expect(clientMock.LastMessage.MsgType).To(Equal(shared.MsgTypeUserCreated))
				Expect(clientMock.LastMessage.ResourceKey).To(Equal(userID))
				Expect(clientMock.LastMessage.Payload).ToNot(BeNil())

				payload := &profileeventpb.UserCreatedEvent{}
				err = proto.Unmarshal(clientMock.LastMessage.Payload, payload)
				Expect(err).To(BeNil())
				Expect(payload.UserId).To(Equal(userID.String()))
				Expect(payload.Email).To(Equal("user@example.com"))
				Expect(payload.PhoneNumber).To(Equal("+447700900123"))
				Expect(payload.CountryCode).To(Equal("GB"))
				Expect(payload.HuggingfaceTokenCiphertext).To(Equal("ciphertext-1"))
			})
		})

		When("the profile account is invalid", func() {
			It("returns an argument error and does not publish", func() {
				err := publisher.PublishUserCreatedEvent(ctx, &domain.ProfileAccount{})

				Expect(err).To(MatchError("invalid arguments to publish user event"))
				Expect(clientMock.PublishCalled).To(BeFalse())
			})
		})

		When("the underlying publisher fails", func() {
			It("wraps the error and returns it", func() {
				clientMock.NextError = errors.New("kafka down")

				err := publisher.PublishUserCreatedEvent(ctx, &domain.ProfileAccount{
					ID:    userID,
					Email: "user@example.com",
				})

				Expect(err).To(MatchError("publish event failed"))
				Expect(clientMock.PublishCalled).To(BeTrue())
				Expect(clientMock.LastMessage.MsgType).To(Equal(shared.MsgTypeUserCreated))
			})
		})
	})

	Describe("PublishEmailVerificationRequestedEvent", func() {
		When("the arguments are valid", func() {
			It("publishes to the profile topic", func() {
				err := publisher.PublishEmailVerificationRequestedEvent(ctx, &domain.ProfileAccount{
					ID:               userID,
					Email:            "user@example.com",
					EmailVerifyToken: "token-1",
				})

				Expect(err).To(BeNil())
				Expect(clientMock.PublishCalled).To(BeTrue())
				Expect(clientMock.LastTopic).To(Equal("profile"))
				Expect(clientMock.LastMessage.MsgType).To(Equal(shared.MsgTypeEmailVerificationRequested))
				Expect(clientMock.LastMessage.ResourceKey).To(Equal(userID))

				payload := &profileeventpb.EmailVerificationRequestedEvent{}
				err = proto.Unmarshal(clientMock.LastMessage.Payload, payload)
				Expect(err).To(BeNil())
				Expect(payload.UserId).To(Equal(userID.String()))
				Expect(payload.Email).To(Equal("user@example.com"))
				Expect(payload.EmailVerifyToken).To(Equal("token-1"))
			})
		})
	})

	Describe("PublishUserUpdatedEvent", func() {
		When("the arguments are valid", func() {
			It("publishes a user updated event", func() {
				err := publisher.PublishUserUpdatedEvent(ctx, &domain.Profile{
					ProfileAccount: domain.ProfileAccount{
						ID:                         userID,
						Email:                      "user@example.com",
						PhoneNumber:                "+447700900123",
						CountryCode:                "GB",
						EmailVerified:              true,
						HuggingFaceTokenCiphertext: "ciphertext-2",
					},
					FirstName: "Ada",
					LastName:  "Lovelace",
				})

				Expect(err).To(BeNil())
				Expect(clientMock.PublishCalled).To(BeTrue())
				Expect(clientMock.LastTopic).To(Equal("profile"))
				Expect(clientMock.LastMessage.MsgType).To(Equal(shared.MsgTypeUserUpdated))
				Expect(clientMock.LastMessage.ResourceKey).To(Equal(userID))

				payload := &profileeventpb.UserUpdatedEvent{}
				err = proto.Unmarshal(clientMock.LastMessage.Payload, payload)
				Expect(err).To(BeNil())
				Expect(payload.UserId).To(Equal(userID.String()))
				Expect(payload.Email).To(Equal("user@example.com"))
				Expect(payload.FirstName).To(Equal("Ada"))
				Expect(payload.LastName).To(Equal("Lovelace"))
				Expect(payload.HuggingfaceTokenCiphertext).To(Equal("ciphertext-2"))
				verified, err := shared.EmailVerificationStatusFromProfileEventProto("user updated", payload.EmailVerificationStatus)
				Expect(err).To(BeNil())
				Expect(verified).To(BeTrue())
			})
		})
	})

	Describe("PublishUserDeletedEvent", func() {
		When("the arguments are valid", func() {
			It("publishes a user deleted event", func() {
				err := publisher.PublishUserDeletedEvent(ctx, userID)

				Expect(err).To(BeNil())
				Expect(clientMock.PublishCalled).To(BeTrue())
				Expect(clientMock.LastTopic).To(Equal("profile"))
				Expect(clientMock.LastMessage.MsgType).To(Equal(shared.MsgTypeUserDeleted))
				Expect(clientMock.LastMessage.ResourceKey).To(Equal(userID))
				Expect(clientMock.LastMessage.Payload).ToNot(BeNil())

				payload := &profileeventpb.UserDeletedEvent{}
				err = proto.Unmarshal(clientMock.LastMessage.Payload, payload)
				Expect(err).To(BeNil())
				Expect(payload.UserId).To(Equal(userID.String()))
			})
		})

		When("the userID is nil", func() {
			It("returns an argument error and does not publish", func() {
				err := publisher.PublishUserDeletedEvent(ctx, uuid.Nil)

				Expect(err).To(MatchError("invalid arguments to publish user event"))
				Expect(clientMock.PublishCalled).To(BeFalse())
			})
		})

		When("the underlying publisher fails", func() {
			It("wraps the error and returns it", func() {
				clientMock.NextError = errors.New("broker unavailable")

				err := publisher.PublishUserDeletedEvent(ctx, userID)

				Expect(err).To(MatchError("publish event failed"))
				Expect(clientMock.PublishCalled).To(BeTrue())
				Expect(clientMock.LastMessage.MsgType).To(Equal(shared.MsgTypeUserDeleted))
			})
		})
	})
})
