package tenant_test

import (
	"context"
	profilepb "lib/data_contracts_lib/profile"
	sharedDomain "lib/shared_lib/domain"
	"lib/shared_lib/tenant"
	"testing"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type projectionStoreStub struct {
	upserted *sharedDomain.Tenant
	deleted  uuid.UUID
}

func (s *projectionStoreStub) Upsert(_ context.Context, tenant *sharedDomain.Tenant) error {
	copy := *tenant
	s.upserted = &copy
	return nil
}

func (s *projectionStoreStub) Delete(_ context.Context, tenantID uuid.UUID) error {
	s.deleted = tenantID
	return nil
}

func TestTenantProjection(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tenant projection listener suite")
}

var _ = Describe("profile projection listeners", func() {
	var (
		ctx    context.Context
		userID uuid.UUID
		store  *projectionStoreStub
	)

	BeforeEach(func() {
		ctx = context.Background()
		userID = uuid.New()
		store = &projectionStoreStub{}
	})

	It("projects user created events into tenants", func() {
		listener := tenant.NewUserCreatedProjectionListener(store)

		err := listener.Handle(ctx, uuid.Nil, &profilepb.UserCreatedEvent{
			UserId:                     userID.String(),
			Email:                      "user@example.com",
			HuggingfaceTokenCiphertext: "ciphertext-1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(store.upserted).NotTo(BeNil())
		Expect(store.upserted.TenantID).To(Equal(userID))
		Expect(store.upserted.Email).To(Equal("user@example.com"))
		Expect(store.upserted.HuggingFaceTokenCiphertext).To(Equal("ciphertext-1"))
	})

	It("projects user updated events into tenants", func() {
		listener := tenant.NewUserUpdatedProjectionListener(store)

		err := listener.Handle(ctx, uuid.Nil, &profilepb.UserUpdatedEvent{
			UserId:                     userID.String(),
			Email:                      "new@example.com",
			HuggingfaceTokenCiphertext: "ciphertext-2",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(store.upserted).NotTo(BeNil())
		Expect(store.upserted.TenantID).To(Equal(userID))
		Expect(store.upserted.Email).To(Equal("new@example.com"))
		Expect(store.upserted.HuggingFaceTokenCiphertext).To(Equal("ciphertext-2"))
	})

	It("projects user deleted events into deleted tenants", func() {
		listener := tenant.NewUserDeletedProjectionListener(store)

		err := listener.Handle(ctx, userID, &profilepb.UserDeletedEvent{})

		Expect(err).NotTo(HaveOccurred())
		Expect(store.deleted).To(Equal(userID))
	})
})
