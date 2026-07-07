package ctxutil_test

import (
	"context"
	"errors"
	"lib/shared_lib/ctxutil"
	"testing"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCtxutil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Context utility unit test suite")
}

var _ = Describe("IsCanceled", func() {
	It("matches context cancellation errors", func() {
		Expect(ctxutil.IsCanceled(context.Canceled)).To(BeTrue())
		Expect(ctxutil.IsCanceled(context.DeadlineExceeded)).To(BeTrue())
		Expect(ctxutil.IsCanceled(errors.New("other"))).To(BeFalse())
	})
})

var _ = Describe("TenantID", func() {
	It("stores and reads a tenant id", func() {
		tenantID := uuid.New()
		ctx := ctxutil.WithTenantID(context.Background(), tenantID)

		got, ok := ctxutil.TenantID(ctx)
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal(tenantID))
	})

	It("does not store nil tenant ids", func() {
		ctx := ctxutil.WithTenantID(context.Background(), uuid.Nil)

		_, ok := ctxutil.TenantID(ctx)
		Expect(ok).To(BeFalse())
	})
})

var _ = Describe("OrgID", func() {
	It("stores and reads an org id", func() {
		orgID := uuid.New()
		ctx := ctxutil.WithOrgID(context.Background(), orgID)

		got, ok := ctxutil.OrgID(ctx)
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal(orgID))
	})

	It("does not store nil org ids", func() {
		ctx := ctxutil.WithOrgID(context.Background(), uuid.Nil)

		_, ok := ctxutil.OrgID(ctx)
		Expect(ok).To(BeFalse())
	})
})

var _ = Describe("WithActorOrg", func() {
	It("stores actor and org ids together", func() {
		userID := uuid.New()
		orgID := uuid.New()

		ctx := ctxutil.WithActorOrg(context.Background(), userID, orgID)

		gotUserID, ok := ctxutil.TenantID(ctx)
		Expect(ok).To(BeTrue())
		Expect(gotUserID).To(Equal(userID))
		gotOrgID, ok := ctxutil.OrgID(ctx)
		Expect(ok).To(BeTrue())
		Expect(gotOrgID).To(Equal(orgID))
	})
})

var _ = Describe("TransactionContext", func() {
	It("marks transaction-scoped contexts", func() {
		ctx := ctxutil.WithTransactionContext(context.Background())

		Expect(ctxutil.IsTransactionContext(ctx)).To(BeTrue())
		Expect(ctxutil.IsTransactionContext(context.Background())).To(BeFalse())
	})
})
