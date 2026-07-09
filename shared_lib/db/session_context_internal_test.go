package database

import (
	"context"

	"lib/shared_lib/ctxutil"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type sessionContextExecRecorder struct {
	sqls []string
	args [][]any
}

func (r *sessionContextExecRecorder) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.sqls = append(r.sqls, sql)
	r.args = append(r.args, args)
	return pgconn.CommandTag{}, nil
}

var _ = Describe("session context hooks", func() {
	It("does not require a tenant projection table in the profile database", func() {
		recorder := &sessionContextExecRecorder{}
		userID := uuid.New()
		orgID := uuid.New()
		ctx := ctxutil.WithOrgID(ctxutil.WithTenantID(context.Background(), userID), orgID)

		err := applyConnectionSessionContext(ctx, recorder, "bighill_tenant_db")

		Expect(err).NotTo(HaveOccurred())
		Expect(recorder.sqls).To(HaveLen(1))
		Expect(recorder.sqls[0]).To(ContainSubstring("set_config('app.current_user_id'"))
		Expect(recorder.args[0]).To(Equal([]any{userID.String(), orgID.String(), ""}))
	})

	It("sets tenant context without mutating tenant projections for tenant-scoped service databases", func() {
		recorder := &sessionContextExecRecorder{}
		userID := uuid.New()
		orgID := uuid.New()
		ctx := ctxutil.WithOrgID(ctxutil.WithTenantID(context.Background(), userID), orgID)

		err := applyConnectionSessionContext(ctx, recorder, "bighill_data_registry_db")

		Expect(err).NotTo(HaveOccurred())
		Expect(recorder.sqls).To(HaveLen(1))
		Expect(recorder.sqls[0]).To(ContainSubstring("set_config('app.current_user_id'"))
		Expect(recorder.args[0]).To(Equal([]any{userID.String(), orgID.String(), ""}))
	})
})
