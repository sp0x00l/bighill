package authz_test

import (
	"net/http"
	"testing"

	"lib/shared_lib/authz"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAuthz(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Authz unit test suite")
}

var _ = Describe("Role permissions", func() {
	It("derives consumer permissions without write access", func() {
		permissions := authz.PermissionsForRole(authz.RoleConsumer)

		Expect(authz.HasPermission(permissions, authz.PermissionInferenceInvoke)).To(BeTrue())
		Expect(authz.HasPermission(permissions, authz.PermissionTrainingStart)).To(BeFalse())
	})

	It("derives org admin membership permissions", func() {
		permissions := authz.PermissionsForRole(authz.RoleOrgAdmin)

		Expect(authz.HasPermission(permissions, authz.PermissionOrgMembersWrite)).To(BeTrue())
		Expect(authz.ValidRole(authz.RoleOrgAdmin)).To(BeTrue())
		Expect(authz.ValidRole("owner")).To(BeFalse())
	})
})

var _ = Describe("Header helpers", func() {
	It("reads org ids and permissions from trusted headers", func() {
		orgID := uuid.New()
		req, err := http.NewRequest(http.MethodGet, "/", nil)
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set(authz.HeaderOrgID, orgID.String())
		req.Header.Set(authz.HeaderPermissions, authz.EncodeStringSlice([]string{authz.PermissionInferenceInvoke}))

		gotOrgID, err := authz.ReadOrgIDHeader(req.Context(), req)
		Expect(err).NotTo(HaveOccurred())
		Expect(gotOrgID).To(Equal(orgID))

		permissions, err := authz.ReadPermissionsHeader(req.Context(), req)
		Expect(err).NotTo(HaveOccurred())
		Expect(permissions).To(Equal([]string{authz.PermissionInferenceInvoke}))
	})

	It("rejects invalid org ids", func() {
		req, err := http.NewRequest(http.MethodGet, "/", nil)
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set(authz.HeaderOrgID, "not-a-uuid")

		_, err = authz.ReadOrgIDHeader(req.Context(), req)
		Expect(err).To(MatchError("invalid org ID"))
	})
})
