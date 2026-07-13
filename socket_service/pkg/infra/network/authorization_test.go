package network

import (
	"socket_service/pkg/domain"

	"lib/shared_lib/authz"
	"lib/shared_lib/userevents"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("socket event authorization", func() {
	It("requires the envelope permission for org-scoped events", func() {
		event := userevents.Event{
			EventID:            "event-1",
			EventType:          userevents.EventTypeModelServingFailed,
			Severity:           userevents.SeverityError,
			UserID:             "owner-user",
			OrgID:              "org-1",
			RequiredPermission: authz.PermissionModelRead,
			Resource:           userevents.Resource{Type: userevents.ResourceTypeModel, ID: "model-1"},
			Title:              "Model serving failed",
			Message:            "Model serving failed",
		}

		consumer := domain.Session{UserID: "other-user", OrgID: "org-1", Permissions: []string{authz.PermissionInferenceInvoke}}
		researcher := domain.Session{UserID: "other-user", OrgID: "org-1", Permissions: []string{authz.PermissionModelRead}}

		Expect(authorizedEvent(consumer, event)).To(BeFalse())
		Expect(authorizedEvent(researcher, event)).To(BeTrue())
	})
})
