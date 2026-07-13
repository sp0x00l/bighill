package userevents_test

import (
	"lib/shared_lib/userevents"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("User event rooms", func() {
	It("builds user and permission-gated org rooms", func() {
		event := userevents.Event{
			UserID:             "user-1",
			OrgID:              "org-1",
			RequiredPermission: "model:read",
			Resource: userevents.Resource{
				Type: userevents.ResourceTypeModel,
				ID:   "model-1",
			},
		}
		rooms := userevents.EventRooms("mlops", event)
		Expect(rooms).To(Equal([]userevents.Room{
			{Key: "mlops:user:user-1:events"},
			{Key: "mlops:org:org-1:events"},
		}))
	})

	It("does not fan out to an org room without an explicit required permission", func() {
		event := userevents.Event{
			UserID: "user-1",
			OrgID:  "org-1",
			Resource: userevents.Resource{
				Type: userevents.ResourceTypeModel,
				ID:   "model-1",
			},
		}

		rooms := userevents.EventRooms("mlops", event)

		Expect(rooms).To(Equal([]userevents.Room{{Key: "mlops:user:user-1:events"}}))
	})

	It("uses the default prefix", func() {
		Expect(userevents.UserRoom("", "user-1").Key).To(Equal("mlops:user:user-1:events"))
		rooms := userevents.EventRooms("", userevents.Event{UserID: "user-1"})
		Expect(rooms[0].Key).To(Equal("mlops:user:user-1:events"))
	})
})
