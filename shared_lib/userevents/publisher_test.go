package userevents_test

import (
	"context"
	"time"

	"lib/shared_lib/userevents"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

var _ = Describe("RedisPublisher", func() {
	It("writes each resolved room to Redis Stream and Pub/Sub", func() {
		ctrl := gomock.NewController(GinkgoT())
		defer ctrl.Finish()

		client := mock.NewClient(ctrl)
		publisher := userevents.NewRedisPublisherWithClient(client, userevents.Config{
			ChannelPrefix:  "mlops",
			PublishTimeout: time.Second,
			StreamMaxLen:   1000,
		}, false)
		ctx := context.Background()
		event := userevents.Event{
			EventID:            "event-1",
			OccurredAt:         time.Unix(100, 0).UTC(),
			SourceService:      "model_registry_service",
			EventType:          userevents.EventTypeModelServingLoaded,
			Severity:           userevents.SeveritySuccess,
			RequiredPermission: "model:read",
			UserID:             "user-1",
			OrgID:              "org-1",
			Resource: userevents.Resource{
				Type: userevents.ResourceTypeModel,
				ID:   "model-1",
			},
			Status:  userevents.Status{State: "LOADED", Phase: userevents.StatusPhaseServing},
			Title:   "Model ready",
			Message: "The model is ready.",
		}

		for _, room := range []string{
			"mlops:user:user-1:events",
			"mlops:org:org-1:events",
		} {
			client.EXPECT().
				Do(gomock.Any(), mock.MatchFn(func(cmd []string) bool {
					return len(cmd) == 8 &&
						cmd[0] == "XADD" &&
						cmd[1] == room &&
						cmd[2] == "MAXLEN" &&
						cmd[3] == "~" &&
						cmd[4] == "1000" &&
						cmd[5] == "*" &&
						cmd[6] == "payload" &&
						cmd[7] != ""
				}, "XADD user event")).
				Return(mock.Result(mock.RedisString("1700000000000-0")))
			client.EXPECT().
				Do(gomock.Any(), mock.MatchFn(func(cmd []string) bool {
					return len(cmd) == 3 &&
						cmd[0] == "PUBLISH" &&
						cmd[1] == room &&
						cmd[2] != ""
				}, "PUBLISH user event")).
				Return(mock.Result(mock.RedisInt64(1)))
		}

		Expect(publisher.Publish(ctx, event)).To(Succeed())
	})
})
