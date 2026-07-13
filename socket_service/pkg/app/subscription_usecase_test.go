package app_test

import (
	"context"
	"time"

	"socket_service/pkg/app"
	"socket_service/pkg/domain"

	"lib/shared_lib/userevents"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type ticketStoreStub struct {
	session       domain.Session
	err           error
	token         string
	issuedSession domain.Session
	issuedTTL     time.Duration
}

func (s *ticketStoreStub) Issue(_ context.Context, session domain.Session, ttl time.Duration) (domain.SocketTicket, error) {
	s.issuedSession = session
	s.issuedTTL = ttl
	return domain.SocketTicket{Token: "ticket-1", ExpiresAt: time.Now().Add(ttl)}, s.err
}

func (s *ticketStoreStub) Consume(_ context.Context, token string) (domain.Session, error) {
	s.token = token
	return s.session, s.err
}

type eventStreamReaderStub struct {
	replaySubscription domain.Subscription
	liveSubscription   domain.Subscription
	cursors            map[string]string
	limit              int
	events             []domain.StreamEvent
	err                error
}

func (s *eventStreamReaderStub) Replay(_ context.Context, subscription domain.Subscription, cursors map[string]string, limit int) ([]domain.StreamEvent, error) {
	s.replaySubscription = subscription
	s.cursors = cursors
	s.limit = limit
	return s.events, s.err
}

func (s *eventStreamReaderStub) ReadLive(_ context.Context, subscription domain.Subscription, cursors map[string]string, limit int) ([]domain.StreamEvent, error) {
	s.liveSubscription = subscription
	s.cursors = cursors
	s.limit = limit
	return s.events, s.err
}

var _ = Describe("SubscriptionUsecase", func() {
	It("issues a socket ticket through the ticket store port", func() {
		store := &ticketStoreStub{}
		uc := app.NewSubscriptionUsecase(store, app.NewRoomResolver("mlops"), &eventStreamReaderStub{})
		session := domain.Session{
			UserID:      "user-1",
			OrgID:       "org-1",
			SessionID:   "session-1",
			Permissions: []string{"model:read"},
			ExpiresAt:   time.Now().Add(time.Hour),
		}

		ticket, err := uc.IssueTicket(context.Background(), session, 5*time.Minute)

		Expect(err).NotTo(HaveOccurred())
		Expect(ticket.Token).To(Equal("ticket-1"))
		Expect(store.issuedSession.Permissions).To(Equal([]string{"model:read"}))
		Expect(store.issuedTTL).To(Equal(5 * time.Minute))
	})

	It("opens a subscription from a consumed socket ticket", func() {
		store := &ticketStoreStub{session: domain.Session{
			UserID:      "user-1",
			OrgID:       "org-1",
			SessionID:   "session-1",
			Permissions: []string{"model:read"},
			ExpiresAt:   time.Now().Add(time.Hour),
		}}
		reader := &eventStreamReaderStub{}
		uc := app.NewSubscriptionUsecase(store, app.NewRoomResolver("mlops"), reader)

		subscription, err := uc.Open(context.Background(), "token")

		Expect(err).NotTo(HaveOccurred())
		Expect(store.token).To(Equal("token"))
		Expect(subscription.ConnectionID).NotTo(BeEmpty())
		Expect(subscription.RoomKeys()).To(ConsistOf("mlops:user:user-1:events", "mlops:org:org-1:events"))
	})

	It("returns a domain unauthorized error for missing auth", func() {
		uc := app.NewSubscriptionUsecase(&ticketStoreStub{}, app.NewRoomResolver("mlops"), &eventStreamReaderStub{})

		_, err := uc.Open(context.Background(), "")

		Expect(err).To(MatchError(ContainSubstring("socket ticket is required")))
		Expect(err).To(MatchError(MatchRegexp("unauthorized.*")))
	})

	It("reads replay events through the stream reader port", func() {
		reader := &eventStreamReaderStub{events: []domain.StreamEvent{{
			Room:     domain.Room{Key: "mlops:user:user-1:events", Type: domain.RoomTypeUser},
			StreamID: "1-0",
			Event: userevents.Event{
				EventID:       "event-1",
				OccurredAt:    time.Now(),
				SourceService: "service",
				EventType:     userevents.EventTypeModelServingLoaded,
				Severity:      userevents.SeveritySuccess,
				UserID:        "user-1",
				Resource:      userevents.Resource{Type: userevents.ResourceTypeModel, ID: "model-1"},
				Title:         "ready",
				Message:       "ready",
			},
		}}}
		uc := app.NewSubscriptionUsecase(&ticketStoreStub{}, app.NewRoomResolver("mlops"), reader)
		subscription := domain.Subscription{Rooms: []domain.Room{{Key: "mlops:user:user-1:events", Type: domain.RoomTypeUser}}}

		events, err := uc.Replay(context.Background(), subscription, map[string]string{"mlops:user:user-1:events": "0-0"}, 25)

		Expect(err).NotTo(HaveOccurred())
		Expect(events).To(HaveLen(1))
		Expect(reader.limit).To(Equal(25))
		Expect(reader.replaySubscription.Rooms).To(HaveLen(1))
	})

	It("maps stream read failures to domain dependency errors", func() {
		reader := &eventStreamReaderStub{err: domain.ErrDependencyFailed.Extend("redis unavailable")}
		uc := app.NewSubscriptionUsecase(&ticketStoreStub{}, app.NewRoomResolver("mlops"), reader)
		subscription := domain.Subscription{Rooms: []domain.Room{{Key: "mlops:user:user-1:events", Type: domain.RoomTypeUser}}}

		_, err := uc.ReadLive(context.Background(), subscription, map[string]string{}, 10)

		Expect(err).To(MatchError(ContainSubstring("dependency failed")))
	})
})
