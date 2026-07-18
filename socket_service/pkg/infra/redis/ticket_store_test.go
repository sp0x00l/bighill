package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"socket_service/pkg/domain"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRedisInfra(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Socket service redis unit test suite")
}

var _ = Describe("TicketStore", func() {
	var (
		ctx     context.Context
		ctrl    *gomock.Controller
		client  *mock.Client
		store   *TicketStore
		now     time.Time
		session domain.Session
	)

	BeforeEach(func() {
		ctx = context.Background()
		ctrl = gomock.NewController(GinkgoT())
		client = mock.NewClient(ctrl)
		now = time.Now().UTC()
		store = NewTicketStore(client)
		store.now = func() time.Time { return now }
		session = domain.Session{
			UserID:      " user-1 ",
			OrgID:       " org-1 ",
			Roles:       []string{"admin"},
			Permissions: []string{"model:read"},
			SessionID:   " session-1 ",
			ExpiresAt:   now.Add(10 * time.Minute),
		}
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("Issue", func() {
		It("stores a one-shot ticket with a TTL capped by the session expiry", func() {
			var capturedKey string
			client.EXPECT().
				Do(gomock.Any(), mock.MatchFn(func(cmd []string) bool {
					if len(cmd) < 6 || cmd[0] != "SET" || !strings.HasPrefix(cmd[1], socketTicketKeyPrefix) {
						return false
					}
					capturedKey = cmd[1]
					var record socketTicketRecord
					if err := json.Unmarshal([]byte(cmd[2]), &record); err != nil {
						return false
					}
					return record.UserID == "user-1" &&
						record.OrgID == "org-1" &&
						record.SessionID == "session-1" &&
						record.ExpiresAt.Equal(session.ExpiresAt) &&
						hasCommandToken(cmd, "NX") &&
						expiresWithin(cmd, 1, 600)
				}, "SET socket ticket")).
				Return(mock.Result(mock.RedisString("OK")))

			ticket, err := store.Issue(ctx, session, time.Hour)

			Expect(err).NotTo(HaveOccurred())
			Expect(ticket.Token).NotTo(BeEmpty())
			Expect(capturedKey).To(Equal(socketTicketKeyPrefix + ticket.Token))
			Expect(ticket.ExpiresAt).To(Equal(session.ExpiresAt))
		})

		It("maps Redis write failures to a dependency error", func() {
			client.EXPECT().
				Do(gomock.Any(), mock.MatchFn(func(cmd []string) bool {
					return len(cmd) > 0 && cmd[0] == "SET"
				}, "SET socket ticket")).
				Return(mock.ErrorResult(errors.New("redis unavailable")))

			ticket, err := store.Issue(ctx, session, time.Minute)

			Expect(ticket).To(Equal(domain.SocketTicket{}))
			Expect(errors.Is(err, domain.ErrDependencyFailed)).To(BeTrue())
		})

		It("rejects expired sessions without touching Redis", func() {
			session.ExpiresAt = now.Add(-time.Second)

			ticket, err := store.Issue(ctx, session, time.Minute)

			Expect(ticket).To(Equal(domain.SocketTicket{}))
			Expect(errors.Is(err, domain.ErrUnauthorized)).To(BeTrue())
		})
	})

	Describe("Consume", func() {
		It("consumes a ticket with GETDEL and returns the session", func() {
			record := socketTicketRecord{
				UserID:      "user-1",
				OrgID:       "org-1",
				Roles:       []string{"admin"},
				Permissions: []string{"model:read"},
				SessionID:   "session-1",
				ExpiresAt:   now.Add(time.Hour),
			}
			payload, err := json.Marshal(record)
			Expect(err).NotTo(HaveOccurred())
			client.EXPECT().
				Do(ctx, mock.Match("GETDEL", socketTicketKeyPrefix+"ticket-1")).
				Return(mock.Result(mock.RedisString(string(payload))))

			got, err := store.Consume(ctx, " ticket-1 ")

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(domain.Session{
				UserID:      "user-1",
				OrgID:       "org-1",
				Roles:       []string{"admin"},
				Permissions: []string{"model:read"},
				SessionID:   "session-1",
				ExpiresAt:   record.ExpiresAt,
			}))
		})

		It("maps missing tickets to unauthorized", func() {
			client.EXPECT().
				Do(ctx, mock.Match("GETDEL", socketTicketKeyPrefix+"missing")).
				Return(mock.ErrorResult(rueidis.Nil))

			got, err := store.Consume(ctx, "missing")

			Expect(got).To(Equal(domain.Session{}))
			Expect(errors.Is(err, domain.ErrUnauthorized)).To(BeTrue())
		})

		It("maps invalid payloads to unauthorized", func() {
			client.EXPECT().
				Do(ctx, mock.Match("GETDEL", socketTicketKeyPrefix+"bad-json")).
				Return(mock.Result(mock.RedisString("{")))

			got, err := store.Consume(ctx, "bad-json")

			Expect(got).To(Equal(domain.Session{}))
			Expect(errors.Is(err, domain.ErrUnauthorized)).To(BeTrue())
		})

		It("rejects empty tokens without touching Redis", func() {
			got, err := store.Consume(ctx, " ")

			Expect(got).To(Equal(domain.Session{}))
			Expect(errors.Is(err, domain.ErrUnauthorized)).To(BeTrue())
		})
	})
})

func hasCommandToken(cmd []string, token string) bool {
	for _, part := range cmd {
		if strings.EqualFold(part, token) {
			return true
		}
	}
	return false
}

func expiresWithin(cmd []string, minSeconds int, maxSeconds int) bool {
	for i := 0; i < len(cmd)-1; i++ {
		if !strings.EqualFold(cmd[i], "EX") {
			continue
		}
		ttl, err := strconv.Atoi(cmd[i+1])
		return err == nil && ttl >= minSeconds && ttl <= maxSeconds
	}
	return false
}
