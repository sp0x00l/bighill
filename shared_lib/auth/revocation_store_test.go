package provider_test

import (
	"context"
	"fmt"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	auth "lib/shared_lib/auth"

	"go.uber.org/mock/gomock"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/mock"
)

var _ = Describe("AuthRevocationStore with rueidis/mock", func() {
	var (
		ctrl   *gomock.Controller
		client *mock.Client
		store  auth.RevocationStore
		ctx    context.Context
		now    time.Time
		prefix string
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		client = mock.NewClient(ctrl)
		ctx = context.Background()
		prefix = "test:auth:"
		now = time.Unix(1_700_000_000, 0) // fixed clock for deterministic TTL

		store = auth.NewAuthRevocationStore(
			client,
			auth.WithKeyPrefix(prefix),
			auth.WithClock(func() time.Time { return now }),
		)
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("per-token denylist (JTI)", func() {
		It("RevokeJTI sets key with EX TTL and IsRevoked detects it", func() {
			jti := "jti-123"
			exp := now.Add(10 * time.Minute).Unix()
			key := prefix + "revoked:jti:" + jti
			wantTTL := "600" // seconds

			// Expect SET key 1 EX 600
			client.EXPECT().
				Do(ctx, mock.Match("SET", key, "1", "EX", wantTTL)).
				Return(mock.Result(mock.RedisString("OK")))

			Expect(store.RevokeToken(ctx, jti, exp)).To(Succeed())

			// Expect EXISTS key => 1
			client.EXPECT().
				Do(ctx, mock.Match("EXISTS", key)).
				Return(mock.Result(mock.RedisInt64(1)))

			revoked, err := store.IsRevoked(ctx, jti)
			Expect(err).ToNot(HaveOccurred())
			Expect(revoked).To(BeTrue())
		})

		It("uses minimal TTL when token is already expired", func() {
			jti := "old-jti"
			exp := now.Add(-5 * time.Minute).Unix()
			key := prefix + "revoked:jti:" + jti

			// Because exp is in the past, store forces EX 1 second
			client.EXPECT().
				Do(ctx, mock.Match("SET", key, "1", "EX", "1")).
				Return(mock.Result(mock.RedisString("OK")))

			Expect(store.RevokeToken(ctx, jti, exp)).To(Succeed())
		})

		It("errors on empty JTI without touching Redis", func() {
			err := store.RevokeToken(ctx, "", now.Add(60*time.Second).Unix())
			Expect(err).To(MatchError(ContainSubstring("jti is empty")))

			revoked, err := store.IsRevoked(ctx, "")
			Expect(err).To(MatchError(ContainSubstring("jti is empty")))
			Expect(revoked).To(BeFalse())
		})

		It("propagates Redis errors from IsRevoked", func() {
			jti := "jti-x"
			key := prefix + "revoked:jti:" + jti

			client.EXPECT().
				Do(ctx, mock.Match("EXISTS", key)).
				Return(mock.ErrorResult(fmt.Errorf("boom")))

			revoked, err := store.IsRevoked(ctx, jti)
			Expect(err).To(HaveOccurred())
			Expect(revoked).To(BeFalse())
		})
	})

	Describe("per-user revoked_after", func() {
		It("SetUserRevokedAfter and GetUserRevokedAfter round-trip a timestamp", func() {
			userID := "user-42"
			ts := now.Unix()
			key := prefix + "user:revoked_after:" + userID

			// SET key ts
			client.EXPECT().
				Do(ctx, mock.Match("SET", key, strconv.FormatInt(ts, 10))).
				Return(mock.Result(mock.RedisString("OK")))
			Expect(store.SetUserRevokedAfter(ctx, userID, ts)).To(Succeed())

			// GET key -> ts
			client.EXPECT().
				Do(ctx, mock.Match("GET", key)).
				Return(mock.Result(mock.RedisString(strconv.FormatInt(ts, 10))))
			got, err := store.GetUserRevokedAfter(ctx, userID)
			Expect(err).ToNot(HaveOccurred())
			Expect(got).To(Equal(ts))
		})

		It("ClearUserRevokedAfter removes the timestamp", func() {
			userID := "user-99"
			key := prefix + "user:revoked_after:" + userID

			// DEL key -> 1
			client.EXPECT().
				Do(ctx, mock.Match("DEL", key)).
				Return(mock.Result(mock.RedisInt64(1)))
			Expect(store.ClearUserRevokedAfter(ctx, userID)).To(Succeed())
		})

		It("GetUserRevokedAfter returns 0 for missing key (Redis nil)", func() {
			userID := "unknown-user"
			key := prefix + "user:revoked_after:" + userID

			client.EXPECT().
				Do(ctx, mock.Match("GET", key)).
				Return(mock.ErrorResult(rueidis.Nil)) // simulate Redis nil

			got, err := store.GetUserRevokedAfter(ctx, userID)
			Expect(err).ToNot(HaveOccurred())
			Expect(got).To(Equal(int64(0)))
		})

		It("errors on empty userID without touching Redis", func() {
			Expect(store.SetUserRevokedAfter(ctx, "", now.Unix())).
				To(MatchError(ContainSubstring("userID is empty")))
			_, err := store.GetUserRevokedAfter(ctx, "")
			Expect(err).To(MatchError(ContainSubstring("userID is empty")))
		})
	})

	Describe("session allowlist", func() {
		It("CreateSession sets with TTL, SessionExists checks, DeleteSession removes", func() {
			sid := "sid-abc"
			exp := now.Add(3 * time.Minute).Unix()
			key := prefix + "session:" + sid

			// SET key 1 EX 180
			client.EXPECT().
				Do(ctx, mock.Match("SET", key, "1", "EX", "180")).
				Return(mock.Result(mock.RedisString("OK")))
			Expect(store.CreateSession(ctx, sid, exp)).To(Succeed())

			// EXISTS key -> 1
			client.EXPECT().
				Do(ctx, mock.Match("EXISTS", key)).
				Return(mock.Result(mock.RedisInt64(1)))
			ok, err := store.SessionExists(ctx, sid)
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())

			// DEL key -> 1
			client.EXPECT().
				Do(ctx, mock.Match("DEL", key)).
				Return(mock.Result(mock.RedisInt64(1)))
			Expect(store.DeleteSession(ctx, sid)).To(Succeed())

			// EXISTS key -> 0
			client.EXPECT().
				Do(ctx, mock.Match("EXISTS", key)).
				Return(mock.Result(mock.RedisInt64(0)))
			ok, err = store.SessionExists(ctx, sid)
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeFalse())
		})

		It("CreateSession uses minimal TTL when exp is in the past", func() {
			sid := "sid-old"
			exp := now.Add(-1 * time.Minute).Unix()
			key := prefix + "session:" + sid

			// EX 1 second when past
			client.EXPECT().
				Do(ctx, mock.Match("SET", key, "1", "EX", "1")).
				Return(mock.Result(mock.RedisString("OK")))

			Expect(store.CreateSession(ctx, sid, exp)).To(Succeed())
		})

		It("errors on empty SID without touching Redis", func() {
			Expect(store.CreateSession(ctx, "", now.Add(60*time.Second).Unix())).
				To(MatchError(ContainSubstring("sid is empty")))
			_, err := store.SessionExists(ctx, "")
			Expect(err).To(MatchError(ContainSubstring("sid is empty")))
			Expect(store.DeleteSession(ctx, "")).
				To(MatchError(ContainSubstring("sid is empty")))
		})
	})
})
