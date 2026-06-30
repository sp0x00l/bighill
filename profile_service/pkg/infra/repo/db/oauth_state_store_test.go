package db_test

import (
	"context"
	"fmt"
	"time"

	"profile_service/pkg/domain"
	"profile_service/pkg/infra/repo/db"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

var _ = Describe("OAuthStateStore", func() {
	var (
		ctrl   *gomock.Controller
		client *mock.Client
		store  *db.OAuthStateStore
		ctx    context.Context
		prefix string
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		client = mock.NewClient(ctrl)
		ctx = context.Background()
		prefix = "test:oauth:"
		store = db.NewOAuthStateStore(client, prefix)
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	It("saves oauth state with a TTL", func() {
		state := domain.OAuthState{
			State:         "state-1",
			Provider:      "google",
			RedirectURI:   "https://app.example/callback",
			CodeChallenge: "challenge",
		}

		client.EXPECT().
			Do(ctx, mock.Match("SET", prefix+"state:state-1", `{"State":"state-1","Provider":"google","RedirectURI":"https://app.example/callback","CodeChallenge":"challenge"}`, "EX", "600")).
			Return(mock.Result(mock.RedisString("OK")))

		Expect(store.Save(ctx, state, 10*time.Minute)).To(Succeed())
	})

	It("loads oauth state from redis", func() {
		client.EXPECT().
			Do(ctx, mock.Match("GET", prefix+"state:state-1")).
			Return(mock.Result(mock.RedisString(`{"State":"state-1","Provider":"discord","RedirectURI":"https://app.example/callback","CodeChallenge":"challenge"}`)))

		state, err := store.Load(ctx, "state-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(state).NotTo(BeNil())
		Expect(state.Provider).To(Equal("discord"))
		Expect(state.RedirectURI).To(Equal("https://app.example/callback"))
	})

	It("returns nil when oauth state is missing", func() {
		client.EXPECT().
			Do(ctx, mock.Match("GET", prefix+"state:missing")).
			Return(mock.ErrorResult(rueidis.Nil))

		state, err := store.Load(ctx, "missing")
		Expect(err).NotTo(HaveOccurred())
		Expect(state).To(BeNil())
	})

	It("deletes oauth state", func() {
		client.EXPECT().
			Do(ctx, mock.Match("DEL", prefix+"state:state-1")).
			Return(mock.Result(mock.RedisInt64(1)))

		Expect(store.Delete(ctx, "state-1")).To(Succeed())
	})

	It("returns an error when stored oauth state is invalid json", func() {
		client.EXPECT().
			Do(ctx, mock.Match("GET", prefix+"state:bad-json")).
			Return(mock.Result(mock.RedisString(`{bad-json}`)))

		_, err := store.Load(ctx, "bad-json")
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(ContainSubstring("failed to unmarshal oauth state")))
	})

	It("propagates redis errors", func() {
		client.EXPECT().
			Do(ctx, mock.Match("DEL", prefix+"state:state-1")).
			Return(mock.ErrorResult(fmt.Errorf("boom")))

		err := store.Delete(ctx, "state-1")
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(ContainSubstring("boom")))
	})
})
