package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	auth "lib/shared_lib/auth"
	sharedclock "lib/shared_lib/clock"
	dbConn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	kms "lib/shared_lib/key_management"
	msgConn "lib/shared_lib/messaging"
	"lib/shared_lib/transport"
	shareduow "lib/shared_lib/uow"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"

	usecase "profile_service/pkg/app"
	"profile_service/pkg/infra/network/messaging"
	"profile_service/pkg/infra/network/rest"
	"profile_service/pkg/infra/repo/db"
)

var _ = Describe("Login/Logout Integration Tests", Ordered, func() {
	var (
		profileDB           db.ProfileDB
		profilesUseCase     usecase.ProfilesUseCase
		httpServer          *transport.HttpServer
		dtoProfileAdapter   rest.ProfilesDTOAdapter
		database            *dbConn.Database
		messagingFactory    msgConn.Messenger
		kafkaPublisherTopic string
		redisClient         rueidis.Client

		port    int
		baseURL string

		ctx                context.Context
		cancelCtxPublisher context.Context
		cancelFtnPublisher context.CancelFunc
		relayDone          chan struct{}

		// Test data
		email       string
		password    string
		newPassword string
		userID      uuid.UUID
		sessionID   string
		authToken   string
	)

	BeforeAll(func() {
		kafkaPublisherTopic = env.WithDefaultString("PROFILE_SERVICE_KAFKA_PUBLISHER_TOPIC", "profile")

		ctx = context.Background()
		purgeTopic(ctx, "localhost:9092", kafkaPublisherTopic)

		dbConfig := dbConn.DatabaseConfig{}
		dbConfig.WithDbName("PROFILE_SERVICE_DB_NAME", "bighill_profile_db")
		dbConfig.WithDbUser("PROFILE_SERVICE_DB_USER", "bighill_profile_db_user")
		dbConfig.WithDbPassword("PROFILE_SERVICE_DB_PASSWORD", "")
		dbConfig.WithDbMaxConnections("PROFILE_SERVICE_DB_MAX_CONNECTIONS", "60")
		dbConnectionStr := dbConfig.GetConnectionString()
		log.Info("Using DB connection string: ", dbConnectionStr)
		var err error
		database, err = dbConn.InitDatabase(ctx, dbConfig.GetName(), dbConnectionStr, log.StandardLogger())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(purgeProfileDatabase(ctx, database)).To(Succeed())
		profileDB = db.NewProfileDB(database)

		baseURL = fmt.Sprintf("http://localhost:%d", port)

		dtoProfileAdapter = rest.NewProfilesDTOAdapter()
	})

	BeforeEach(func() {
		port = freeProfileIntegrationPort()
		baseURL = fmt.Sprintf("http://localhost:%d", port)
		cancelCtxPublisher, cancelFtnPublisher = context.WithCancel(context.Background())
		groupID := "profile-login-test-" + uuid.New().String()
		messagingFactory = msgConn.NewMessenger(msgConn.MessengerConfig{
			DlqURL:  "http://localhost:4566/profile-dev-env-queue/",
			GroupID: groupID,
			Brokers: "localhost:9092",
		}, cancelFtnPublisher)

		msgPublisher, err := messagingFactory.Publisher(cancelCtxPublisher)
		Expect(err).ShouldNot(HaveOccurred())
		outboxWriter, err := msgConn.NewPostgresOutbox(database.Pool, database.Name, "")
		Expect(err).ShouldNot(HaveOccurred())
		orderedOutbox, ok := outboxWriter.(msgConn.OrderedOutbox)
		Expect(ok).To(BeTrue())
		outboxSignal := make(chan struct{}, 1)
		signaledOutbox := msgConn.NewSignaledOutbox(outboxWriter, outboxSignal)
		relayOutbox, ok := signaledOutbox.(msgConn.RelayOutbox)
		Expect(ok).To(BeTrue())
		relayPublisher, ok := msgPublisher.(msgConn.RelayPublisher)
		Expect(ok).To(BeTrue())
		relay := msgConn.NewOutboxRelay(relayOutbox, relayPublisher, msgConn.OutboxRelayConfig{
			PollInterval:   25 * time.Millisecond,
			FailureBackoff: 25 * time.Millisecond,
			BatchSize:      10,
			Signal:         outboxSignal,
		})
		relayDone = make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(relayDone)
			err := relay.Run(cancelCtxPublisher)
			if err != nil && !errors.Is(err, context.Canceled) {
				Fail(fmt.Sprintf("profile outbox relay failed: %v", err))
			}
		}()
		profileUnitOfWork := shareduow.New(database.Pool,
			shareduow.WithTransactionalOutbox(orderedOutbox),
			shareduow.WithOutboxSignal(func() { msgConn.NotifyOutboxSignal(outboxSignal) }),
		)
		profileEventBuilder := messaging.NewUserEventBuilder(kafkaPublisherTopic)

		redisAddr := env.WithDefaultString("PROFILE_SERVICE_REDIS_ADDRESS", "localhost:6379")
		redisUser := env.WithDefaultString("PROFILE_SERVICE_REDIS_USERNAME", "")
		redisPassword := env.WithDefaultString("PROFILE_SERVICE_REDIS_PASSWORD", "")
		opt := rueidis.ClientOption{
			InitAddress: []string{redisAddr},
			Username:    redisUser,
			Password:    redisPassword,
		}
		redisClient, err = rueidis.NewClient(opt)
		if err != nil {
			log.WithContext(ctx).WithError(err).Fatal("failed to initialize redis client")
		}
		authStore := auth.NewRevocationStore(redisClient, auth.WithKeyPrefix("auth:"))

		kmsClient, err := kms.NewKMSClient(ctx)
		Expect(err).ShouldNot(HaveOccurred())
		authProvider, err := auth.NewAuthProvider(ctx, kmsClient)
		Expect(err).ShouldNot(HaveOccurred())
		authExpirationInMinutes := env.WithDefaultInt("PROFILE_SERVICE_AUTH_EXPIRATION_MINUTES", "15")
		profilesUseCase = usecase.NewProfilesUseCase(
			usecase.ProfilesUseCaseDeps{
				ProfilesRepository: profileDB,
				UnitOfWork:         profileUnitOfWork,
				EventBuilder:       profileEventBuilder,
				AuthStore:          authStore,
				AuthProvider:       authProvider,
			},
			usecase.ProfilesUseCaseConfig{
				AuthExpirationInMinutes: authExpirationInMinutes,
				EmailValidationTTL:      60 * time.Minute,
				UseStagingTestToken:     env.WithDefaultBool("PROFILE_SERVICE_USE_STAGING_TEST_EMAIL_TOKEN", false),
			},
			usecase.WithProfileClock(sharedclock.System{}),
		)

		routes := rest.NewHttpHandler(profilesUseCase, dtoProfileAdapter).GetRoutes()

		tracer := otel.Tracer("test-trace")
		httpServer = transport.NewHttpServer(tracer, routes, port, "login-integration-test")

		go func() {
			defer GinkgoRecover()
			if err := httpServer.Connect(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				Fail(fmt.Sprintf("login test server failed: %v", err))
			}
		}()

		// Wait for server to start
		time.Sleep(200 * time.Millisecond)
	})

	AfterEach(func() {
		if httpServer != nil {
			httpServer.Close()
		}
		if cancelFtnPublisher != nil {
			cancelFtnPublisher()
		}
		if relayDone != nil {
			Eventually(relayDone, 5*time.Second).Should(BeClosed())
		}
		if redisClient != nil {
			redisClient.Close()
		}
	})

	Context("Complete login/logout flow", func() {
		It("creates profile, login, logout, login again, change password, logout, login with new password", func() {
			randomStr := strings.Split(uuid.New().String(), "-")[0]
			email = fmt.Sprintf("logintest-%s@test.com", randomStr)
			password = "TestPassword123!"
			newPassword = "NewPassword456!"
			phone := uniqueGBPhone()
			profilePayload := map[string]any{
				"email":       email,
				"password":    password,
				"phoneNumber": phone,
				"countryCode": "GB",
			}
			userID = createProfileAccount(port, profilePayload)
			verifyToken := stagingEmailVerifyToken(email)

			log.Infof("Created profile with userID: %s", userID.String())

			verifyPayload, err := json.Marshal(map[string]any{"token": verifyToken})
			Expect(err).NotTo(HaveOccurred())

			verifyReq, err := http.NewRequest(http.MethodPost, baseURL+"/public/v1/profiles/email/verify", bytes.NewBuffer(verifyPayload))
			Expect(err).NotTo(HaveOccurred())
			verifyReq.Header.Set("Content-Type", "application/json")

			verifyResp, err := http.DefaultClient.Do(verifyReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(verifyResp.StatusCode).To(Equal(http.StatusNoContent))
			verifyResp.Body.Close()

			loginPayload := map[string]interface{}{
				"email":    email,
				"password": password,
			}
			loginBytes, err := json.Marshal(loginPayload)
			Expect(err).NotTo(HaveOccurred())

			loginReq, err := http.NewRequest(http.MethodPost, baseURL+"/public/v1/profiles/password/verify", bytes.NewBuffer(loginBytes))
			Expect(err).NotTo(HaveOccurred())
			loginReq.Header.Set("Content-Type", "application/json")

			loginResp, err := http.DefaultClient.Do(loginReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(loginResp.StatusCode).To(Equal(http.StatusOK))
			loginBody, err := io.ReadAll(loginResp.Body)
			Expect(err).NotTo(HaveOccurred())
			loginResp.Body.Close()

			var loginResult map[string]interface{}
			err = json.Unmarshal(loginBody, &loginResult)
			Expect(err).NotTo(HaveOccurred())
			Expect(loginResult["isValid"]).To(BeTrue())
			Expect(loginResult["token"]).NotTo(BeEmpty())

			authToken = loginResult["token"].(string)

			parts := strings.Split(authToken, ".")
			Expect(len(parts)).To(Equal(3))

			payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
			Expect(err).NotTo(HaveOccurred())

			var payload map[string]interface{}
			err = json.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())
			sessionID = payload["sid"].(string)

			log.Infof("First login successful, sessionID: %s", sessionID)

			logoutReq, err := http.NewRequest(http.MethodPost, baseURL+"/private/v1/profiles/logout", nil)
			Expect(err).NotTo(HaveOccurred())
			logoutReq.Header.Set("X-User-ID", userID.String())
			logoutReq.Header.Set("X-Session-ID", sessionID)

			logoutResp, err := http.DefaultClient.Do(logoutReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(logoutResp.StatusCode).To(Equal(http.StatusNoContent))
			logoutResp.Body.Close()

			log.Info("First logout successful")

			loginReq2, err := http.NewRequest(http.MethodPost, baseURL+"/public/v1/profiles/password/verify", bytes.NewBuffer(loginBytes))
			Expect(err).NotTo(HaveOccurred())
			loginReq2.Header.Set("Content-Type", "application/json")

			loginResp2, err := http.DefaultClient.Do(loginReq2)
			Expect(err).NotTo(HaveOccurred())
			Expect(loginResp2.StatusCode).To(Equal(http.StatusOK))
			loginBody2, err := io.ReadAll(loginResp2.Body)
			Expect(err).NotTo(HaveOccurred())
			loginResp2.Body.Close()

			var loginResult2 map[string]interface{}
			err = json.Unmarshal(loginBody2, &loginResult2)
			Expect(err).NotTo(HaveOccurred())
			Expect(loginResult2["isValid"]).To(BeTrue())

			authToken = loginResult2["token"].(string)
			parts = strings.Split(authToken, ".")
			payloadBytes, err = base64.RawURLEncoding.DecodeString(parts[1])
			Expect(err).NotTo(HaveOccurred())
			err = json.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())
			sessionID = payload["sid"].(string)

			log.Infof("Second login successful, sessionID: %s", sessionID)

			changePasswordPayload := map[string]interface{}{
				"password": newPassword,
			}
			changePasswordBytes, _ := json.Marshal(changePasswordPayload)

			changePwdReq, err := http.NewRequest(http.MethodPut, baseURL+"/private/v1/profiles/password", bytes.NewBuffer(changePasswordBytes))
			Expect(err).NotTo(HaveOccurred())
			changePwdReq.Header.Set("Content-Type", "application/json")
			changePwdReq.Header.Set("X-User-ID", userID.String())

			changePwdResp, err := http.DefaultClient.Do(changePwdReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(changePwdResp.StatusCode).To(Equal(http.StatusNoContent))
			changePwdResp.Body.Close()

			log.Info("Password changed successfully")

			logoutReq2, err := http.NewRequest(http.MethodPost, baseURL+"/private/v1/profiles/logout", nil)
			Expect(err).NotTo(HaveOccurred())
			logoutReq2.Header.Set("X-User-ID", userID.String())
			logoutReq2.Header.Set("X-Session-ID", sessionID)

			logoutResp2, err := http.DefaultClient.Do(logoutReq2)
			Expect(err).NotTo(HaveOccurred())
			Expect(logoutResp2.StatusCode).To(Equal(http.StatusNoContent))
			logoutResp2.Body.Close()

			log.Info("Second logout successful")

			loginOldPwdReq, err := http.NewRequest(http.MethodPost, baseURL+"/public/v1/profiles/password/verify", bytes.NewBuffer(loginBytes))
			Expect(err).NotTo(HaveOccurred())
			loginOldPwdReq.Header.Set("Content-Type", "application/json")

			loginOldPwdResp, err := http.DefaultClient.Do(loginOldPwdReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(loginOldPwdResp.StatusCode).To(Equal(http.StatusOK))
			loginOldPwdBody, err := io.ReadAll(loginOldPwdResp.Body)
			Expect(err).NotTo(HaveOccurred())
			loginOldPwdResp.Body.Close()

			var loginOldResult map[string]interface{}
			err = json.Unmarshal(loginOldPwdBody, &loginOldResult)
			Expect(err).NotTo(HaveOccurred())
			Expect(loginOldResult["isValid"]).To(BeFalse())

			log.Info("Login with old password correctly failed")

			// Step 8: Login with new password (should succeed)
			newLoginPayload := map[string]interface{}{
				"email":    email,
				"password": newPassword,
			}
			newLoginBytes, err := json.Marshal(newLoginPayload)
			Expect(err).NotTo(HaveOccurred())

			loginNewPwdReq, err := http.NewRequest(http.MethodPost, baseURL+"/public/v1/profiles/password/verify", bytes.NewBuffer(newLoginBytes))
			Expect(err).NotTo(HaveOccurred())
			loginNewPwdReq.Header.Set("Content-Type", "application/json")

			loginNewPwdResp, err := http.DefaultClient.Do(loginNewPwdReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(loginNewPwdResp.StatusCode).To(Equal(http.StatusOK))
			loginNewPwdBody, err := io.ReadAll(loginNewPwdResp.Body)
			Expect(err).NotTo(HaveOccurred())
			loginNewPwdResp.Body.Close()

			var loginNewResult map[string]interface{}
			err = json.Unmarshal(loginNewPwdBody, &loginNewResult)
			Expect(err).NotTo(HaveOccurred())
			Expect(loginNewResult["isValid"]).To(BeTrue())
			Expect(loginNewResult["token"]).NotTo(BeEmpty())

			log.Info("Login with new password successful - test completed!")
		})
	})
})
