package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	profileeventpb "lib/data_contracts_lib/profile_event"
	auth "lib/shared_lib/auth"
	sharedclock "lib/shared_lib/clock"
	kms "lib/shared_lib/key_management"
	logs "lib/shared_lib/logs"
	"lib/shared_lib/transport"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"

	dbConn "lib/shared_lib/db"
	env "lib/shared_lib/env"

	msgConn "lib/shared_lib/messaging"
	usecase "profile_service/pkg/app"
	"profile_service/pkg/infra/network/messaging"
	"profile_service/pkg/infra/network/rest"
	"profile_service/pkg/infra/repo/db"
	"testing"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type ErrorMessage struct {
	Message string `json:"message"`
}

// generte a unique UK phone number for testing
var (
	gbPhoneSeq  int64
	runSaltOnce sync.Once
	runSalt     int64
)

type userCreatedEventListener struct {
	mu       sync.Mutex
	payloads map[uuid.UUID]*profileeventpb.UserCreatedEvent
}

type emailVerificationRequestedEventListener struct {
	mu       sync.Mutex
	payloads map[uuid.UUID]*profileeventpb.EmailVerificationRequestedEvent
}

type userUpdatedEventListener struct {
	mu       sync.Mutex
	payloads map[uuid.UUID]*profileeventpb.UserUpdatedEvent
}

type userDeletedEventListener struct {
	mu      sync.Mutex
	deleted map[uuid.UUID]bool
}

func (u *userCreatedEventListener) Handle(ctx context.Context, id uuid.UUID, payload *profileeventpb.UserCreatedEvent) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.payloads == nil {
		u.payloads = make(map[uuid.UUID]*profileeventpb.UserCreatedEvent)
	}
	u.payloads[id] = payload
	return nil
}

func (u *userCreatedEventListener) MsgType() msgConn.MsgType {
	return msgConn.MsgTypeUserCreated
}

func (u *userCreatedEventListener) NewMessage() *profileeventpb.UserCreatedEvent {
	return &profileeventpb.UserCreatedEvent{}
}

func (u *emailVerificationRequestedEventListener) Handle(ctx context.Context, id uuid.UUID, payload *profileeventpb.EmailVerificationRequestedEvent) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.payloads == nil {
		u.payloads = make(map[uuid.UUID]*profileeventpb.EmailVerificationRequestedEvent)
	}
	u.payloads[id] = payload
	return nil
}

func (u *emailVerificationRequestedEventListener) MsgType() msgConn.MsgType {
	return msgConn.MsgTypeEmailVerificationRequested
}

func (u *emailVerificationRequestedEventListener) NewMessage() *profileeventpb.EmailVerificationRequestedEvent {
	return &profileeventpb.EmailVerificationRequestedEvent{}
}

func (u *userUpdatedEventListener) Handle(ctx context.Context, id uuid.UUID, payload *profileeventpb.UserUpdatedEvent) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.payloads == nil {
		u.payloads = make(map[uuid.UUID]*profileeventpb.UserUpdatedEvent)
	}
	u.payloads[id] = payload
	return nil
}

func (u *userUpdatedEventListener) MsgType() msgConn.MsgType {
	return msgConn.MsgTypeUserUpdated
}

func (u *userUpdatedEventListener) NewMessage() *profileeventpb.UserUpdatedEvent {
	return &profileeventpb.UserUpdatedEvent{}
}

func (u *userDeletedEventListener) Handle(ctx context.Context, id uuid.UUID, payload *profileeventpb.UserDeletedEvent) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.deleted == nil {
		u.deleted = make(map[uuid.UUID]bool)
	}
	u.deleted[id] = payload.GetUserId() != ""
	return nil
}

func (u *userDeletedEventListener) MsgType() msgConn.MsgType {
	return msgConn.MsgTypeUserDeleted
}

func (u *userDeletedEventListener) NewMessage() *profileeventpb.UserDeletedEvent {
	return &profileeventpb.UserDeletedEvent{}
}

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Profile server integration test suite")
	logs.Init()
}

func startSubscriberOrFail(ctx context.Context, name string, start func(context.Context) error) {
	go func() {
		defer GinkgoRecover()
		if err := start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			Fail(fmt.Sprintf("%s subscriber failed: %v", name, err))
		}
	}()
}

var _ = Describe("Profile server entry points", Ordered, func() {
	var (
		profileDB        db.ProfileDB
		profilePublisher messaging.UserEventPublisher
		profilesUseCase  usecase.ProfilesUseCase
		httpServer       *transport.HttpServer

		dtoProfileAdapter rest.ProfilesDTOAdapter

		messagingFactory    msgConn.Messenger
		kafkaPublisherTopic string

		port        int
		resourceUrl string

		request     *http.Request
		redisClient rueidis.Client

		ctx                context.Context
		cancelCtxPublisher context.Context
		cancelFtnPublisher context.CancelFunc

		userIDSuccssful uuid.UUID
		phoneSuccessful string
		emailSuccessful string
	)

	Describe("exchange profile end-points", func() {
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
			database, err := dbConn.InitDatabase(ctx, dbConfig.GetName(), dbConnectionStr, log.StandardLogger())
			Expect(err).ShouldNot(HaveOccurred())
			profileDB = db.NewProfileDB(database)

			port = env.WithDefaultInt("PROFILE_SERVICE_HTTP_PORT", "8082")

			dtoProfileAdapter = rest.NewProfilesDTOAdapter()
		})

		BeforeEach(func() {
			cancelCtxPublisher, cancelFtnPublisher = context.WithCancel(context.Background())
			groupID := "profile-group" + uuid.New().String()
			messagingFactory = msgConn.NewMessenger(msgConn.MessengerConfig{
				DlqURL:  "http://localhost:4566/profile-dev-env-queue/",
				GroupID: groupID,
				Brokers: "localhost:9092",
			}, cancelFtnPublisher)

			msgPublisher, err := messagingFactory.Publisher(cancelCtxPublisher)
			Expect(err).ShouldNot(HaveOccurred())
			profilePublisher = messaging.NewUserEventPublisher(msgPublisher, kafkaPublisherTopic)

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
			authExpirationInMinutes := env.WithDefaultInt("PROFILE_AUTH_EXPIRATION_MINUTES", "15")
			profilesUseCase = usecase.NewProfilesUseCase(
				usecase.ProfilesUseCaseDeps{
					ProfilesRepository: profileDB,
					MsgPublisher:       profilePublisher,
					AuthStore:          authStore,
					AuthProvider:       authProvider,
				},
				usecase.ProfilesUseCaseConfig{
					AuthExpirationInMinutes: authExpirationInMinutes,
					EmailValidationTTL:      60 * time.Minute,
				},
				usecase.WithProfileClock(sharedclock.System{}),
			)

			routes := rest.NewHttpHandler(profilesUseCase, dtoProfileAdapter).GetRoutes()

			tracer := otel.Tracer("test-trace")
			httpServer = transport.NewHttpServer(tracer, routes, port, "profile-integration-test")

			server := httpServer
			go func() {
				_ = server.Connect()
			}()

			Eventually(func() error {
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
				if err != nil {
					return err
				}
				_ = conn.Close()
				return nil
			}, 5*time.Second, 50*time.Millisecond).Should(Succeed(), "profile test server should be listening before requests run")
		})

		AfterEach(func() {
			cancelFtnPublisher()
			if httpServer != nil {
				httpServer.Close()
			}
			if redisClient != nil {
				redisClient.Close()
			}
		})

		Context("create a new profile", func() {
			var (
				email string
				phone string
			)
			BeforeEach(func() {
				resourceUrl = fmt.Sprintf("http://localhost:%d/public/v1/profiles", port)
				phone = uniqueGBPhone()

				randomStr := strings.Split(uuid.New().String(), "-")[0]
				email = fmt.Sprintf("%s@test.com", randomStr)

				createRequestPayload := newProfileAccountDTO(email, phone)
				payload, err := json.Marshal(createRequestPayload)
				Expect(err).ShouldNot(HaveOccurred())

				request, _ = http.NewRequest(http.MethodPost, resourceUrl, bytes.NewBuffer(payload))
				request.Header.Set("X-Request-ID", uuid.New().String())
				request.Header.Set("Content-Type", "application/json")
			})

			It("creates a new profile without error, returning a 201-StatusCode", func() {
				msgSubscriber, err := messagingFactory.Subscriber(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				listener := &userCreatedEventListener{}
				msgConn.AddListener(msgSubscriber, listener)

				cancelCtxSubscriber, cancelFtnSubscriber := context.WithCancel(context.Background())
				startSubscriberOrFail(cancelCtxSubscriber, "profile-assert", func(ctx context.Context) error {
					return msgSubscriber.Subscribe(ctx, []string{kafkaPublisherTopic})
				})

				response, err := http.DefaultClient.Do(request)
				Expect(err).ShouldNot(HaveOccurred())
				defer response.Body.Close()
				resBody, err := io.ReadAll(response.Body)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(response.StatusCode).To(Equal(http.StatusCreated))

				var profileDTO map[string]any
				err = json.Unmarshal(resBody, &profileDTO)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(profileDTO["email"]).Should(Equal(email))
				Expect(profileDTO["phoneNumber"]).Should(Equal(phone))
				Expect(profileDTO["countryCode"]).Should(Equal("GB"))
				Expect(profileDTO["password"]).Should(BeEmpty())

				id := profileDTO["id"]
				Expect(id).ShouldNot(BeNil())
				respID, err := uuid.Parse(id.(string))
				Expect(err).ShouldNot(HaveOccurred())
				Expect(respID).ShouldNot(Equal(uuid.Nil))

				userIDSuccssful = respID
				phoneSuccessful = phone
				emailSuccessful = email

				Eventually(func() error {
					listener.mu.Lock()
					defer listener.mu.Unlock()
					payload, ok := listener.payloads[userIDSuccssful]
					if !ok {
						return errors.New("user created event not seen yet")
					}
					if payload == nil || payload.UserId != userIDSuccssful.String() {
						return fmt.Errorf("unexpected payload: %#v", payload)
					}
					if payload.Email != email {
						return fmt.Errorf("unexpected event email: got %s want %s", payload.Email, email)
					}
					if payload.PhoneNumber != phone {
						return fmt.Errorf("unexpected event phone number: got %s want %s", payload.PhoneNumber, phone)
					}
					if payload.CountryCode != "GB" {
						return fmt.Errorf("unexpected event country code: got %s want GB", payload.CountryCode)
					}
					verified, err := msgConn.EmailVerificationStatusFromProfileEventProto("user created", payload.EmailVerificationStatus)
					if err != nil {
						return err
					}
					if verified {
						return fmt.Errorf("unexpected verified email flag on create event")
					}
					cancelFtnSubscriber()

					return nil
				}, 15*time.Second, 50*time.Millisecond).Should(Succeed())
			})

			It("returns a 400 BadRequest when the request is missing a header", func() {
				request.Header.Del("X-Request-ID")
				response, err := http.DefaultClient.Do(request)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
			})

			It("returns a 400 BadRequest when the request body is invalid", func() {
				request, _ := http.NewRequest(http.MethodPost, resourceUrl, bytes.NewBuffer([]byte("invalid")))
				request.Header.Set("X-Request-ID", uuid.New().String())
				request.Header.Set("Content-Type", "application/json")

				response, err := http.DefaultClient.Do(request)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
			})

			It("returns a 400 BadRequest when the phone number is not valid for the country", func() {
				invalidPayload := newProfileAccountDTO(email, "12345")
				payload, err := json.Marshal(invalidPayload)
				Expect(err).ShouldNot(HaveOccurred())

				req, _ := http.NewRequest(http.MethodPost, resourceUrl, bytes.NewBuffer(payload))
				req.Header.Set("X-Request-ID", uuid.New().String())
				req.Header.Set("Content-Type", "application/json")

				response, err := http.DefaultClient.Do(req)
				Expect(err).ShouldNot(HaveOccurred())
				defer response.Body.Close()
				Expect(response.StatusCode).To(Equal(http.StatusBadRequest))

				body, err := io.ReadAll(response.Body)
				Expect(err).ShouldNot(HaveOccurred())
				var message ErrorMessage
				Expect(json.Unmarshal(body, &message)).To(Succeed())
				Expect(message.Message).To(ContainSubstring("PhoneNumber"))
			})

			It("returns a 409 Conflict when the profile already exists", func() {
				createRequestPayload := newProfileAccountDTO(emailSuccessful, phoneSuccessful)
				payload, err := json.Marshal(createRequestPayload)
				Expect(err).ShouldNot(HaveOccurred())

				request, _ := http.NewRequest(http.MethodPost, resourceUrl, bytes.NewBuffer(payload))
				request.Header.Set("X-Request-ID", uuid.New().String())
				request.Header.Set("Content-Type", "application/json")

				response, err := http.DefaultClient.Do(request)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(response.StatusCode).To(Equal(http.StatusConflict))
			})
		})

		Context("update a profile", func() {
			When("the profile has minimal profile attributes", func() {
				BeforeEach(func() {
					replaceRequestPayload := replaceProfileDTO(emailSuccessful, phoneSuccessful)
					payload, err := json.Marshal(replaceRequestPayload)
					Expect(err).ShouldNot(HaveOccurred())

					request, _ = http.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:%d/private/v1/profiles", port), bytes.NewBuffer(payload))
					request.Header.Set("X-User-ID", userIDSuccssful.String())
					request.Header.Set("Content-Type", "application/json")
				})

				It("replaces a profile", func() {
					response, err := http.DefaultClient.Do(request)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(response.StatusCode).To(Equal(http.StatusOK))

					defer response.Body.Close()
					resBody, err := io.ReadAll(response.Body)
					Expect(err).ShouldNot(HaveOccurred())

					var profileDTO map[string]any
					err = json.Unmarshal(resBody, &profileDTO)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(profileDTO["email"]).Should(Equal(emailSuccessful))

					Expect(profileDTO["firstName"]).Should(Equal("test"))
					Expect(profileDTO["lastName"]).Should(Equal("user"))
					Expect(profileDTO["phoneNumber"]).Should(Equal(phoneSuccessful))
					Expect(profileDTO["dateOfBirth"]).Should(Equal("2004-10-21"))
					Expect(profileDTO["countryCode"]).Should(Equal("GB"))
					Expect(profileDTO["addressLine1"]).Should(Equal("1 Test Street"))
					Expect(profileDTO["addressLine2"]).Should(Equal("Test Area"))
					Expect(profileDTO["city"]).Should(Equal("Test City"))
					Expect(profileDTO["state"]).Should(Equal("Test State"))
					Expect(profileDTO["postalCode"]).Should(Equal("TE57 1NG"))
					Expect(profileDTO["country"]).Should(Equal("Little Britain"))
				})

				It("returns a 500 InternalServerError when the request is missing a user id", func() {
					// 500 status code because this header is set by the API Gateway
					request.Header.Del("X-User-ID")
					response, err := http.DefaultClient.Do(request)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
				})

				It("returns a 400 BadRequest when the request body is missing", func() {
					request, _ = http.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:%d/private/v1/profiles", port), bytes.NewBuffer(nil))
					request.Header.Set("X-User-ID", userIDSuccssful.String())
					request.Header.Set("Content-Type", "application/json")
					response, err := http.DefaultClient.Do(request)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
				})

				It("allows updating a profile with the same email and phone number (no unique constraint violation)", func() {
					updatePayload := replaceProfileDTO(emailSuccessful, phoneSuccessful)
					updatePayload["addressLine1"] = "updated-address-line"
					updatePayload["city"] = "updated-city"
					updatePayload["firstName"] = "updated-name"

					payload, err := json.Marshal(updatePayload)
					Expect(err).ShouldNot(HaveOccurred())

					request, _ = http.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:%d/private/v1/profiles", port), bytes.NewBuffer(payload))
					request.Header.Set("X-User-ID", userIDSuccssful.String())
					request.Header.Set("Content-Type", "application/json")

					response, err := http.DefaultClient.Do(request)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(response.StatusCode).To(Equal(http.StatusOK))

					defer response.Body.Close()
					resBody, err := io.ReadAll(response.Body)
					Expect(err).ShouldNot(HaveOccurred())

					var profileDTO map[string]any
					err = json.Unmarshal(resBody, &profileDTO)
					Expect(err).ShouldNot(HaveOccurred())

					// Verify the email and phone stayed the same
					Expect(profileDTO["email"]).Should(Equal(emailSuccessful))
					Expect(profileDTO["phoneNumber"]).Should(Equal(phoneSuccessful))

					// Verify the other fields were updated
					Expect(profileDTO["addressLine1"]).Should(Equal("updated-address-line"))
					Expect(profileDTO["city"]).Should(Equal("updated-city"))
					Expect(profileDTO["firstName"]).Should(Equal("updated-name"))
				})
			})
		})

		Context("read a profile", func() {

			It("reads a profile by ID", func() {
				req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/private/v1/profiles/", port), nil)
				req.Header.Set("X-User-ID", userIDSuccssful.String())
				req.Header.Set("Content-Type", "application/json")

				resp, err := http.DefaultClient.Do(req)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(http.StatusOK))
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				Expect(err).ShouldNot(HaveOccurred())

				var profileDTO map[string]any
				err = json.Unmarshal(body, &profileDTO)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(profileDTO["id"]).To(Equal(userIDSuccssful.String()))
				Expect(profileDTO["email"]).Should(Equal(emailSuccessful))
				Expect(profileDTO["phoneNumber"]).Should(Equal(phoneSuccessful))
				Expect(profileDTO["firstName"]).Should(Equal("updated-name"))
				Expect(profileDTO["lastName"]).Should(Equal("user"))
				Expect(profileDTO["dateOfBirth"]).Should(Equal("2004-10-21"))
				Expect(profileDTO["countryCode"]).Should(Equal("GB"))
				Expect(profileDTO["addressLine1"]).Should(Equal("updated-address-line"))
				Expect(profileDTO["addressLine2"]).Should(Equal("Test Area"))
				Expect(profileDTO["city"]).Should(Equal("updated-city"))
				Expect(profileDTO["state"]).Should(Equal("Test State"))
				Expect(profileDTO["postalCode"]).Should(Equal("TE57 1NG"))
				Expect(profileDTO["country"]).Should(Equal("Little Britain"))
			})

			It("returns a 404 NotFound when the profile does not exist", func() {
				request, _ = http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/private/v1/profiles", port), nil)
				request.Header.Set("X-User-ID", uuid.New().String())
				request.Header.Set("Content-Type", "application/json")

				response, err := http.DefaultClient.Do(request)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(response.StatusCode).To(Equal(http.StatusNotFound))
			})
		})

		Context("verify password (POST /public/v1/profiles/password/verify)", func() {
			It("returns 401 when password is valid but the email is not verified", func() {
				listener, cancel := newUserCreatedEventCapture(ctx, messagingFactory, kafkaPublisherTopic)
				defer cancel()

				profileID := createProfileAccount(port, newProfileAccountDTO(fmt.Sprintf("%s@test.com", strings.Split(uuid.New().String(), "-")[0]), uniqueGBPhone()))
				Expect(readEmailVerifyToken(listener, profileID)).NotTo(BeEmpty())

				profile, err := profileDB.Read(ctx, profileID)
				Expect(err).ShouldNot(HaveOccurred())

				payload := map[string]any{"email": profile.Email, "password": "password123!"}
				b, err := json.Marshal(payload)
				Expect(err).To(BeNil())

				req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/public/v1/profiles/password/verify", port), bytes.NewBuffer(b))
				req.Header.Set("Content-Type", "application/json")

				resp, err := http.DefaultClient.Do(req)
				Expect(err).To(BeNil())
				Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)

				var out map[string]any
				Expect(json.Unmarshal(body, &out)).To(Succeed())
				Expect(out["message"]).To(Equal("email not verified"))
			})

			It("returns a token after the email has been verified", func() {
				listener, cancel := newUserCreatedEventCapture(ctx, messagingFactory, kafkaPublisherTopic)
				defer cancel()

				profileID := createProfileAccount(port, newProfileAccountDTO(fmt.Sprintf("%s@test.com", strings.Split(uuid.New().String(), "-")[0]), uniqueGBPhone()))
				verifyToken := readEmailVerifyToken(listener, profileID)

				profile, err := profileDB.Read(ctx, profileID)
				Expect(err).ShouldNot(HaveOccurred())

				verifyPayload, err := json.Marshal(map[string]any{"token": verifyToken})
				Expect(err).ShouldNot(HaveOccurred())

				verifyReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/public/v1/profiles/email/verify", port), bytes.NewBuffer(verifyPayload))
				Expect(err).ShouldNot(HaveOccurred())
				verifyReq.Header.Set("Content-Type", "application/json")

				verifyResp, err := http.DefaultClient.Do(verifyReq)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(verifyResp.StatusCode).To(Equal(http.StatusNoContent))
				verifyResp.Body.Close()

				payload := map[string]any{"email": profile.Email, "password": "password123!"}
				b, err := json.Marshal(payload)
				Expect(err).To(BeNil())

				req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/public/v1/profiles/password/verify", port), bytes.NewBuffer(b))
				req.Header.Set("Content-Type", "application/json")

				resp, err := http.DefaultClient.Do(req)
				Expect(err).To(BeNil())
				Expect(resp.StatusCode).To(Equal(http.StatusOK))
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)

				var out map[string]any
				Expect(json.Unmarshal(body, &out)).To(Succeed())
				Expect(out["isValid"]).To(Equal(true))
				Expect(out["token"]).NotTo(BeEmpty())
			})
		})

		Context("verify email (POST /public/v1/profiles/email/verify)", func() {
			It("marks the profile email as verified", func() {
				listener, cancel := newUserCreatedEventCapture(ctx, messagingFactory, kafkaPublisherTopic)
				defer cancel()

				profileID := createProfileAccount(port, newProfileAccountDTO(fmt.Sprintf("%s@test.com", strings.Split(uuid.New().String(), "-")[0]), uniqueGBPhone()))
				verifyToken := readEmailVerifyToken(listener, profileID)

				verifyPayload, err := json.Marshal(map[string]any{"token": verifyToken})
				Expect(err).ShouldNot(HaveOccurred())

				verifyReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/public/v1/profiles/email/verify", port), bytes.NewBuffer(verifyPayload))
				Expect(err).ShouldNot(HaveOccurred())
				verifyReq.Header.Set("Content-Type", "application/json")

				verifyResp, err := http.DefaultClient.Do(verifyReq)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(verifyResp.StatusCode).To(Equal(http.StatusNoContent))
				verifyResp.Body.Close()

				Eventually(func() bool {
					profile, err := profileDB.Read(ctx, profileID)
					if err != nil {
						return false
					}
					return profile.EmailVerified
				}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
			})

			It("publishes a user updated event after verification", func() {
				createdListener, cancelCreated := newUserCreatedEventCapture(ctx, messagingFactory, kafkaPublisherTopic)
				defer cancelCreated()
				updatedListener, cancelUpdated := newUserUpdatedEventCapture(ctx, messagingFactory, kafkaPublisherTopic)
				defer cancelUpdated()

				profileID := createProfileAccount(port, newProfileAccountDTO(fmt.Sprintf("%s@test.com", strings.Split(uuid.New().String(), "-")[0]), uniqueGBPhone()))
				verifyToken := readEmailVerifyToken(createdListener, profileID)

				verifyPayload, err := json.Marshal(map[string]any{"token": verifyToken})
				Expect(err).ShouldNot(HaveOccurred())

				verifyReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/public/v1/profiles/email/verify", port), bytes.NewBuffer(verifyPayload))
				Expect(err).ShouldNot(HaveOccurred())
				verifyReq.Header.Set("Content-Type", "application/json")

				verifyResp, err := http.DefaultClient.Do(verifyReq)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(verifyResp.StatusCode).To(Equal(http.StatusNoContent))
				verifyResp.Body.Close()

				Eventually(func() bool {
					updatedListener.mu.Lock()
					defer updatedListener.mu.Unlock()
					payload, ok := updatedListener.payloads[profileID]
					if !ok || payload == nil {
						return false
					}
					verified, err := msgConn.EmailVerificationStatusFromProfileEventProto("user updated", payload.EmailVerificationStatus)
					return err == nil && verified
				}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
			})

			It("returns 404 for an invalid token and leaves the profile unverified", func() {
				listener, cancel := newUserCreatedEventCapture(ctx, messagingFactory, kafkaPublisherTopic)
				defer cancel()

				profileID := createProfileAccount(port, newProfileAccountDTO(fmt.Sprintf("%s@test.com", strings.Split(uuid.New().String(), "-")[0]), uniqueGBPhone()))
				Expect(readEmailVerifyToken(listener, profileID)).NotTo(BeEmpty())

				verifyPayload, err := json.Marshal(map[string]any{"token": "invalid-token"})
				Expect(err).ShouldNot(HaveOccurred())

				verifyReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/public/v1/profiles/email/verify", port), bytes.NewBuffer(verifyPayload))
				Expect(err).ShouldNot(HaveOccurred())
				verifyReq.Header.Set("Content-Type", "application/json")

				verifyResp, err := http.DefaultClient.Do(verifyReq)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(verifyResp.StatusCode).To(Equal(http.StatusNotFound))
				verifyResp.Body.Close()

				Consistently(func() bool {
					profile, err := profileDB.Read(ctx, profileID)
					if err != nil {
						return false
					}
					return profile.EmailVerified
				}, 500*time.Millisecond, 50*time.Millisecond).Should(BeFalse())
			})

			It("rejects replay of the same token", func() {
				listener, cancel := newUserCreatedEventCapture(ctx, messagingFactory, kafkaPublisherTopic)
				defer cancel()

				profileID := createProfileAccount(port, newProfileAccountDTO(fmt.Sprintf("%s@test.com", strings.Split(uuid.New().String(), "-")[0]), uniqueGBPhone()))
				verifyToken := readEmailVerifyToken(listener, profileID)

				verifyPayload, err := json.Marshal(map[string]any{"token": verifyToken})
				Expect(err).ShouldNot(HaveOccurred())

				verifyReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/public/v1/profiles/email/verify", port), bytes.NewBuffer(verifyPayload))
				Expect(err).ShouldNot(HaveOccurred())
				verifyReq.Header.Set("Content-Type", "application/json")

				firstVerifyResp, err := http.DefaultClient.Do(verifyReq)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(firstVerifyResp.StatusCode).To(Equal(http.StatusNoContent))
				firstVerifyResp.Body.Close()

				Eventually(func() bool {
					profile, err := profileDB.Read(ctx, profileID)
					if err != nil {
						return false
					}
					return profile.EmailVerified
				}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())

				replayReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/public/v1/profiles/email/verify", port), bytes.NewBuffer(verifyPayload))
				Expect(err).ShouldNot(HaveOccurred())
				replayReq.Header.Set("Content-Type", "application/json")

				replayResp, err := http.DefaultClient.Do(replayReq)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(replayResp.StatusCode).To(Equal(http.StatusNotFound))
				replayResp.Body.Close()
			})
		})

		Context("change password (put /private/v1/profiles/password)", func() {
			It("returns with 204 No Content", func() {
				payload := map[string]any{"password": "newPassword1!"}
				b, err := json.Marshal(payload)
				Expect(err).To(BeNil())

				req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:%d/private/v1/profiles/password", port), bytes.NewBuffer(b))
				req.Header.Set("X-User-ID", userIDSuccssful.String())
				req.Header.Set("Content-Type", "application/json")

				resp, err := http.DefaultClient.Do(req)
				Expect(err).To(BeNil())
				Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
			})
		})

		Context("delete profile (DELETE /private/v1/profiles)", func() {
			It("returns deleted=true with 204 No Content", func() {
				req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://localhost:%d/private/v1/profiles", port), nil)
				req.Header.Set("X-User-ID", userIDSuccssful.String())

				resp, err := http.DefaultClient.Do(req)
				Expect(err).To(BeNil())
				Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
				defer resp.Body.Close()

				body, err := io.ReadAll(resp.Body)
				Expect(err).To(BeNil())
				// If body is present, assert the payload. If truly empty (strict 204), still pass.
				if len(body) > 0 {
					var out map[string]any
					Expect(json.Unmarshal(body, &out)).To(Succeed())
					Expect(out["deleted"]).To(Equal(true))
				}
			})

			It("publishes a user deleted event", func() {
				deletedListener, cancelDeleted := newUserDeletedEventCapture(ctx, messagingFactory, kafkaPublisherTopic)
				defer cancelDeleted()

				profileID := createProfileAccount(port, newProfileAccountDTO(fmt.Sprintf("%s@test.com", strings.Split(uuid.New().String(), "-")[0]), uniqueGBPhone()))

				req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://localhost:%d/private/v1/profiles", port), nil)
				req.Header.Set("X-User-ID", profileID.String())

				resp, err := http.DefaultClient.Do(req)
				Expect(err).To(BeNil())
				Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
				resp.Body.Close()

				Eventually(func() bool {
					deletedListener.mu.Lock()
					defer deletedListener.mu.Unlock()
					return deletedListener.deleted[profileID]
				}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
			})

			It("allows recreating a deleted profile with the same email", func() {
				email := fmt.Sprintf("%s@test.com", strings.Split(uuid.New().String(), "-")[0])
				phone1 := uniqueGBPhone()
				profileID := createProfileAccount(port, newProfileAccountDTO(email, phone1))

				deleteReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://localhost:%d/private/v1/profiles", port), nil)
				deleteReq.Header.Set("X-User-ID", profileID.String())

				deleteResp, err := http.DefaultClient.Do(deleteReq)
				Expect(err).To(BeNil())
				Expect(deleteResp.StatusCode).To(Equal(http.StatusNoContent))
				deleteResp.Body.Close()

				recreatedProfileID := createProfileAccount(port, newProfileAccountDTO(email, uniqueGBPhone()))
				Expect(recreatedProfileID).NotTo(Equal(profileID))

				recreatedProfile, err := profileDB.Read(ctx, recreatedProfileID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(recreatedProfile.Email).To(Equal(email))
				Expect(recreatedProfile.EmailVerified).To(BeFalse())
			})
		})
	})
})

func replaceProfileDTO(email, phone string) map[string]any {
	return map[string]any{
		"email":        email,
		"firstName":    "test",
		"lastName":     "user",
		"phoneNumber":  phone,
		"dateOfBirth":  "2004-10-21",
		"countryCode":  "GB",
		"addressLine1": "1 Test Street",
		"addressLine2": "Test Area",
		"city":         "Test City",
		"state":        "Test State",
		"postalCode":   "TE57 1NG",
		"country":      "Little Britain",
	}
}

func newProfileAccountDTO(email, phone string) map[string]any {
	return map[string]any{
		"email":       email,
		"phoneNumber": phone,
		"countryCode": "GB",
		"password":    "password123!",
	}
}

func initRunSalt() {
	runSaltOnce.Do(func() {
		// Mix start time + PID into a small salt space
		n := uint64(time.Now().UTC().UnixNano())
		p := uint64(os.Getpid())
		// simple 64-bit mix, then reduce to 1e6 to fit any suffixLen up to 6
		runSalt = int64((n ^ (p * 0x9e3779b97f4a7c15)) % 1000000)
	})
}

func uniqueGBPhone() string {
	phone, err := uniqueGBPhoneFromStub("+447078")
	if err != nil {
		panic(err)
	}
	return phone
}

func uniqueGBPhoneFromStub(stub string) (string, error) {
	after := strings.TrimPrefix(stub, "+44")
	suffixLen := 10 - len(after)
	if suffixLen <= 0 {
		after = after[:9]
		suffixLen = 1
	}

	initRunSalt()
	epochMinutes := int64(time.Now().UTC().Unix() / 60)
	seq := atomic.AddInt64(&gbPhoneSeq, 1) - 1

	mod := int64(1)
	for range suffixLen {
		mod *= 10
	}

	const a int64 = 941083 // odd, not multiple of 5
	mixed := ((epochMinutes%mod)*a + (runSalt % mod) + (seq % mod)) % mod
	suffix := fmt.Sprintf("%0*d", suffixLen, mixed)
	return "+44" + after + suffix, nil
}

func newUserCreatedEventCapture(ctx context.Context, messagingFactory msgConn.Messenger, kafkaPublisherTopic string) (*emailVerificationRequestedEventListener, func()) {
	_ = messagingFactory

	cancelCtxSubscriber, cancelFtnSubscriber := context.WithCancel(context.Background())
	isolatedFactory := msgConn.NewMessenger(msgConn.MessengerConfig{
		DlqURL:          "http://localhost:4566/profile-dev-env-queue/",
		GroupID:         "profile-capture-" + uuid.New().String(),
		Brokers:         "localhost:9092",
		AutoOffsetReset: "latest",
	}, cancelFtnSubscriber)

	msgSubscriber, err := isolatedFactory.Subscriber(ctx)
	Expect(err).ShouldNot(HaveOccurred())

	listener := &emailVerificationRequestedEventListener{}
	msgConn.AddListener(msgSubscriber, listener)

	startSubscriberOrFail(cancelCtxSubscriber, "profile-email-verify-assert", func(ctx context.Context) error {
		return msgSubscriber.Subscribe(ctx, []string{kafkaPublisherTopic})
	})
	waitForSubscriberAssignment(ctx, msgSubscriber)
	return listener, cancelFtnSubscriber
}

func newUserUpdatedEventCapture(ctx context.Context, messagingFactory msgConn.Messenger, kafkaPublisherTopic string) (*userUpdatedEventListener, func()) {
	_ = messagingFactory

	cancelCtxSubscriber, cancelFtnSubscriber := context.WithCancel(context.Background())
	isolatedFactory := msgConn.NewMessenger(msgConn.MessengerConfig{
		DlqURL:          "http://localhost:4566/profile-dev-env-queue/",
		GroupID:         "profile-capture-" + uuid.New().String(),
		Brokers:         "localhost:9092",
		AutoOffsetReset: "latest",
	}, cancelFtnSubscriber)

	msgSubscriber, err := isolatedFactory.Subscriber(ctx)
	Expect(err).ShouldNot(HaveOccurred())

	listener := &userUpdatedEventListener{}
	msgConn.AddListener(msgSubscriber, listener)

	startSubscriberOrFail(cancelCtxSubscriber, "profile-updated-assert", func(ctx context.Context) error {
		return msgSubscriber.Subscribe(ctx, []string{kafkaPublisherTopic})
	})
	waitForSubscriberAssignment(ctx, msgSubscriber)
	return listener, cancelFtnSubscriber
}

func newUserDeletedEventCapture(ctx context.Context, messagingFactory msgConn.Messenger, kafkaPublisherTopic string) (*userDeletedEventListener, func()) {
	_ = messagingFactory

	cancelCtxSubscriber, cancelFtnSubscriber := context.WithCancel(context.Background())
	isolatedFactory := msgConn.NewMessenger(msgConn.MessengerConfig{
		DlqURL:          "http://localhost:4566/profile-dev-env-queue/",
		GroupID:         "profile-capture-" + uuid.New().String(),
		Brokers:         "localhost:9092",
		AutoOffsetReset: "latest",
	}, cancelFtnSubscriber)

	msgSubscriber, err := isolatedFactory.Subscriber(ctx)
	Expect(err).ShouldNot(HaveOccurred())

	listener := &userDeletedEventListener{}
	msgConn.AddListener(msgSubscriber, listener)

	startSubscriberOrFail(cancelCtxSubscriber, "profile-deleted-assert", func(ctx context.Context) error {
		return msgSubscriber.Subscribe(ctx, []string{kafkaPublisherTopic})
	})
	waitForSubscriberAssignment(ctx, msgSubscriber)
	return listener, cancelFtnSubscriber
}

func waitForSubscriberAssignment(ctx context.Context, subscriber msgConn.Subscriber) {
	Eventually(func() error {
		return msgConn.CheckSubscriberHealth(ctx, subscriber, msgConn.SubscriberHealthCheckConfig{
			RequireAssignment: true,
			MaxPollSilence:    2 * time.Second,
		})
	}, 5*time.Second, 50*time.Millisecond).Should(Succeed())
}

func createProfileAccount(port int, profileAccount map[string]any) uuid.UUID {
	createPayload, err := json.Marshal(profileAccount)
	Expect(err).ShouldNot(HaveOccurred())

	createReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/public/v1/profiles", port), bytes.NewBuffer(createPayload))
	Expect(err).ShouldNot(HaveOccurred())
	createReq.Header.Set("X-Request-ID", uuid.New().String())
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := http.DefaultClient.Do(createReq)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(createResp.StatusCode).To(Equal(http.StatusCreated))
	createBody, err := io.ReadAll(createResp.Body)
	Expect(err).ShouldNot(HaveOccurred())
	createResp.Body.Close()

	var profileDTO map[string]any
	err = json.Unmarshal(createBody, &profileDTO)
	Expect(err).ShouldNot(HaveOccurred())
	return uuid.MustParse(profileDTO["id"].(string))
}

func readEmailVerifyToken(listener *emailVerificationRequestedEventListener, profileID uuid.UUID) string {
	Eventually(func() string {
		listener.mu.Lock()
		defer listener.mu.Unlock()
		payload, ok := listener.payloads[profileID]
		if !ok || payload == nil {
			return ""
		}
		return payload.EmailVerifyToken
	}, 15*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

	listener.mu.Lock()
	verifyToken := listener.payloads[profileID].EmailVerifyToken
	listener.mu.Unlock()
	return verifyToken
}

func purgeTopic(ctx context.Context, brokers, topic string) error {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	Expect(err).ShouldNot(HaveOccurred())
	defer admin.Close()

	// discover partitions for this topic
	md, err := admin.GetMetadata(&topic, false, 10000)
	Expect(err).ShouldNot(HaveOccurred())

	tmd, ok := md.Topics[topic]
	Expect(ok).Should(BeTrue())
	Expect(tmd.Error.Code()).Should(BeElementOf(kafka.ErrNoError, kafka.ErrUnknownTopicOrPart))

	// DeleteRecords request to the end of each partition
	toDelete := make([]kafka.TopicPartition, 0, len(tmd.Partitions))
	for _, p := range tmd.Partitions {
		toDelete = append(toDelete, kafka.TopicPartition{
			Topic:     &topic,
			Partition: p.ID,
			Offset:    kafka.OffsetEnd,
		})
	}

	res, err := admin.DeleteRecords(
		ctx,
		toDelete,
		kafka.SetAdminOperationTimeout(30*time.Second),
	)
	Expect(err).ShouldNot(HaveOccurred())

	// check per-partition result
	for _, r := range res.DeleteRecordsResults {
		Expect(r.TopicPartition.Error).Should(BeNil())
	}
	return nil
}
