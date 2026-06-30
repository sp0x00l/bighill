package messaging_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	"lib/shared_lib/messaging"
)

// inMemoryOutbox simulates DynamoDB for integration testing
type inMemoryOutbox struct {
	mu       sync.Mutex
	items    map[string]map[string]types.AttributeValue
	claimErr error
}

func newInMemoryOutbox() *inMemoryOutbox {
	return &inMemoryOutbox{
		items: make(map[string]map[string]types.AttributeValue),
	}
}

func (m *inMemoryOutbox) PutItem(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk := params.Item["pk"].(*types.AttributeValueMemberS).Value
	sk := params.Item["sk"].(*types.AttributeValueMemberS).Value
	key := pk + "|" + sk

	// Deep copy item
	item := make(map[string]types.AttributeValue)
	for k, v := range params.Item {
		item[k] = v
	}
	m.items[key] = item
	return &dynamodb.PutItemOutput{}, nil
}

func (m *inMemoryOutbox) Query(_ context.Context, params *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	gsiPkFilter := params.ExpressionAttributeValues[":pending"]
	if gsiPkFilter == nil {
		gsiPkFilter = params.ExpressionAttributeValues[":processing"]
	}
	targetStatus := gsiPkFilter.(*types.AttributeValueMemberS).Value

	var results []map[string]types.AttributeValue
	for _, item := range m.items {
		gsiPk, ok := item["gsi_pk"].(*types.AttributeValueMemberS)
		if !ok {
			continue
		}
		if gsiPk.Value == targetStatus {
			// Deep copy to avoid concurrent map access
			itemCopy := make(map[string]types.AttributeValue)
			for k, v := range item {
				itemCopy[k] = v
			}
			results = append(results, itemCopy)
		}
	}
	return &dynamodb.QueryOutput{Items: results}, nil
}

func (m *inMemoryOutbox) UpdateItem(_ context.Context, params *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.claimErr != nil {
		return nil, m.claimErr
	}

	pk := params.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := params.Key["sk"].(*types.AttributeValueMemberS).Value
	key := pk + "|" + sk

	item, exists := m.items[key]
	if !exists {
		return nil, &types.ConditionalCheckFailedException{}
	}

	currentStatus, _ := item["status"].(*types.AttributeValueMemberS)

	// Simulate conditional check for claiming PENDING->PROCESSING
	if _, ok := params.ExpressionAttributeValues[":new_processing"]; ok {
		nowStr := params.ExpressionAttributeValues[":now"].(*types.AttributeValueMemberS).Value

		// If currently PROCESSING, only allow if lease expired
		if currentStatus != nil && currentStatus.Value == "PROCESSING" {
			leaseExp, hasLease := item["lease_expires_at"].(*types.AttributeValueMemberS)
			if !hasLease || leaseExp.Value > nowStr {
				return nil, &types.ConditionalCheckFailedException{}
			}
		}
		// If not PENDING and not expired PROCESSING, reject
		if currentStatus != nil && currentStatus.Value != "PENDING" && currentStatus.Value != "PROCESSING" {
			return nil, &types.ConditionalCheckFailedException{}
		}
		// If PENDING, check next_attempt_at
		if currentStatus != nil && currentStatus.Value == "PENDING" {
			nextAttempt, hasNext := item["next_attempt_at"].(*types.AttributeValueMemberS)
			if hasNext && nextAttempt.Value > nowStr {
				return nil, &types.ConditionalCheckFailedException{}
			}
		}

		item["status"] = params.ExpressionAttributeValues[":new_processing"]
		item["gsi_pk"] = params.ExpressionAttributeValues[":new_processing"]
		if leaseExp, ok := params.ExpressionAttributeValues[":lease_expires_at"]; ok {
			item["gsi_sk"] = leaseExp
			item["lease_expires_at"] = leaseExp
		}
		if claimToken, ok := params.ExpressionAttributeValues[":claim_token"]; ok {
			item["claim_token"] = claimToken
		}
		if owner, ok := params.ExpressionAttributeValues[":owner"]; ok {
			item["processing_owner"] = owner
		}
	} else if newStatus, ok := params.ExpressionAttributeValues[":sent"]; ok {
		// Mark as SENT - verify claim token matches
		if expectedToken, ok := params.ExpressionAttributeValues[":claim_token"]; ok {
			actualToken, hasToken := item["claim_token"].(*types.AttributeValueMemberS)
			if !hasToken || actualToken.Value != expectedToken.(*types.AttributeValueMemberS).Value {
				return nil, &types.ConditionalCheckFailedException{}
			}
		}
		item["status"] = newStatus
		delete(item, "gsi_pk")
		delete(item, "gsi_sk")
		if ttl, ok := params.ExpressionAttributeValues[":ttl"]; ok {
			item["ttl"] = ttl
		}
	} else if pendingStatus, ok := params.ExpressionAttributeValues[":pending"]; ok {
		// Mark as PENDING (retry)
		item["status"] = pendingStatus
		item["gsi_pk"] = pendingStatus
		if nextAttempt, ok := params.ExpressionAttributeValues[":next_attempt_at"]; ok {
			item["gsi_sk"] = nextAttempt
			item["next_attempt_at"] = nextAttempt
		}
	}

	m.items[key] = item
	return &dynamodb.UpdateItemOutput{}, nil
}

// mockPublisherForIntegration tracks published messages
type mockPublisherForIntegration struct {
	mu        sync.Mutex
	published []messaging.OutboxPendingMessage
	failCount int32
	failUntil int32
}

func (m *mockPublisherForIntegration) Publish(_ context.Context, _ string, _ messaging.Message, _ proto.Message) error {
	return nil
}

func (m *mockPublisherForIntegration) PublishOutboxMessage(_ context.Context, topic string, payload []byte, headers []kafka.Header) error {
	current := atomic.AddInt32(&m.failCount, 1)
	if current <= m.failUntil {
		return errors.New("simulated-publish-failure")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, messaging.OutboxPendingMessage{
		Topic:   topic,
		Payload: payload,
		Headers: headers,
	})
	return nil
}

var _ = Describe("Outbox Integration", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		db        *inMemoryOutbox
		outbox    messaging.OutboxWriter
		publisher *mockPublisherForIntegration
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		db = newInMemoryOutbox()
		outbox = messaging.NewTestDynamoOutbox(db, "test-outbox")
		publisher = &mockPublisherForIntegration{}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Full flow: write -> relay -> publish -> mark sent", func() {
		It("successfully publishes and marks sent", func() {
			// Write message to outbox
			msg := messaging.OutboxMessage{
				Topic: "profile",
				Message: messaging.Message{
					MsgType:     messaging.MsgTypeUserCreated,
					ResourceKey: uuid.New(),
				},
				Payload: []byte("test-payload"),
			}
			err := outbox.WriteMessage(ctx, msg)
			Expect(err).ToNot(HaveOccurred())

			// Create relay
			relayOutbox := outbox.(messaging.RelayOutbox)
			relay := messaging.NewOutboxRelay(relayOutbox, publisher, messaging.OutboxRelayConfig{
				PollInterval:   10 * time.Millisecond,
				FailureBackoff: 10 * time.Millisecond,
				BatchSize:      10,
			})

			// Run relay briefly
			go func() {
				time.Sleep(50 * time.Millisecond)
				cancel()
			}()
			_ = relay.Run(ctx)

			// Verify message was published
			Expect(publisher.published).To(HaveLen(1))
			Expect(publisher.published[0].Topic).To(Equal("profile"))
			Expect(publisher.published[0].Payload).To(Equal([]byte("test-payload")))

			// Verify item is marked SENT with TTL
			db.mu.Lock()
			var sentItem map[string]types.AttributeValue
			for _, item := range db.items {
				if status, ok := item["status"].(*types.AttributeValueMemberS); ok && status.Value == "SENT" {
					sentItem = item
					break
				}
			}
			db.mu.Unlock()

			Expect(sentItem).ToNot(BeNil())
			_, hasTTL := sentItem["ttl"]
			Expect(hasTTL).To(BeTrue())
			_, hasGsiPk := sentItem["gsi_pk"]
			Expect(hasGsiPk).To(BeFalse())
		})
	})

	Describe("Retry on publish failure", func() {
		It("retries failed publishes with backoff", func() {
			publisher.failUntil = 1 // Fail first attempt

			msg := messaging.OutboxMessage{
				Topic: "profile",
				Message: messaging.Message{
					MsgType:     messaging.MsgTypeUserCreated,
					ResourceKey: uuid.New(),
				},
				Payload: []byte("retry-payload"),
			}
			err := outbox.WriteMessage(ctx, msg)
			Expect(err).ToNot(HaveOccurred())

			relayOutbox := outbox.(messaging.RelayOutbox)
			relay := messaging.NewOutboxRelay(relayOutbox, publisher, messaging.OutboxRelayConfig{
				PollInterval:   10 * time.Millisecond,
				FailureBackoff: 5 * time.Millisecond,
				BatchSize:      10,
			})

			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			_ = relay.Run(ctx)

			// Eventually succeeds after retry
			Expect(publisher.published).To(HaveLen(1))
		})
	})

	Describe("Concurrent relay instances", func() {
		It("only one relay claims each message", func() {
			// Write multiple messages
			for i := 0; i < 5; i++ {
				msg := messaging.OutboxMessage{
					Topic: "profile",
					Message: messaging.Message{
						MsgType:     messaging.MsgTypeUserCreated,
						ResourceKey: uuid.New(),
					},
					Payload: []byte("concurrent-" + string(rune('0'+i))),
				}
				err := outbox.WriteMessage(ctx, msg)
				Expect(err).ToNot(HaveOccurred())
			}

			relayOutbox := outbox.(messaging.RelayOutbox)

			var wg sync.WaitGroup
			publishers := make([]*mockPublisherForIntegration, 3)
			for i := 0; i < 3; i++ {
				publishers[i] = &mockPublisherForIntegration{}
				relay := messaging.NewOutboxRelay(relayOutbox, publishers[i], messaging.OutboxRelayConfig{
					PollInterval:   5 * time.Millisecond,
					FailureBackoff: 5 * time.Millisecond,
					BatchSize:      10,
					InstanceID:     uuid.NewString(),
				})

				wg.Add(1)
				go func(r *messaging.OutboxRelay) {
					defer wg.Done()
					_ = r.Run(ctx)
				}(relay)
			}

			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			wg.Wait()

			// Count total published - should be exactly 5 (no duplicates)
			totalPublished := 0
			for _, p := range publishers {
				totalPublished += len(p.published)
			}
			Expect(totalPublished).To(Equal(5))
		})
	})

	Describe("Lease expiration recovery", func() {
		It("recovers messages from dead relay instances", func() {
			msg := messaging.OutboxMessage{
				Topic: "profile",
				Message: messaging.Message{
					MsgType:     messaging.MsgTypeUserCreated,
					ResourceKey: uuid.New(),
				},
				Payload: []byte("orphaned-payload"),
			}
			err := outbox.WriteMessage(ctx, msg)
			Expect(err).ToNot(HaveOccurred())

			relayOutbox := outbox.(messaging.RelayOutbox)

			// First relay claims but "dies" (simulate by making claim fail after first)
			deadPublisher := &mockPublisherForIntegration{failUntil: 1000}
			deadRelay := messaging.NewOutboxRelay(relayOutbox, deadPublisher, messaging.OutboxRelayConfig{
				PollInterval:   5 * time.Millisecond,
				FailureBackoff: 5 * time.Millisecond,
				BatchSize:      10,
				InstanceID:     "dead-relay",
				LeaseDuration:  20 * time.Millisecond,
			})

			deadCtx, deadCancel := context.WithCancel(ctx)
			go func() {
				_ = deadRelay.Run(deadCtx)
			}()

			// Let dead relay claim message, then kill it
			time.Sleep(30 * time.Millisecond)
			deadCancel()

			// Manually expire the lease by updating gsi_sk to past time
			db.mu.Lock()
			for _, item := range db.items {
				if status, ok := item["status"].(*types.AttributeValueMemberS); ok && status.Value == "PROCESSING" {
					item["gsi_sk"] = &types.AttributeValueMemberS{Value: "2020-01-01T00:00:00Z"}
					item["lease_expires_at"] = &types.AttributeValueMemberS{Value: "2020-01-01T00:00:00Z"}
				}
			}
			db.mu.Unlock()

			// New relay picks it up
			livePublisher := &mockPublisherForIntegration{}
			liveRelay := messaging.NewOutboxRelay(relayOutbox, livePublisher, messaging.OutboxRelayConfig{
				PollInterval:   5 * time.Millisecond,
				FailureBackoff: 5 * time.Millisecond,
				BatchSize:      10,
				InstanceID:     "live-relay",
			})

			liveCtx, liveCancel := context.WithCancel(ctx)
			go func() {
				time.Sleep(50 * time.Millisecond)
				liveCancel()
			}()
			_ = liveRelay.Run(liveCtx)

			Expect(livePublisher.published).To(HaveLen(1))
			Expect(livePublisher.published[0].Payload).To(Equal([]byte("orphaned-payload")))
		})
	})
})
