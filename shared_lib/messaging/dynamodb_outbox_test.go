package messaging_test

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"lib/shared_lib/messaging"
)

type mockDynamoDBClient struct {
	input           *dynamodb.PutItemInput
	updateInput     *dynamodb.UpdateItemInput
	queryInputs     []*dynamodb.QueryInput
	pendingItems    []map[string]types.AttributeValue
	processingItems []map[string]types.AttributeValue
	err             error
	queryErr        error
	updateErr       error
}

func (m *mockDynamoDBClient) PutItem(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.input = params
	if m.err != nil {
		return nil, m.err
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamoDBClient) Query(_ context.Context, params *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	m.queryInputs = append(m.queryInputs, params)
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	// Return items based on which status is being queried
	if _, ok := params.ExpressionAttributeValues[":pending"]; ok {
		return &dynamodb.QueryOutput{Items: m.pendingItems}, nil
	}
	if _, ok := params.ExpressionAttributeValues[":processing"]; ok {
		return &dynamodb.QueryOutput{Items: m.processingItems}, nil
	}
	return &dynamodb.QueryOutput{}, nil
}

func (m *mockDynamoDBClient) UpdateItem(_ context.Context, params *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.updateInput = params
	if m.updateErr != nil {
		return nil, m.updateErr
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

var _ = Describe("DynamoOutbox", func() {
	var (
		ctx    context.Context
		client *mockDynamoDBClient
		outbox messaging.OutboxWriter
	)

	BeforeEach(func() {
		ctx = context.Background()
		client = &mockDynamoDBClient{}
		outbox = messaging.NewTestDynamoOutbox(client, "ml-ops-outbox")
	})

	It("writes an outbox item to dynamodb", func() {
		msg := messaging.OutboxMessage{
			Topic: "profile",
			Message: messaging.Message{
				MsgType:     messaging.MsgTypeUserCreated,
				ResourceKey: uuid.New(),
			},
			Payload: []byte("payload"),
			Headers: nil,
		}

		err := outbox.WriteMessage(ctx, msg)
		Expect(err).ToNot(HaveOccurred())
		Expect(client.input).ToNot(BeNil())
		Expect(*client.input.TableName).To(Equal("ml-ops-outbox"))

		_, ok := client.input.Item["pk"].(*types.AttributeValueMemberS)
		Expect(ok).To(BeTrue())
		_, ok = client.input.Item["sk"].(*types.AttributeValueMemberS)
		Expect(ok).To(BeTrue())
		_, ok = client.input.Item["payload"].(*types.AttributeValueMemberB)
		Expect(ok).To(BeTrue())

		// Verify GSI attributes for scalable queries
		gsiPk, ok := client.input.Item["gsi_pk"].(*types.AttributeValueMemberS)
		Expect(ok).To(BeTrue())
		Expect(gsiPk.Value).To(Equal("PENDING"))

		_, ok = client.input.Item["gsi_sk"].(*types.AttributeValueMemberS)
		Expect(ok).To(BeTrue())
	})

	It("enqueues outbound messages as full ML envelopes", func() {
		message := messaging.Message{
			MsgType:     messaging.MsgTypeUserCreated,
			ResourceKey: uuid.New(),
		}
		_, err := message.Serialize(ctx, wrapperspb.String("envelope-payload"))
		Expect(err).ToNot(HaveOccurred())

		enqueueOutbox, ok := outbox.(messaging.Outbox)
		Expect(ok).To(BeTrue())
		err = enqueueOutbox.Enqueue(ctx, messaging.OutboundMessage{
			Topic:       "profile",
			Message:     message,
			DispatchKey: "test-dispatch-key",
		})
		Expect(err).ToNot(HaveOccurred())

		payload, ok := client.input.Item["payload"].(*types.AttributeValueMemberB)
		Expect(ok).To(BeTrue())
		var envelope messaging.Message
		Expect(envelope.Deserialize(ctx, payload.Value)).To(Succeed())
		var body wrapperspb.StringValue
		Expect(envelope.DeserializePayload(&body)).To(Succeed())
		Expect(body.Value).To(Equal("envelope-payload"))
	})

	It("returns an error when put item fails", func() {
		client.err = errors.New("dynamodb-put-failed")

		err := outbox.WriteMessage(ctx, messaging.OutboxMessage{
			Topic: "profile",
			Message: messaging.Message{
				MsgType:     messaging.MsgTypeUserCreated,
				ResourceKey: uuid.New(),
			},
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("dynamodb-put-failed"))
	})

	It("reads pending messages using GSI query", func() {
		client.pendingItems = []map[string]types.AttributeValue{
			{
				"pk":      &types.AttributeValueMemberS{Value: "RESOURCE#abc"},
				"sk":      &types.AttributeValueMemberS{Value: "EVENT#2025-01-01T00:00:00Z#123"},
				"topic":   &types.AttributeValueMemberS{Value: "profile"},
				"payload": &types.AttributeValueMemberB{Value: []byte("test-payload")},
				"status":  &types.AttributeValueMemberS{Value: "PENDING"},
			},
		}

		relayOutbox, ok := outbox.(messaging.RelayOutbox)
		Expect(ok).To(BeTrue())

		pending, err := relayOutbox.ReadPending(ctx, 10)
		Expect(err).ToNot(HaveOccurred())
		Expect(pending).To(HaveLen(1))
		Expect(pending[0].Topic).To(Equal("profile"))
		Expect(pending[0].Payload).To(Equal([]byte("test-payload")))

		// Verify Query was used instead of Scan
		Expect(client.queryInputs).To(HaveLen(2)) // PENDING + PROCESSING queries
		Expect(*client.queryInputs[0].IndexName).To(Equal("gsi_status_next_attempt"))
	})

	It("marks sent with TTL attribute", func() {
		relayOutbox, ok := outbox.(messaging.RelayOutbox)
		Expect(ok).To(BeTrue())

		pending := messaging.OutboxPendingMessage{
			PK:              "RESOURCE#abc",
			SK:              "EVENT#2025-01-01T00:00:00Z#123",
			ClaimToken:      "claim-123",
			ProcessingOwner: "owner-1",
		}

		err := relayOutbox.MarkSent(ctx, pending)
		Expect(err).ToNot(HaveOccurred())
		Expect(client.updateInput).ToNot(BeNil())

		// Verify TTL is set and GSI attributes are removed
		Expect(*client.updateInput.UpdateExpression).To(ContainSubstring("#ttl = :ttl"))
		Expect(*client.updateInput.UpdateExpression).To(ContainSubstring("REMOVE gsi_pk, gsi_sk"))
	})

	It("marks failed with updated GSI sort key", func() {
		relayOutbox, ok := outbox.(messaging.RelayOutbox)
		Expect(ok).To(BeTrue())

		pending := messaging.OutboxPendingMessage{
			PK:              "RESOURCE#abc",
			SK:              "EVENT#2025-01-01T00:00:00Z#123",
			ClaimToken:      "claim-123",
			ProcessingOwner: "owner-1",
			Attempts:        1,
		}

		nextAttempt := time.Now().Add(5 * time.Second)
		err := relayOutbox.MarkFailed(ctx, pending, "publish-error", nextAttempt)
		Expect(err).ToNot(HaveOccurred())
		Expect(client.updateInput).ToNot(BeNil())

		// Verify GSI is updated for retry scheduling
		Expect(*client.updateInput.UpdateExpression).To(ContainSubstring("gsi_pk = :pending"))
		Expect(*client.updateInput.UpdateExpression).To(ContainSubstring("gsi_sk = :next_attempt_at"))
	})

	It("recovers expired leases via PROCESSING GSI query", func() {
		// Simulate a message stuck in PROCESSING with expired lease
		client.processingItems = []map[string]types.AttributeValue{
			{
				"pk":               &types.AttributeValueMemberS{Value: "RESOURCE#expired"},
				"sk":               &types.AttributeValueMemberS{Value: "EVENT#2025-01-01T00:00:00Z#456"},
				"topic":            &types.AttributeValueMemberS{Value: "profile"},
				"payload":          &types.AttributeValueMemberB{Value: []byte("stuck-payload")},
				"status":           &types.AttributeValueMemberS{Value: "PROCESSING"},
				"lease_expires_at": &types.AttributeValueMemberS{Value: "2020-01-01T00:00:00Z"},
			},
		}

		relayOutbox, ok := outbox.(messaging.RelayOutbox)
		Expect(ok).To(BeTrue())

		pending, err := relayOutbox.ReadPending(ctx, 10)
		Expect(err).ToNot(HaveOccurred())
		Expect(pending).To(HaveLen(1))
		Expect(pending[0].PK).To(Equal("RESOURCE#expired"))
	})

	It("returns error when query fails", func() {
		client.queryErr = errors.New("dynamodb-query-failed")

		relayOutbox, ok := outbox.(messaging.RelayOutbox)
		Expect(ok).To(BeTrue())

		_, err := relayOutbox.ReadPending(ctx, 10)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("dynamodb-query-failed"))
	})
})
