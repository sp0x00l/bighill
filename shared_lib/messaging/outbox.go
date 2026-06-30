package messaging

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"

	"lib/shared_lib/idem"
	metrics "lib/shared_lib/metrics"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

type OutboxMessage struct {
	Topic   string
	Message Message
	Payload []byte
	Headers []kafka.Header
}

type OutboxWriter interface {
	WriteMessage(ctx context.Context, message OutboxMessage) error
}

type RelayOutbox interface {
	ReadPending(ctx context.Context, maxMessages int32) ([]OutboxPendingMessage, error)
	MarkSent(ctx context.Context, pending OutboxPendingMessage) error
	MarkFailed(ctx context.Context, pending OutboxPendingMessage, lastError string, nextAttemptAt time.Time) error
}

type OutboxPendingMessage struct {
	PK              string
	SK              string
	Topic           string
	Payload         []byte
	Headers         []kafka.Header
	Attempts        int
	ProcessingOwner string
	ClaimToken      string
}

type DynamoDBAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

type noopOutbox struct{}

type dynamoOutbox struct {
	DynamoDB      DynamoDBAPI
	TableName     string
	GSIName       string
	SentTTLDays   int
	mu            sync.RWMutex
	RelayOwnerID  string
	RelayLeaseTTL time.Duration
}

var _ Outbox = (*noopOutbox)(nil)
var _ Outbox = (*dynamoOutbox)(nil)
var _ OutboxWriter = (*noopOutbox)(nil)
var _ OutboxWriter = (*dynamoOutbox)(nil)

func (d *dynamoOutbox) ConfigureRelayIdentity(ownerID string, leaseDuration time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if ownerID != "" {
		d.RelayOwnerID = ownerID
	}
	if leaseDuration > 0 {
		d.RelayLeaseTTL = leaseDuration
	}
}

func NewOutbox(ctx context.Context, outboxURL string) OutboxWriter {
	log.Trace("NewOutbox")

	if outboxURL == "" {
		return NewNoopOutbox()
	}

	if strings.HasPrefix(outboxURL, "noop://") {
		return NewNoopOutbox()
	}

	if strings.HasPrefix(outboxURL, "dynamodb://") {
		tableName := strings.Trim(strings.TrimPrefix(outboxURL, "dynamodb://"), "/")
		if tableName == "" {
			log.WithContext(ctx).Warn("empty dynamodb table name for outbox; using NoopOutbox")
			return NewNoopOutbox()
		}
		outbox, err := NewDynamoOutbox(ctx, tableName)
		if err != nil {
			log.WithContext(ctx).WithError(err).Warn("failed to initialize dynamodb outbox; using NoopOutbox")
			return NewNoopOutbox()
		}
		return outbox
	}

	log.WithContext(ctx).Warnf("unsupported outbox backend %q; using NoopOutbox", outboxURL)
	return NewNoopOutbox()
}

func isNoopOutbox(outbox OutboxWriter) bool {
	switch o := outbox.(type) {
	case *noopOutbox:
		return true
	case *signalOutbox:
		return isNoopOutbox(o.base)
	default:
		return false
	}
}

func NewNoopOutbox() OutboxWriter {
	log.Trace("NewNoopOutbox")
	return &noopOutbox{}
}

func NewDynamoOutbox(ctx context.Context, tableName string) (OutboxWriter, error) {
	log.Trace("NewDynamoOutbox")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return &dynamoOutbox{
		DynamoDB:      dynamodb.NewFromConfig(cfg),
		TableName:     tableName,
		GSIName:       "gsi_status_next_attempt",
		SentTTLDays:   7,
		RelayLeaseTTL: 30 * time.Second,
	}, nil
}

func NewTestDynamoOutbox(client DynamoDBAPI, tableName string) OutboxWriter {
	log.Trace("NewTestDynamoOutbox")
	return &dynamoOutbox{
		DynamoDB:      client,
		TableName:     tableName,
		GSIName:       "gsi_status_next_attempt",
		SentTTLDays:   7,
		RelayLeaseTTL: 30 * time.Second,
	}
}

func (n *noopOutbox) WriteMessage(_ context.Context, _ OutboxMessage) error {
	log.Trace("NoopOutbox WriteMessage")
	return nil
}

func (n *noopOutbox) Enqueue(_ context.Context, _ OutboundMessage) error {
	log.Trace("NoopOutbox Enqueue")
	return nil
}

func (n *noopOutbox) ReadPending(_ context.Context, _ int32) ([]OutboxPendingMessage, error) {
	log.Trace("NoopOutbox ReadPending")
	return []OutboxPendingMessage{}, nil
}

func (n *noopOutbox) MarkSent(_ context.Context, _ OutboxPendingMessage) error {
	log.Trace("NoopOutbox MarkSent")
	return nil
}

func (n *noopOutbox) MarkFailed(_ context.Context, _ OutboxPendingMessage, _ string, _ time.Time) error {
	log.Trace("NoopOutbox MarkFailed")
	return nil
}

func (d *dynamoOutbox) WriteMessage(ctx context.Context, message OutboxMessage) error {
	log.Trace("DynamoOutbox WriteMessage")

	payload := message.Payload
	if payload == nil {
		payload = []byte{}
	}

	now := time.Now().UTC()
	createdAt := now.Format(time.RFC3339Nano)
	eventID := deriveOutboxEventID(message.Topic, message.Message, payload, createdAt)

	item := map[string]types.AttributeValue{
		"pk":              &types.AttributeValueMemberS{Value: fmt.Sprintf("RESOURCE#%s", message.Message.ResourceKey.String())},
		"sk":              &types.AttributeValueMemberS{Value: fmt.Sprintf("EVENT#%s#%s", createdAt, eventID)},
		"event_id":        &types.AttributeValueMemberS{Value: eventID},
		"topic":           &types.AttributeValueMemberS{Value: message.Topic},
		"event_type":      &types.AttributeValueMemberS{Value: message.Message.MsgType.String()},
		"resource_key":    &types.AttributeValueMemberS{Value: message.Message.ResourceKey.String()},
		"payload":         &types.AttributeValueMemberB{Value: payload},
		"created_at":      &types.AttributeValueMemberS{Value: createdAt},
		"next_attempt_at": &types.AttributeValueMemberS{Value: createdAt},
		"status":          &types.AttributeValueMemberS{Value: "PENDING"},
		"gsi_pk":          &types.AttributeValueMemberS{Value: "PENDING"},
		"gsi_sk":          &types.AttributeValueMemberS{Value: createdAt},
		"attempts":        &types.AttributeValueMemberN{Value: "0"},
		"last_error":      &types.AttributeValueMemberS{Value: ""},
	}

	if len(message.Headers) > 0 {
		headers := make([]types.AttributeValue, 0, len(message.Headers))
		for _, h := range message.Headers {
			headers = append(headers, &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"key":   &types.AttributeValueMemberS{Value: h.Key},
					"value": &types.AttributeValueMemberB{Value: h.Value},
				},
			})
		}
		item["headers"] = &types.AttributeValueMemberL{Value: headers}
	}

	_, err := d.DynamoDB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.TableName),
		Item:      item,
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_write", metrics.ClassifyDB(err), "")
		log.WithContext(ctx).WithError(err).Errorf("failed to write outbox message to table: %s", d.TableName)
		return fmt.Errorf("failed to write outbox message: %w", err)
	}

	return nil
}

func (d *dynamoOutbox) Enqueue(ctx context.Context, msg OutboundMessage) error {
	log.Trace("DynamoOutbox Enqueue")
	outboxMessage, err := outboxMessageFromOutbound(ctx, msg)
	if err != nil {
		return err
	}
	return d.WriteMessage(ctx, outboxMessage)
}

func outboxMessageFromOutbound(ctx context.Context, msg OutboundMessage) (OutboxMessage, error) {
	log.Trace("outboxMessageFromOutbound")
	if err := msg.Validate(); err != nil {
		return OutboxMessage{}, err
	}
	payload, err := msg.Message.SerializeEnvelope(ctx)
	if err != nil {
		return OutboxMessage{}, fmt.Errorf("serialize outbound message envelope: %w", err)
	}
	return OutboxMessage{
		Topic:   msg.Topic,
		Message: msg.Message,
		Payload: payload,
		Headers: traceHeaders(ctx, msg.Headers),
	}, nil
}

func traceHeaders(ctx context.Context, headers []kafka.Header) []kafka.Header {
	log.Trace("traceHeaders")
	propagator := otel.GetTextMapPropagator()
	carrier := TraceHeadersCarrier{}
	propagator.Inject(ctx, &carrier)
	out := append([]kafka.Header{}, headers...)
	return append(out, []kafka.Header(carrier)...)
}

func deriveOutboxEventID(topic string, message Message, payload []byte, createdAt string) string {
	payloadHash := sha256.Sum256(payload)
	// DynamoDB cannot assign a server-side item key. Use a deterministic UUID
	// from the row identity plus write timestamp instead of a random UUID.
	return idem.FromParts(
		idem.Outbox,
		topic,
		message.MsgType.String(),
		message.ResourceKey.String(),
		fmt.Sprintf("%x", payloadHash),
		createdAt,
	).String()
}

func (d *dynamoOutbox) ReadPending(ctx context.Context, maxMessages int32) ([]OutboxPendingMessage, error) {
	log.Trace("DynamoOutbox ReadPending")

	// Read relay identity with lock
	d.mu.Lock()
	if d.RelayOwnerID == "" {
		// Process-instance identity; this is not a persisted domain ID.
		d.RelayOwnerID = uuid.NewString()
	}
	if d.RelayLeaseTTL <= 0 {
		d.RelayLeaseTTL = 30 * time.Second
	}
	relayOwnerID := d.RelayOwnerID
	relayLeaseTTL := d.RelayLeaseTTL
	d.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Query PENDING items via GSI where gsi_sk (next_attempt_at) <= now
	pendingOut, err := d.DynamoDB.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(d.TableName),
		IndexName:              aws.String(d.GSIName),
		Limit:                  aws.Int32(maxMessages * 3),
		KeyConditionExpression: aws.String("gsi_pk = :pending AND gsi_sk <= :now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pending": &types.AttributeValueMemberS{Value: "PENDING"},
			":now":     &types.AttributeValueMemberS{Value: now},
		},
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_read_pending", metrics.ClassifyDB(err), "")
		return nil, fmt.Errorf("failed to query pending outbox messages: %w", err)
	}

	// Query PROCESSING items with expired leases via GSI
	processingOut, err := d.DynamoDB.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(d.TableName),
		IndexName:              aws.String(d.GSIName),
		Limit:                  aws.Int32(maxMessages * 3),
		KeyConditionExpression: aws.String("gsi_pk = :processing AND gsi_sk <= :now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":processing": &types.AttributeValueMemberS{Value: "PROCESSING"},
			":now":        &types.AttributeValueMemberS{Value: now},
		},
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_read_processing", metrics.ClassifyDB(err), "")
		return nil, fmt.Errorf("failed to query processing outbox messages: %w", err)
	}

	// Combine results
	allItems := append(pendingOut.Items, processingOut.Items...)

	pending := make([]OutboxPendingMessage, 0, maxMessages)
	for _, item := range allItems {
		if int32(len(pending)) >= maxMessages {
			break
		}
		pkVal, ok := item["pk"].(*types.AttributeValueMemberS)
		if !ok {
			continue
		}
		skVal, ok := item["sk"].(*types.AttributeValueMemberS)
		if !ok {
			continue
		}
		topicVal, ok := item["topic"].(*types.AttributeValueMemberS)
		if !ok {
			continue
		}
		payloadVal, ok := item["payload"].(*types.AttributeValueMemberB)
		if !ok {
			continue
		}
		statusVal, _ := item["status"].(*types.AttributeValueMemberS)

		attempts := 0
		if attemptsVal, ok := item["attempts"].(*types.AttributeValueMemberN); ok {
			if _, err := fmt.Sscanf(attemptsVal.Value, "%d", &attempts); err != nil {
				attempts = 0
			}
		}

		headers := []kafka.Header{}
		if hdrList, ok := item["headers"].(*types.AttributeValueMemberL); ok {
			headers = make([]kafka.Header, 0, len(hdrList.Value))
			for _, hdr := range hdrList.Value {
				hdrMap, ok := hdr.(*types.AttributeValueMemberM)
				if !ok {
					continue
				}
				keyAttr, keyOK := hdrMap.Value["key"].(*types.AttributeValueMemberS)
				valAttr, valOK := hdrMap.Value["value"].(*types.AttributeValueMemberB)
				if !keyOK || !valOK {
					continue
				}
				headers = append(headers, kafka.Header{Key: keyAttr.Value, Value: valAttr.Value})
			}
		}

		// Build condition based on current status
		var conditionExpr string
		if statusVal != nil && statusVal.Value == "PROCESSING" {
			conditionExpr = "#status = :processing AND lease_expires_at <= :now"
		} else {
			conditionExpr = "#status = :pending AND next_attempt_at <= :now"
		}

		// Claim attempt identity; used only to prove relay ownership of a lease.
		claimToken := uuid.NewString()
		leaseExpires := time.Now().UTC().Add(relayLeaseTTL).Format(time.RFC3339Nano)
		_, claimErr := d.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(d.TableName),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: pkVal.Value},
				"sk": &types.AttributeValueMemberS{Value: skVal.Value},
			},
			ConditionExpression: aws.String(conditionExpr),
			UpdateExpression:    aws.String("SET #status = :new_processing, processing_owner = :owner, claim_token = :claim_token, lease_expires_at = :lease_expires_at, gsi_pk = :new_processing, gsi_sk = :lease_expires_at"),
			ExpressionAttributeNames: map[string]string{
				"#status": "status",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pending":          &types.AttributeValueMemberS{Value: "PENDING"},
				":processing":       &types.AttributeValueMemberS{Value: "PROCESSING"},
				":new_processing":   &types.AttributeValueMemberS{Value: "PROCESSING"},
				":now":              &types.AttributeValueMemberS{Value: now},
				":owner":            &types.AttributeValueMemberS{Value: relayOwnerID},
				":claim_token":      &types.AttributeValueMemberS{Value: claimToken},
				":lease_expires_at": &types.AttributeValueMemberS{Value: leaseExpires},
			},
		})
		if claimErr != nil {
			// Another relay instance likely won this row.
			continue
		}

		pending = append(pending, OutboxPendingMessage{
			PK:              pkVal.Value,
			SK:              skVal.Value,
			Topic:           topicVal.Value,
			Payload:         payloadVal.Value,
			Headers:         headers,
			Attempts:        attempts,
			ProcessingOwner: relayOwnerID,
			ClaimToken:      claimToken,
		})
	}
	return pending, nil
}

func (d *dynamoOutbox) MarkSent(ctx context.Context, pending OutboxPendingMessage) error {
	log.Trace("DynamoOutbox MarkSent")

	ttlDays := d.SentTTLDays
	if ttlDays <= 0 {
		ttlDays = 7
	}
	ttlEpoch := time.Now().UTC().AddDate(0, 0, ttlDays).Unix()

	_, err := d.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(d.TableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pending.PK},
			"sk": &types.AttributeValueMemberS{Value: pending.SK},
		},
		ConditionExpression: aws.String("#status = :processing AND claim_token = :claim_token AND processing_owner = :owner"),
		UpdateExpression:    aws.String("SET #status = :sent, sent_at = :sent_at, #ttl = :ttl REMOVE gsi_pk, gsi_sk"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
			"#ttl":    "ttl",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":processing":  &types.AttributeValueMemberS{Value: "PROCESSING"},
			":claim_token": &types.AttributeValueMemberS{Value: pending.ClaimToken},
			":owner":       &types.AttributeValueMemberS{Value: pending.ProcessingOwner},
			":sent":        &types.AttributeValueMemberS{Value: "SENT"},
			":sent_at":     &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339Nano)},
			":ttl":         &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)},
		},
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_mark_sent", metrics.ClassifyDB(err), "")
		return fmt.Errorf("failed to mark outbox message sent: %w", err)
	}
	return nil
}

func (d *dynamoOutbox) MarkFailed(ctx context.Context, pending OutboxPendingMessage, lastError string, nextAttemptAt time.Time) error {
	log.Trace("DynamoOutbox MarkFailed")

	nextAttemptStr := nextAttemptAt.UTC().Format(time.RFC3339Nano)

	_, err := d.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(d.TableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pending.PK},
			"sk": &types.AttributeValueMemberS{Value: pending.SK},
		},
		ConditionExpression: aws.String("#status = :processing AND claim_token = :claim_token AND processing_owner = :owner"),
		UpdateExpression:    aws.String("SET #status = :pending, last_error = :last_error, next_attempt_at = :next_attempt_at, attempts = :attempts, gsi_pk = :pending, gsi_sk = :next_attempt_at"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":processing":      &types.AttributeValueMemberS{Value: "PROCESSING"},
			":claim_token":     &types.AttributeValueMemberS{Value: pending.ClaimToken},
			":owner":           &types.AttributeValueMemberS{Value: pending.ProcessingOwner},
			":pending":         &types.AttributeValueMemberS{Value: "PENDING"},
			":last_error":      &types.AttributeValueMemberS{Value: lastError},
			":next_attempt_at": &types.AttributeValueMemberS{Value: nextAttemptStr},
			":attempts":        &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", pending.Attempts+1)},
		},
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_mark_failed", metrics.ClassifyDB(err), "")
		return fmt.Errorf("failed to mark outbox message failed: %w", err)
	}
	return nil
}
