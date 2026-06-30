package messaging

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"runtime/debug"

	"github.com/cenkalti/backoff/v4"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	log "github.com/sirupsen/logrus"

	metrics "lib/shared_lib/metrics"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("kafka-consumer")
}

const maxBackoffSeconds = 10
const maxElapsedBackoffSeconds = 30
const MaxReplayAttempts = 3
const MaxCommitAttempts = 3 // includes first attempt
const DefaultNumShards = 16
const DefaultChannelBuffer = 100

type KafkaConsumer interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	Poll(timeoutMs int) (event kafka.Event)
	CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error)
	Seek(partition kafka.TopicPartition, timeoutMs int) error
	Assign(partitions []kafka.TopicPartition) error
	Unassign() error
	Close() error
}

type pausableKafkaConsumer interface {
	Pause(partitions []kafka.TopicPartition) error
	Resume(partitions []kafka.TopicPartition) error
}

type lagAwareKafkaConsumer interface {
	Position(partitions []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	GetWatermarkOffsets(topic string, partition int32) (low, high int64, err error)
}

type Subscriber interface {
	Subscribe(ctx context.Context, topics []string) error
	RegisterListener(msgType MsgType, listener func(context.Context, Message) error)
	AddTopics(ctx context.Context, topics []string) error
}

// shardedMessage wraps a message with its context for dispatch to workers
type shardedMessage struct {
	ctx        context.Context
	msg        *kafka.Message
	message    Message
	assignment assignmentToken
}

type commitRequest struct {
	msg         *kafka.Message
	assignment  assignmentToken
	messageType string
}

type topicPartitionKey struct {
	topic     string
	partition int32
}

type partitionCommitState struct {
	nextOffset kafka.Offset
	ready      map[kafka.Offset]commitRequest
}

type assignmentEntry struct {
	partition  kafka.TopicPartition
	generation uint64
}

type assignmentToken struct {
	key        topicPartitionKey
	generation uint64
	valid      bool
}

type partitionAssignment struct {
	mutex      sync.RWMutex
	generation uint64
	assigned   map[topicPartitionKey]assignmentEntry
}

func newPartitionAssignment() *partitionAssignment {
	return &partitionAssignment{
		assigned: make(map[topicPartitionKey]assignmentEntry),
	}
}

func (a *partitionAssignment) Assign(partitions []kafka.TopicPartition) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.generation++
	a.assigned = make(map[topicPartitionKey]assignmentEntry, len(partitions))
	for _, partition := range partitions {
		a.assigned[newTopicPartitionKey(partition)] = assignmentEntry{
			partition:  partition,
			generation: a.generation,
		}
	}
}

func (a *partitionAssignment) RevokeAll() []kafka.TopicPartition {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	revoked := make([]kafka.TopicPartition, 0, len(a.assigned))
	for _, entry := range a.assigned {
		revoked = append(revoked, entry.partition)
	}
	a.generation++
	a.assigned = make(map[topicPartitionKey]assignmentEntry)
	return revoked
}

func (a *partitionAssignment) Current() []kafka.TopicPartition {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	partitions := make([]kafka.TopicPartition, 0, len(a.assigned))
	for _, entry := range a.assigned {
		partitions = append(partitions, entry.partition)
	}
	return partitions
}

func (a *partitionAssignment) Token(partition kafka.TopicPartition) assignmentToken {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	key := newTopicPartitionKey(partition)
	entry, ok := a.assigned[key]
	if !ok {
		return assignmentToken{key: key}
	}
	return assignmentToken{
		key:        key,
		generation: entry.generation,
		valid:      true,
	}
}

func (a *partitionAssignment) IsCurrent(token assignmentToken) bool {
	if !token.valid {
		return false
	}

	a.mutex.RLock()
	defer a.mutex.RUnlock()

	entry, ok := a.assigned[token.key]
	return ok && entry.generation == token.generation
}

type subscriber struct {
	Consumer       KafkaConsumer
	Dlq            DLQ
	ReplayMap      map[string]int
	mutex          sync.Mutex
	listenerMutex  sync.RWMutex
	BackoffFactory func() *backoff.ExponentialBackOff
	EventListeners map[MsgType]func(context.Context, Message) error
	closeOnce      sync.Once
	closed         atomic.Bool

	// Sharding
	numShards     int
	channelBuffer int
	shardChannels []chan shardedMessage
	commitCh      chan commitRequest
	commitMutex   sync.Mutex
	commitStates  map[topicPartitionKey]*partitionCommitState

	// Track in-flight message processing
	processingAdmissionMutex sync.Mutex
	processingWg             sync.WaitGroup

	// Dynamic topic management
	brokers          string
	groupID          string
	subscribedTopics map[string]struct{}
	topicsMutex      sync.RWMutex

	assignments        *partitionAssignment
	lastLagRefreshNano atomic.Int64

	healthMutex sync.RWMutex
	health      SubscriberHealth

	errorPolicyMutex sync.RWMutex
	errorPolicy      ErrorPolicy
}

type SubscriberOption func(*subscriberOptions)

type subscriberOptions struct {
	autoOffsetReset string
}

func WithAutoOffsetReset(autoOffsetReset string) SubscriberOption {
	return func(opts *subscriberOptions) {
		opts.autoOffsetReset = autoOffsetReset
	}
}

func NewSubscriber(brokers, groupID string, dlq DLQ, backoffFactory func() *backoff.ExponentialBackOff, numShards, channelBuffer int, options ...SubscriberOption) (Subscriber, error) {
	log.Trace("NewSubscriber")

	if numShards <= 0 {
		numShards = DefaultNumShards
	}
	if channelBuffer <= 0 {
		channelBuffer = DefaultChannelBuffer
	}
	opts := subscriberOptions{autoOffsetReset: "earliest"}
	for _, option := range options {
		option(&opts)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    brokers,
		"group.id":             groupID,
		"auto.offset.reset":    opts.autoOffsetReset,
		"enable.auto.commit":   false,
		"enable.partition.eof": true,
		"session.timeout.ms":   timeoutMillisecond,
		// "ssl.ca.location": "/etc/ssl/certs",
		// "security.protocol": "ssl",
		// "debug": "consumer,cgrp,topic,fetch",
	})
	if err != nil {
		metrics.Default().RecordError(context.Background(), metrics.BoundaryKafka, "create_consumer", metrics.ClassifyKafka(err), "")
		log.WithError(err).Error("failed to create Kafka consumer")
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	shardChannels := make([]chan shardedMessage, numShards)
	for i := 0; i < numShards; i++ {
		shardChannels[i] = make(chan shardedMessage, channelBuffer)
	}

	sub := &subscriber{
		Consumer:         c,
		Dlq:              dlq,
		ReplayMap:        make(map[string]int),
		BackoffFactory:   backoffFactory,
		EventListeners:   make(map[MsgType]func(context.Context, Message) error),
		numShards:        numShards,
		channelBuffer:    channelBuffer,
		shardChannels:    shardChannels,
		commitCh:         make(chan commitRequest, numShards*channelBuffer),
		commitStates:     make(map[topicPartitionKey]*partitionCommitState),
		brokers:          brokers,
		groupID:          groupID,
		subscribedTopics: make(map[string]struct{}),
		assignments:      newPartitionAssignment(),
		health: SubscriberHealth{
			GroupID:       groupID,
			QueueCapacity: numShards * channelBuffer,
			LagByTopic:    make(map[string]int64),
		},
	}
	return sub, nil
}

func NewTestSubscriber(consumer KafkaConsumer, dlq DLQ, backoffFactory func() *backoff.ExponentialBackOff, numShards, channelBuffer int) Subscriber {
	log.Trace("NewTestSubscriber")

	if numShards <= 0 {
		numShards = DefaultNumShards
	}
	if channelBuffer <= 0 {
		channelBuffer = DefaultChannelBuffer
	}

	shardChannels := make([]chan shardedMessage, numShards)
	for i := 0; i < numShards; i++ {
		shardChannels[i] = make(chan shardedMessage, channelBuffer)
	}

	sub := &subscriber{
		Consumer:         consumer,
		Dlq:              dlq,
		ReplayMap:        make(map[string]int),
		BackoffFactory:   backoffFactory,
		EventListeners:   make(map[MsgType]func(context.Context, Message) error),
		numShards:        numShards,
		channelBuffer:    channelBuffer,
		shardChannels:    shardChannels,
		commitCh:         make(chan commitRequest, numShards*channelBuffer),
		commitStates:     make(map[topicPartitionKey]*partitionCommitState),
		subscribedTopics: make(map[string]struct{}),
		groupID:          "test-subscriber",
		assignments:      newPartitionAssignment(),
		health: SubscriberHealth{
			GroupID:       "test-subscriber",
			QueueCapacity: numShards * channelBuffer,
			LagByTopic:    make(map[string]int64),
		},
	}
	return sub
}

func (sc *subscriber) Subscribe(ctx context.Context, topics []string) error {
	log.Trace("subscriber Subscribe")

	// Track subscribed topics
	sc.topicsMutex.Lock()
	for _, topic := range topics {
		sc.subscribedTopics[topic] = struct{}{}
	}
	sc.topicsMutex.Unlock()
	sc.markStarted(topics)

	if err := sc.Consumer.SubscribeTopics(topics, sc.rebalanceCallback()); err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryKafka, "subscribe", metrics.ClassifyKafka(err), "")
		sc.recordSubscriberError(err)
		return fmt.Errorf("failed to subscribe to topics %v: %w", topics, err)
	}
	log.Infof("Subscribing to topics: %v with %d shards", topics, sc.numShards)

	grp, egCtx := errgroup.WithContext(ctx)

	// Start shard workers
	for i := 0; i < sc.numShards; i++ {
		shardID := i
		grp.Go(func() error {
			return sc.shardWorker(egCtx, shardID)
		})
	}

	// Start commit coordinator
	grp.Go(func() error {
		return sc.commitCoordinator(egCtx)
	})

	// Start Kafka consumer loop
	grp.Go(func() error {
		log.Info("Kafka consumer loop started")
		var backlog []shardedMessage
		consumerErrorBackoff := sc.BackoffFactory()
		consumerErrorBackoff.MaxElapsedTime = 0
		defer func() {
			log.Info("Kafka consumer loop stopping")
			// Signal shutdown before closing channels to prevent sends on closed channels
			sc.stopProcessingAdmission()
			sc.markClosed()
			sc.releaseBacklog(backlog)

			// Wait for all in-flight message processing to complete
			// This ensures no goroutines are trying to send on channels after close
			sc.processingWg.Wait()

			sc.closeOnce.Do(func() {
				for i := 0; i < sc.numShards; i++ {
					close(sc.shardChannels[i])
				}
				close(sc.commitCh)
				log.Info("Closing Kafka consumer")
				_ = sc.Consumer.Close()
			})
		}()

		backpressurePaused := false
		for {
			if egCtx.Err() != nil {
				return egCtx.Err()
			}

			if len(backlog) > 0 {
				backlog = sc.drainDispatchBacklog(backlog)
				sc.setBacklogDepth(len(backlog))
			}
			if len(backlog) == 0 && backpressurePaused {
				sc.resumeAssignedPartitions(egCtx)
				backpressurePaused = false
			}

			ev := sc.Consumer.Poll(100)
			sc.recordPollAttempt()
			sc.refreshLag(egCtx)

			if egCtx.Err() != nil {
				return egCtx.Err()
			}

			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				consumerErrorBackoff.Reset()
				sm, ok, err := sc.prepareMessage(egCtx, e)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				if !sc.tryDispatch(sm) {
					backlog = append(backlog, sm)
					sc.setBacklogDepth(len(backlog))
					if !backpressurePaused {
						sc.pauseAssignedPartitions(egCtx)
						backpressurePaused = true
					}
				}
			case kafka.AssignedPartitions:
				consumerErrorBackoff.Reset()
				sc.recordAssignedPartitions(e.Partitions)
			case kafka.RevokedPartitions:
				consumerErrorBackoff.Reset()
				revoked := sc.recordRevokedPartitions(e.Partitions)
				backlog = sc.releaseBacklogForPartitions(backlog, revoked)
				sc.releaseShardQueuesForRevokedAssignments()
				sc.setBacklogDepth(len(backlog))
				backpressurePaused = false
			case kafka.Error:
				metrics.Default().RecordError(egCtx, metrics.BoundaryKafka, "consume", metrics.ClassifyKafka(e), "")
				if e.IsFatal() {
					sc.recordSubscriberError(e)
					log.WithContext(egCtx).WithError(e).Error("Kafka consumer error")
					return e
				}
				sc.recordTransientSubscriberError(e)
				log.WithContext(egCtx).WithError(e).Warn("Kafka consumer error")
				if err := sc.backoff(egCtx, consumerErrorBackoff); err != nil {
					return err
				}
			default:
			}
		}
	})

	return grp.Wait()
}

func (sc *subscriber) shardWorker(ctx context.Context, shardID int) error {
	log.Infof("Shard worker %d started", shardID)
	defer log.Infof("Shard worker %d stopped", shardID)

	for {
		select {
		case sm, ok := <-sc.shardChannels[shardID]:
			if !ok {
				return nil
			}
			sc.processMessage(sm)
			sc.processingWg.Done()
		case <-ctx.Done():
			// Drain remaining messages from channel to balance WaitGroup
			for sm := range sc.shardChannels[shardID] {
				sc.processMessage(sm)
				sc.processingWg.Done()
			}
			return ctx.Err()
		}
	}
}

func (sc *subscriber) rebalanceCallback() kafka.RebalanceCb {
	return func(_ *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			sc.recordAssignedPartitions(e.Partitions)
		case kafka.RevokedPartitions:
			sc.recordRevokedPartitions(e.Partitions)
		default:
		}
		return nil
	}
}

func (sc *subscriber) recordAssignedPartitions(partitions []kafka.TopicPartition) {
	log.WithField("partitions", partitions).Info("Kafka consumer partitions assigned")
	sc.assignments.Assign(partitions)
	sc.setAssignedPartitionCount(len(partitions))
}

func (sc *subscriber) recordRevokedPartitions(partitions []kafka.TopicPartition) []kafka.TopicPartition {
	log.WithField("partitions", partitions).Info("Kafka consumer partitions revoked")
	revoked := sc.assignments.RevokeAll()
	if len(revoked) == 0 {
		revoked = partitions
	}
	sc.clearCommitStateForPartitions(revoked)
	sc.setAssignedPartitionCount(0)
	return revoked
}

func (sc *subscriber) commitCoordinator(ctx context.Context) error {
	log.Info("Commit coordinator started")
	defer log.Info("Commit coordinator stopped")

	for req := range sc.commitCh {
		sc.commitReadyMessages(ctx, req)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (sc *subscriber) commitReadyMessages(ctx context.Context, completed commitRequest) {
	for _, req := range sc.readyToCommit(completed) {
		if !sc.assignments.IsCurrent(req.assignment) {
			continue
		}
		if err := sc.commitMessageWithRetry(ctx, req); err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}
			log.WithContext(ctx).WithError(err).Error("failed to commit ready message")
		}
	}
}

// getShardID maps a message's ResourceKey to a shard worker.
//
// Shard contract (the canonical serialization unit for the subscriber):
//
//   - All messages with the same ResourceKey are processed sequentially by a
//     single shard worker, in the order they were dispatched.
//   - Messages with different ResourceKeys may run concurrently on different
//     shards. Listeners MUST NOT rely on cross-key serialization and MUST NOT
//     hold listener-wide mutexes around their Handle implementation; doing so
//     collapses the shard parallelism back to one-thread-at-a-time and is the
//     source of the per-listener mutex regression flagged in past reviews.
//   - Publishers MUST choose a ResourceKey that matches the mutation unit they
//     want serialized. For events that mutate one aggregate, that unit should
//     be the aggregate ID. Choosing
//     a key below the mutation unit pushes serialization from cheap shard
//     channels into expensive DB row/advisory locks.
func (sc *subscriber) getShardID(resourceKey uuid.UUID) int {
	h := fnv.New32a()
	h.Write(resourceKey[:])
	return int(h.Sum32()) % sc.numShards
}

func (sc *subscriber) prepareMessage(ctx context.Context, msg *kafka.Message) (shardedMessage, bool, error) {
	propagator := otel.GetTextMapPropagator()
	carrier := TraceHeadersCarrier(msg.Headers)
	msgCtx := propagator.Extract(ctx, &carrier)

	recordKafkaLag(msgCtx, msg)
	sc.recordMessage(msg)
	assignment := sc.assignments.Token(msg.TopicPartition)
	if !assignment.valid {
		log.WithContext(msgCtx).WithField("topic_partition", msg.TopicPartition).Warn("skipping Kafka message without active partition assignment")
		return shardedMessage{}, false, nil
	}

	var message Message
	if err := message.Deserialize(msgCtx, msg.Value); err != nil {
		log.WithContext(msgCtx).WithError(err).Error("Invalid Kafka message. Sending to DLQ and committing.")
		sc.registerInFlight(msg)
		sc.sendToDlq(msgCtx, msg)
		sc.requestCommit(msgCtx, commitRequest{msg: msg, assignment: assignment, messageType: "deserialize_error"})
		return shardedMessage{}, false, nil
	}

	if !sc.startProcessing() {
		return shardedMessage{}, false, context.Canceled
	}

	sc.registerInFlight(msg)

	return shardedMessage{ctx: msgCtx, msg: msg, message: message, assignment: assignment}, true, nil
}

func (sc *subscriber) tryDispatch(sm shardedMessage) bool {
	if !sc.assignments.IsCurrent(sm.assignment) {
		sc.processingWg.Done()
		return true
	}

	shardID := sc.getShardID(sm.message.ResourceKey)
	select {
	case sc.shardChannels[shardID] <- sm:
		return true
	default:
		return false
	}
}

func (sc *subscriber) drainDispatchBacklog(backlog []shardedMessage) []shardedMessage {
	if len(backlog) == 0 {
		return backlog
	}
	for len(backlog) > 0 {
		if !sc.tryDispatch(backlog[0]) {
			return backlog
		}
		backlog = backlog[1:]
	}
	return backlog
}

func (sc *subscriber) processMessage(sm shardedMessage) {
	ctx := sm.ctx
	msg := sm.msg
	message := sm.message
	topic := topicName(msg.TopicPartition)
	// Keep span-name cardinality bounded: topic names are configured statically,
	// and message type is an enum. Per-message IDs belong in attributes only.
	ctx, span := tracer.Start(ctx, "kafka.consume "+topic+" "+message.MsgType.String(),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation", "process"),
			attribute.String("messaging.message.type", message.MsgType.String()),
			attribute.String("messaging.message.resource_key", message.ResourceKey.String()),
			attribute.Int64("messaging.kafka.message.offset", int64(msg.TopicPartition.Offset)),
			attribute.Int64("messaging.kafka.partition", int64(msg.TopicPartition.Partition)),
		),
	)
	defer span.End()

	// Defense-in-depth: AddListener already recovers panics inside typed
	// listeners and returns them as NonRetryable errors. This recover protects
	// the shard worker from panics in raw RegisterListener handlers, in the
	// classification switch below, or in any future code path that bypasses
	// the AddListener wrapper. Without it, a single panic would tear down the
	// shard worker goroutine and stall every key hashed to that shard.
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			log.WithContext(ctx).
				WithField("msg_type", message.MsgType.String()).
				WithField("resource_key", message.ResourceKey).
				WithField("topic_partition", msg.TopicPartition).
				WithField("panic", r).
				WithField("stack", string(stack)).
				Error("processMessage panic recovered; sending message to DLQ")
			sc.recordNonRetryableFailure()
			// Only commit after the DLQ write has durably accepted the poison
			// message. If the DLQ is unreachable, leave the offset uncommitted
			// so a future re-poll (after rebalance or restart) gets another
			// chance to durably persist it. The shard worker is still safe:
			// the panic was already recovered above.
			if err := sc.sendToDlq(ctx, msg); err != nil {
				log.WithContext(ctx).WithError(err).
					WithField("topic_partition", msg.TopicPartition).
					Error("DLQ write failed after panic; offset will not be committed")
				sc.removeReplayKey(msg.TopicPartition)
				return
			}
			sc.requestCommit(ctx, sc.commitRequestFor(sm))
			sc.removeReplayKey(msg.TopicPartition)
		}
	}()

	if !sc.assignments.IsCurrent(sm.assignment) {
		log.WithContext(ctx).WithField("topic_partition", msg.TopicPartition).Warn("skipping stale Kafka message from revoked partition")
		return
	}

	sc.listenerMutex.RLock()
	eventListener, exists := sc.EventListeners[message.MsgType]
	sc.listenerMutex.RUnlock()

	if !exists {
		// log.WithContext(ctx).Warnf("No listener for event type: %s. Skipping.", message.MsgType.String())
		sc.requestCommit(ctx, sc.commitRequestFor(sm))
		return
	}

	if err := eventListener(ctx, message); err != nil {
		switch {
		case IsAlreadyProcessed(err):
			log.WithContext(ctx).WithError(err).WithFields(messageLogFields(sm)).Warn("Message already processed. Committing and moving on.")
			sc.requestCommit(ctx, sc.commitRequestFor(sm))
			sc.removeReplayKey(msg.TopicPartition)
			return

		case sc.isNonRetryableError(err):
			log.WithContext(ctx).WithError(err).WithFields(messageLogFields(sm)).Error("Non-retryable handler failure. Sending to DLQ and committing.")
			sc.recordNonRetryableFailure()
			sc.sendToDlq(ctx, msg)
			sc.requestCommit(ctx, sc.commitRequestFor(sm))
			sc.removeReplayKey(msg.TopicPartition)
			return

		default:
			sc.recordSubscriberError(err)
			if retryErr := sc.replayMessageToShard(sm, err); retryErr != nil {
				if !errors.Is(retryErr, context.Canceled) {
					log.WithContext(ctx).WithError(retryErr).Error("retry handling failed")
					sc.recordSubscriberError(retryErr)
				}
			}
			return
		}
	}

	sc.requestCommit(ctx, sc.commitRequestFor(sm))
	sc.removeReplayKey(msg.TopicPartition)
}

func (sc *subscriber) commitRequestFor(sm shardedMessage) commitRequest {
	return commitRequest{
		msg:         sm.msg,
		assignment:  sm.assignment,
		messageType: sm.message.MsgType.String(),
	}
}

func (sc *subscriber) requestCommit(ctx context.Context, req commitRequest) {
	if !sc.assignments.IsCurrent(req.assignment) {
		return
	}

	select {
	case sc.commitCh <- req:
	default:
		// Preserve partition ordering even under backpressure by letting the coordinator own commits.
		sc.commitCh <- req
	}
}

func (sc *subscriber) registerInFlight(msg *kafka.Message) {
	key := newTopicPartitionKey(msg.TopicPartition)

	sc.commitMutex.Lock()
	defer sc.commitMutex.Unlock()

	if _, exists := sc.commitStates[key]; exists {
		return
	}

	sc.commitStates[key] = &partitionCommitState{
		nextOffset: msg.TopicPartition.Offset,
		ready:      make(map[kafka.Offset]commitRequest),
	}
}

func (sc *subscriber) readyToCommit(req commitRequest) []commitRequest {
	if !sc.assignments.IsCurrent(req.assignment) {
		return nil
	}

	key := newTopicPartitionKey(req.msg.TopicPartition)

	sc.commitMutex.Lock()
	defer sc.commitMutex.Unlock()

	state, exists := sc.commitStates[key]
	if !exists {
		state = &partitionCommitState{
			nextOffset: req.msg.TopicPartition.Offset,
			ready:      make(map[kafka.Offset]commitRequest),
		}
		sc.commitStates[key] = state
	}

	state.ready[req.msg.TopicPartition.Offset] = req

	ready := make([]commitRequest, 0)
	for {
		next, ok := state.ready[state.nextOffset]
		if !ok {
			break
		}
		ready = append(ready, next)
		delete(state.ready, state.nextOffset)
		state.nextOffset++
	}

	if len(state.ready) == 0 {
		delete(sc.commitStates, key)
	}

	return ready
}

func newTopicPartitionKey(tp kafka.TopicPartition) topicPartitionKey {
	topic := ""
	if tp.Topic != nil {
		topic = *tp.Topic
	}

	return topicPartitionKey{
		topic:     topic,
		partition: tp.Partition,
	}
}

func (sc *subscriber) replayMessageToShard(sm shardedMessage, cause error) error {
	ctx := sm.ctx
	msg := sm.msg
	tp := msg.TopicPartition
	topic := topicName(tp)
	key := fmt.Sprintf("%s-%d-%d", topic, tp.Partition, tp.Offset)

	sc.mutex.Lock()
	attempts := sc.ReplayMap[key]
	sc.mutex.Unlock()

	if attempts >= MaxReplayAttempts {
		sc.logRetryableHandlerFailure(ctx, sm, cause, attempts, true)
		sc.recordMaxReplayDLQ()
		sc.sendToDlq(ctx, msg)
		sc.requestCommit(ctx, sc.commitRequestFor(sm))
		sc.mutex.Lock()
		delete(sc.ReplayMap, key)
		sc.mutex.Unlock()
		return nil
	}

	nextAttempt := attempts + 1
	sc.logRetryableHandlerFailure(ctx, sm, cause, nextAttempt, false)

	expBackoff := sc.BackoffFactory()
	expBackoff.MaxElapsedTime = maxElapsedBackoffSeconds * time.Second

	sc.mutex.Lock()
	sc.ReplayMap[key] = nextAttempt
	sc.mutex.Unlock()
	sc.recordRetry()

	delay := expBackoff.NextBackOff()
	if delay == backoff.Stop {
		delay = time.Second
	}

	// Use context-aware sleep to respond to shutdown
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return ctx.Err()
	}

	if !sc.assignments.IsCurrent(sm.assignment) {
		return nil
	}

	if !sc.startProcessing() {
		return context.Canceled
	}
	if !sc.assignments.IsCurrent(sm.assignment) {
		sc.processingWg.Done()
		return nil
	}

	// Re-dispatch to the same shard
	shardID := sc.getShardID(sm.message.ResourceKey)
	select {
	case sc.shardChannels[shardID] <- sm:
		return nil
	case <-ctx.Done():
		sc.processingWg.Done()
		return ctx.Err()
	}
}

func (sc *subscriber) logRetryableHandlerFailure(ctx context.Context, sm shardedMessage, err error, attempt int, maxAttemptsReached bool) {
	fields := messageLogFields(sm)
	fields["group_id"] = sc.groupID
	fields["attempt"] = attempt
	fields["max_attempts"] = MaxReplayAttempts
	fields["max_replay_dlq"] = maxAttemptsReached
	if maxAttemptsReached {
		log.WithContext(ctx).WithError(err).WithFields(fields).Error("retryable handler failure reached max replay attempts; sending to DLQ and committing")
		return
	}
	log.WithContext(ctx).WithError(err).WithFields(fields).Warn("retryable handler failure; scheduling Kafka message replay")
}

func messageLogFields(sm shardedMessage) log.Fields {
	tp := sm.msg.TopicPartition
	return log.Fields{
		"topic":        topicName(tp),
		"partition":    tp.Partition,
		"offset":       tp.Offset,
		"message_type": sm.message.MsgType.String(),
		"resource_key": sm.message.ResourceKey,
	}
}

func (sc *subscriber) RegisterListener(msgType MsgType, listener func(context.Context, Message) error) {
	log.Trace("subscriber RegisterListener")

	sc.listenerMutex.Lock()
	defer sc.listenerMutex.Unlock()
	sc.EventListeners[msgType] = listener
}

// AddTopics dynamically adds new topics to the subscription.
// It creates the topics if they don't exist, then re-subscribes to include them.
func (sc *subscriber) AddTopics(ctx context.Context, topics []string) error {
	if len(topics) == 0 {
		return nil
	}

	sc.topicsMutex.Lock()
	newTopics := make([]string, 0)
	for _, topic := range topics {
		if _, exists := sc.subscribedTopics[topic]; !exists {
			newTopics = append(newTopics, topic)
			sc.subscribedTopics[topic] = struct{}{}
		}
	}

	if len(newTopics) == 0 {
		sc.topicsMutex.Unlock()
		return nil
	}

	// Get all topics for re-subscription
	allTopics := make([]string, 0, len(sc.subscribedTopics))
	for topic := range sc.subscribedTopics {
		allTopics = append(allTopics, topic)
	}
	sc.topicsMutex.Unlock()

	// Create topics before subscribing
	if sc.brokers != "" {
		if err := CreateTopics(ctx, sc.brokers, newTopics); err != nil {
			log.WithError(err).WithField("topics", newTopics).Warn("failed to create topics, will attempt subscription anyway")
		}
	}

	// Re-subscribe with all topics including new ones
	if err := sc.Consumer.SubscribeTopics(allTopics, sc.rebalanceCallback()); err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryKafka, "add_topics", metrics.ClassifyKafka(err), "")
		sc.recordSubscriberError(err)
		return fmt.Errorf("failed to add topics %v: %w", newTopics, err)
	}
	sc.updateHealthTopics(allTopics)

	log.WithField("new_topics", newTopics).WithField("total_topics", len(allTopics)).Info("dynamically added topics to subscription")
	return nil
}

func (sc *subscriber) commitMessageWithRetry(ctx context.Context, req commitRequest) error {
	log.Trace("subscriber commitMessageWithRetry")

	expBackoff := sc.BackoffFactory()
	expBackoff.MaxElapsedTime = maxElapsedBackoffSeconds * time.Second

	var err error
	for attempt := 1; attempt <= MaxCommitAttempts; attempt++ {
		_, err = sc.Consumer.CommitMessage(req.msg)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.WithContext(ctx).WithError(err).Warnf("Attempt %d/%d: failed to commit message, retrying...", attempt, MaxCommitAttempts)
			backoffErr := sc.backoff(ctx, expBackoff)
			if backoffErr != nil {
				return backoffErr
			}
			continue
		}
		sc.recordCommit(ctx, req)
		break
	}
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("failed to commit message after %d attempts", MaxCommitAttempts)
		metrics.Default().RecordError(ctx, metrics.BoundaryKafka, "commit", metrics.ClassifyKafka(err), "")
		return fmt.Errorf("failed to commit message after %d attempts: %w", MaxCommitAttempts, err)
	}
	return nil
}

func recordKafkaLag(ctx context.Context, msg *kafka.Message) {
	log.Trace("recordKafkaLag")
	if msg == nil || msg.Timestamp.IsZero() {
		return
	}
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}
	lagSeconds := time.Since(msg.Timestamp).Seconds()
	if lagSeconds < 0 {
		lagSeconds = 0
	}
	metrics.Default().RecordKafkaLag(ctx, topic, lagSeconds)
}

func topicName(tp kafka.TopicPartition) string {
	if tp.Topic == nil {
		return ""
	}
	return *tp.Topic
}

func (sc *subscriber) removeReplayKey(tp kafka.TopicPartition) {
	log.Trace("subscriber removeReplayKey")

	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	key := fmt.Sprintf("%s-%d-%d", topicName(tp), tp.Partition, tp.Offset)
	delete(sc.ReplayMap, key)
}

func (sc *subscriber) backoff(ctx context.Context, expBackoff *backoff.ExponentialBackOff) error {
	log.Trace("subscriber backoff")

	waitTime := expBackoff.NextBackOff()
	if waitTime == backoff.Stop {
		return nil
	}
	select {
	case <-time.After(waitTime):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sc *subscriber) sendToDlq(ctx context.Context, msg *kafka.Message) error {
	log.Trace("subscriber sendToDlq")

	expBackoff := sc.BackoffFactory()
	expBackoff.MaxElapsedTime = maxElapsedBackoffSeconds * time.Second

	var err error
	for attempt := 1; attempt <= MaxCommitAttempts; attempt++ {
		err = sc.Dlq.WriteMessage(ctx, msg.Value)
		if err == nil {
			sc.recordDLQ()
			return nil
		}
		log.WithContext(ctx).WithError(err).Warnf(
			"Attempt %d/%d: failed to send message to DLQ, retrying...", attempt, MaxCommitAttempts,
		)
		if backoffErr := sc.backoff(ctx, expBackoff); backoffErr != nil {
			return fmt.Errorf("backoff while DLQ write: %w", backoffErr)
		}
	}
	return fmt.Errorf("failed to send message to DLQ after %d attempts: %w", MaxCommitAttempts, err)
}

func (sc *subscriber) releaseBacklog(backlog []shardedMessage) {
	for range backlog {
		sc.processingWg.Done()
	}
	sc.setBacklogDepth(0)
}

func (sc *subscriber) startProcessing() bool {
	sc.processingAdmissionMutex.Lock()
	defer sc.processingAdmissionMutex.Unlock()

	if sc.closed.Load() {
		return false
	}

	sc.processingWg.Add(1)
	return true
}

func (sc *subscriber) stopProcessingAdmission() {
	sc.processingAdmissionMutex.Lock()
	sc.closed.Store(true)
	sc.processingAdmissionMutex.Unlock()
}

func (sc *subscriber) releaseBacklogForPartitions(backlog []shardedMessage, partitions []kafka.TopicPartition) []shardedMessage {
	if len(backlog) == 0 || len(partitions) == 0 {
		return backlog
	}

	revoked := make(map[topicPartitionKey]struct{}, len(partitions))
	for _, partition := range partitions {
		revoked[newTopicPartitionKey(partition)] = struct{}{}
	}

	remaining := backlog[:0]
	released := 0
	for _, sm := range backlog {
		if _, ok := revoked[newTopicPartitionKey(sm.msg.TopicPartition)]; ok {
			sc.processingWg.Done()
			released++
			continue
		}
		remaining = append(remaining, sm)
	}
	if released > 0 {
		log.WithField("released_backlog_messages", released).Warn("released subscriber backlog for revoked Kafka partitions")
	}
	return remaining
}

func (sc *subscriber) releaseShardQueuesForRevokedAssignments() {
	released := 0
	for _, ch := range sc.shardChannels {
	drain:
		for {
			select {
			case <-ch:
				sc.processingWg.Done()
				released++
			default:
				break drain
			}
		}
	}
	if released > 0 {
		log.WithField("released_shard_messages", released).Warn("released subscriber shard queues for revoked Kafka assignments")
	}
}

func (sc *subscriber) clearCommitStateForPartitions(partitions []kafka.TopicPartition) {
	if len(partitions) == 0 {
		return
	}
	sc.commitMutex.Lock()
	defer sc.commitMutex.Unlock()
	for _, partition := range partitions {
		delete(sc.commitStates, newTopicPartitionKey(partition))
	}
}

func (sc *subscriber) pauseAssignedPartitions(ctx context.Context) {
	consumer, ok := sc.Consumer.(pausableKafkaConsumer)
	if !ok {
		log.WithContext(ctx).Warn("Kafka consumer does not support pause under subscriber backpressure")
		return
	}

	partitions := sc.assignments.Current()
	if len(partitions) == 0 {
		return
	}
	if err := consumer.Pause(partitions); err != nil {
		if !sameTopicPartitions(partitions, sc.assignments.Current()) {
			log.WithContext(ctx).WithError(err).WithField("partitions", partitions).Warn("ignored Kafka pause failure after assignment changed")
			return
		}
		log.WithContext(ctx).WithError(err).Error("failed to pause Kafka partitions under subscriber backpressure")
		sc.recordSubscriberError(err)
		return
	}
	sc.setPausedPartitions(len(partitions))
	sc.recordBackpressurePause()
	log.WithContext(ctx).WithField("partitions", partitions).Warn("paused Kafka partitions because subscriber shard queues are full")
}

func (sc *subscriber) resumeAssignedPartitions(ctx context.Context) {
	consumer, ok := sc.Consumer.(pausableKafkaConsumer)
	if !ok {
		return
	}

	partitions := sc.assignments.Current()
	if len(partitions) == 0 {
		sc.setPausedPartitions(0)
		return
	}
	if err := consumer.Resume(partitions); err != nil {
		if !sameTopicPartitions(partitions, sc.assignments.Current()) {
			log.WithContext(ctx).WithError(err).WithField("partitions", partitions).Warn("ignored Kafka resume failure after assignment changed")
			sc.setPausedPartitions(0)
			return
		}
		log.WithContext(ctx).WithError(err).Error("failed to resume Kafka partitions after subscriber backpressure")
		sc.recordSubscriberError(err)
		return
	}
	sc.setPausedPartitions(0)
	log.WithContext(ctx).WithField("partitions", partitions).Info("resumed Kafka partitions after subscriber backpressure")
}

func sameTopicPartitions(left, right []kafka.TopicPartition) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[topicPartitionKey]int, len(left))
	for _, partition := range left {
		counts[newTopicPartitionKey(partition)]++
	}
	for _, partition := range right {
		key := newTopicPartitionKey(partition)
		if counts[key] == 0 {
			return false
		}
		counts[key]--
		if counts[key] == 0 {
			delete(counts, key)
		}
	}
	return len(counts) == 0
}

func (sc *subscriber) setAssignedPartitionCount(count int) {
	sc.healthMutex.Lock()
	sc.health.AssignedPartitions = count
	if count == 0 {
		sc.health.PausedPartitions = 0
		sc.health.MaxLag = 0
		sc.health.LagByTopic = make(map[string]int64)
	}
	sc.healthMutex.Unlock()
}

func (sc *subscriber) setPausedPartitions(count int) {
	sc.healthMutex.Lock()
	sc.health.PausedPartitions = count
	sc.healthMutex.Unlock()
}

func (sc *subscriber) markStarted(topics []string) {
	now := time.Now()
	sc.healthMutex.Lock()
	sc.health.Started = true
	sc.health.Closed = false
	sc.health.Topics = append([]string(nil), topics...)
	sc.health.LastProgressAt = now
	sc.healthMutex.Unlock()
}

func (sc *subscriber) updateHealthTopics(topics []string) {
	sc.healthMutex.Lock()
	sc.health.Topics = append([]string(nil), topics...)
	sc.healthMutex.Unlock()
}

func (sc *subscriber) markClosed() {
	sc.healthMutex.Lock()
	sc.health.Closed = true
	sc.healthMutex.Unlock()
}

func (sc *subscriber) recordPollAttempt() {
	sc.healthMutex.Lock()
	sc.health.LastPollAt = time.Now()
	sc.health.PollAttempts++
	sc.healthMutex.Unlock()
}

func (sc *subscriber) recordMessage(msg *kafka.Message) {
	_ = msg
	sc.healthMutex.Lock()
	sc.health.LastMessageAt = time.Now()
	sc.healthMutex.Unlock()
}

func (sc *subscriber) recordCommit(ctx context.Context, req commitRequest) {
	now := time.Now()
	sc.healthMutex.Lock()
	sc.health.LastCommitAt = now
	sc.health.LastProgressAt = now
	sc.health.MessagesCommitted++
	sc.healthMutex.Unlock()

	metrics.Default().RecordKafkaMessageConsumed(
		ctx,
		sc.groupID,
		topicName(req.msg.TopicPartition),
		req.msg.TopicPartition.Partition,
		req.messageType,
	)
}

func (sc *subscriber) recordDLQ() {
	sc.healthMutex.Lock()
	sc.health.MessagesDLQ++
	sc.healthMutex.Unlock()
}

func (sc *subscriber) recordRetry() {
	sc.healthMutex.Lock()
	sc.health.RetryCount++
	sc.healthMutex.Unlock()
}

func (sc *subscriber) recordNonRetryableFailure() {
	sc.healthMutex.Lock()
	sc.health.NonRetryableFailures++
	sc.healthMutex.Unlock()
}

func (sc *subscriber) recordBackpressurePause() {
	sc.healthMutex.Lock()
	sc.health.BackpressurePauses++
	sc.healthMutex.Unlock()
}

func (sc *subscriber) recordMaxReplayDLQ() {
	sc.healthMutex.Lock()
	sc.health.MaxReplayDLQ++
	sc.healthMutex.Unlock()
}

func (sc *subscriber) recordSubscriberError(err error) {
	if err == nil {
		return
	}
	sc.healthMutex.Lock()
	sc.health.LastErrorAt = time.Now()
	sc.health.LastError = err.Error()
	sc.healthMutex.Unlock()
}

func (sc *subscriber) recordTransientSubscriberError(err error) {
	if err == nil {
		return
	}
	sc.healthMutex.Lock()
	sc.health.LastTransientErrorAt = time.Now()
	sc.health.LastTransientError = err.Error()
	sc.healthMutex.Unlock()
}

func (sc *subscriber) setBacklogDepth(depth int) {
	sc.healthMutex.Lock()
	sc.health.BacklogDepth = depth
	sc.healthMutex.Unlock()
}

func (sc *subscriber) Health() SubscriberHealth {
	sc.healthMutex.RLock()
	health := sc.health
	health.Topics = append([]string(nil), sc.health.Topics...)
	health.LagByTopic = make(map[string]int64, len(sc.health.LagByTopic))
	for topic, lag := range sc.health.LagByTopic {
		health.LagByTopic[topic] = lag
	}
	sc.healthMutex.RUnlock()

	depth := health.BacklogDepth
	for _, ch := range sc.shardChannels {
		depth += len(ch)
	}
	health.QueueDepth = depth
	return health
}

func (sc *subscriber) ConfigureErrorPolicy(policy ErrorPolicy) {
	sc.errorPolicyMutex.Lock()
	sc.errorPolicy = policy
	sc.errorPolicyMutex.Unlock()
}

func (sc *subscriber) isNonRetryableError(err error) bool {
	if IsNonRetryable(err) {
		return true
	}

	sc.errorPolicyMutex.RLock()
	policy := sc.errorPolicy
	sc.errorPolicyMutex.RUnlock()
	if policy == nil {
		return false
	}
	return policy.IsNonRetryableError(err)
}

func (sc *subscriber) refreshLag(ctx context.Context) {
	consumer, ok := sc.Consumer.(lagAwareKafkaConsumer)
	if !ok {
		return
	}
	now := time.Now()
	last := time.Unix(0, sc.lastLagRefreshNano.Load())
	if !last.IsZero() && now.Sub(last) < 5*time.Second {
		return
	}
	sc.lastLagRefreshNano.Store(now.UnixNano())

	partitions := sc.assignments.Current()
	if len(partitions) == 0 {
		return
	}

	positions, err := consumer.Position(partitions)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to query Kafka subscriber positions")
		sc.recordSubscriberError(err)
		return
	}

	lagByTopic := make(map[string]int64)
	var maxLag int64
	for _, position := range positions {
		if position.Topic == nil || position.Offset < 0 {
			continue
		}
		_, high, err := consumer.GetWatermarkOffsets(*position.Topic, position.Partition)
		if err != nil {
			continue
		}
		lag := high - int64(position.Offset)
		if lag < 0 {
			lag = 0
		}
		if lag > lagByTopic[*position.Topic] {
			lagByTopic[*position.Topic] = lag
		}
		if lag > maxLag {
			maxLag = lag
		}
		metrics.Default().RecordKafkaConsumerLag(ctx, sc.groupID, *position.Topic, position.Partition, lag)
	}

	sc.healthMutex.Lock()
	sc.health.MaxLag = maxLag
	sc.health.LagByTopic = lagByTopic
	sc.healthMutex.Unlock()
}
