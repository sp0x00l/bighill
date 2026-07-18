package userevents

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"lib/shared_lib/metrics"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

const (
	SeverityInfo    = "INFO"
	SeveritySuccess = "SUCCESS"
	SeverityWarning = "WARNING"
	SeverityError   = "ERROR"

	EventTypeModelRegistered         = "model.registered"
	EventTypeModelStatusUpdated      = "model.status.updated"
	EventTypeModelServingRequested   = "model.serving.requested"
	EventTypeModelServingLoaded      = "model.serving.loaded"
	EventTypeModelServingFailed      = "model.serving.failed"
	EventTypeModelPromotionRequested = "model.promotion.requested"
	EventTypeModelPromotionPassed    = "model.promotion.passed"
	EventTypeModelPromotionFailed    = "model.promotion.failed"

	EventTypeTrainingRunRequested     = "training.run.requested"
	EventTypeTrainingRunPreparingData = "training.run.preparing_data"
	EventTypeTrainingRunTraining      = "training.run.training"
	EventTypeTrainingRunEvaluating    = "training.run.evaluating"
	EventTypeTrainingRunCompleted     = "training.run.completed"
	EventTypeTrainingRunFailed        = "training.run.failed"

	EventTypeDatasetRegistered                = "dataset.registered"
	EventTypeDatasetMaterializationRequested  = "dataset.materialization.requested"
	EventTypeDatasetMaterializationReady      = "dataset.materialization.ready"
	EventTypeDatasetMaterializationFailed     = "dataset.materialization.failed"
	EventTypeSnapshotRawReady                 = "snapshot.raw.ready"
	EventTypeSnapshotRawFailed                = "snapshot.raw.failed"
	EventTypeSnapshotFeatureReady             = "snapshot.feature.ready"
	EventTypeSnapshotFeatureFailed            = "snapshot.feature.failed"
	EventTypeSnapshotEmbeddingReady           = "snapshot.embedding.ready"
	EventTypeSnapshotEmbeddingFailed          = "snapshot.embedding.failed"
	EventTypeSnapshotGraphReady               = "snapshot.graph.ready"
	EventTypeSnapshotGraphFailed              = "snapshot.graph.failed"
	EventTypeUploadAccepted                   = "upload.accepted"
	EventTypeUploadCompleted                  = "upload.completed"
	EventTypeUploadFailed                     = "upload.failed"
	EventTypeInferencePreferenceDatasetReady  = "inference.preference_dataset.ready"
	EventTypeInferencePreferenceDatasetFailed = "inference.preference_dataset.failed"

	EventTypeAgentRunStarted    = "agent.run.started"
	EventTypeAgentStepCompleted = "agent.step.completed"
	EventTypeAgentToolResult    = "agent.tool.result"
	EventTypeAgentRunCompleted  = "agent.run.completed"
	EventTypeAgentRunFailed     = "agent.run.failed"

	ResourceTypeModel    = "model"
	ResourceTypeTraining = "training_run"
	ResourceTypeDataset  = "dataset"
	ResourceTypeSnapshot = "snapshot"
	ResourceTypeUpload   = "upload"
	ResourceTypeAgentRun = "agent_run"

	StatusPhaseServing         = "SERVING"
	StatusPhaseTraining        = "TRAINING"
	StatusPhaseMaterialization = "MATERIALIZATION"
	StatusPhaseUpload          = "UPLOAD"
	StatusPhasePromotion       = "PROMOTION"
	StatusPhaseAgent           = "AGENT"
)

var (
	ErrInvalidEvent = errors.New("invalid user event")
)

type Publisher interface {
	Publish(ctx context.Context, event Event) error
	Close()
}

type Event struct {
	EventID            string            `json:"event_id"`
	OccurredAt         time.Time         `json:"occurred_at"`
	SourceService      string            `json:"source_service"`
	EventType          string            `json:"event_type"`
	Severity           string            `json:"severity"`
	RequiredPermission string            `json:"required_permission,omitempty"`
	UserID             string            `json:"user_id,omitempty"`
	OrgID              string            `json:"org_id,omitempty"`
	Resource           Resource          `json:"resource"`
	Status             Status            `json:"status"`
	Title              string            `json:"title"`
	Message            string            `json:"message"`
	ActionLabel        string            `json:"action_label,omitempty"`
	ActionHref         string            `json:"action_href,omitempty"`
	Error              *ErrorDetail      `json:"error,omitempty"`
	CorrelationID      string            `json:"correlation_id,omitempty"`
	TraceID            string            `json:"trace_id,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

type Resource struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
	Href string `json:"href,omitempty"`
}

type Status struct {
	State         string `json:"state,omitempty"`
	PreviousState string `json:"previous_state,omitempty"`
	Phase         string `json:"phase,omitempty"`
	Progress      int    `json:"progress,omitempty"`
}

type ErrorDetail struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	TechnicalDetail string `json:"technical_detail,omitempty"`
	Retryable       bool   `json:"retryable"`
	Remediation     string `json:"remediation,omitempty"`
}

func (e Event) Validate() error {
	log.Trace("Event Validate")

	if strings.TrimSpace(e.EventID) == "" {
		return fmt.Errorf("%w: event_id is required", ErrInvalidEvent)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(e.SourceService) == "" {
		return fmt.Errorf("%w: source_service is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(e.EventType) == "" {
		return fmt.Errorf("%w: event_type is required", ErrInvalidEvent)
	}
	if !IsKnownSeverity(e.Severity) {
		return fmt.Errorf("%w: invalid severity %q", ErrInvalidEvent, e.Severity)
	}
	if strings.TrimSpace(e.UserID) == "" && strings.TrimSpace(e.OrgID) == "" {
		return fmt.Errorf("%w: user_id or org_id is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(e.Resource.Type) == "" {
		return fmt.Errorf("%w: resource.type is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(e.Resource.ID) == "" {
		return fmt.Errorf("%w: resource.id is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(e.Title) == "" {
		return fmt.Errorf("%w: title is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(e.Message) == "" {
		return fmt.Errorf("%w: message is required", ErrInvalidEvent)
	}
	if e.Error != nil && strings.TrimSpace(e.Error.Code) == "" {
		return fmt.Errorf("%w: error.code is required", ErrInvalidEvent)
	}
	return nil
}

func (e Event) MarshalJSONPayload() ([]byte, error) {
	log.Trace("Event MarshalJSONPayload")

	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

func ParseEventPayload(payload []byte) (Event, error) {
	log.Trace("ParseEventPayload")

	var event Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return Event{}, err
	}
	if err := event.Validate(); err != nil {
		return Event{}, err
	}
	return event, nil
}

func IsKnownSeverity(severity string) bool {
	log.Trace("IsKnownSeverity")

	switch strings.TrimSpace(severity) {
	case SeverityInfo, SeveritySuccess, SeverityWarning, SeverityError:
		return true
	default:
		return false
	}
}

func EnsureEventDefaults(ctx context.Context, event Event) Event {
	log.Trace("EnsureEventDefaults")

	if event.EventID == "" {
		event.EventID = DeterministicEventID(event.Resource.Type, event.Resource.ID, event.EventType, event.Status.State, failureHash(event))
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if event.TraceID == "" {
		if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
			event.TraceID = spanContext.TraceID().String()
		}
	}
	if event.Metadata == nil {
		event.Metadata = map[string]string{}
	}
	return event
}

func DeterministicEventID(parts ...string) string {
	log.Trace("DeterministicEventID")

	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		normalized = append(normalized, trimmed)
	}
	sum := sha256.Sum256([]byte(strings.Join(normalized, ":")))
	return hex.EncodeToString(sum[:16])
}

func NewResource(resourceType string, id uuid.UUID, name string, href string) Resource {
	log.Trace("NewResource")

	return Resource{
		Type: strings.TrimSpace(resourceType),
		ID:   id.String(),
		Name: strings.TrimSpace(name),
		Href: strings.TrimSpace(href),
	}
}

func failureHash(event Event) string {
	log.Trace("failureHash")

	if event.Error != nil {
		return HashString(event.Error.Code + ":" + event.Error.TechnicalDetail)
	}
	return HashString(event.Status.State + ":" + event.Message)
}

func HashString(value string) string {
	log.Trace("HashString")

	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:8])
}

func SHA256String(value string) string {
	log.Trace("SHA256String")

	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])
}

func LogPublishFailure(ctx context.Context, err error, event Event) {
	log.Trace("LogPublishFailure")

	if err == nil {
		return
	}
	metrics.Default().RecordError(ctx, metrics.BoundaryRedis, "user_event_publish", metrics.ClassifyRedis(err), "")
	log.WithContext(ctx).
		WithError(err).
		WithField("event_type", event.EventType).
		WithField("event_id", event.EventID).
		Warn("user event publish failed")
}
