package messaging

import (
	"context"
	"fmt"
	"strings"

	"training_service/pkg/domain/model"

	modelregistrypb "lib/data_contracts_lib/model_registry"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type PromotionReportExecutor interface {
	RunPromotionReport(ctx context.Context, spec model.PromotionReportJobSpec) (*model.PromotionReport, error)
}

type PromotionReportPublisher interface {
	PublishPromotionReportReady(ctx context.Context, report *model.PromotionReport) error
}

type PromotionReportRunner struct {
	executor             PromotionReportExecutor
	publisher            PromotionReportPublisher
	reportURIPrefix      string
	artifactBucketRegion string
	promotionProfile     string
}

func NewPromotionReportRunner(executor PromotionReportExecutor, publisher PromotionReportPublisher, reportURIPrefix string, artifactBucketRegion string, promotionProfile string) *PromotionReportRunner {
	log.Trace("NewPromotionReportRunner")

	return &PromotionReportRunner{
		executor:             executor,
		publisher:            publisher,
		reportURIPrefix:      strings.TrimRight(strings.TrimSpace(reportURIPrefix), "/"),
		artifactBucketRegion: strings.TrimSpace(artifactBucketRegion),
		promotionProfile:     strings.TrimSpace(promotionProfile),
	}
}

func (r *PromotionReportRunner) RunPromotionReport(ctx context.Context, request model.PromotionReportJobSpec) error {
	log.Trace("PromotionReportRunner RunPromotionReport")

	report, err := r.executor.RunPromotionReport(ctx, request)
	if err != nil {
		return err
	}
	return r.publisher.PublishPromotionReportReady(ctx, report)
}

func (r *PromotionReportRunner) SpecFromEvent(resourceKey uuid.UUID, payload *modelregistrypb.PromotionRequestedEvent) (model.PromotionReportJobSpec, error) {
	log.Trace("PromotionReportRunner SpecFromEvent")

	if payload == nil {
		return model.PromotionReportJobSpec{}, fmt.Errorf("promotion requested payload is required")
	}
	modelID, err := msgConn.ParseUUID("model_id", payload.GetModelId())
	if err != nil {
		return model.PromotionReportJobSpec{}, err
	}
	if modelID != resourceKey {
		return model.PromotionReportJobSpec{}, fmt.Errorf("model id %s does not match resource key %s", modelID, resourceKey)
	}
	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return model.PromotionReportJobSpec{}, err
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return model.PromotionReportJobSpec{}, err
	}
	trainingRunID, err := msgConn.ParseUUID("training_run_id", payload.GetTrainingRunId())
	if err != nil {
		return model.PromotionReportJobSpec{}, err
	}
	candidateReportURI := strings.TrimSpace(payload.GetCandidateReportUri())
	if candidateReportURI == "" {
		return model.PromotionReportJobSpec{}, fmt.Errorf("candidate report uri is required")
	}
	candidateMetrics := strings.TrimSpace(payload.GetCandidateMetricsMetadata())
	if candidateMetrics == "" {
		return model.PromotionReportJobSpec{}, fmt.Errorf("candidate metrics metadata is required")
	}
	if r.reportURIPrefix == "" {
		return model.PromotionReportJobSpec{}, fmt.Errorf("promotion report uri prefix is required")
	}
	reportURI := fmt.Sprintf("%s/%s/promotion_report.json", r.reportURIPrefix, modelID)
	return model.PromotionReportJobSpec{
		UserID:                   userID.String(),
		OrgID:                    orgID.String(),
		ModelID:                  modelID.String(),
		TrainingRunID:            trainingRunID.String(),
		CandidateReportURI:       candidateReportURI,
		CandidateMetricsMetadata: candidateMetrics,
		ChampionModelID:          strings.TrimSpace(payload.GetChampionModelId()),
		ChampionReportURI:        strings.TrimSpace(payload.GetChampionReportUri()),
		ChampionMetricsMetadata:  strings.TrimSpace(payload.GetChampionMetricsMetadata()),
		PromotionProfile:         r.promotionProfile,
		ReportURI:                reportURI,
		ReportManifestURI:        reportURI,
		ArtifactBucketRegion:     r.artifactBucketRegion,
		SubmissionID:             "promotion-" + modelID.String(),
	}, nil
}

type promotionRequestedEventListener struct {
	runner *PromotionReportRunner
}

func NewPromotionRequestedEventListener(runner *PromotionReportRunner) *promotionRequestedEventListener {
	log.Trace("NewPromotionRequestedEventListener")

	return &promotionRequestedEventListener{
		runner: runner,
	}
}

func (l *promotionRequestedEventListener) MsgType() msgConn.MsgType {
	log.Trace("promotionRequestedEventListener MsgType")

	return msgConn.MsgTypePromotionRequested
}

func (l *promotionRequestedEventListener) NewMessage() *modelregistrypb.PromotionRequestedEvent {
	log.Trace("promotionRequestedEventListener NewMessage")

	return &modelregistrypb.PromotionRequestedEvent{}
}

func (l *promotionRequestedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *modelregistrypb.PromotionRequestedEvent) error {
	log.Trace("promotionRequestedEventListener Handle")

	spec, err := l.runner.SpecFromEvent(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	return l.runner.RunPromotionReport(ctx, spec)
}
