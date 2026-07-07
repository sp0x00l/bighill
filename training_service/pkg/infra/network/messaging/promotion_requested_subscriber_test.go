package messaging_test

import (
	"context"
	"errors"

	"training_service/pkg/domain/model"
	trainingmessaging "training_service/pkg/infra/network/messaging"

	modelregistrypb "lib/data_contracts_lib/model_registry"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type recordingPromotionExecutor struct {
	spec   model.PromotionReportJobSpec
	report *model.PromotionReport
	err    error
}

func (e *recordingPromotionExecutor) RunPromotionReport(_ context.Context, spec model.PromotionReportJobSpec) (*model.PromotionReport, error) {
	e.spec = spec
	return e.report, e.err
}

type recordingPromotionPublisher struct {
	report *model.PromotionReport
	err    error
}

func (p *recordingPromotionPublisher) PublishPromotionReportReady(_ context.Context, report *model.PromotionReport) error {
	p.report = report
	return p.err
}

var _ = Describe("PromotionRequestedEventListener", func() {
	It("runs a promotion report job and publishes the report fact", func() {
		modelID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		trainingRunID := uuid.New()
		executor := &recordingPromotionExecutor{report: &model.PromotionReport{
			UserID:             userID.String(),
			OrgID:              orgID.String(),
			ModelID:            modelID.String(),
			TrainingRunID:      trainingRunID.String(),
			PromotionReportURI: "s3://promotion/" + modelID.String() + "/promotion_report.json",
		}}
		publisher := &recordingPromotionPublisher{}
		runner := trainingmessaging.NewPromotionReportRunner(executor, publisher, "s3://promotion", "eu-west-1", `{"promotion":{"require_deepchecks":true}}`)
		listener := trainingmessaging.NewPromotionRequestedEventListener(runner)

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.PromotionRequestedEvent{
			UserId:                   userID.String(),
			OrgId:                    orgID.String(),
			ModelId:                  modelID.String(),
			TrainingRunId:            trainingRunID.String(),
			DatasetId:                uuid.NewString(),
			ModelName:                "movie-ranker",
			ModelVersion:             7,
			CandidateReportUri:       "s3://evals/candidate.json",
			CandidateMetricsMetadata: `{"metrics":{"faithfulness":0.9}}`,
			ChampionModelId:          uuid.NewString(),
			ChampionReportUri:        "s3://evals/champion.json",
			ChampionMetricsMetadata:  `{"metrics":{"faithfulness":0.8}}`,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(executor.spec.UserID).To(Equal(userID.String()))
		Expect(executor.spec.OrgID).To(Equal(orgID.String()))
		Expect(executor.spec.ModelID).To(Equal(modelID.String()))
		Expect(executor.spec.TrainingRunID).To(Equal(trainingRunID.String()))
		Expect(executor.spec.CandidateReportURI).To(Equal("s3://evals/candidate.json"))
		Expect(executor.spec.ReportURI).To(Equal("s3://promotion/" + modelID.String() + "/promotion_report.json"))
		Expect(executor.spec.ArtifactBucketRegion).To(Equal("eu-west-1"))
		Expect(publisher.report).To(Equal(executor.report))
	})

	It("rejects malformed promotion requests as non-retryable", func() {
		modelID := uuid.New()
		runner := trainingmessaging.NewPromotionReportRunner(&recordingPromotionExecutor{}, &recordingPromotionPublisher{}, "s3://promotion", "eu-west-1", "{}")
		listener := trainingmessaging.NewPromotionRequestedEventListener(runner)

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.PromotionRequestedEvent{
			UserId:  uuid.NewString(),
			OrgId:   uuid.NewString(),
			ModelId: uuid.NewString(),
		})

		Expect(err).To(HaveOccurred())
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
	})

	It("rejects promotion requests when the report uri prefix is not configured", func() {
		modelID := uuid.New()
		runner := trainingmessaging.NewPromotionReportRunner(&recordingPromotionExecutor{}, &recordingPromotionPublisher{}, "", "eu-west-1", "{}")
		listener := trainingmessaging.NewPromotionRequestedEventListener(runner)

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.PromotionRequestedEvent{
			UserId:                   uuid.NewString(),
			OrgId:                    uuid.NewString(),
			ModelId:                  modelID.String(),
			TrainingRunId:            uuid.NewString(),
			CandidateReportUri:       "s3://evals/candidate.json",
			CandidateMetricsMetadata: `{"metrics":{"faithfulness":0.9}}`,
		})

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(ContainSubstring("promotion report uri prefix is required")))
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
	})

	It("propagates promotion report execution failures", func() {
		modelID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		trainingRunID := uuid.New()
		expectedErr := errors.New("ray unavailable")
		runner := trainingmessaging.NewPromotionReportRunner(&recordingPromotionExecutor{err: expectedErr}, &recordingPromotionPublisher{}, "s3://promotion", "eu-west-1", "{}")
		listener := trainingmessaging.NewPromotionRequestedEventListener(runner)

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.PromotionRequestedEvent{
			UserId:                   userID.String(),
			OrgId:                    orgID.String(),
			ModelId:                  modelID.String(),
			TrainingRunId:            trainingRunID.String(),
			CandidateReportUri:       "s3://evals/candidate.json",
			CandidateMetricsMetadata: `{"metrics":{"faithfulness":0.9}}`,
		})

		Expect(errors.Is(err, expectedErr)).To(BeTrue())
	})
})
