package messaging_test

import (
	"context"
	"errors"

	agentapp "agent_registry_service/pkg/app"
	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	agentmessaging "agent_registry_service/pkg/infra/network/messaging"
	trainingpb "lib/data_contracts_lib/training"
	sharedmessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ agentapp.AgentRegistryUsecase = (*trainingSubscriberUsecaseStub)(nil)

var _ = Describe("Agent training event subscribers", func() {
	Describe("completed training events", func() {
		It("exposes the model training completed message type", func() {
			listener := agentmessaging.NewAgentTrainingCompletedEventListener(&trainingSubscriberUsecaseStub{})

			Expect(listener.MsgType()).To(Equal(sharedmessaging.MsgTypeModelTrainingCompleted))
			Expect(listener.NewMessage()).To(Equal(&trainingpb.ModelTrainingCompletedEvent{}))
		})

		It("maps completed training events into the agent registry usecase", func() {
			trainingRunID := uuid.New()
			orgID := uuid.New()
			modelID := uuid.New()
			usecase := &trainingSubscriberUsecaseStub{}
			listener := agentmessaging.NewAgentTrainingCompletedEventListener(usecase)

			err := listener.Handle(context.Background(), uuid.New(), validTrainingCompletedEvent(trainingRunID, orgID, modelID))

			Expect(err).NotTo(HaveOccurred())
			Expect(usecase.completionCalled).To(BeTrue())
			Expect(usecase.completion.TrainingRunID).To(Equal(trainingRunID))
			Expect(usecase.completion.OrgID).To(Equal(orgID))
			Expect(usecase.completion.ServingModelID).To(Equal(modelID))
			Expect(usecase.completion.AdapterURI).To(Equal("s3://models/adapters/agent"))
			Expect(usecase.completion.AdapterChecksum).To(Equal("sha256:adapter"))
			Expect(usecase.completion.TrainingProvider).To(Equal("training-service-agent-adapter"))
		})

		It("marks malformed completed training events as non-retryable", func() {
			trainingRunID := uuid.New()
			orgID := uuid.New()
			modelID := uuid.New()
			usecase := &trainingSubscriberUsecaseStub{}
			listener := agentmessaging.NewAgentTrainingCompletedEventListener(usecase)
			payload := validTrainingCompletedEvent(trainingRunID, orgID, modelID)
			payload.AdapterUri = ""

			err := listener.Handle(context.Background(), uuid.New(), payload)

			Expect(err).To(HaveOccurred())
			Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
			Expect(usecase.completionCalled).To(BeFalse())
		})

		It("propagates completed training usecase errors as retryable", func() {
			storeErr := errors.New("database unavailable")
			usecase := &trainingSubscriberUsecaseStub{err: storeErr}
			listener := agentmessaging.NewAgentTrainingCompletedEventListener(usecase)

			err := listener.Handle(context.Background(), uuid.New(), validTrainingCompletedEvent(uuid.New(), uuid.New(), uuid.New()))

			Expect(err).To(Equal(storeErr))
			Expect(sharedmessaging.IsNonRetryable(err)).To(BeFalse())
		})

		It("ignores completed training events that do not belong to an agent adapter", func() {
			usecase := &trainingSubscriberUsecaseStub{err: domain.ErrAgentAdapterNotFound}
			listener := agentmessaging.NewAgentTrainingCompletedEventListener(usecase)

			err := listener.Handle(context.Background(), uuid.New(), validTrainingCompletedEvent(uuid.New(), uuid.New(), uuid.New()))

			Expect(err).NotTo(HaveOccurred())
			Expect(usecase.completionCalled).To(BeTrue())
		})
	})

	Describe("failed training events", func() {
		It("exposes the model training failed message type", func() {
			listener := agentmessaging.NewAgentTrainingFailedEventListener(&trainingSubscriberUsecaseStub{})

			Expect(listener.MsgType()).To(Equal(sharedmessaging.MsgTypeModelTrainingFailed))
			Expect(listener.NewMessage()).To(Equal(&trainingpb.ModelTrainingFailedEvent{}))
		})

		It("maps failed training events into the agent registry usecase", func() {
			trainingRunID := uuid.New()
			orgID := uuid.New()
			usecase := &trainingSubscriberUsecaseStub{}
			listener := agentmessaging.NewAgentTrainingFailedEventListener(usecase)

			err := listener.Handle(context.Background(), uuid.New(), validTrainingFailedEvent(trainingRunID, orgID))

			Expect(err).NotTo(HaveOccurred())
			Expect(usecase.failureCalled).To(BeTrue())
			Expect(usecase.failure.TrainingRunID).To(Equal(trainingRunID))
			Expect(usecase.failure.OrgID).To(Equal(orgID))
			Expect(usecase.failure.FailureReason).To(Equal("ray job failed"))
		})

		It("marks malformed failed training events as non-retryable", func() {
			trainingRunID := uuid.New()
			orgID := uuid.New()
			usecase := &trainingSubscriberUsecaseStub{}
			listener := agentmessaging.NewAgentTrainingFailedEventListener(usecase)
			payload := validTrainingFailedEvent(trainingRunID, orgID)
			payload.FailureReason = ""

			err := listener.Handle(context.Background(), uuid.New(), payload)

			Expect(err).To(HaveOccurred())
			Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
			Expect(usecase.failureCalled).To(BeFalse())
		})

		It("propagates failed training usecase errors as retryable", func() {
			storeErr := errors.New("database unavailable")
			usecase := &trainingSubscriberUsecaseStub{err: storeErr}
			listener := agentmessaging.NewAgentTrainingFailedEventListener(usecase)

			err := listener.Handle(context.Background(), uuid.New(), validTrainingFailedEvent(uuid.New(), uuid.New()))

			Expect(err).To(Equal(storeErr))
			Expect(sharedmessaging.IsNonRetryable(err)).To(BeFalse())
		})

		It("ignores failed training events that do not belong to an agent adapter", func() {
			usecase := &trainingSubscriberUsecaseStub{err: domain.ErrAgentAdapterNotFound}
			listener := agentmessaging.NewAgentTrainingFailedEventListener(usecase)

			err := listener.Handle(context.Background(), uuid.New(), validTrainingFailedEvent(uuid.New(), uuid.New()))

			Expect(err).NotTo(HaveOccurred())
			Expect(usecase.failureCalled).To(BeTrue())
		})
	})
})

type trainingSubscriberUsecaseStub struct {
	completionCalled bool
	failureCalled    bool
	completion       model.AgentAdapterTrainingCompletion
	failure          model.AgentAdapterTrainingFailure
	err              error
}

func (s *trainingSubscriberUsecaseStub) RegisterAgentSpecVersion(context.Context, model.RegisterAgentSpecVersionCommand) (*model.AgentSpecVersion, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) RegisterEndpointBinding(context.Context, model.RegisterEndpointBindingCommand) (*model.AgentEndpointBinding, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) PromoteSpecChampion(context.Context, model.PromoteSpecChampionCommand) (*model.AgentChampionState, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) ImportGoldenTasks(context.Context, model.ImportGoldenTasksCommand) ([]*model.GoldenTask, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) ListGoldenTasks(context.Context, model.ListGoldenTasksCommand) ([]*model.GoldenTask, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) LabelAgentRun(context.Context, model.LabelAgentRunCommand) (*model.AgentRunLabel, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) ListAgentRunLabels(context.Context, model.ListAgentRunLabelsCommand) ([]*model.AgentRunLabel, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) BuildTrajectoryDataset(context.Context, model.BuildTrajectoryDatasetCommand) (*model.AgentTrajectoryDataset, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) ReadTrajectoryDataset(context.Context, uuid.UUID, uuid.UUID) (*model.AgentTrajectoryDataset, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) DispatchAgentAdapterTraining(context.Context, model.DispatchAgentAdapterTrainingCommand) (*model.AgentAdapter, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) RecordAgentAdapterTrainingCompleted(_ context.Context, completion model.AgentAdapterTrainingCompletion) (*model.AgentAdapter, error) {
	s.completionCalled = true
	s.completion = completion
	return nil, s.err
}

func (s *trainingSubscriberUsecaseStub) RecordAgentAdapterTrainingFailed(_ context.Context, failure model.AgentAdapterTrainingFailure) (*model.AgentAdapter, error) {
	s.failureCalled = true
	s.failure = failure
	return nil, s.err
}

func (s *trainingSubscriberUsecaseStub) ReadAgentAdapter(context.Context, uuid.UUID, uuid.UUID) (*model.AgentAdapter, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) EvaluateAdapterCandidate(context.Context, model.EvaluateAdapterCandidateCommand) (*model.AgentEvalReport, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) PromoteAgentAdapter(context.Context, model.PromoteAgentAdapterCommand) (*model.AgentAdapter, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) EvaluateSpecChampion(context.Context, model.EvaluateSpecChampionCommand) (*model.AgentEvalReport, error) {
	return nil, nil
}

func (s *trainingSubscriberUsecaseStub) ReadAgentEvalReport(context.Context, uuid.UUID, uuid.UUID) (*model.AgentEvalReport, error) {
	return nil, nil
}

func validTrainingCompletedEvent(trainingRunID uuid.UUID, orgID uuid.UUID, modelID uuid.UUID) *trainingpb.ModelTrainingCompletedEvent {
	return &trainingpb.ModelTrainingCompletedEvent{
		TrainingRunId:    trainingRunID.String(),
		OrgId:            orgID.String(),
		ModelId:          modelID.String(),
		AdapterUri:       "s3://models/adapters/agent",
		ArtifactChecksum: "sha256:adapter",
	}
}

func validTrainingFailedEvent(trainingRunID uuid.UUID, orgID uuid.UUID) *trainingpb.ModelTrainingFailedEvent {
	return &trainingpb.ModelTrainingFailedEvent{
		TrainingRunId: trainingRunID.String(),
		OrgId:         orgID.String(),
		FailureReason: "ray job failed",
	}
}
