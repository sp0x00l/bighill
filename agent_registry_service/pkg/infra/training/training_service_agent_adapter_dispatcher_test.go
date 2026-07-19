package training_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	trainingadapter "agent_registry_service/pkg/infra/network/adapter"
	"agent_registry_service/pkg/infra/training"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTrainingServiceAgentAdapterDispatcher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Agent registry training dispatcher unit test suite")
}

var _ = Describe("TrainingServiceAgentAdapterDispatcher", func() {
	It("maps training service transport failures to the training domain error", func() {
		client := &http.Client{Transport: trainingRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial failed")
		})}
		dispatcher := training.NewTrainingServiceAgentAdapterDispatcherWithClient(training.TrainingServiceAgentAdapterDispatcherConfig{
			BaseURL:        "http://training.local",
			RequestTimeout: time.Second,
		}, client, trainingadapter.NewAgentAdapterTrainingDTOAdapter(serializers.NewJSONSerializer()))

		result, err := dispatcher.DispatchAgentAdapterTraining(context.Background(), model.AgentAdapterTrainingRequest{
			OrgID:           uuid.New(),
			UserID:          uuid.New(),
			DatasetID:       uuid.New(),
			DatasetURI:      "s3://bucket/dataset.jsonl",
			ContentHash:     "sha256:dataset",
			SourceModelID:   uuid.New(),
			AgentLineage:    "support-agent",
			TrainingProfile: "agent-sft",
		})

		Expect(result).To(BeNil())
		Expect(errors.Is(err, domain.ErrAgentTrainingFailed)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("dial failed")))
	})
})

type trainingRoundTripFunc func(*http.Request) (*http.Response, error)

func (f trainingRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
