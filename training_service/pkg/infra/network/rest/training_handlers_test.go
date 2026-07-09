package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	"lib/shared_lib/ctxutil"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training service REST unit test suite")
}

type trainingCommandUsecaseStub struct {
	command model.StartTrainingRunCommand
	tenant  uuid.UUID
	org     uuid.UUID
	result  *model.TrainingRunStartResult
	status  *model.TrainingRunStatusResult
	err     error
	readErr error
}

func (s *trainingCommandUsecaseStub) StartTrainingRun(ctx context.Context, command model.StartTrainingRunCommand) (*model.TrainingRunStartResult, error) {
	s.command = command
	if tenantID, ok := ctxutil.TenantID(ctx); ok {
		s.tenant = tenantID
	}
	if orgID, ok := ctxutil.OrgID(ctx); ok {
		s.org = orgID
	}
	return s.result, s.err
}

func (s *trainingCommandUsecaseStub) ReadTrainingRun(_ context.Context, trainingRunID uuid.UUID) (*model.TrainingRunStatusResult, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	if s.status != nil {
		return s.status, nil
	}
	return &model.TrainingRunStatusResult{TrainingRunID: trainingRunID.String(), Status: "RUNNING"}, nil
}

var _ = Describe("TrainingHandlers", func() {
	var (
		usecase   *trainingCommandUsecaseStub
		handlers  *TrainingHandlers
		userID    uuid.UUID
		orgID     uuid.UUID
		requestID uuid.UUID
	)

	BeforeEach(func() {
		usecase = &trainingCommandUsecaseStub{
			result: &model.TrainingRunStartResult{
				TrainingRunID: uuid.NewString(),
				StatusURL:     "/v1/private/training-runs/run",
			},
			status: &model.TrainingRunStatusResult{
				TrainingRunID: uuid.NewString(),
				Status:        "RUNNING",
			},
		}
		handlers = NewTrainingHandlers(usecase, NewTrainingRunDTOAdapter(serializers.NewJSONSerializer()))
		userID = uuid.New()
		orgID = uuid.New()
		requestID = uuid.New()
	})

	It("starts training runs with auth user and X-Request-ID idempotency", func() {
		datasetID := uuid.New()
		modelID := uuid.New()
		req := newTrainingRequest(`{"dataset_id":"`+datasetID.String()+`","source_model_id":"`+modelID.String()+`","training_profile":"sft-default@v1"}`, userID, orgID, requestID)

		res, err := handlers.StartTrainingRun(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusAccepted))
		Expect(usecase.tenant).To(Equal(userID))
		Expect(usecase.org).To(Equal(orgID))
		Expect(usecase.command.IdempotencyKey).To(Equal(requestID))
		Expect(usecase.command.DatasetID).To(Equal(datasetID))
		Expect(usecase.command.SourceModelID).To(Equal(modelID))
		var dto StartTrainingRunResponseDTO
		Expect(json.Unmarshal(res.Payload(), &dto)).To(Succeed())
		Expect(dto.TrainingRunID).To(Equal(usecase.result.TrainingRunID))
	})

	It("rejects missing idempotency headers", func() {
		req := newTrainingRequest(`{"dataset_id":"`+uuid.NewString()+`","source_model_id":"`+uuid.NewString()+`"}`, userID, orgID, uuid.Nil)
		req.Header.Del("X-Request-ID")

		_, err := handlers.StartTrainingRun(context.Background(), req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusBadRequest))
	})

	It("maps usecase validation failures to bad requests", func() {
		usecase.err = domain.ErrValidationFailed.Extend("dataset is not materialized")
		req := newTrainingRequest(`{"dataset_id":"`+uuid.NewString()+`","source_model_id":"`+uuid.NewString()+`"}`, userID, orgID, requestID)

		_, err := handlers.StartTrainingRun(context.Background(), req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusBadRequest))
	})

	It("maps dependency failures to bad gateway", func() {
		usecase.err = domain.ErrDependencyFailed.Extend("model registry unavailable")
		req := newTrainingRequest(`{"dataset_id":"`+uuid.NewString()+`","source_model_id":"`+uuid.NewString()+`"}`, userID, orgID, requestID)

		_, err := handlers.StartTrainingRun(context.Background(), req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusBadGateway))
	})

	It("reads training run status", func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/training-runs/"+usecase.status.TrainingRunID, nil)
		req = mux.SetURLVars(req, map[string]string{"trainingRunId": usecase.status.TrainingRunID})

		res, err := handlers.ReadTrainingRun(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		var dto TrainingRunStatusDTO
		Expect(json.Unmarshal(res.Payload(), &dto)).To(Succeed())
		Expect(dto.TrainingRunID).To(Equal(usecase.status.TrainingRunID))
		Expect(dto.Status).To(Equal("RUNNING"))
	})

	It("maps missing training runs to not found", func() {
		usecase.readErr = domain.ErrTrainingRunNotFound.Extend("missing")
		req := httptest.NewRequest(http.MethodGet, "/v1/training-runs/"+uuid.NewString(), nil)
		req = mux.SetURLVars(req, map[string]string{"trainingRunId": uuid.NewString()})

		_, err := handlers.ReadTrainingRun(context.Background(), req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusNotFound))
	})

	It("rejects malformed training run ids", func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/training-runs/not-a-uuid", nil)
		req = mux.SetURLVars(req, map[string]string{"trainingRunId": "not-a-uuid"})

		_, err := handlers.ReadTrainingRun(context.Background(), req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusBadRequest))
	})
})

func newTrainingRequest(body string, userID uuid.UUID, orgID uuid.UUID, requestID uuid.UUID) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/training-runs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if userID != uuid.Nil {
		req.Header.Set("X-User-ID", userID.String())
	}
	if orgID != uuid.Nil {
		req.Header.Set("X-Org-ID", orgID.String())
	}
	if requestID != uuid.Nil {
		req.Header.Set("X-Request-ID", requestID.String())
	}
	return req
}
