package rest_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"
	"model_registry_service/pkg/infra/network/adapter"
	"model_registry_service/pkg/infra/network/rest"

	serializers "lib/shared_lib/serializer"
	transport "lib/shared_lib/transport"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModelRegistryRest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry rest unit test suite")
}

type modelUsecaseStub struct {
	models     []*model.Model
	model      *model.Model
	total      int
	listUserID uuid.UUID
	readUserID uuid.UUID
	readID     uuid.UUID
	filter     model.ListFilter
	listErr    error
	readErr    error
}

func (s *modelUsecaseStub) RegisterModel(context.Context, *model.Model, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (s *modelUsecaseStub) ReadModelSystem(context.Context, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (s *modelUsecaseStub) ReadModelForUser(_ context.Context, userID uuid.UUID, modelID uuid.UUID) (*model.Model, error) {
	s.readUserID = userID
	s.readID = modelID
	return s.model, s.readErr
}

func (s *modelUsecaseStub) ListModels(_ context.Context, userID uuid.UUID, _ transport.Pagination, filter model.ListFilter) ([]*model.Model, int, error) {
	s.listUserID = userID
	s.filter = filter
	return s.models, s.total, s.listErr
}

func (s *modelUsecaseStub) MarkModelReady(context.Context, uuid.UUID, string) (*model.Model, error) {
	return nil, nil
}

func (s *modelUsecaseStub) MarkModelFailed(context.Context, uuid.UUID, string) (*model.Model, error) {
	return nil, nil
}

func (s *modelUsecaseStub) RecordModelTrainingCompleted(context.Context, *model.Model, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (s *modelUsecaseStub) RecordModelTrainingFailed(context.Context, *model.Model, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (s *modelUsecaseStub) RecordModelArtifactIngested(context.Context, *model.Model, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (s *modelUsecaseStub) RecordModelServingStatus(context.Context, *model.ServedModelStatus, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (s *modelUsecaseStub) RecordPromotionReportReady(context.Context, model.PromotionReportResult, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (s *modelUsecaseStub) PromoteCandidate(context.Context, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

var _ = Describe("ModelHandlers", func() {
	var (
		ctx      context.Context
		userID   uuid.UUID
		orgID    uuid.UUID
		modelID  uuid.UUID
		usecase  *modelUsecaseStub
		handlers *rest.ModelHandlers
	)

	BeforeEach(func() {
		ctx = context.Background()
		userID = uuid.New()
		orgID = uuid.New()
		modelID = uuid.New()
		usecase = &modelUsecaseStub{}
		handlers = rest.NewModelHandlers(usecase, adapter.NewModelDTOAdapter(serializers.NewJSONSerializer()))
	})

	It("lists selectable models with pagination and filters", func() {
		usecase.models = []*model.Model{readyHFModel(modelID)}
		usecase.total = 1
		req := httptest.NewRequest(http.MethodGet, "/v1/models?source=HUGGING_FACE&kind=BASE&status=READY&limit=10&page=1", nil)
		req.Header.Set("X-User-ID", userID.String())
		req.Header.Set("X-Org-ID", orgID.String())

		resp, err := handlers.ListModels(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode()).To(Equal(http.StatusOK))
		Expect(resp.Payload()).To(MatchJSON(`{
			"resources":[{
				"id":"` + modelID.String() + `",
				"model_kind":"BASE",
				"source":"HUGGING_FACE",
				"name":"rag-e2e-huggingface-base",
				"lineage_name":"",
				"model_version":1,
				"base_model":"bighill/rag-e2e-huggingface-base",
				"artifact_location":"s3://local-dev-bucket/models/huggingface/model/snapshot",
				"artifact_format":"HF_MODEL",
				"artifact_checksum":"sha256:abc",
				"artifact_size_bytes":128,
				"serving_load_status":"LOADED",
				"status":"READY",
				"links":{"self":{"href":"/v1/models/` + modelID.String() + `"}}
			}],
			"metadata":{"total":1,"page":1,"limit":10}
		}`))
		Expect(usecase.listUserID).To(Equal(userID))
		Expect(usecase.filter.KindSet).To(BeTrue())
		Expect(usecase.filter.Kind).To(Equal(model.ModelKindBase))
		Expect(usecase.filter.SourceSet).To(BeTrue())
		Expect(usecase.filter.Source).To(Equal(model.ModelSourceHuggingFace))
		Expect(usecase.filter.StatusSet).To(BeTrue())
		Expect(usecase.filter.Status).To(Equal(model.ModelStatusReady))
	})

	It("rejects invalid model list filters", func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/models?source=unknown", nil)
		req.Header.Set("X-User-ID", userID.String())
		req.Header.Set("X-Org-ID", orgID.String())

		resp, err := handlers.ListModels(ctx, req)

		Expect(resp).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("Invalid model filters")))
	})

	It("rejects invalid pagination", func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/models?limit=0", nil)
		req.Header.Set("X-User-ID", userID.String())
		req.Header.Set("X-Org-ID", orgID.String())

		resp, err := handlers.ListModels(ctx, req)

		Expect(resp).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("Invalid pagination")))
	})

	It("reads a selected model for the authenticated user", func() {
		usecase.model = readyHFModel(modelID)
		req := httptest.NewRequest(http.MethodGet, "/v1/models/"+modelID.String(), nil)
		req.Header.Set("X-User-ID", userID.String())
		req.Header.Set("X-Org-ID", orgID.String())
		req = mux.SetURLVars(req, map[string]string{"modelId": modelID.String()})

		resp, err := handlers.ReadModel(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode()).To(Equal(http.StatusOK))
		Expect(resp.Payload()).To(ContainSubstring(`"id":"` + modelID.String() + `"`))
		Expect(resp.Payload()).To(ContainSubstring(`"source":"HUGGING_FACE"`))
		Expect(usecase.readUserID).To(Equal(userID))
		Expect(usecase.readID).To(Equal(modelID))
	})

	It("returns not found for a missing selected model", func() {
		usecase.readErr = domain.ErrModelNotFound
		req := httptest.NewRequest(http.MethodGet, "/v1/models/"+modelID.String(), nil)
		req.Header.Set("X-User-ID", userID.String())
		req.Header.Set("X-Org-ID", orgID.String())
		req = mux.SetURLVars(req, map[string]string{"modelId": modelID.String()})

		resp, err := handlers.ReadModel(ctx, req)

		Expect(resp).To(BeNil())
		Expect(err).To(MatchError("Model not found"))
	})

	It("wraps unexpected list errors", func() {
		usecase.listErr = errors.New("db unavailable")
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		req.Header.Set("X-User-ID", userID.String())
		req.Header.Set("X-Org-ID", orgID.String())

		resp, err := handlers.ListModels(ctx, req)

		Expect(resp).To(BeNil())
		Expect(err).To(MatchError("Failed to list models"))
	})
})

func readyHFModel(modelID uuid.UUID) *model.Model {
	return &model.Model{
		ModelID:           modelID,
		ModelKind:         model.ModelKindBase,
		Source:            model.ModelSourceHuggingFace,
		Name:              "rag-e2e-huggingface-base",
		ModelVersion:      1,
		BaseModel:         "bighill/rag-e2e-huggingface-base",
		ArtifactLocation:  "s3://local-dev-bucket/models/huggingface/model/snapshot",
		ArtifactFormat:    "HF_MODEL",
		ArtifactChecksum:  "sha256:abc",
		ArtifactSizeBytes: 128,
		ServingLoadStatus: model.ModelLoadStatusLoaded,
		Status:            model.ModelStatusReady,
	}
}
