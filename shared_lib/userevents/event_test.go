package userevents_test

import (
	"context"
	"errors"
	"time"

	"lib/shared_lib/userevents"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("User event envelope", func() {
	validEvent := func() userevents.Event {
		return userevents.Event{
			EventID:       "event-1",
			OccurredAt:    time.Unix(100, 0).UTC(),
			SourceService: "test_service",
			EventType:     userevents.EventTypeModelServingFailed,
			Severity:      userevents.SeverityError,
			UserID:        "user-1",
			OrgID:         "org-1",
			Resource: userevents.Resource{
				Type: userevents.ResourceTypeModel,
				ID:   "model-1",
				Name: "support-model",
				Href: "/models/model-1",
			},
			Status: userevents.Status{
				State:         "FAILED",
				PreviousState: "LOADING",
				Phase:         userevents.StatusPhaseServing,
			},
			Title:   "Model serving failed",
			Message: "The model could not be exposed as a chat model.",
			Error: &userevents.ErrorDetail{
				Code:      userevents.ErrorCodeModelServingChatDefinitionUnusable,
				Message:   "The model could not be exposed as a chat model.",
				Retryable: false,
			},
		}
	}

	It("validates a complete event", func() {
		Expect(validEvent().Validate()).To(Succeed())
	})

	It("rejects missing routing identity", func() {
		event := validEvent()
		event.UserID = ""
		event.OrgID = ""
		Expect(event.Validate()).To(MatchError(ContainSubstring("user_id or org_id is required")))
	})

	It("creates deterministic event ids", func() {
		first := userevents.DeterministicEventID("model", "abc", "FAILED", "reason")
		second := userevents.DeterministicEventID("model", "abc", "FAILED", "reason")
		other := userevents.DeterministicEventID("model", "abc", "READY", "reason")
		Expect(first).To(Equal(second))
		Expect(first).NotTo(Equal(other))
	})

	It("sanitizes technical details", func() {
		event := validEvent()
		event.Error.TechnicalDetail = "failed at /Users/person/secret/model.gguf?token=abc&password=def"
		sanitized := userevents.SanitizeEvent(event)
		Expect(sanitized.Error.TechnicalDetail).To(ContainSubstring("<path>"))
		Expect(sanitized.Error.TechnicalDetail).To(ContainSubstring("token=<redacted>"))
		Expect(sanitized.Error.TechnicalDetail).To(ContainSubstring("password=<redacted>"))
		Expect(sanitized.Error.TechnicalDetail).NotTo(ContainSubstring("abc"))
		Expect(sanitized.Error.TechnicalDetail).NotTo(ContainSubstring("def"))
	})

	It("classifies Ollama chat-definition failures", func() {
		detail := userevents.ClassifyError(userevents.ClassificationInput{
			RawFailureReason: "Ollama did not infer a usable chat model from GGUF metadata",
		})
		Expect(detail.Code).To(Equal(userevents.ErrorCodeModelServingChatDefinitionUnusable))
		Expect(detail.Retryable).To(BeFalse())
		Expect(detail.Message).To(Equal("The model could not be exposed as a chat model."))
	})

	It("records events in the recording publisher", func() {
		publisher := userevents.NewRecordingPublisher()
		Expect(publisher.Publish(context.Background(), validEvent())).To(Succeed())
		Expect(publisher.Events()).To(HaveLen(1))
	})

	It("returns recording publisher errors without mutating events", func() {
		publisher := userevents.NewRecordingPublisher()
		publisher.SetError(errors.New("boom"))
		Expect(publisher.Publish(context.Background(), validEvent())).To(MatchError("boom"))
		Expect(publisher.Events()).To(BeEmpty())
	})
})
