package app_test

import (
	"context"
	"errors"

	"training_service/pkg/app"
	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StaticTrainingProfileCatalog", func() {
	var catalog app.TrainingProfileCatalog

	BeforeEach(func() {
		catalog = app.NewStaticTrainingProfileCatalog(
			[]model.TrainingProfile{{Name: "sft-default@v1", Trainer: "sft"}},
			"sft-default@v1",
			map[string]string{"ragas-default@v1": `{"version":"v1"}`},
			"ragas-default@v1",
		)
	})

	It("resolves default and explicit profiles", func() {
		trainingProfile, err := catalog.ResolveTrainingProfile(context.Background(), "")
		Expect(err).NotTo(HaveOccurred())
		Expect(trainingProfile.Name).To(Equal("sft-default@v1"))

		evaluationProfile, err := catalog.ResolveEvaluationProfile(context.Background(), "ragas-default@v1")
		Expect(err).NotTo(HaveOccurred())
		Expect(evaluationProfile).To(MatchJSON(`{"version":"v1"}`))
	})

	It("rejects unknown profiles", func() {
		_, err := catalog.ResolveTrainingProfile(context.Background(), "unknown")
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())

		_, err = catalog.ResolveEvaluationProfile(context.Background(), "unknown")
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects unpinned profile names", func() {
		_, err := catalog.ResolveTrainingProfile(context.Background(), "sft-default")
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())

		_, err = catalog.ResolveEvaluationProfile(context.Background(), "ragas-default")
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects unpinned default profile names", func() {
		catalog = app.NewStaticTrainingProfileCatalog(
			[]model.TrainingProfile{{Name: "sft-default", Trainer: "sft"}},
			"sft-default",
			map[string]string{"ragas-default": `{"version":"v1"}`},
			"ragas-default",
		)

		_, err := catalog.ResolveTrainingProfile(context.Background(), "")
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())

		_, err = catalog.ResolveEvaluationProfile(context.Background(), "")
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})
})
