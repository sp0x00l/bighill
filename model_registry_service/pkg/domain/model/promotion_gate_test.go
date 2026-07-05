package model_test

import (
	"model_registry_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PromotionGate", func() {
	policy := model.DefaultGatePolicy()

	candidateMetrics := func(values map[string]float64) *model.EvalMetrics {
		return &model.EvalMetrics{
			Metrics:          values,
			EvaluatorName:    "ragas",
			EvaluatorVersion: "ragas-v1",
			MetricSuite:      "rag",
			EvalDatasetURI:   "s3://evals/held-out.jsonl",
			EvalDatasetMode:  "labeled",
		}
	}

	It("promotes the first model in a lineage when floors pass", func() {
		decision := model.EvaluatePromotion(candidateMetrics(map[string]float64{
			"faithfulness":      0.82,
			"answer_relevancy":  0.83,
			"context_precision": 0.81,
		}), nil, nil, policy)

		Expect(decision.Promote).To(BeTrue())
		Expect(decision.Reason).To(Equal("first model in lineage"))
	})

	It("promotes a challenger that does not regress against the champion", func() {
		champion := candidateMetrics(map[string]float64{
			"faithfulness":      0.80,
			"answer_relevancy":  0.82,
			"context_precision": 0.81,
		})
		candidate := candidateMetrics(map[string]float64{
			"faithfulness":      0.84,
			"answer_relevancy":  0.82,
			"context_precision": 0.83,
		})

		decision := model.EvaluatePromotion(candidate, champion, nil, policy)

		Expect(decision.Promote).To(BeTrue())
		Expect(decision.Deltas).To(HaveKeyWithValue("faithfulness", BeNumerically("~", 0.04, 0.0001)))
	})

	It("rejects a challenger that regresses against the champion", func() {
		champion := candidateMetrics(map[string]float64{
			"faithfulness":      0.86,
			"answer_relevancy":  0.83,
			"context_precision": 0.81,
		})
		candidate := candidateMetrics(map[string]float64{
			"faithfulness":      0.82,
			"answer_relevancy":  0.84,
			"context_precision": 0.82,
		})

		decision := model.EvaluatePromotion(candidate, champion, nil, policy)

		Expect(decision.Promote).To(BeFalse())
		Expect(decision.Reason).To(ContainSubstring("faithfulness"))
	})

	It("rejects metrics below an absolute floor", func() {
		decision := model.EvaluatePromotion(candidateMetrics(map[string]float64{
			"faithfulness":      0.59,
			"answer_relevancy":  0.80,
			"context_precision": 0.80,
		}), nil, nil, policy)

		Expect(decision.Promote).To(BeFalse())
		Expect(decision.Reason).To(ContainSubstring("below floor"))
	})

	It("rejects built-in artifact metrics", func() {
		candidate := candidateMetrics(map[string]float64{
			"faithfulness":      1,
			"answer_relevancy":  1,
			"context_precision": 1,
		})
		candidate.EvaluatorName = "built_in"

		decision := model.EvaluatePromotion(candidate, nil, nil, policy)

		Expect(decision.Promote).To(BeFalse())
		Expect(decision.Reason).To(ContainSubstring("built-in artifact metrics"))
	})

	It("uses evidence flags from candidate metrics when the policy requires them", func() {
		strict := policy
		strict.RequireDeepchecks = true
		strict.RequireEvidently = true
		candidate := candidateMetrics(map[string]float64{
			"faithfulness":      0.95,
			"answer_relevancy":  0.95,
			"context_precision": 0.95,
		})
		candidate.DeepchecksPassed = true
		candidate.EvidentlyPassed = true

		decision := model.EvaluatePromotion(candidate, nil, nil, strict)

		Expect(decision.Promote).To(BeTrue())
	})

	It("falls back to absolute floors when champion metrics are incomparable", func() {
		champion := candidateMetrics(map[string]float64{
			"faithfulness":      0.80,
			"answer_relevancy":  0.82,
			"context_precision": 0.81,
		})
		champion.EvalDatasetURI = "s3://evals/old.jsonl"
		candidate := candidateMetrics(map[string]float64{
			"faithfulness":      0.95,
			"answer_relevancy":  0.95,
			"context_precision": 0.95,
		})

		decision := model.EvaluatePromotion(candidate, champion, nil, policy)

		Expect(decision.Promote).To(BeTrue())
		Expect(decision.Reason).To(ContainSubstring("floor-only"))
		Expect(decision.Reason).To(ContainSubstring("eval dataset uri differ"))
		Expect(decision.Deltas).To(BeEmpty())
	})

	It("requires Deepchecks and Evidently reports when the policy does", func() {
		strict := policy
		strict.RequireDeepchecks = true
		strict.RequireEvidently = true
		candidate := candidateMetrics(map[string]float64{
			"faithfulness":      0.95,
			"answer_relevancy":  0.95,
			"context_precision": 0.95,
		})

		champion := candidateMetrics(map[string]float64{
			"faithfulness":      0.90,
			"answer_relevancy":  0.90,
			"context_precision": 0.90,
		})

		decision := model.EvaluatePromotion(candidate, champion, &model.PromotionReport{DeepchecksPassed: true}, strict)

		Expect(decision.Promote).To(BeFalse())
		Expect(decision.Reason).To(ContainSubstring("evidently"))
	})

	It("parses valid metrics metadata and rejects missing metrics", func() {
		metrics, err := model.ParseEvalMetrics(`{"metrics":{"faithfulness":0.8},"eval_dataset_uri":"s3://evals"}`)

		Expect(err).NotTo(HaveOccurred())
		Expect(metrics.Metrics).To(HaveKeyWithValue("faithfulness", 0.8))

		_, err = model.ParseEvalMetrics(`{"passed":true}`)
		Expect(err).To(HaveOccurred())
	})

	It("formats and parses promotion decision outcomes", func() {
		Expect(model.PromotionDecisionOutcomeAccepted.String()).To(Equal("PROMOTION_ACCEPTED"))
		Expect(model.PromotionDecisionOutcomeRejected.String()).To(Equal("PROMOTION_REJECTED"))
		Expect(model.PromotionDecisionReason(model.PromotionDecisionOutcomeAccepted, "candidate beats champion gate")).To(Equal("PROMOTION_ACCEPTED: candidate beats champion gate"))

		outcome, err := model.ToPromotionDecisionOutcome("PROMOTION_REJECTED")
		Expect(err).NotTo(HaveOccurred())
		Expect(outcome).To(Equal(model.PromotionDecisionOutcomeRejected))

		_, err = model.ToPromotionDecisionOutcome("PROMOTED")
		Expect(err).To(HaveOccurred())
	})
})
