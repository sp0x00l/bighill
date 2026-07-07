package model

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Lineage struct {
	UserID uuid.UUID
	OrgID  uuid.UUID
	Name   string
}

type EvalMetrics struct {
	Passed           bool               `json:"passed"`
	Metrics          map[string]float64 `json:"metrics"`
	Thresholds       map[string]float64 `json:"thresholds"`
	ReportURI        string             `json:"report_uri"`
	EvaluatorName    string             `json:"evaluator_name"`
	EvaluatorVersion string             `json:"evaluator_version"`
	MetricSuite      string             `json:"metric_suite"`
	EvalDatasetURI   string             `json:"eval_dataset_uri"`
	EvalDatasetMode  string             `json:"eval_dataset_mode"`
	DeepchecksPassed bool               `json:"deepchecks_passed,omitempty"`
	DeepchecksURI    string             `json:"deepchecks_report_uri,omitempty"`
	EvidentlyPassed  bool               `json:"evidently_passed,omitempty"`
	EvidentlyURI     string             `json:"evidently_report_uri,omitempty"`
}

type PromotionReport struct {
	DeepchecksPassed bool
	EvidentlyPassed  bool
}

type PromotionReportResult struct {
	UserID              uuid.UUID
	OrgID               uuid.UUID
	ModelID             uuid.UUID
	TrainingRunID       uuid.UUID
	PromotionReportURI  string
	DeepchecksPassed    bool
	DeepchecksReportURI string
	EvidentlyPassed     bool
	EvidentlyReportURI  string
	Deltas              map[string]float64
	FailureReason       string
}

type GatePolicy struct {
	AbsoluteFloors       map[string]float64
	MinDeltaVsChampion   map[string]float64
	NoRegressMetrics     []string
	RequireEvalDataset   bool
	RequireDeepchecks    bool
	RequireEvidently     bool
	RejectBuiltinMetrics bool
}

type GateDecision struct {
	Promote bool
	Reason  string
	Deltas  map[string]float64
}

func DefaultGatePolicy() GatePolicy {
	log.Trace("DefaultGatePolicy")

	return GatePolicy{
		AbsoluteFloors: map[string]float64{
			"faithfulness":      0.6,
			"answer_relevancy":  0.6,
			"context_precision": 0.6,
		},
		MinDeltaVsChampion: map[string]float64{
			"faithfulness":      0,
			"answer_relevancy":  0,
			"context_precision": 0,
		},
		NoRegressMetrics: []string{
			"faithfulness",
			"answer_relevancy",
			"context_precision",
		},
		RequireEvalDataset:   true,
		RejectBuiltinMetrics: true,
	}
}

func LineageForModel(modelRecord *Model) Lineage {
	log.Trace("LineageForModel")

	return Lineage{
		UserID: modelRecord.UserID,
		OrgID:  modelRecord.OrgID,
		Name:   modelRecord.Name,
	}
}

func ParseEvalMetrics(metricsMetadata string) (*EvalMetrics, error) {
	log.Trace("ParseEvalMetrics")

	metadata := strings.TrimSpace(metricsMetadata)
	if metadata == "" {
		return nil, fmt.Errorf("metrics metadata is required")
	}
	var metrics EvalMetrics
	if err := json.Unmarshal([]byte(metadata), &metrics); err != nil {
		return nil, fmt.Errorf("parse metrics metadata: %w", err)
	}
	if len(metrics.Metrics) == 0 {
		return nil, fmt.Errorf("metrics metadata must include metrics")
	}
	return &metrics, nil
}

func EvaluatePromotion(candidate *EvalMetrics, champion *EvalMetrics, report *PromotionReport, policy GatePolicy) GateDecision {
	log.Trace("EvaluatePromotion")

	if candidate == nil {
		return reject("candidate metrics are required", nil)
	}
	if policy.RequireEvalDataset && strings.TrimSpace(candidate.EvalDatasetURI) == "" {
		return reject("candidate eval dataset uri is required", nil)
	}
	if policy.RejectBuiltinMetrics && isBuiltinEvaluator(candidate.EvaluatorName) {
		return reject("built-in artifact metrics cannot promote a candidate", nil)
	}
	if err := enforceFloors(candidate, policy.AbsoluteFloors); err != nil {
		return reject(err.Error(), nil)
	}
	if policy.RequireDeepchecks && !deepchecksPassed(candidate, report) {
		return reject("deepchecks report did not pass", nil)
	}
	if champion == nil {
		return GateDecision{Promote: true, Reason: "first model in lineage", Deltas: map[string]float64{}}
	}
	if policy.RequireEvidently && !evidentlyPassed(candidate, report) {
		return reject("evidently report did not pass", nil)
	}
	if err := comparableEvalSets(candidate, champion); err != nil {
		return GateDecision{
			Promote: true,
			Reason:  "champion metrics incomparable; floor-only: " + err.Error(),
			Deltas:  map[string]float64{},
		}
	}
	deltas, err := metricDeltas(candidate, champion, policy.NoRegressMetrics, policy.MinDeltaVsChampion)
	if err != nil {
		return reject(err.Error(), deltas)
	}
	return GateDecision{Promote: true, Reason: "candidate beats champion gate", Deltas: deltas}
}

func enforceFloors(metrics *EvalMetrics, floors map[string]float64) error {
	log.Trace("enforceFloors")

	for _, name := range sortedMetricNames(floors) {
		value, ok := metrics.Metrics[name]
		if !ok {
			return fmt.Errorf("candidate metric %s is missing", name)
		}
		if value < floors[name] {
			return fmt.Errorf("candidate metric %s %.4f is below floor %.4f", name, value, floors[name])
		}
	}
	return nil
}

func comparableEvalSets(candidate *EvalMetrics, champion *EvalMetrics) error {
	log.Trace("comparableEvalSets")

	if strings.TrimSpace(candidate.EvalDatasetURI) != strings.TrimSpace(champion.EvalDatasetURI) {
		return fmt.Errorf("candidate and champion eval dataset uri differ")
	}
	if strings.TrimSpace(candidate.MetricSuite) != strings.TrimSpace(champion.MetricSuite) {
		return fmt.Errorf("candidate and champion metric suite differ")
	}
	if strings.TrimSpace(candidate.EvaluatorVersion) != strings.TrimSpace(champion.EvaluatorVersion) {
		return fmt.Errorf("candidate and champion evaluator version differ")
	}
	return nil
}

func metricDeltas(candidate *EvalMetrics, champion *EvalMetrics, metrics []string, minimums map[string]float64) (map[string]float64, error) {
	log.Trace("metricDeltas")

	deltas := map[string]float64{}
	for _, name := range metrics {
		candidateValue, ok := candidate.Metrics[name]
		if !ok {
			return deltas, fmt.Errorf("candidate metric %s is missing", name)
		}
		championValue, ok := champion.Metrics[name]
		if !ok {
			return deltas, fmt.Errorf("champion metric %s is missing", name)
		}
		delta := candidateValue - championValue
		deltas[name] = delta
		if delta < minimums[name] {
			return deltas, fmt.Errorf("candidate metric %s regressed by %.4f", name, -delta)
		}
	}
	return deltas, nil
}

func isBuiltinEvaluator(name string) bool {
	log.Trace("isBuiltinEvaluator")

	switch strings.ToLower(strings.TrimSpace(name)) {
	case "built_in", "builtin", "builtin_artifact_evaluator":
		return true
	default:
		return false
	}
}

func deepchecksPassed(candidate *EvalMetrics, report *PromotionReport) bool {
	log.Trace("deepchecksPassed")

	if report != nil {
		return report.DeepchecksPassed
	}
	return candidate.DeepchecksPassed
}

func evidentlyPassed(candidate *EvalMetrics, report *PromotionReport) bool {
	log.Trace("evidentlyPassed")

	if report != nil {
		return report.EvidentlyPassed
	}
	return candidate.EvidentlyPassed
}

func sortedMetricNames(values map[string]float64) []string {
	log.Trace("sortedMetricNames")

	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func reject(reason string, deltas map[string]float64) GateDecision {
	log.Trace("reject")

	if deltas == nil {
		deltas = map[string]float64{}
	}
	return GateDecision{Promote: false, Reason: reason, Deltas: deltas}
}
