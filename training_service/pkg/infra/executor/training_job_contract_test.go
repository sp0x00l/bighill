package executor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"training_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type trainingJobContract struct {
	GoTrainingEnvKeys               []string `json:"go_training_env_keys"`
	GoEvaluationEnvKeys             []string `json:"go_evaluation_env_keys"`
	GoPromotionEnvKeys              []string `json:"go_promotion_env_keys"`
	PythonTrainingRequiredEnvKeys   []string `json:"python_training_required_env_keys"`
	PythonTrainingOptionalEnvKeys   []string `json:"python_training_optional_env_keys"`
	PythonEvaluationRequiredEnvKeys []string `json:"python_evaluation_required_env_keys"`
	PythonEvaluationOptionalEnvKeys []string `json:"python_evaluation_optional_env_keys"`
	PythonPromotionRequiredEnvKeys  []string `json:"python_promotion_required_env_keys"`
	PythonPromotionOptionalEnvKeys  []string `json:"python_promotion_optional_env_keys"`
	EnvKeyContract                  map[string]struct {
		Direction string `json:"direction"`
		Type      string `json:"type"`
	} `json:"env_key_contract"`
	TrainingManifestKeys         []string          `json:"training_manifest_keys"`
	EvaluationManifestKeys       []string          `json:"evaluation_manifest_keys"`
	PromotionManifestKeys        []string          `json:"promotion_manifest_keys"`
	TrainingManifestFieldTypes   map[string]string `json:"training_manifest_field_types"`
	EvaluationManifestFieldTypes map[string]string `json:"evaluation_manifest_field_types"`
	PromotionManifestFieldTypes  map[string]string `json:"promotion_manifest_field_types"`
}

var _ = Describe("Training job contract", func() {
	It("matches Go env keys, Python env keys, and manifest types", func() {
		spec := readTrainingJobContract()

		Expect(sortedMapKeys(trainingEnv(model.TrainingJobSpec{}))).To(Equal(spec.GoTrainingEnvKeys))
		Expect(sortedMapKeys(evaluationEnv(model.EvaluationJobSpec{}))).To(Equal(spec.GoEvaluationEnvKeys))
		Expect(sortedMapKeys(promotionReportEnv(model.PromotionReportJobSpec{}))).To(Equal(spec.GoPromotionEnvKeys))
		for _, key := range spec.PythonTrainingRequiredEnvKeys {
			expectContains(spec.GoTrainingEnvKeys, key)
		}
		for _, key := range spec.PythonEvaluationRequiredEnvKeys {
			expectContains(spec.GoEvaluationEnvKeys, key)
		}
		for _, key := range spec.PythonPromotionRequiredEnvKeys {
			expectContains(spec.GoPromotionEnvKeys, key)
		}
		for _, key := range append(append([]string{}, spec.PythonTrainingRequiredEnvKeys...), spec.PythonTrainingOptionalEnvKeys...) {
			expectEnvKeyContract(spec, key)
		}
		for _, key := range append(append([]string{}, spec.PythonEvaluationRequiredEnvKeys...), spec.PythonEvaluationOptionalEnvKeys...) {
			expectEnvKeyContract(spec, key)
		}
		for _, key := range append(append([]string{}, spec.PythonPromotionRequiredEnvKeys...), spec.PythonPromotionOptionalEnvKeys...) {
			expectEnvKeyContract(spec, key)
		}
		for _, key := range append(append(append([]string{}, spec.GoTrainingEnvKeys...), spec.GoEvaluationEnvKeys...), spec.GoPromotionEnvKeys...) {
			expectEnvKeyContract(spec, key)
		}
		Expect(jsonFieldNames[model.TrainedModelArtifact]()).To(Equal(spec.TrainingManifestKeys))
		Expect(jsonFieldNames[model.EvaluationReport]()).To(Equal(spec.EvaluationManifestKeys))
		Expect(jsonFieldNames[model.PromotionReport]()).To(Equal(spec.PromotionManifestKeys))
		Expect(jsonFieldTypes[model.TrainedModelArtifact]()).To(Equal(spec.TrainingManifestFieldTypes))
		Expect(jsonFieldTypes[model.EvaluationReport]()).To(Equal(spec.EvaluationManifestFieldTypes))
		Expect(jsonFieldTypes[model.PromotionReport]()).To(Equal(spec.PromotionManifestFieldTypes))
	})
})

func readTrainingJobContract() trainingJobContract {
	path := filepath.Join("..", "..", "..", "test", "training_jobs", "contracts", "training_job_contract.json")
	raw, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	var spec trainingJobContract
	Expect(json.Unmarshal(raw, &spec)).To(Succeed())
	return spec
}

func sortedMapKeys(values map[string]string) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func expectContains(values []string, expected string) {
	Expect(values).To(ContainElement(expected))
}

func expectEnvKeyContract(spec trainingJobContract, key string) {
	contract, ok := spec.EnvKeyContract[key]
	Expect(ok).To(BeTrue(), "expected env key contract for %q", key)
	Expect(contract.Direction).NotTo(BeEmpty(), "expected env key %q to define a direction", key)
	Expect(contract.Type).To(Equal("string"), "expected env key %q to be string", key)
}

func jsonFieldNames[T any]() []string {
	var zero T
	typ := reflect.TypeOf(zero)
	out := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if name == "" || name == "-" {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func jsonFieldTypes[T any]() map[string]string {
	var zero T
	typ := reflect.TypeOf(zero)
	out := make(map[string]string, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if name == "" || name == "-" {
			continue
		}
		out[name] = contractType(field.Type)
	}
	return out
}

func contractType(typ reflect.Type) string {
	if typ.Kind() == reflect.Map && typ.Key().Kind() == reflect.String && typ.Elem().Kind() == reflect.Float64 {
		return "map_string_float64"
	}
	switch typ.Kind() {
	case reflect.String:
		return "string"
	case reflect.Int64:
		return "int64"
	case reflect.Bool:
		return "bool"
	default:
		return typ.String()
	}
}
