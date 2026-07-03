package executor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"training_service/pkg/domain/model"
)

type trainingJobContract struct {
	GoTrainingEnvKeys               []string `json:"go_training_env_keys"`
	GoEvaluationEnvKeys             []string `json:"go_evaluation_env_keys"`
	PythonTrainingRequiredEnvKeys   []string `json:"python_training_required_env_keys"`
	PythonTrainingOptionalEnvKeys   []string `json:"python_training_optional_env_keys"`
	PythonEvaluationRequiredEnvKeys []string `json:"python_evaluation_required_env_keys"`
	PythonEvaluationOptionalEnvKeys []string `json:"python_evaluation_optional_env_keys"`
	EnvKeyContract                  map[string]struct {
		Direction string `json:"direction"`
		Type      string `json:"type"`
	} `json:"env_key_contract"`
	TrainingManifestKeys         []string          `json:"training_manifest_keys"`
	EvaluationManifestKeys       []string          `json:"evaluation_manifest_keys"`
	TrainingManifestFieldTypes   map[string]string `json:"training_manifest_field_types"`
	EvaluationManifestFieldTypes map[string]string `json:"evaluation_manifest_field_types"`
}

func TestTrainingJobContract(t *testing.T) {
	spec := readTrainingJobContract(t)

	assertStringSliceEqual(t, sortedMapKeys(trainingEnv(model.TrainingJobSpec{})), spec.GoTrainingEnvKeys)
	assertStringSliceEqual(t, sortedMapKeys(evaluationEnv(model.EvaluationJobSpec{})), spec.GoEvaluationEnvKeys)
	for _, key := range spec.PythonTrainingRequiredEnvKeys {
		assertContains(t, spec.GoTrainingEnvKeys, key)
	}
	for _, key := range spec.PythonEvaluationRequiredEnvKeys {
		assertContains(t, spec.GoEvaluationEnvKeys, key)
	}
	for _, key := range append(append([]string{}, spec.PythonTrainingRequiredEnvKeys...), spec.PythonTrainingOptionalEnvKeys...) {
		assertEnvKeyContract(t, spec, key)
	}
	for _, key := range append(append([]string{}, spec.PythonEvaluationRequiredEnvKeys...), spec.PythonEvaluationOptionalEnvKeys...) {
		assertEnvKeyContract(t, spec, key)
	}
	for _, key := range append(append([]string{}, spec.GoTrainingEnvKeys...), spec.GoEvaluationEnvKeys...) {
		assertEnvKeyContract(t, spec, key)
	}
	assertStringSliceEqual(t, jsonFieldNames[model.TrainedModelArtifact](), spec.TrainingManifestKeys)
	assertStringSliceEqual(t, jsonFieldNames[model.EvaluationReport](), spec.EvaluationManifestKeys)
	assertStringMapEqual(t, jsonFieldTypes[model.TrainedModelArtifact](), spec.TrainingManifestFieldTypes)
	assertStringMapEqual(t, jsonFieldTypes[model.EvaluationReport](), spec.EvaluationManifestFieldTypes)
}

func readTrainingJobContract(t *testing.T) trainingJobContract {
	t.Helper()

	path := filepath.Join("..", "..", "..", "..", "training_jobs", "contracts", "training_job_contract.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read training job contract: %v", err)
	}
	var spec trainingJobContract
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("decode training job contract: %v", err)
	}
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

func assertContains(t *testing.T, values []string, expected string) {
	t.Helper()

	for _, value := range values {
		if value == expected {
			return
		}
	}
	t.Fatalf("expected %q in %v", expected, values)
}

func assertEnvKeyContract(t *testing.T, spec trainingJobContract, key string) {
	t.Helper()

	contract, ok := spec.EnvKeyContract[key]
	if !ok {
		t.Fatalf("expected env key contract for %q", key)
	}
	if contract.Direction == "" {
		t.Fatalf("expected env key %q to define a direction", key)
	}
	if contract.Type != "string" {
		t.Fatalf("expected env key %q to be string, got %q", key, contract.Type)
	}
}

func assertStringSliceEqual(t *testing.T, actual []string, expected []string) {
	t.Helper()

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}

func assertStringMapEqual(t *testing.T, actual map[string]string, expected map[string]string) {
	t.Helper()

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
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
