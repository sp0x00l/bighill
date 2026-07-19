package db_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("inference system context allowlist", func() {
	It("allows only audited production call sites", func() {
		_, filename, _, ok := runtime.Caller(0)
		Expect(ok).To(BeTrue())
		serviceRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", "..", ".."))
		allowed := map[string]int{
			"pkg/infra/repo/db/agent_trajectory_repository.go": 1,
		}

		violations := inferenceSystemContextViolations(serviceRoot, allowed)
		Expect(violations).To(BeEmpty(), strings.Join(violations, "\n"))
	})
})

func inferenceSystemContextViolations(serviceRoot string, allowed map[string]int) []string {
	violations := []string{}
	err := filepath.WalkDir(serviceRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "build", "node_modules", "__pycache__":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		count := strings.Count(string(content), "WithSystemContext")
		if count == 0 {
			return nil
		}
		rel, err := filepath.Rel(serviceRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if allowedCount, ok := allowed[rel]; !ok {
			violations = append(violations, fmt.Sprintf("WithSystemContext is not allowed in production file %s", rel))
		} else if count != allowedCount {
			violations = append(violations, fmt.Sprintf("WithSystemContext count changed in %s: got %d, want %d", rel, count, allowedCount))
		}
		return nil
	})
	Expect(err).NotTo(HaveOccurred())
	return violations
}
