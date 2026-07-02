package app_test

import (
	"context"
	"strings"

	"inference_service/pkg/app"
	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("RAG pipeline", func() {
	It("packs retrieved contexts within chunk and character limits", func() {
		packer := app.NewContextWindowPacker(model.PromptStrategy{
			Version:          "test-prompt",
			MaxContextChars:  10,
			MaxContextChunks: 2,
		})

		contexts, err := packer.Pack(context.Background(), model.ContextPackRequest{
			Contexts: []model.RetrievedContext{
				{EmbeddingRecordID: uuid.New(), SourceText: "  "},
				{EmbeddingRecordID: uuid.New(), SourceText: "first context is long"},
				{EmbeddingRecordID: uuid.New(), SourceText: "second"},
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(contexts).To(HaveLen(1))
		Expect(contexts[0].SourceText).To(Equal("first cont"))
	})

	It("builds a prompt with dataset lineage and retrieved context", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		builder := app.NewDefaultPromptBuilder(model.PromptStrategy{
			Version:          "test-prompt",
			SystemPrompt:     "Use context only.",
			MaxContextChars:  1000,
			MaxContextChunks: 4,
		})

		prompt, err := builder.BuildPrompt(context.Background(), model.PromptBuildRequest{
			Query:   "what is stored?",
			Dataset: dataset,
			Model:   inferenceModel,
			Contexts: []model.RetrievedContext{{
				EmbeddingRecordID:   uuid.New(),
				EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
				ChunkIndex:          3,
				SourceText:          "The registry stores dataset lineage.",
				Similarity:          0.91,
			}},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(prompt.Strategy.Version).To(Equal("test-prompt"))
		Expect(prompt.Prompt).To(ContainSubstring("Use context only."))
		Expect(prompt.Prompt).To(ContainSubstring(dataset.EmbeddingSnapshotID.String()))
		Expect(prompt.Prompt).To(ContainSubstring("The registry stores dataset lineage."))
		Expect(strings.TrimSpace(prompt.Prompt)).To(HaveSuffix("Answer:"))
	})
})
