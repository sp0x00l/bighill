package prompts

import _ "embed"

//go:embed graph_extraction_prompt_v1.md
var graphExtractionPromptV1 []byte

func GraphExtractionPromptV1() []byte {
	out := make([]byte, len(graphExtractionPromptV1))
	copy(out, graphExtractionPromptV1)
	return out
}
