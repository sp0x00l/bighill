package schemas

import _ "embed"

//go:embed agent_spec.schema.json
var agentSpecSchema []byte

//go:embed graph_extraction_v1.schema.json
var graphExtractionV1Schema []byte

func AgentSpecSchema() []byte {
	out := make([]byte, len(agentSpecSchema))
	copy(out, agentSpecSchema)
	return out
}

func GraphExtractionV1Schema() []byte {
	out := make([]byte, len(graphExtractionV1Schema))
	copy(out, graphExtractionV1Schema)
	return out
}
