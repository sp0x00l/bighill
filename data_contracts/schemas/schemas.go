package schemas

import _ "embed"

//go:embed agent_spec.schema.json
var agentSpecSchema []byte

func AgentSpecSchema() []byte {
	out := make([]byte, len(agentSpecSchema))
	copy(out, agentSpecSchema)
	return out
}
