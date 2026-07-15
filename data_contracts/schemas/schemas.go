package schemas

import _ "embed"

//go:embed agent_spec.schema.json
var agentSpecSchema []byte

//go:embed golden_task.schema.json
var goldenTaskSchema []byte

//go:embed trajectory.schema.json
var trajectorySchema []byte

func AgentSpecSchema() []byte {
	out := make([]byte, len(agentSpecSchema))
	copy(out, agentSpecSchema)
	return out
}

func GoldenTaskSchema() []byte {
	out := make([]byte, len(goldenTaskSchema))
	copy(out, goldenTaskSchema)
	return out
}

func TrajectorySchema() []byte {
	out := make([]byte, len(trajectorySchema))
	copy(out, trajectorySchema)
	return out
}
