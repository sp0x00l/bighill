package temporalworker

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
)

func TestWorkflowExecutionStatusString(t *testing.T) {
	tests := map[string]struct {
		status enumspb.WorkflowExecutionStatus
		want   string
	}{
		"running":          {status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, want: workflowStatusRunning},
		"completed":        {status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, want: workflowStatusCompleted},
		"failed":           {status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, want: workflowStatusFailed},
		"timed out":        {status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, want: workflowStatusTimedOut},
		"continued as new": {status: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, want: workflowStatusContinuedAsNew},
		"unspecified":      {status: enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, want: workflowStatusUnspecified},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := workflowExecutionStatusString(tt.status); got != tt.want {
				t.Fatalf("workflowExecutionStatusString() = %q, want %q", got, tt.want)
			}
		})
	}
}
