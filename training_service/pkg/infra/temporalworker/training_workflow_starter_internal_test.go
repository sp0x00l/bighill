package temporalworker

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	enumspb "go.temporal.io/api/enums/v1"
)

var _ = Describe("workflowExecutionStatusString", func() {
	DescribeTable("maps Temporal workflow statuses to platform statuses",
		func(status enumspb.WorkflowExecutionStatus, expected string) {
			Expect(workflowExecutionStatusString(status)).To(Equal(expected))
		},
		Entry("running", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, workflowStatusRunning),
		Entry("completed", enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, workflowStatusCompleted),
		Entry("failed", enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, workflowStatusFailed),
		Entry("timed out", enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, workflowStatusTimedOut),
		Entry("continued as new", enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, workflowStatusContinuedAsNew),
		Entry("unspecified", enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, workflowStatusUnspecified),
	)
})
