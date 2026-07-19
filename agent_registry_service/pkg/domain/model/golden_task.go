package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type GoldenTaskSplit int

const (
	GoldenTaskSplitUnknown GoldenTaskSplit = iota
	GoldenTaskSplitSeedTrain
	GoldenTaskSplitDevEval
	GoldenTaskSplitPromotionHoldout
)

func (s GoldenTaskSplit) String() string {
	switch s {
	case GoldenTaskSplitSeedTrain:
		return "SEED_TRAIN"
	case GoldenTaskSplitDevEval:
		return "DEV_EVAL"
	case GoldenTaskSplitPromotionHoldout:
		return "PROMOTION_HOLDOUT"
	default:
		return "UNKNOWN"
	}
}

func ToGoldenTaskSplit(value string) (GoldenTaskSplit, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "SEED_TRAIN":
		return GoldenTaskSplitSeedTrain, nil
	case "DEV_EVAL":
		return GoldenTaskSplitDevEval, nil
	case "PROMOTION_HOLDOUT":
		return GoldenTaskSplitPromotionHoldout, nil
	default:
		return GoldenTaskSplitUnknown, fmt.Errorf("invalid golden task split %q", value)
	}
}

type GoldenTask struct {
	TaskID                   uuid.UUID
	OrgID                    uuid.UUID
	AgentLineage             string
	Split                    GoldenTaskSplit
	SplitVersion             int
	GroupKey                 string
	Prompt                   string
	NormalizedPromptHash     string
	ContentFingerprint       string
	NearDuplicateFingerprint string
	ExpectedToolPlanHash     string
	ExpectedAnswer           string
	ExpectedAnswerRubricID   string
	LabelsHash               string
	CreatedByUserID          uuid.UUID
	CreatedAt                time.Time
}

type GoldenTaskInput struct {
	GroupKey               string
	Prompt                 string
	ExpectedToolPlanHash   string
	ExpectedAnswer         string
	ExpectedAnswerRubricID string
	LabelsHash             string
}

type ImportGoldenTasksCommand struct {
	OrgID        uuid.UUID
	UserID       uuid.UUID
	AgentLineage string
	Split        GoldenTaskSplit
	SplitVersion int
	Tasks        []GoldenTaskInput
}

type ListGoldenTasksCommand struct {
	OrgID        uuid.UUID
	AgentLineage string
	Split        GoldenTaskSplit
	SplitVersion int
}

type GoldenTaskLeakConflict struct {
	TaskID                   uuid.UUID
	Split                    GoldenTaskSplit
	GroupKey                 string
	ContentFingerprint       string
	NearDuplicateFingerprint string
}
