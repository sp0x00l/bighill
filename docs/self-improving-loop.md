# The Self-Improving Loop

## What it is

A closed feedback-to-model loop: user feedback on generated answers is turned into
preference data, used to train a challenger model with DPO, evaluated against the
current champion on a frozen held-out eval set, and — only if it passes a fail-closed
gate — promoted to serve live traffic. The goal is a model per lineage that measurably
improves generation after generation, without regressing safety/grounding.

This document describes the loop as it exists in the code today, the guarantees each
stage provides, and the parts that are **not yet built**. It is deliberately honest
about the gaps: "self-improving" is the single biggest papering-over risk in an ML
platform, so every claim below is tied to a concrete file.

## The spine (end to end)

```
Generate (inference_service)
  └─ RecordFeedback ─────────────► inference_feedback + preference_examples   (per answer)
        └─ BuildPreferenceDataset ─► PreferenceDataset (train/eval split, frozen eval set)
              └─ preference_dataset_snapshots
                    └─ POST /v1/private/training-runs/dpo ─► DPO training run (Temporal + Ray)
                          └─ TrainingCompletedEvent ─► model_registry_service
                                └─ createCandidateModel(champion) + PromotionRequested
                                      └─ PromotionReport (Deepchecks / Evidently, Ragas eval)
                                            └─ evaluateCandidatePromotion (gate)
                                                  └─ PromoteCandidate ─► serving swap
```

Key source locations:

| Stage | File |
| --- | --- |
| Feedback capture | `inference_service/pkg/infra/repo/db/inference_feedback_repository.go` (`RecordFeedback`) |
| Preference dataset build | `inference_service/pkg/app/inference_usecase.go` (`BuildPreferenceDataset`) |
| Eligibility / split SQL | `inference_service/pkg/infra/repo/db/inference_feedback_repository.go` (`ReadPreferenceDataset`) |
| Frozen eval set | `inference_service/pkg/infra/repo/db/lineage_eval_repository.go` |
| DPO trigger | `training_service/pkg/infra/network/rest/training_handlers.go` (`StartDPOTrainingRun`) |
| Offline eval | `training_service/training_jobs/training_jobs/evaluate.py` |
| Candidate creation | `model_registry_service/pkg/app/model_registry_usecase.go` (`RecordModelTrainingCompleted`) |
| Promotion gate | `model_registry_service/pkg/domain/model/promotion_gate.go` (`EvaluatePromotion`) |
| Promotion / serving swap | `model_registry_service/pkg/app/model_registry_usecase.go` (`PromoteCandidate`, `ensureServedModel`) |

## Stage detail

### 1. Feedback → preference examples

`RecordFeedback` writes the raw feedback and, in the same statement, upserts a
`preference_examples` row. Accepted answers become the `accepted_answer`; rejected
answers put the model's answer in `rejected_answer` and the user's `preferred_answer`
in `accepted_answer`. This is the atomic unit the loop learns from.

### 2. Export → eligibility filter (fail-closed by construction)

`ReadPreferenceDataset` is not a dump — it is a filter. It only emits examples that are:

- **Complete pairs** — both `accepted_answer` and `rejected_answer` are non-empty.
- **Negative signal** — `feedback_label = 'REJECTED'` and `rating < 0`.
- **Deduplicated** — `DISTINCT ON (prompt_text, accepted_answer, rejected_answer)`.
- **Not previously frozen into an eval set** — `NOT EXISTS` against `lineage_eval_examples`
  (prevents train/eval leakage across generations).
- **Capped per user** — `ROW_NUMBER() OVER (PARTITION BY user_id ...)` bounded by
  `MaxPerUser` (anti-poisoning; a single user cannot dominate a dataset). `0` = unlimited.

Garbage feedback in would mean a loop that learns noise; these filters are the first
line of defence.

### 3. Frozen per-lineage held-out eval set (hybrid)

The highest-risk hole in any self-improving loop is a leaking or drifting eval set:
if the eval set changes every generation, every challenger can "win" while getting
worse. To prevent this, the eval set is **pinned per lineage** the first time a lineage
produces preference data, and reused for all subsequent generations.

Seeding is **hybrid** (`inference_usecase.go` `preparePreferenceEvalSet`):

- **Curated set present** → use the registered curated eval set
  (`RegisterCuratedEvalSet`); route all new examples to `TRAIN`.
- **No set present (generation 0)** → freeze this export's `EVAL` split as the pinned
  set (`FreezeEvalSet`, `source = FROZEN_GEN0`) inside the snapshot transaction.
- **Active frozen set present** → reuse the pinned `eval_dataset_uri`; all new examples
  go to `TRAIN`.

Storage: `lineage_eval_sets` (one active row per `(org_id, lineage_name)`, enforced by a
partial unique index) and `lineage_eval_examples` (the frozen example ids, referenced by
the export anti-leak filter). Both carry tenant RLS in the greenfield init migration.

### 4. DPO training

DPO is an explicit training command. `POST /v1/private/training-runs/dpo` carries a
`preference_dataset_id`; `training_service` resolves that id from `inference_service`
at the infra boundary, sets `profile.Trainer = "dpo"`, uses
`PreferenceDatasetURI = <train uri>`, and injects evaluation profile
`dataset_uri = <frozen eval uri>`. The training run id is content-addressed
(idempotent for the same preference dataset + request key). The run executes on
Temporal + Ray (`training_service/pkg/infra/temporalworker`,
`training_service/pkg/infra/executor`).

### 5. Offline evaluation

`evaluate.py` scores the trained model on the eval `dataset_uri`. The Ragas evaluator
(`run_ragas_evaluator`) requires a real dataset and emits `faithfulness`,
`answer_relevancy`, `context_precision`, the `eval_dataset_uri`, `metric_suite`, and
`evaluator_version` into the report. The **built-in** evaluator returns `1.0` for
everything when no dataset is present — this is intentionally *not* promotable (see gate).

### 6. Promotion gate (fail-closed)

`EvaluatePromotion` (`promotion_gate.go`) is the safety spine. `DefaultGatePolicy`:

- **Absolute floors** on guardrail metrics (`faithfulness`, `answer_relevancy`,
  `context_precision` ≥ 0.6) — a challenger below any floor is rejected outright.
- **Non-regression vs champion** (`MinDeltaVsChampion = 0`) — any regression on a
  guarded metric rejects. Deltas are recorded on the promotion decision.
- **RejectBuiltinMetrics** — a model scored by the built-in `1.0`-everything evaluator
  can never promote.
- **RequireEvalDataset** — the candidate must carry an eval dataset uri.
- **RequireComparableEvalSet** — champion and challenger must have been scored on the
  **same** `eval_dataset_uri`, `metric_suite`, and `evaluator_version`. If not, the gate
  rejects (fail-closed) instead of falling back to floor-only promotion.
- **Generation 0** — the first model in a lineage has no champion; it promotes on floors
  alone (bootstrap, no head-to-head). This is expected and flagged in the decision reason.

Every champion switch records provenance: the preference dataset, training run, deltas,
report uris, and the accept/reject reason (`recordPromotionDecision`). No silent promotion.

### 7. Promotion → serving

`PromoteCandidate` flips the candidate to `EVALUATED`/`READY` and `ensureServedModel`
deploys it. Serving loading is handled by `model_serving_service` (see
[Multi-LoRA serving](multi-lora-serving.md)); the champion adapter can be pinned so it
stays resident for fast rollback.

## What is NOT built yet

The loop is closed **structurally** but is not yet **autonomous** or **online-safe**.
The following are deliberately absent today:

1. **Auto-trigger.** Preference dataset builds are exposed through the authenticated
   inference REST resource. There is no cron, feedback-accrual watcher, cooldown, or
   backpressure yet — a human/API kicks off the build.
2. **Canary / shadow serving.** `PromoteCandidate` swaps the champion directly. There is
   no traffic split or shadow-eval phase before cut-over.
3. **Online eval + auto-rollback.** Nothing watches live acceptance/guardrail metrics
   against the champion baseline after promotion, and nothing rolls back on regression.
   Without this, the loop can self-degrade rather than self-improve.
4. **Loop orchestration.** The stages are event-chained through the outbox, not a single
   durable per-lineage workflow (accrue → export → train → gate → canary → promote/rollback).

## Known caveat: lineage-key stability across generations

The frozen eval set and champion lookup are both keyed on the model **`Name`**
(`LineageForModel` uses `Name`; the eval set uses `ParentModelName` = `m.name`). DPO
child models are currently named `"dpo-" + modelID` in the training subscriber, so each
generation can resolve to a **different lineage key**. If that holds, a challenger is
treated as "first in lineage" and promotes via the generation-0 bootstrap path instead of
a real head-to-head against its parent — which defeats cross-generation pinning.

Before the loop can be called truly self-improving, a **stable lineage identifier**
(propagated from the root model through training, independent of per-generation display
name) is required, plus a cross-generation e2e test that proves champion and challenger
are scored on the same frozen eval uri. See the e2e section below.

## Testing

Unit:

- Gate: `model_registry_service/pkg/domain/model/promotion_gate_test.go`
- Export / freeze / cap: `inference_service/pkg/app/inference_usecase_test.go`,
  `inference_service/pkg/infra/repo/db/inference_feedback_repository_test.go`

End-to-end (`api_gateway/test`, real services over gRPC/REST):

- `rag_inference_workflow_test.go` — endpoint generate → feedback → preference dataset
  build/read → DPO command, and a second export asserting the **eval uri is stable**
  while the **train uri differs**, plus a train/eval leakage check.
- **Recommended next (not yet present):** a cross-generation test — export → wait on
  the explicit DPO training run to reach `COMPLETED` → poll model registry to
  `CANDIDATE`/`EVALUATED` → assert the promotion reason is `candidate beats champion
  gate` (a real head-to-head), and a regression case that lands the challenger in
  `FAILED` with the champion still served.
