# Aegis AI Feedback Engine v1

## Purpose

Aegis AI Feedback Engine v1 reviews EOD/EOW sleeve and trade performance and turns observed outcomes into human-reviewable Research Lab feedback. It is a safety-gated analysis layer, not an execution or promotion system.

The loop is:

`Performance Data -> Evidence Gate -> AI Feedback Engine -> Research Task Gate -> Human/Governance Review -> Research Lab -> Promotion/Demotion Review`

## Evidence Gate

`evidence_gate.v1` decides whether the input data is clean enough for review.

Inputs may include:

- `manual_trade_packet.v1`
- `manual_execution_receipt.v1`
- `outcome_ledger.v1`
- `sleeve_performance_report.v1`
- `event_awareness_ledger.v1`
- `trade_capture_alert_ledger.v1`
- regime/event context when present

The gate evaluates:

- sample count
- missing receipts
- missing outcomes
- stale data
- missing regime labels
- missing event labels
- missing price/outcome data
- data quality score

Sample bands:

- `NO_EVIDENCE`: no trade rows
- `OBSERVATION_ONLY`: 1-2 rows; no automatic Research task creation
- `WEAK_SIGNAL`: 3-9 rows; low-priority offline Research tasks may be created
- `REVIEWABLE_PATTERN`: 10-19 rows; normal Research tasks may be created
- `STRONGER_PATTERN`: 20+ rows; higher-priority Research tasks may be created

Stale or low-quality data blocks strong conclusions and automatic Research task creation.

## AI Feedback Review

`ai_feedback_review.v1` summarizes the gated review. Current v1 uses deterministic fallback only:

- `ai_used=false`
- `deterministic_fallback_used=true`
- `model_used=DETERMINISTIC_FALLBACK_NO_LLM_RUNTIME`

It records:

- evidence gate status
- sleeves/trades/events/regimes/alerts reviewed
- top findings
- hypothesis suggestions
- Research tasks created by the task gate
- human review requirement
- safety prohibitions

## Finding Types

Supported finding types:

- `SLEEVE_DEGRADATION`
- `SLEEVE_IMPROVEMENT`
- `REGIME_DEPENDENCY`
- `EVENT_FALSE_POSITIVE`
- `EVENT_SUCCESS_PATTERN`
- `STOP_BEHAVIOR_ISSUE`
- `SLIPPAGE_ISSUE`
- `EDGE_OVERLAP`
- `DATA_QUALITY_ISSUE`
- `MISSED_TRADE_PATTERN`
- `HYPOTHESIS_REFINEMENT`
- `PROMOTION_CANDIDATE`
- `DEMOTION_CANDIDATE`

Every finding must contain evidence references. No finding is allowed to authorize production action.

## Research Task Gate

The task gate controls whether a finding becomes an offline Research Lab task.

Rules:

- no Evidence Gate pass means no automatic task creation
- `OBSERVATION_ONLY` creates review notes only
- `WEAK_SIGNAL` creates low-priority Research tasks only
- `REVIEWABLE_PATTERN` creates normal Research tasks
- `STRONGER_PATTERN` creates higher-priority Research tasks

Allowed task types:

- `sleeve_failure_review`
- `sleeve_success_review`
- `regime_dependency_review`
- `event_false_positive_review`
- `event_success_review`
- `slippage_review`
- `stop_behavior_review`
- `edge_overlap_review`
- `hypothesis_refinement_review`
- `promotion_candidate_review`
- `demotion_candidate_review`

Tasks preserve lineage to:

`ai_feedback_review_id -> finding_id -> evidence_gate_id -> source artifacts`

## Operator Workflow

Build EOD feedback:

```bash
python3 ops/tools/build_ai_eod_feedback_review_v1.py --truth_root <path> --day <YYYY-MM-DD>
```

Build EOW feedback:

```bash
python3 ops/tools/build_ai_eow_feedback_review_v1.py --truth_root <path> --week_ending <YYYY-MM-DD>
```

Outputs:

- `reports/evidence_gate_v1/<EOD|EOW>/<period>/evidence_gate.v1.json`
- `reports/ai_feedback_review_v1/<EOD|EOW>/<period>/ai_feedback_review.v1.json`
- offline `research_task_queue.v1` entries only when the Evidence Gate and Research Task Gate allow them

CLI output is operator-readable and includes evidence status, sample band, finding count, task count, and safety boundaries.

## AI Responsibilities

Future AI may be used only for:

- summarization
- classification
- failure-mode explanation
- hypothesis refinement suggestions
- Research task recommendation text

## AI Prohibitions

AI must never:

- create trades
- change sleeve logic
- change thresholds
- promote sleeves
- demote sleeves
- mutate production state
- bypass Research Lab governance
- submit broker orders

## Safety Boundaries

Every AI feedback review records:

- `human_review_required=true`
- `production_mutation=false`
- `broker_action_allowed=false`
- `auto_promotion_allowed=false`
- `auto_demotion_allowed=false`

This engine is advisory and offline. It can create Research Lab work only through the Evidence Gate and Research Task Gate.
