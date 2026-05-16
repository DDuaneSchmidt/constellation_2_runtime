# Aegis Sleeve Review Feedback Loop

## Purpose

Aegis Lite reviews paper-trade performance after EOD and EOW so outcome evidence can flow back into offline Research Lab work without changing trades, sleeves, strategy logic, or broker behavior.

The loop is:

`manual_trade_packet.v1 -> manual_execution_receipt.v1 -> outcome_ledger.v1 -> sleeve_performance_report.v1 -> eod_sleeve_review.v1 / eow_sleeve_review.v1 -> Research Lab feedback tasks`

## Current Footprint

| Capability | Status | Notes |
| --- | --- | --- |
| outcome ledger | PROVEN | Existing `outcome_ledger.v1` records recommendation-vs-result rows without IB integration. |
| sleeve performance report | PROVEN | `sleeve_performance_report.v1` joins packet, receipt, outcome, attribution, event, alert, and promoted sleeve lineage. |
| trade outcome attribution | PARTIAL | Supported as optional input where present; not required for review generation. |
| Research Lab feedback task generation | PROVEN | EOD/EOW review can write offline Research task queues only when explicitly requested. |
| EOD sleeve review | PROVEN | `build_eod_sleeve_review_v1.py` writes `eod_sleeve_review.v1`. |
| EOW sleeve review | PROVEN | `build_eow_sleeve_review_v1.py` writes `eow_sleeve_review.v1`. |
| AI-assisted interpretation | SCAFFOLDED | Reviews use deterministic summaries and record `ai_used=false`. No LLM runtime is wired. |
| failure-mode classification | PARTIAL | Deterministic rules flag missing receipts, missing outcomes, slippage, stop issues, event false positives, repeated failures, and regime/overlap hints. |
| regime/event attribution | PARTIAL | Uses available performance rows and event ledgers; quality depends on upstream regime/event data. |
| automatic Research task creation | OPERATOR_ONLY | Review commands recommend tasks by default and write task queues only with `--write_research_tasks`. |

## EOD Review

Command:

```bash
python3 ops/tools/build_eod_sleeve_review_v1.py --truth_root <path> --day <YYYY-MM-DD>
```

Optional offline Research task queue write:

```bash
python3 ops/tools/build_eod_sleeve_review_v1.py --truth_root <path> --day <YYYY-MM-DD> --write_research_tasks
```

Inputs:

- `sleeve_performance_report.v1`
- `event_awareness_ledger.v1`, if available
- `event_rules_registry.v1`, if available

Outputs:

- `reports/eod_sleeve_review_v1/<day>/eod_sleeve_review.v1.json`
- optional `research_lab/research_task_queue_v1/<day>/index/research_task_queue.v1.json`

The review flags trades opened/closed, missing receipts, missing outcomes, slippage issues, stop behavior issues, event/regime attribution, sleeve warnings, and offline Research recommendations.

## EOW Review

Command:

```bash
python3 ops/tools/build_eow_sleeve_review_v1.py --truth_root <path> --week_ending <YYYY-MM-DD>
```

Optional offline Research task queue write:

```bash
python3 ops/tools/build_eow_sleeve_review_v1.py --truth_root <path> --week_ending <YYYY-MM-DD> --write_research_tasks
```

Inputs:

- daily `sleeve_performance_report.v1`
- daily `eod_sleeve_review.v1`, if available
- event ledgers, if available
- Research task queues, if available

Outputs:

- `reports/eow_sleeve_review_v1/<week_ending>/eow_sleeve_review.v1.json`
- optional `research_lab/research_task_queue_v1/<week_ending>/index/research_task_queue.v1.json`

The review builds a weekly scorecard, strongest/weakest sleeve lists, repeated failure modes, event false positives, review-only promotion/demotion candidates, and offline Research recommendations.

## AI Boundary

AI interpretation is intentionally not active in v1.

Current review artifacts always record:

- `ai_used=false`
- `ai_model_source=DETERMINISTIC_PLACEHOLDER_NO_LLM_RUNTIME`

Future AI may summarize, classify, explain failure modes, and suggest Research task text. It must not create trades, promote/demote sleeves, mutate strategy logic, bypass Research governance, or touch broker paths.

## Research Feedback

Allowed offline task types include:

- `sleeve_failure_review`
- `sleeve_success_review`
- `regime_dependency_review`
- `event_false_positive_review`
- `slippage_review`
- `stop_behavior_review`
- `edge_overlap_review`
- `hypothesis_refinement_review`
- `promotion_candidate_review`
- `demotion_candidate_review`

These tasks are Research Lab work requests only. They do not authorize production action.

## Safety Boundaries

Every review artifact confirms:

- manual execution only
- no broker submit
- no trade creation
- no Lite runtime mutation
- no strategy logic mutation
- no automatic promotion
- no automatic demotion

Missing inputs produce review warnings and deterministic reason codes, not broker actions or production changes.
