# Aegis Research v1 Test Waiver

Date: 2026-05-14

Scope: Aegis Research v1 closed-loop scaffolding and safety hardening before commit. This waiver covers the full common-suite failures observed during the Research v1 regression review. It does not waive the focused Research/Lite/manual artifact suites.

## Changed Files

Tracked changes from `git diff --name-only`:

- `constellation_2/common/aegis_research_lab_v1.py`
- `constellation_2/common/tests/test_aegis_research_lab_v1.py`
- `docs/aegis_lite_research_architecture.md`
- `docs/aegis_research_lab.md`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/hypothesis_registry.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/manual_execution_receipt.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/research_lab_awareness_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/research_task_queue.v1.schema.json`
- `ops/tools/research_lab_register_hypothesis_v1.py`
- `ops/tools/run_research_lab_v1.py`

New Research v1 files:

- `governance/04_DATA/SCHEMAS/C2/REPORTS/experiment_result.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/manual_trade_packet.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/promotion_review.v1.schema.json`
- `ops/tools/run_aegis_research_lab_v1.py`

## Full Common-Suite Result

Command:

```bash
pytest constellation_2/common/tests -q
```

Result:

- 2646 passed
- 235 failed

The 235 failures were collected with:

```bash
pytest constellation_2/common/tests --lf --collect-only -q
```

No failed node IDs matched `research_lab`, `aegis_lite`, `manual_feedback`, `candidate_observability`, `manual_trade_packet`, `experiment_result`, or Research v1 promotion-review surfaces.

## Failure Classification

Directly related to Research v1 change: none found.

Indirectly related to Research v1 change: none found.

Unrelated legacy failure domains:

- legacy advisor trade bridge and advisory evidence gateway
- advisory kernel and execution package compatibility
- Aegis day/run ledgers and truth-integrity projections
- bundle B / submit-boundary / post-entry boundary execution paths
- paper session fact plane, startup materialization, and trading-day state machine
- runtime replay, readiness policy architecture, and runtime authority bridge tests
- fill lifecycle, orphan submission backfill, and upper-layer intervention/orchestration tests
- market-data preopen prepare and old paper/autonomous execution readiness paths

These failures exercise legacy execution, advisory, runtime replay, session, submit-boundary, fill lifecycle, and paper automation behavior. The Research v1 changes add offline research artifacts, explicit research-root CLIs, schema validation, manual trade packet gating, and outcome-ledger learning tasks. They do not modify the failed legacy modules or their tests.

## Focused Suites That Passed

Commands:

```bash
pytest constellation_2/common/tests/test_aegis_research_lab_v1.py -q
pytest constellation_2/common/tests/test_aegis_research_lab_v1.py constellation_2/common/tests/test_aegis_lite_eod_v1.py constellation_2/common/tests/test_aegis_lite_manual_feedback_v1.py constellation_2/common/tests/test_candidate_observability_v1.py -q
```

Results:

- Research Lab focused suite: 23 passed
- Core Research/Lite/manual/observability suite: 57 passed

Additional validation:

```bash
python -m py_compile constellation_2/common/aegis_research_lab_v1.py ops/tools/research_lab_register_hypothesis_v1.py ops/tools/run_research_lab_v1.py ops/tools/run_aegis_research_lab_v1.py
git diff --check
```

Both passed.

CLI smoke was run only against `/tmp/aegis_research_v1_cli_smoke` with explicit `--truth_root`, proving:

- register hypothesis
- enqueue research task
- run offline Research Lab runner
- write experiment result
- update task queue/test plan
- write awareness report

## Safety Boundaries Confirmed

- Research Lab remains offline and non-executable.
- Research CLIs require explicit `--truth_root`; there is no implicit production/runtime truth root.
- Research artifacts have `research_lab_only=true` and do not grant runtime authority.
- Research artifacts cannot create trades.
- Experiment results cannot skip test-plan stages.
- `validated_candidate` cannot be produced before the full test protocol reaches promotion review.
- Promotion review requires completed protocol evidence, positive friction-adjusted evidence, out-of-sample support, regime notes, failure-mode notes, edge-overlap review, and human approval.
- `manual_trade_packet.v1` candidates are actionable only when they match a promoted sleeve-library source and have complete entry, stop, risk, sizing, symbol, side, and instrument fields.
- Outcome-ledger learning creates offline research tasks only.
- No broker submit, transmit automation, fill lifecycle automation, or autonomous IB execution path was added.
