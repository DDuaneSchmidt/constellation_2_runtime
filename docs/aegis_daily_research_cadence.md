# Aegis Daily Research Cadence

Date: 2026-05-15

This cadence describes the daily operating rhythm for the pivoted Aegis Lite + Aegis Research Lab ecosystem. It is documentation only. It does not activate production, schedule Research Lab automation, submit broker orders, enable IB transmit, or make Research artifacts authoritative for trading.

## Operating Boundary

Aegis Lite is the canonical manual-paper trading path. Its official daily production decision state is the once-per-trading-day EOD run at 15:50 ET.

Aegis Research Lab is offline-only. It may ingest hypotheses, process queued research tasks, write evidence/results, and learn from Lite outcomes. It must not mutate Aegis Lite runtime, create trades, promote sleeves automatically, or authorize execution.

Event awareness is non-canonical. It may create awareness ledgers, tactical packets, validity gates, and trade-capture alert ledgers. It must not overwrite EOD state or send operator alerts unless the trade capture alert gate passes and transport is explicitly wired later.

## Daily Workflow

### 1. Pre-Market / Morning

Purpose: inspect yesterday's learning state before the market session starts.

Operator actions:
- Review prior outcome ledger and trade outcome attribution updates.
- Review open `research_task_queue.v1` tasks.
- Review blocked, rejected, invalidated, and stale hypotheses.
- Capture any new manual hypotheses explicitly through Research Lab intake.
- Do not promote anything to Lite from a conversation or ad hoc note.

Existing commands:
```bash
python3 ops/tools/list_research_hypotheses_v1.py --truth_root /path/to/offline_research_truth --day_utc YYYY-MM-DD
python3 ops/tools/research_architecture_integrity_review_v1.py --truth_root /path/to/offline_research_truth
python3 ops/tools/research_lab_register_hypothesis_v1.py --truth_root /path/to/offline_research_truth --day_utc YYYY-MM-DD --title "..." --edge_family "..." --behavioral_thesis "..." --market_regime "..." --trigger_conditions "..." --expected_outcome "..." --failure_modes "..."
```

Manual-only / gaps:
- There is no Research Lab UI for hypothesis/task review.
- There is no advisor benchmark review command proven in the Lite manual-paper path.
- Missing or incomplete manual receipts/outcomes must be supplied by the operator; Aegis must not invent them.

### 2. During Market Day

Purpose: allow non-canonical event awareness without turning Research into production automation.

Allowed:
- Event awareness may write `event_awareness_ledger.v1`.
- Tactical review may write `event_tactical_packet.v1`.
- Event validity and trade capture alert gates may write gate/ledger artifacts.
- Ambiguous observations may later become offline Research Lab tasks.

Forbidden:
- No broker submit.
- No IB transmit.
- No EOD state overwrite.
- No Research Lab promotion or Lite mutation.
- No production Research mutation except explicit offline tasks or ledgers.

Existing commands:
```bash
python3 ops/tools/run_event_awareness_v1.py --truth_root /path/to/offline_or_runtime_truth --day_utc YYYY-MM-DD --event_id EVENT_ID --event_type PANIC_EXHAUSTION
python3 ops/tools/run_event_tactical_review_v1.py --truth_root /path/to/offline_or_runtime_truth ...
python3 ops/tools/run_event_validity_gate_v1.py --truth_root /path/to/offline_or_runtime_truth ...
python3 ops/tools/run_trade_capture_alert_gate_v1.py --truth_root /path/to/offline_or_runtime_truth --event_packet_json /path/to/event_tactical_packet.v1.json --event_validity_gate_json /path/to/event_validity_gate.v1.json
```

Manual-only / gaps:
- Trade capture alert v1 currently writes gate/ledger artifacts and message bodies; actual email/SMS transport is not proven wired.
- Event alert UI is not present.

### 3. Near Close

Purpose: run the canonical Aegis Lite EOD decision process.

Canonical time:
- 15:50 America/New_York on NYSE trading days.
- One official EOD run per trading day.
- Event runs must not overwrite canonical EOD state.

Existing command:
```bash
python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py --day_utc YYYY-MM-DD --environment PAPER --manual-only --allow-not-ready-exit-zero
```

Official outputs:
- `aegis_lite_eod_report.v1`
- `operator_execution_queue.v1`
- `manual_trade_packet.v1`
- `edge_cluster.v1`
- `sleeve_edge_overlap_review.v1`
- `aegis_lite_operating_status.v1`

Manual-only / gaps:
- Active runtime/systemd is aligned to 15:50 ET.
- Current real promoted non-demo candidate proof is still required before first trade.

### 4. After Close

Purpose: convert operator action or non-action into auditable evidence.

Operator actions:
- Record any IB paper fills manually.
- Confirm whether protective stop was entered and accepted.
- Record skipped, modified, or missed trades.
- Update manual execution receipt.
- Update outcome ledger or trade outcome attribution with actual/manual evidence.
- Enqueue Research Lab learning tasks only when the outcome suggests learning is needed.

Existing commands:
```bash
python3 ops/tools/run_outcome_attribution_v1.py ...
python3 ops/tools/run_trade_outcome_v1.py ...
python3 ops/tools/build_sleeve_performance_report_v1.py --truth_root /path/to/truth --day YYYY-MM-DD
python3 ops/tools/ingest_trade_outcome_attribution_to_research_v1.py --truth_root /path/to/offline_research_truth --day_utc YYYY-MM-DD --trade_outcome_attribution_json /path/to/trade_outcome_attribution.v1.json
```

Manual-only / gaps:
- No Lite UI receipt form is proven.
- No automatic non-IB mark/exit updater is present.
- Manual execution receipt remains the source of truth when IB is not integrated.
- `sleeve_performance_report.v1` recommends Research follow-up tasks but does not write `research_task_queue.v1`.

### 5. Research Lab Daily Run

Purpose: process offline queued Research Lab work after operational facts are recorded.

Existing command:
```bash
python3 ops/tools/run_research_lab_task_queue_v1.py --truth_root /path/to/offline_research_truth --day_utc YYYY-MM-DD --max_tasks 10
```

Expected outputs:
- Updated `research_task_queue.v1`.
- `research_evidence_packet.v1`.
- `research_result_ledger.v1`.
- Updated `research_hypothesis.v1` lifecycle/confidence when supported by evidence/results.
- Follow-up tasks when deterministic rules require more work.

Compatibility warning:
- `ops/tools/run_research_lab_v1.py` / `ops/tools/run_aegis_research_lab_v1.py` are legacy compatibility runners and should not be the default path for new Research Lab work unless explicitly adapted.

Manual-only / gaps:
- "Awareness report" generation is not proven as a first-class daily Research Lab output in the new canonical path.
- Updating a human-facing "test plan" is compatibility-only for older Research v1 artifacts; the canonical new path is task queue plus evidence/result/hypothesis updates.

### 6. Weekly Review

Purpose: make slower human decisions that should not be automated daily.

Review:
- Sleeve performance and rankings.
- Rejected, invalidated, promising, and stale hypotheses.
- Promotion candidates.
- Edge overlap and regime-specific performance.
- Advisor benchmark comparison once a fee-adjusted benchmark artifact exists.

Existing commands:
```bash
python3 ops/tools/run_sleeve_performance_control_v1.py ...
python3 ops/tools/run_sleeve_evaluation_kernel_v1.py ...
python3 ops/tools/run_promotion_manual_review_v1.py ...
```

Manual-only / gaps:
- Advisor benchmark comparison is missing in the pivoted Lite manual-paper path.
- Research Lab promotion candidate UI is missing.
- Weekly decisions remain human review only; no automatic promotion is allowed.

## Cadence Status Summary

| Step | Status | Command support | Notes |
| --- | --- | --- | --- |
| Pre-market Research review | `READY_WITH_MANUAL_STEPS` | Partial | Hypotheses can be listed/registered; task review is CLI/JSON. |
| During-market event awareness | `PRESENT_UNPROVEN` | Partial | Event/gate/ledger commands exist; no proven email/SMS transport or UI. |
| Near-close Lite EOD | `READY_WITH_MANUAL_STEPS` | Present | Repo, active runtime, and systemd use 15:50 ET; real manual trading still requires a promoted executable candidate. |
| After-close manual receipts/outcomes | `READY_WITH_MANUAL_STEPS` | Partial | Artifacts and `sleeve_performance_report.v1` exist; operator entry is manual and no UI form is proven. |
| Research Lab daily run | `READY_WITH_MANUAL_STEPS` | Present | Offline executor exists; real dataset bindings remain incomplete. |
| Weekly review | `PRESENT_UNPROVEN` | Partial | Sleeve/promotion tools exist; advisor benchmark comparison is missing. |

## Stop Rules

Stop and classify the day as not ready for real manual paper trading if:
- Active Lite EOD timer/status is not 15:50 ET.
- No current Lite EOD report exists.
- No current operator queue or manual trade packet exists.
- Any candidate lacks entry, stop, risk, sizing, sleeve, or promoted-source lineage.
- Research artifacts are the only source of a trade idea.
- Manual receipt or outcome evidence is missing for an executed trade.
- Any flow requires broker/IB automation to complete.
