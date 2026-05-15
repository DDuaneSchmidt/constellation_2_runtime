# Aegis Hostile Audit Grade

Date: 2026-05-15
Scope: Aegis Lite, Research Lab, Operator Inbox, Event Awareness, Trade Capture Alert Gate, Manual Trade Packet, Manual Execution Receipt, Outcome Ledger, Sleeve Performance Report, promotion boundaries, daily research cadence, AI and self-healing boundaries.

This is a closed-world audit of current repo/runtime evidence. It does not authorize production activation, broker submit, IB transmit, autonomous execution, automatic promotion, or Lite runtime mutation.

## Executive Grade

Overall grade: **78/100**

Current readiness: **ADVISORY_ONLY for real IB paper trading**. The architecture is now coherent and mostly fail-closed, but current runtime truth has no promoted executable candidates and no real manual-paper trade lifecycle has been proven against an actual operator-entered IB paper trade.

| Area | Grade | Hostile Assessment |
|---|---:|---|
| Architecture coherence | 88 | Operator Inbox -> Research Lab -> Aegis Lite is conceptually clean. Remaining risk is artifact sprawl and stale docs. |
| Determinism | 88 | Explicit truth roots, deterministic reports, sorted task/report logic, and fail-closed gates are strong. Dataset binding remains weak. |
| Safety / fail-closed behavior | 91 | Broker submit/transmit are consistently false in new surfaces. Missing candidates and incomplete packets block. |
| Research integrity | 84 | Canonical `research_hypothesis.v1` and result ledger are strong, but legacy research concepts still coexist. |
| Promotion discipline | 89 | Promotion requires evidence/result lineage and human approval. Lite consumes promoted sleeves only. |
| Manual execution readiness | 70 | Artifacts and CLI paths exist, but current runtime queue is empty and real IB paper trade workflow is unproven. |
| Sleeve performance attribution | 82 | Offline report joins packet/receipt/outcome and avoids zero-return lies. Real paper-trade evidence is not yet present. |
| Event awareness / alert trust | 76 | Validity and alert gates are strict. Actual SMS/email delivery is not implemented/proven. |
| Operator usability | 64 | `/aegis-lite` queue is present and degraded-safe. Receipts, outcome ledger, event alerts, inbox, and sleeve report are still mostly artifact/CLI surfaces. |
| Data readiness | 45 | Research executor can run fixture-backed checks, but real price/volatility/breadth/macro/regime dataset contracts are incomplete or missing. |
| AI functionality clarity | 72 | AI is bounded/scaffolded; no autonomous idea generation or promotion is proven. Docs mostly say this clearly. |
| Self-healing safety | 84 | Self-healing is documented as diagnostic/derived-artifact repair only. Coverage is uneven but boundaries are conservative. |
| Production readiness | 38 | Not production-ready and not intended to be. Broker/IB automation remains deferred. |
| Complexity control | 70 | The pivoted design is safer, but the repo still carries many legacy PAPER/IB paths and overlapping research artifacts. |
| Long-term evolvability | 86 | The layered boundary is good enough to evolve without contaminating Lite, if dataset and UI gaps are addressed deliberately. |

## Direct Answers

1. **Is the three-layer architecture coherent?**
   Yes. `operator_inbox.v1` is pre-research capture, Research Lab is offline evidence/result work, and Aegis Lite is manual operational evaluation. The boundary is documented in `docs/aegis_operator_inbox.md:11`, `docs/aegis_research_lab.md:65`, and `docs/research_to_lite_promotion.md:15`.

2. **Can casual ideas leak into research tasks, sleeves, or trades?**
   Not through the new Operator Inbox path. Inbox artifacts carry `research_task_creation_allowed=false`, `trade_creation_allowed=false`, and `sleeve_creation_allowed=false` in `constellation_2/common/aegis_operator_inbox_v1.py:108`. Tests verify no task/trade/sleeve artifacts are created by inbox review or promotion-to-idea (`constellation_2/common/tests/test_aegis_operator_inbox_v1.py:127`).

3. **Can exploratory/promising hypotheses influence production?**
   Not through the canonical path. Promotion requires evidence/result refs and human approval (`constellation_2/common/aegis_research_lab_v1.py:2038`, `constellation_2/common/aegis_research_lab_v1.py:2052`), and Lite requires promoted sleeve library matching (`constellation_2/common/aegis_research_lab_v1.py:2533`).

4. **Can Research Lab artifacts directly create trades?**
   No in the new path. Research artifacts are marked offline/non-runtime and promotion evidence only (`docs/research_to_lite_promotion.md:13`, `docs/aegis_research_lab.md:89`).

5. **Can event tactical runs overwrite canonical EOD state?**
   No evidence that they can. Event tools/reporting mark canonical EOD mutation false (`ops/tools/run_event_awareness_v1.py:72`), and event docs state EOD remains canonical (`docs/aegis_lite_event_awareness.md:3`).

6. **Can incomplete, stale, or invalid trade packets become actionable?**
   The new gates block incomplete packets. Manual packet tests cover missing stop fields (`constellation_2/common/tests/test_aegis_lite_operational_spine_v1.py:251`), and event validity blocks missing `valid_until`, stop/risk/slippage, stale packets, and `EXTREME` sensitivity (`constellation_2/common/aegis_lite_event_awareness_v1.py:234`, `constellation_2/common/aegis_lite_event_awareness_v1.py:249`).

7. **Can trade-capture alerts fire without a complete and valid manual trade candidate?**
   Not by artifact logic. Alert gate requires validity `PASS`, complete fields, non-`EXTREME` sensitivity, and time remaining (`constellation_2/common/aegis_lite_event_awareness_v1.py:320`, `constellation_2/common/aegis_lite_event_awareness_v1.py:382`). Transport is not proven.

8. **Can missing manual receipts/outcomes be misclassified as zero return?**
   The sleeve report explicitly classifies `MISSING_RECEIPT` and `MISSING_OUTCOME` (`constellation_2/common/aegis_sleeve_performance_report_v1.py:18`) and docs state missing evidence is never treated as zero return (`docs/aegis_sleeve_performance_lifecycle.md:27`).

9. **Can sleeve performance reporting produce misleading results?**
   It can still mislead if the operator treats synthetic/offline smoke results as real performance or if manual outcome inputs are wrong. The report logic is conservative, but it depends on human-entered receipt/outcome evidence.

10. **Can outcome-ledger feedback create production actions instead of offline research tasks?**
   New feedback/report paths generate or recommend offline tasks only. Sleeve report feedback has `lite_runtime_mutation_allowed=false`, `writes_research_task_queue=false`, and `automatic_promotion_allowed=false` in the smoke output and code (`constellation_2/common/aegis_sleeve_performance_report_v1.py:291`).

11. **Is the 15:50 ET canonical EOD timer aligned across repo/docs/runtime/systemd?**
   Yes. Repo timer uses `OnCalendar=Mon..Fri *-*-* 15:50:00 America/New_York` (`ops/systemd/user/aegis-lite-eod-report-v1.timer:6`), operating status code uses `LITE_EOD_TARGET_TIME_ET = "15:50"` (`constellation_2/common/aegis_lite_operating_status_v1.py:17`), active user systemd currently reports a 15:50 trigger, and runtime `aegis_lite_operating_status.v1` reports `target_time_et=15:50`.

12. **Is actual email/SMS delivery proven or merely scaffolded?**
   Scaffolded/gated only. The alert ledger records `WOULD_SEND` or `NOT_SENT` (`constellation_2/common/aegis_lite_event_awareness_v1.py:541`), and the CLI prints `email_sms_allowed` but does not call a transport (`ops/tools/run_trade_capture_alert_gate_v1.py:87`).

13. **Is daily Research cadence executable or only documented?**
   Command-supported in pieces, not automated. `docs/aegis_daily_research_cadence.md` lists commands, and `run_research_lab_task_queue_v1.py` exists, but the cadence is manual and dataset-dependent.

14. **Are Research datasets sufficiently bound for real hypothesis testing?**
   No. The executor supports explicit `required_inputs` and optional `--fixture_json` (`ops/tools/run_research_lab_task_queue_v1.py:152`), but real volatility, breadth, macro event calendar, and regime-label dataset contracts are missing (`docs/aegis_lite_manual_paper_readiness.md:142`).

15. **Are AI-generated hypotheses operational, scaffolded, or missing?**
   Scaffolded/legacy. Legacy AI hypothesis batch intake exists, but canonical `research_hypothesis.v1` uses explicit seed ingestion; no autonomous AI idea generation is proven (`docs/aegis_lite_research_high_level_functions.md:15`).

16. **Are self-healing capabilities limited to safe derived-artifact repair / diagnostics?**
   Yes by documented boundary. Self-healing may repair/rebuild safe derived artifacts and enqueue offline tasks, but must not submit trades, bypass gates, auto-promote, mutate finalized EOD, or invent evidence (`docs/aegis_ai_functionality_audit.md:9`).

17. **Is anything still coupled to obsolete IB automation?**
   New Lite/Research/Inbox surfaces are not coupled, but the repo and runtime still contain many legacy PAPER/IB tools and runtime artifacts. They remain a confusion risk unless kept diagnostic/deferred (`docs/legacy_paper_runtime_deferred.md:19`).

18. **Is the system too complex relative to the current proven edge?**
   Yes, at the ecosystem level. The safety architecture is justified, but current proven trading edge is still zero real paper trades. The number of artifacts exceeds the proven operational learning loop.

19. **Is the manual paper-trade lifecycle actually proven end-to-end?**
   Not for real IB paper trading. A safe offline smoke exists for packet -> receipt -> outcome -> sleeve report, but current runtime queue is empty and no actual operator-entered IB paper fill/stop/outcome cycle is proven.

20. **Top 10 remaining failure modes** are listed below.

## Current Runtime Truth

- Active user systemd timer currently shows the Lite EOD timer waiting for **15:50 ET** on 2026-05-15.
- Runtime `aegis_lite_operating_status.v1` reports:
  - `readiness_classification=ADVISORY_ONLY`
  - `manual_execution_only=true`
  - `broker_submit_required=false`
  - `release_repo_match_status=MATCH`
  - blockers: `CANDIDATE_INPUT_MISSING`, `PROMOTED_SLEEVE_LIBRARY_REQUIRED`, `NO_TRADE_CANDIDATES`, `OPERATOR_QUEUE_MISSING`
- Current runtime EOD report path exists:
  `/home/node/constellation_runtime_data/truth/reports/aegis_lite_eod_report_v1/2026-05-15/operational_gap_alignment_2026-05-15/aegis_lite_eod_report.v1.json`
- Current runtime queue path exists but `execution_queue=[]`:
  `/home/node/constellation_runtime_data/truth/reports/operator_execution_queue_v1/2026-05-15/operational_gap_alignment_2026-05-15/operator_execution_queue.v1.json`
- Current runtime manual packet exists but `trade_candidates=[]`:
  `/home/node/constellation_runtime_data/truth/reports/manual_trade_packet_v1/2026-05-15/operational_gap_alignment_2026-05-15/manual_trade_packet.v1.json`

Hostile conclusion: current runtime is safe, but not actionable.

## Top Strengths

1. The three-layer architecture is coherent: loose ideas stay in Operator Inbox, Research Lab is offline, and Lite is the only manual operational layer.
2. Operator Inbox is low-friction but non-authoritative. It cannot create tasks, trades, sleeves, or production state.
3. Research Lab promotion is strict and human-gated.
4. Lite operational status and EOD report are fail-closed when candidate/promoted-sleeve inputs are missing.
5. Current active release and repo commit match in runtime status.
6. Canonical EOD timer is now operationally aligned to 15:50 ET, despite stale docs.
7. Event tactical packets are non-canonical and validity-gated.
8. Trade capture alert gate is conservative and anti-spam aware.
9. Sleeve performance report does not count missing receipt/outcome as zero return.
10. New paths consistently avoid broker submit, transmit automation, and IB coupling.

## Top Weaknesses

1. Current runtime has no executable promoted candidate queue.
2. Current manual packet has no trade candidates.
3. Real manual IB paper lifecycle is not proven end-to-end.
4. Actual SMS/email transport is not implemented or proven.
5. Dedicated UI surfaces are missing for Operator Inbox, Research Lab status, event alerts, manual receipts, outcome ledger, and sleeve performance report.
6. Research datasets are not sufficiently bound for real testing.
7. Some operator-facing docs were stale before this commit; keep documentation synchronized with runtime evidence after every timer/status change.
8. Legacy PAPER/IB tooling remains present and can confuse operators.
9. The system is artifact-heavy relative to the current proven edge.
10. Systemd service has noisy `Documentation=` warnings for a filesystem path, which is not blocking but weakens operational polish.

## Production Blockers

Production should not proceed. Blockers:

- No production activation is requested or appropriate.
- No automation-ready fill lifecycle exists in the pivoted path.
- No actual promoted production sleeve has proven edge through Research Lab and Lite.
- Real dataset binding is incomplete.
- Current runtime is `ADVISORY_ONLY`.
- Broker/IB automation is intentionally deferred.

## Paper-Trading Blockers

These must be fixed before a real manual IB paper trade:

1. Generate a current non-demo promoted executable candidate through `promoted_sleeve_library.v1`.
2. Produce a current Lite EOD report with `READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING`, not `ADVISORY_ONLY`.
3. Produce a current non-empty `operator_execution_queue.v1` and `manual_trade_packet.v1`.
4. Verify the `/aegis-lite` UI renders that exact current queue, not stale or demo artifacts.
5. Have the operator manually record one receipt with fill, quantity, stop entered, stop price, and notes.
6. Record outcome evidence without IB automation.
7. Build `sleeve_performance_report.v1` from that real receipt/outcome.
8. Keep readiness/high-level docs synchronized with the current 15:50 runtime evidence.

## Misleading Claims To Avoid

- Do not claim Aegis is ready for real manual IB paper trading today.
- Do not claim actual SMS/email trade alerts are sent.
- Do not claim Research datasets are fully bound for real historical testing.
- Do not claim AI autonomously generates canonical hypotheses.
- Do not claim self-healing can repair missing receipts/outcomes or bypass gates.
- Do not claim event awareness is canonical production state.
- Do not claim missing receipts or missing outcomes are measured returns.
- Do not claim legacy PAPER/IB automation is part of the pivoted operating model.
- Do not claim sleeve performance is proven on real trades; it is proven on controlled offline evidence.
- Do not claim Operator Inbox promotion creates Research tasks, hypotheses, sleeves, or trades.

## Top 10 Remaining Failure Modes

1. Operator acts on stale/demo queue artifacts instead of current EOD truth.
2. Stale docs contradict current runtime and create operator uncertainty.
3. No promoted executable candidates means operators try to force a trade outside the governed path.
4. Manual receipt is skipped, making performance attribution impossible.
5. Missing outcome evidence is interpreted informally as performance.
6. Alert gate `WOULD_SEND` is mistaken for real SMS/email delivery.
7. Event tactical packet is treated as canonical EOD state.
8. Legacy PAPER/IB artifacts are mistaken for current operating authority.
9. Fixture-backed research results are mistaken for dataset-backed validation.
10. Artifact complexity makes operators inspect JSON manually and miss blockers.

## What Must Be Fixed Before Manual Paper Trading

- Current runtime must have a real promoted, non-demo, executable candidate.
- UI must show the exact current report/queue and make blocked/advisory items visibly non-executable.
- Operator must complete a dry manual receipt/outcome loop in the intended workflow.
- Stale timer mismatch statements in readiness/high-level docs must be corrected.
- A one-trade supervised paper smoke must include receipt, stop evidence, outcome ledger row, and sleeve report.

## What Can Wait

- Actual SMS/email transport, as long as no one claims alert delivery exists.
- Broad Research dataset architecture, as long as Research remains fixture/manual-input limited.
- Dedicated UI for Operator Inbox and Research Lab.
- Advisor benchmark comparison.
- Legacy PAPER/IB code deletion, as long as legacy surfaces remain deferred/diagnostic.
- Advanced knowledge graph logic.
- Any autonomous execution, fill lifecycle automation, or broker integration.

## Operator Inbox Commit Recommendation

Commit the Operator Inbox changes after validation, but treat the commit as **offline capture only**. The implementation is correctly scoped: it captures ideas, preserves lineage, and does not create tasks, trades, sleeves, Lite candidates, or production state. If committing, isolate it from unrelated stale-doc cleanup where practical.

## Manual Paper-Trading Smoke Recommendation

Proceed only with an **offline/operator workflow smoke** next:

`current Lite EOD report -> non-empty manual packet/queue -> UI review -> operator dry receipt -> outcome ledger row -> sleeve performance report`

Do **not** proceed to a real IB paper trade until the current runtime produces a real promoted executable candidate and the UI/report show `READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING` for that candidate.

## Final Classification

- Architecture: strong enough to keep.
- Safety: strong enough for offline/dry-run operator smoke.
- Research: strong enough to ingest and queue offline work, weak on real datasets.
- Lite: safe but currently non-actionable.
- Operator Inbox: safe to retain and commit after validation.
- Manual IB paper trading today: **No**.
- Next hostile audit target: one supervised dry-run with a non-empty promoted candidate queue, manual receipt, outcome ledger, and sleeve performance report.
