# Aegis Hostile Audit Grade Improvement Plan

Date: 2026-05-15

Source audit: `docs/aegis_hostile_audit_grade.md`  
Current hostile audit grade: **78/100**  
Target: **90+/100** without IB automation, production activation, autonomous execution, automatic promotion, or new architecture.

## Executive Plan

Aegis lost most points for proof, data, and operator usability, not for core architecture. The three-layer design is good enough:

`Operator Inbox -> Research Lab -> Aegis Lite`

The fastest path from 78 to 90+ is not another architecture pass. It is operational proof:

1. Produce a current promoted executable candidate queue.
2. Complete one supervised manual-paper lifecycle from recommendation through receipt, outcome, sleeve report, and Research feedback.
3. Make the operator control plane impossible to misread.
4. Bind enough Research datasets to test the first seeded hypotheses honestly.
5. Keep alert transport truthful: either implement a gated transport or explicitly label it artifact-only everywhere.

## Points Lost By Category

| Area | Grade | Points Lost | Issue Type | Why Points Were Lost | Recovery Change | Required Before Manual Paper Trading |
|---|---:|---:|---|---|---|---|
| Architecture coherence | 88 | 12 | Architectural / complexity | Layering is coherent, but stale docs, legacy artifacts, and many overlapping research/Lite surfaces still create source-of-truth risk. | Add a short operator authority map and diagnostic-vs-authoritative table; keep legacy PAPER/IB surfaces visibly deferred. | Partial: authority clarity is required; broad cleanup can wait. |
| Determinism | 88 | 12 | Data / proof | Core producers are deterministic, but Research tests still depend on fixture/manual inputs and incomplete dataset contracts. | Add minimal dataset binding contracts for price, volatility, breadth, macro events, regime labels, and outcome data; keep explicit `--truth_root`. | Price/outcome minimum is P0/P1 boundary; full breadth/macro can be P1. |
| Safety / fail-closed behavior | 91 | 9 | Proof / UI | Safety is strong, but operator-facing UI does not yet prove every blocked/advisory state is visually impossible to trade. | Add UI/read-model tests and screenshots/proofs showing blocked/advisory/demo/missing-artifact states cannot look executable. | Yes. |
| Research integrity | 84 | 16 | Architectural / data | Canonical Research Lab exists, but legacy research concepts coexist and real datasets are weak. | Mark legacy paths compatibility-only in operator docs/UI; add dataset contracts and one real task run using non-fixture data. | Dataset proof can wait until after first manual trade, but legacy source-of-truth clarity is P0. |
| Promotion discipline | 89 | 11 | Proof | Rules are strong, but current runtime has no real promoted executable candidate proving the boundary end-to-end. | Create one human-approved promoted sleeve library entry from valid Research evidence, then run Lite EOD against it. | Yes. |
| Manual execution readiness | 70 | 30 | Operational / proof / UI | Current runtime queue and manual packet are empty; no actual operator-entered IB paper fill/stop/outcome lifecycle is proven. | Run one supervised paper workflow: promoted candidate -> EOD queue/packet -> UI review -> IB paper entry -> stop -> receipt -> outcome -> sleeve report. | Yes. |
| Sleeve performance attribution | 82 | 18 | Proof / UI | Offline report works and avoids zero-return lies, but proof is controlled/synthetic and lacks dedicated UI. | Build report from a real supervised paper trade; show missing receipt/outcome diagnostics; add operator-visible summary. | Real report proof is P0/P1; dedicated UI can be P1. |
| Event awareness / alert trust | 76 | 24 | Operational / UI / proof | Validity and alert gates are strict, but actual SMS/email delivery is not implemented/proven and event UI is missing. | Either wire a small gated transport with test-mode proof, or relabel all surfaces as `ARTIFACT_ONLY_NO_TRANSPORT`; add alert ledger UI/read model. | Transport can wait; truthfulness label is P0. |
| Operator usability | 64 | 36 | UI / operational | Too many actions require JSON/CLI inspection: receipts, outcome ledger, event alerts, inbox, sleeve report, and Research status. | Add a minimal operator control-plane page or read model summary for current Lite queue, required receipts, latest sleeve report, alert status, and Research blockers. | Minimum current queue/receipt checklist is P0; full control plane is P1. |
| Data readiness | 45 | 55 | Data | Research can run fixture-backed checks, but real price/volatility/breadth/macro/regime data bindings are incomplete or missing. | Add minimal read-only dataset contracts and dataset availability checks; run one deterministic DATA_AVAILABILITY_CHECK using real bound data. | Not required for first manual trade if candidate is already promoted; required for 90+. |
| AI functionality clarity | 72 | 28 | Documentation / architecture | AI intake is bounded but legacy/scaffolded; canonical AI-to-hypothesis behavior is not clearly proven. | Keep AI as explicit seed/intake only; add docs/tests saying no autonomous generation, no promotion authority, no runtime mutation. | Can wait if no AI is used for trading. |
| Self-healing safety | 84 | 16 | Proof / docs | Boundaries are documented, but coverage is uneven and not every repair path has proof it cannot fabricate evidence. | Add a self-healing boundary test/audit artifact covering missing receipts, missing outcomes, gates, promotion, and EOD state. | Can wait, but should be P1 before broad operator reliance. |
| Production readiness | 38 | 62 | Intentional / operational | Production automation is intentionally not ready. This should not be optimized for the manual-paper target. | Keep classification explicit: `NOT_READY_FOR_AUTOMATION`. Do not spend grade work trying to improve production readiness now. | No. |
| Complexity control | 70 | 30 | Architecture / UI | Artifact count and legacy PAPER/IB remnants exceed proven edge; operators can confuse old and new paths. | Add a "current authoritative surfaces" dashboard/doc and hide/label legacy PAPER/IB surfaces as diagnostic-only. Avoid new schemas unless a real gap appears. | Yes for labeling; cleanup can wait. |
| Long-term evolvability | 86 | 14 | Data / complexity | Layering is good, but dataset and UI gaps could force future patchwork if not handled deliberately. | Add minimal dataset contracts and operator read models before adding new Research features. | P1. |

## P0: Required To Start Real Manual Paper Trading

These are blockers for the first real supervised IB paper trade.

### P0.1 Current Promoted Executable Candidate Queue

Lost points recovered: manual execution readiness, promotion discipline, safety, operator usability.

Problem:
- Current runtime status is `ADVISORY_ONLY`.
- Runtime blockers include missing candidate input and missing promoted sleeve library.
- Current `operator_execution_queue.v1` has `execution_queue=[]`.
- Current `manual_trade_packet.v1` has `trade_candidates=[]`.

Concrete fix:
- Produce one real, non-demo, human-approved promoted sleeve entry in `promoted_sleeve_library.v1`.
- Run the canonical 15:50 Lite EOD path with explicit candidate input and promoted sleeve library.
- Generate:
  - `aegis_lite_eod_report.v1`
  - non-empty `operator_execution_queue.v1`
  - non-empty `manual_trade_packet.v1`
  - `edge_cluster.v1`
  - `sleeve_edge_overlap_review.v1`
  - `aegis_lite_operating_status.v1`
- Require readiness no higher than `READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING`; never automation-ready.

Acceptance proof:
- UI/read model shows exactly one executable candidate.
- Candidate has symbol, side, quantity/sizing, entry reference, stop price/logic, risk, sleeve id, source hypothesis id, edge cluster, blockers empty.
- `broker_submit_required=false`, `manual_execution_only=true`, `ib_automation_status=DEFERRED`.

### P0.2 End-To-End Manual Paper Lifecycle Dry Run

Lost points recovered: manual execution readiness, sleeve performance attribution, operator usability.

Problem:
- Offline smoke exists, but no actual operator workflow is proven from current Lite recommendation through manual receipt/outcome/report.

Concrete fix:
- Run a supervised dry workflow before real paper entry:
  1. Current EOD queue/packet exists.
  2. Operator reviews `/aegis-lite`.
  3. Operator records a dry `manual_execution_receipt.v1`.
  4. Operator records an outcome ledger row or attribution row.
  5. Build `sleeve_performance_report.v1`.
  6. Confirm Research feedback remains offline only.

Acceptance proof:
- Report classifies the trade lifecycle correctly.
- Missing receipt/outcome paths are visible and not treated as zero return.
- No broker/IB path is called.

### P0.3 One Supervised Real IB Paper Trade Smoke

Lost points recovered: manual execution readiness, sleeve attribution proof, operator confidence.

Problem:
- The system has no real operator-entered IB paper fill/stop/outcome proof.

Concrete fix:
- After P0.1 and P0.2 pass, allow at most one supervised manual IB paper trade.
- Operator manually enters position and immediately enters protective stop.
- Operator records:
  - receipt id
  - recommended trade id
  - fill timestamp
  - fill price
  - quantity
  - stop entered
  - stop price
  - operator notes
  - deviation from recommendation

Acceptance proof:
- `manual_execution_receipt.v1` exists and validates.
- Outcome row exists when closed or marked.
- `sleeve_performance_report.v1` consumes the actual receipt/outcome.
- Research feedback task/result is offline only.

### P0.4 Operator Control-Plane Clarity For Current Queue

Lost points recovered: safety, operator usability, complexity control.

Problem:
- `/aegis-lite` exists, but receipt, outcome, event alert, sleeve report, and Research status remain mostly CLI/JSON.
- Operators could still act on stale/demo/blocked artifacts.

Concrete fix:
- Add or verify a minimal operator-facing current-state summary that shows:
  - report date/run id/generated time
  - readiness classification
  - executable count
  - blocked/advisory count
  - current blockers
  - release match
  - promoted sleeve/source hypothesis
  - manual-only/no broker flags
  - "receipt required after entry" status
  - latest sleeve report path/status

Acceptance proof:
- No JSON/log inspection required to know whether there is a trade.
- Blocked/advisory/demo states cannot visually resemble executable trades.

### P0.5 Alert Transport Truthfulness

Lost points recovered: event awareness / alert trust, operator usability.

Problem:
- Alert gate writes message bodies and ledger rows with `WOULD_SEND`, but actual SMS/email transport is not proven.

Concrete fix:
- Do not implement transport yet unless deliberately scoped later.
- Rename/label operator-facing alert status as `GATE_ONLY_NO_TRANSPORT` or `ARTIFACT_ONLY_NO_DELIVERY`.
- Ensure docs/UI do not say "sent" unless a configured transport actually sent the message.

Acceptance proof:
- Alert ledger distinguishes `WOULD_SEND` from actual delivery.
- UI/docs explicitly state no real email/SMS delivery exists.

### P0.6 Keep 15:50 Runtime Alignment Proved

Lost points recovered: determinism, operator confidence.

Problem:
- The audit previously found timer drift; current evidence says 15:50. This must stay true before trading.

Concrete fix:
- Before each paper-trading smoke, run/read:
  - active user systemd timer status
  - `aegis_lite_operating_status.v1`
  - release integrity status
- Fail closed if any source is not 15:50 ET or release match is not `MATCH`.

Acceptance proof:
- Current runtime/status artifact says `target_time_et=15:50`.
- Active systemd timer trigger is near 15:50 ET.
- Readiness report has no stale 15:35 claims.

## P1: Required To Reach 90+ Hostile Audit Grade

These are not all required before the first supervised paper trade, but they are required to move the audit materially above 90.

### P1.1 Minimal Research Dataset Binding

Lost points recovered: data readiness, determinism, Research integrity, long-term evolvability.

Problem:
- Research executor supports explicit inputs and fixture metrics, but real dataset contracts are missing for key hypothesis families.

Concrete fix:
- Define and implement minimal read-only dataset availability contracts for:
  - price history
  - volatility/VIX/ATR
  - breadth/participation
  - macro event calendar
  - regime labels
  - Lite outcome/sleeve performance data
- Add a `DATA_AVAILABILITY_CHECK` proof that reports available/missing datasets without fabricating readiness.

Acceptance proof:
- At least one seeded hypothesis runs `DATA_AVAILABILITY_CHECK` against real bound data.
- Missing datasets become blockers, not placeholder success.

### P1.2 Real Offline Research Executor Proof

Lost points recovered: Research integrity, data readiness, AI clarity.

Problem:
- Offline executor is real but still shallow where fixture metrics stand in for actual historical evidence.

Concrete fix:
- Run one deterministic offline task sequence for a seeded hypothesis:
  - `DEFINITION_CHECK`
  - `DATA_AVAILABILITY_CHECK`
  - one of `REPLAY_ANALYSIS` or `BACKTEST`
  - `EXPECTANCY_REVIEW`
- Require evidence packet refs, result ledger refs, methodology version, code version, and data snapshot refs.

Acceptance proof:
- Hypothesis lifecycle changes only through result application.
- Failed/insufficient results are preserved.
- No promotion or Lite mutation occurs.

### P1.3 Operator Surfaces For Receipts, Outcomes, Sleeve Report, And Alerts

Lost points recovered: operator usability, manual execution readiness, sleeve attribution, event trust.

Problem:
- Too much of the current workflow requires CLI/JSON inspection.

Concrete fix:
- Add lightweight read-only surfaces or one consolidated operator page for:
  - manual receipts required/missing
  - latest outcome ledger status
  - latest sleeve performance report
  - event alert gate/ledger status
  - alert transport status
  - Research Lab blockers/promotions summary

Acceptance proof:
- Operator can answer "what do I do next?" without opening JSON.
- Missing receipt/outcome is visible as a blocker.
- Alert status cannot be mistaken for delivered SMS/email.

### P1.4 Sleeve Performance Report From Real Evidence

Lost points recovered: sleeve performance attribution, manual execution readiness, operator confidence.

Problem:
- The report is implemented and tested, but proof is controlled/offline.

Concrete fix:
- Build `sleeve_performance_report.v1` from the first real supervised paper-trade receipt and outcome.
- Include:
  - recommendation vs actual fill
  - slippage
  - stop entered/matched
  - validity compliance if event-driven
  - return when available
  - sleeve/source hypothesis attribution
  - Research follow-up recommendations

Acceptance proof:
- Missing outcome does not become zero return.
- Once outcome exists, return and attribution are computed.
- Research feedback remains offline.

### P1.5 Legacy/Diagnostic Surface Labeling

Lost points recovered: architecture coherence, complexity control, operator usability.

Problem:
- Legacy PAPER/IB tools and artifacts remain in repo/runtime and can confuse the operator.

Concrete fix:
- Add a current-authority matrix:
  - authoritative Lite artifacts
  - advisory Research artifacts
  - pre-research Inbox artifacts
  - diagnostic legacy PAPER/IB artifacts
- Ensure UI/status docs label old PAPER readiness/submission paths as `LEGACY_DIAGNOSTIC_DEFERRED`.

Acceptance proof:
- Operator cannot reasonably infer that a legacy PAPER submit/readiness artifact is the current operating model.

### P1.6 Self-Healing Boundary Proof

Lost points recovered: self-healing safety, safety/fail-closed behavior.

Problem:
- Self-healing is documented safely, but there is uneven proof.

Concrete fix:
- Add a focused self-healing/integrity review proving:
  - missing receipt is not repaired with fake data
  - missing outcome is not converted to zero return
  - promotion cannot be auto-created
  - EOD decisions are not mutated
  - alert delivery is not fabricated

Acceptance proof:
- Review artifact or tests flag unsafe repair attempts.

### P1.7 AI Functionality Boundary Cleanup

Lost points recovered: AI functionality clarity, Research integrity.

Problem:
- Legacy AI hypothesis intake exists, but canonical AI-to-Research behavior is still easy to overstate.

Concrete fix:
- Document and test:
  - AI may provide explicit seed text or structured batch input.
  - AI does not autonomously discover, validate, promote, size, trade, or mutate runtime.
  - Canonical durable hypotheses are `research_hypothesis.v1`, not legacy `edge_hypothesis.v1`.

Acceptance proof:
- No operator-facing doc claims autonomous AI strategy generation.

## P2: Useful Later

These improve polish or future capability but should not block the next manual-paper milestone.

### P2.1 Actual SMS/Email Transport

Only add this after the alert gate is stable and operator wording is correct.

Required guardrails:
- Transport can send only when `trade_capture_alert_gate.v1` is `ACTIONABLE_TRADE` or `URGENT_ACTIONABLE_TRADE`.
- Delivery status must distinguish sent, failed, suppressed, and artifact-only.
- No broker/order/fill side effects.

### P2.2 Dedicated Operator Inbox UI

Useful, but not required for first paper trade. CLI/docs are enough for offline idea capture.

### P2.3 Dedicated Research Lab UI

Useful for weekly review and Research scale, but not required before the first supervised trade.

### P2.4 Advisor Benchmark Comparison

Useful after enough paper-trade performance evidence exists. It should not precede reliable receipt/outcome/sleeve measurement.

### P2.5 Legacy PAPER/IB Code Cleanup

Useful to reduce complexity, but risky if done before the manual-paper path is proven. Prefer labeling/deferment first.

### P2.6 Advanced Knowledge Graph Or AI Research Expansion

Do not add until real usage shows retrieval/relationship pain. Current graph should remain generated/index-only.

## 90+ Grade Recovery Roadmap

| Step | Expected Grade Impact | Why |
|---|---:|---|
| P0.1 promoted executable queue | +4 to +6 | Converts Lite from safe-but-empty to operationally actionable. |
| P0.2 dry lifecycle proof | +3 to +5 | Proves manual packet/receipt/outcome/report loop without IB risk. |
| P0.3 one supervised paper trade smoke | +4 to +6 | Removes the largest proof gap in manual execution readiness. |
| P0.4 operator current-state clarity | +3 to +5 | Reduces chance of unsafe operator interpretation. |
| P0.5 alert truthfulness | +1 to +2 | Prevents misleading claims around SMS/email. |
| P1.1 dataset binding | +5 to +8 | Raises the weakest category: data readiness. |
| P1.2 real Research executor proof | +3 to +5 | Converts Research from credible architecture to useful research engine. |
| P1.3 operator surfaces | +4 to +7 | Raises the lowest non-data category: operator usability. |
| P1.4 real sleeve report proof | +2 to +4 | Makes performance attribution credible. |
| P1.5 legacy labeling | +1 to +3 | Improves complexity control and architecture clarity. |

Expected result after P0 plus the most important P1 items: **90-93/100**.

## Stop Rules

Do not proceed to real manual paper trading if any of these are true:

- Current Lite readiness is `ADVISORY_ONLY` or `NOT_READY`.
- `operator_execution_queue.v1` is missing or empty.
- `manual_trade_packet.v1` is missing or empty.
- Candidate lacks entry, stop, risk, quantity/sizing, symbol, side, sleeve, or source hypothesis.
- Candidate is demo/dry-run only.
- Release status is mismatch.
- Active timer/status does not say 15:50 ET.
- UI does not clearly show blocked/advisory vs executable.
- Operator cannot record receipt immediately after manual entry.
- Any path requires broker submit, IB transmit, or automated fill lifecycle.

## Final Recommendation

Do not redesign. Do not add broker automation. Do not add new architecture.

The next highest-value improvement is **one real promoted executable candidate through the existing Lite EOD path**, followed by a **supervised dry lifecycle proof** and then a **single supervised IB paper trade smoke**. Dataset binding and operator read-model improvements are the next grade movers after that.
