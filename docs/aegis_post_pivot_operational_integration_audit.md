# Aegis Post-Pivot Operational Integration Audit

Generated: 2026-05-15  
Scope: Aegis Lite, Event Monitoring, Event Rules Registry, manual packets, Trade Sizing Engine, receipts, outcome ledger, sleeve performance, AI Feedback Engine, Evidence Gate, Research Task Gate, Research Lab, Operator Inbox, ChatGPT Control Packet, and operator status/control plane.

## Executive Verdict

The post-pivot ecosystem is architecturally coherent and mostly integrated across the manual paper-trade lifecycle. The strongest path is:

`promoted sleeve signal -> sizing -> manual trade packet -> receipt -> outcome ledger -> sleeve performance -> AI feedback -> offline Research task`

This path is implemented and covered by focused tests, but it is not yet fully proven as a live operator workflow against a current real promoted candidate and supervised IB paper trade. The system remains broker-independent and fail-closed by design. Remaining gaps are mostly operational proof, UI completeness, dataset binding, and event-packet sizing parity rather than core architecture.

Overall integration status: `PARTIAL_READY_FOR_SUPERVISED_SMOKE`  
Manual paper readiness implication: ready to rehearse and run one supervised smoke only when a `REAL_RUNTIME` promoted actionable packet exists and all current blockers are clear.

## Integrated Lifecycle Map

1. Promoted sleeve candidate enters Aegis Lite EOD evaluation.
2. Trade Sizing Engine computes deterministic sizing guidance or sizing blockers.
3. Manual trade packet carries entry, stop, risk, quantity, sizing lineage, runtime truth, and blockers.
4. Operator manually enters the trade, then records a manual execution receipt by CLI.
5. Operator records outcome by CLI after exit/close.
6. Sleeve performance report joins packet, receipt, outcome, attribution, event, and alert references.
7. Evidence Gate evaluates sample count and data quality.
8. AI Feedback Engine uses deterministic fallback, writes findings with evidence refs, and gates offline Research Lab tasks.
9. Research Lab receives only offline tasks; no production mutation, trade creation, broker action, auto-promotion, or auto-demotion is allowed.
10. ChatGPT Control Packet summarizes the operator-facing state and blocks trade advice unless runtime truth and actionability are fully proven.

## Verification Findings

| Area | Status | Evidence | Finding |
| --- | --- | --- | --- |
| Trade sizing core | `PROVEN` | `constellation_2/common/aegis_trade_sizing_engine_v1.py:38` blocks invalid/demo/dry/stale/unpromoted candidates; `:69` applies risk tiers and paper cap; `:76` computes `suggested_quantity`; `:92` returns sizing lineage and `ai_selected_size=false`. | Deterministic, conservative sizing exists and fails closed. |
| Manual packet sizing integration | `PROVEN` | `constellation_2/common/aegis_research_lab_v1.py:2548` validates promoted sleeve source; `:2549` calls sizing engine; `:2588` includes sizing blockers in actionability; `:2612`-`:2626` writes sizing fields into the packet. Schema requires those fields at `governance/04_DATA/SCHEMAS/C2/REPORTS/manual_trade_packet.v1.schema.json:25`. | Manual packets now carry the sizing record needed for operator review and later audit. |
| Receipt sizing lineage | `PROVEN` | `ops/tools/record_manual_execution_receipt_v1.py:84` reads suggested quantity; `:85`-`:91` computes expected/actual risk; `:94` records quantity override; `:117`-`:121` writes sizing fields. | Operator overrides and sizing quality survive receipt capture. |
| Outcome sizing lineage | `PROVEN` | `ops/tools/record_trade_outcome_v1.py:89`-`:100` classifies sizing quality; `:146`-`:151` writes suggested/actual quantity, expected/actual risk, override reason, and sizing quality. | Outcome ledger can reconstruct sizing quality without IB integration. |
| Sleeve performance join | `PROVEN` | `constellation_2/common/aegis_sleeve_performance_report_v1.py:119`-`:125` joins sizing fields into lifecycle rows; `:263`-`:275` summarizes quantity overrides and sizing quality rows. | Performance reports do not collapse missing/overridden sizing into zero-return assumptions. |
| Runtime truth in Lite UI read model | `PARTIAL` | `constellation_2/phaseL/ui_api/aegis_lite_execution_queue_read_model.py:160` reads runtime truth; `:170`-`:178` allows executable only with no release mismatch, READY status, EXECUTABLE candidate, and `REAL_RUNTIME`; `:221` surfaces quantity; `:259` exposes runtime truth. | UI correctly blocks demo/dry/runtime mismatch, but it does not expose the full sizing lineage fields as first-class visible fields yet. |
| Event tactical packet guardrails | `PROVEN` | Event packet schema requires runtime truth and manual-only safety fields at `governance/04_DATA/SCHEMAS/C2/REPORTS/event_tactical_packet.v1.schema.json:7`; safety constants are at `:46`-`:52`. Packet classification blocks demo/dry at `constellation_2/common/aegis_event_monitoring_v1.py:520`-`:532`. | Event packets are non-canonical and cannot become actionable without gates. |
| Event packet sizing parity | `PARTIAL` | Event packet schema has `quantity_or_sizing_guidance`, entry, stop, and risk at `event_tactical_packet.v1.schema.json:21`-`:26`; event packet builder writes those at `aegis_event_monitoring_v1.py:472`-`:477`. | Event packets carry basic sizing guidance, but not the full Trade Sizing Engine v1 lineage (`portfolio_value_used`, `risk_pct_used`, `allowed_dollar_risk`, etc.). |
| AI Evidence Gate | `PROVEN` | `constellation_2/common/aegis_ai_feedback_engine_v1.py:98`-`:117` evaluates sample count, missing receipts/outcomes, stale data, labels, and data quality; `:136`-`:164` records no broker action and no production mutation. | Weak or dirty evidence is downgraded or blocked before conclusions/tasks. |
| AI findings and Research Task Gate | `PROVEN` | `aegis_ai_feedback_engine_v1.py:259`-`:282` dedupes/sorts findings and drops findings without evidence refs; `:340` blocks promotion/demotion findings without stronger evidence; `:402`-`:439` creates offline-only tasks only when evidence permits. | AI feedback is deterministic fallback only and cannot create production actions. |
| ChatGPT Control Packet | `PARTIAL` | `constellation_2/common/aegis_chatgpt_control_packet_v1.py:481`-`:502` normalizes trade items with quantity/risk basics; `:505`-`:524` blocks non-real/stale/incomplete/broker-mode-unsafe items; `:543`-`:570` derives runtime truth; `:573`-`:586` blocks/permits trade advice. | Fail-closed behavior exists, but the packet does not yet expose the full sizing-engine field set as a dedicated sizing/risk section. |
| Operator Status / control plane | `PARTIAL` | `ops/tools/build_aegis_operator_status_v1.py:127`-`:154` reports EOD, candidates, receipts, outcomes, sleeve performance, event monitor, dataset blockers, and next action. | Useful daily state exists, but it does not yet show detailed sizing/risk lineage or current AI feedback state. |
| Operator Inbox boundary | `PROVEN` | `constellation_2/common/aegis_operator_inbox_v1.py` records `production_mutation_allowed=false` in inbox/report artifacts. | Inbox remains capture-only and cannot create trades/tasks/sleeves directly. |

## Runtime Truth Propagation

| Surface | Status | Notes |
| --- | --- | --- |
| Manual trade packet | `PROVEN` | Schema supports `REAL_RUNTIME`, `DEMO_ONLY`, `DRY_RUN_ONLY` and requires runtime truth on packet and candidates. |
| Event tactical packet | `PROVEN` | Schema supports `REAL_RUNTIME`, `DEMO_ONLY`, `DRY_RUN_ONLY`; gates and UI/read model block demo/dry. |
| Aegis Lite UI queue | `PROVEN_FOR_ACTIONABILITY`, `PARTIAL_FOR_DETAIL` | Uses runtime truth to separate executable vs blocked/advisory cards, but sizing detail is compressed. |
| ChatGPT Control Packet | `PROVEN_FOR_FAIL_CLOSED`, `PARTIAL_FOR_SIZING_DETAIL` | Supports `REAL_RUNTIME`, `DEMO_ONLY`, `DRY_RUN_ONLY`, `PARTIAL_CONTEXT`, `ADVISORY_ONLY`, `BLOCKED`; trade advice gate is explicit. |
| AI Feedback | `PARTIAL` | Uses source artifact refs and safety fields, but does not carry runtime truth as a first-class review classification. |
| Operator Status | `PARTIAL` | Reports actionability and blockers, but runtime truth is not the central field surfaced to the operator. |

## Risk And Sizing Lineage

Sizing lineage is reconstructable across the core lifecycle:

- Packet: portfolio value, tier, risk percent, allowed dollar risk, risk per share, suggested quantity, position value, max loss, overlap/regime/concentration adjustments, blockers, reason codes.
- Receipt: suggested vs actual quantity, expected vs actual risk, sizing quality, override reason.
- Outcome: suggested vs actual quantity, expected vs actual risk, sizing quality, override reason, return, sleeve/event attribution.
- Sleeve performance: per-trade sizing rows and aggregate quantity-override count.

Status: `PROVEN` for EOD manual packet lifecycle.  
Gap: `PARTIAL` for event packets, which currently carry basic sizing guidance but not full sizing-engine lineage.

## Event Integration

Event Monitoring is safe and auditable: rules are versioned, event packets are non-canonical, demo/dry packets cannot be actionable, and event runs do not mutate EOD state. Event outcomes can be represented in receipts/outcomes and sleeve performance through `event_id`, `event_type`, `execution_sensitivity`, validity-window, and alert fields.

Status: `PARTIAL`

Remaining integration issue: event tactical packets need full sizing-engine field parity before event-driven manual trades have the same historical risk reconstruction quality as EOD packets.

## AI Feedback Integration

The AI Feedback Engine is currently a deterministic review engine with AI behavior disabled (`ai_used=false`, deterministic fallback true). It is properly gated:

- Evidence Gate blocks stale/dirty/insufficient data.
- Findings require evidence refs.
- Promotion/demotion candidates require stronger evidence and are review-only.
- Research task creation is offline-only and blocked when evidence gate does not allow it.
- Production mutation, broker action, auto-promotion, and auto-demotion are all false.

Status: `PROVEN_FOR_SAFETY`, `PARTIAL_FOR_REAL_AI`

This is the right current safety posture. The main missing proof is not governance; it is real outcome volume and dataset quality.

## Operator Usability

| Workflow | Status | Operator impact |
| --- | --- | --- |
| See actionable EOD packet | `READY_WITH_MANUAL_STEPS` | UI read model exists and blocks unsafe items; full sizing lineage may require artifact/packet inspection. |
| Understand quantity/risk | `PARTIAL` | Quantity, stop, and risk are visible; full sizing derivation is in packet/report artifacts, not clearly first-class in UI. |
| Enter receipt | `CLI_ONLY` | CLI exists; no broker access; no full UI form proven. |
| Enter outcome | `CLI_ONLY` | CLI exists and computes return/sizing quality; no UI form proven. |
| Review sleeve performance | `CLI_ONLY/PARTIAL_UI` | Report builder exists; UI completeness for the new sizing fields is not proven. |
| Review AI findings | `CLI_ONLY` | EOD/EOW CLIs and artifacts exist; UI surface is not proven. |
| Understand blockers | `PARTIAL` | Operator status and packet/read models expose blockers; one consolidated full UI control plane remains incomplete. |

## ChatGPT Control Packet Completeness

Status: `PARTIAL`

The packet correctly enforces fail-closed trade advice semantics:

- Non-`REAL_RUNTIME` blocks trade advice.
- Missing actionable item blocks trade advice.
- Non-`MANUAL_ONLY` broker mode blocks trade advice.
- Required trade fields are checked before capture guidance.

Gap: sizing state is represented through `quantity_or_sizing_guidance`, entry, stop, and risk, but the packet does not yet include full sizing lineage (`portfolio_value_used`, `risk_pct_used`, `allowed_dollar_risk`, `risk_per_share`, `suggested_quantity`, `max_loss_if_stopped`, sizing blockers) as a dedicated section.

## Remaining Operational Gaps

| Gap | Status | Blocks manual paper? | Blocks 90+ audit? |
| --- | --- | --- | --- |
| Real current promoted executable candidate proof | `UNPROVEN` | Yes, for actual trade day. | Yes |
| End-to-end supervised IB paper smoke | `UNPROVEN` | Yes, for claiming operational proof. | Yes |
| Event packets lack full sizing-engine lineage | `PARTIAL` | No for EOD-only smoke; yes for event trade parity. | Yes |
| UI does not expose full sizing derivation | `UI_MISSING/PARTIAL` | No if operator uses packet artifact/CLI, but weakens usability. | Yes |
| ChatGPT packet lacks dedicated sizing/risk section | `PARTIAL` | No if packet is advisory-only; yes for robust ChatGPT-assisted manual capture. | Yes |
| Operator status lacks full runtime-truth/sizing/AI feedback rollup | `PARTIAL` | No, but causes operator confusion risk. | Yes |
| Research datasets for real hypothesis tests | `DATA_MISSING/PARTIAL` | No for manual trading; yes for Research claims. | Yes |
| Event monitor current market data operation | `UNPROVEN` | No for EOD-only manual smoke. | Yes |
| Live email/SMS transport | `UNPROVEN/GATE_ONLY` | No; do not claim live alerting. | No if clearly documented |
| Active release/runtime alignment after uncommitted changes | `UNPROVEN` | Yes before using active UI/runtime as truth. | Yes |

## Top 10 Remaining Gaps

1. Prove a real `REAL_RUNTIME` promoted candidate can flow into the EOD packet with sizing.
2. Run one supervised paper smoke: packet -> receipt -> outcome -> sleeve performance -> AI feedback task.
3. Surface full sizing lineage in the Lite UI queue card and/or operator status.
4. Add full sizing-engine parity to event tactical packets or explicitly block event packet actionability until equivalent sizing is present.
5. Extend ChatGPT Control Packet with a dedicated sizing/risk lineage section.
6. Add AI feedback summary and Evidence Gate status to the operator status/control plane.
7. Bind Research datasets enough to test real hypotheses beyond fixtures.
8. Prove event monitor evaluation against current market data, or keep it clearly disabled/unproven.
9. Ensure active runtime release contains the post-pivot UI/event/control packet/sizing changes before operator use.
10. Keep live email/SMS classified as `GATE_ONLY_NO_TRANSPORT` until delivery is actually tested.

## Operational Blockers

Before claiming real manual paper-trading readiness:

- A current active runtime must match the repo/release containing Trade Sizing Engine changes.
- A real promoted sleeve must generate a `REAL_RUNTIME` actionable EOD packet.
- The operator must verify packet fields: symbol, side, entry, stop, risk, suggested quantity, max loss, blockers clear.
- A supervised smoke must prove receipt/outcome/sleeve performance/AI feedback loop on one paper trade.

Before event-driven manual trade use:

- Event packet sizing must be upgraded to full sizing-engine lineage or event packets must remain review-only.
- Event monitor must be proven on current data with no stale-input actionability.

## Remaining Manual Steps

- David must manually review the packet before acting.
- David must manually enter trades in IB paper.
- David must manually enter and confirm protective stops.
- David must record receipts via CLI.
- David must record exits/outcomes via CLI.
- David must run or review sleeve performance and AI feedback reports.
- David must review any Research Lab task recommendations before treating them as research work.

## Production-Quality Architecturally

- Broker-independent manual execution model.
- Deterministic, conservative sizing with explicit blockers.
- Promoted-sleeve source check before manual packet actionability.
- Runtime truth guardrails across manual/event packets and ChatGPT control packet.
- Receipt/outcome/sleeve-performance sizing lineage.
- Evidence Gate and offline Research task gating.
- Operator Inbox isolation from Research tasks, sleeves, and trades.
- Event monitoring separation from canonical EOD state.

## Still Lacking Operational Proof

- Live-day current promoted candidate with full packet and sizing.
- Real supervised manual IB paper lifecycle.
- Active runtime/UI deployment proof after the current uncommitted changes.
- Event monitor running against current data during market hours.
- Real Research dataset coverage for price, volatility, breadth, macro calendar, regime labels, and sleeve outcomes.
- Full operator UI coverage for sizing, AI feedback, and Research task status.

## Audit Conclusion

Aegis is coherent after the Trade Sizing Engine implementation. The major systems now connect without violating the pivot: no broker automation, no autonomous execution, no production mutation from AI or Research, and no demo/dry-run actionability. The next highest-value work is not more architecture. It is operational proof: generate one real current promoted EOD packet, run a supervised paper smoke, and verify the resulting receipt, outcome, sleeve performance, AI feedback, and Research task artifacts end to end.
