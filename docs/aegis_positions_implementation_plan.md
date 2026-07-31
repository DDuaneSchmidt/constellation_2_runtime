# Aegis Positions Implementation Plan

## Mission

Prove the rebuilt Positions screen can be built from existing Aegis data and backend systems before implementation begins.

This is an implementation plan only. It does not authorize UI code changes, route changes, backend logic changes, trading changes, paper execution changes, or canonical artifact changes.

Product authority:

* `docs/aegis_positions_screen_spec.md`
* `docs/aegis_positions_candidates_boundary.md`
* `docs/aegis_today_screen_spec.md`

Primary question:

```text
What do we currently own?
```

## Current State

### Current Screen(s)

* `/aegis-positions`
* `/api/aegis/positions`
* `/api/aegis/positions/latest`
* legacy related route: `/aegis-open-paper-positions`
* diagnostics route: `/aegis-positions-diagnostics`

### Current Components

* Positions shell page rendered by `renderPositionsWorkspace()` in `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`.
* Lightweight positions payload built by `_positions_lightweight_payload_v1()` in `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`.
* Open Positions table.
* Positions primary summary strip.
* Today's Candidates section.
* Candidate capture confirmation panel.
* Closed Positions table.
* Diagnostics link.
* Row details for receipts and position metadata.

### Current Problems

* The page answers two different questions at once: what is open and what candidate workflow needs review.
* Candidate capture status is built inside the positions API payload.
* `today_candidates` appears in the same read model as `open_positions` and `closed_positions`.
* The summary strip includes `Today's Candidates` and `Actionable Today`, which violates the Positions product boundary.
* Candidate readiness and signal evidence text appears on a page whose primary purpose is current ownership.
* Position mark/P&L quality is not elevated enough relative to candidate workflow content.
* The page can visually imply that candidate actions are part of position ownership.

### Current Coupling Issues

* `_positions_lightweight_payload_v1()` reads `candidate_lifecycle_projection_v1`, `signal_evidence_boundary_v1`, and `duplicate_candidate_v1` to build both position and candidate rows.
* `renderPositionsWorkspace()` renders open positions and candidate workflow sections from the same payload.
* `renderPositionsPrimarySummary()` includes candidate counts.
* `renderCandidateCaptureConfirmationPanel()` is called from Positions.
* `positionsTodayCandidateColumns()` and candidate action controls are reachable from the Positions render path.
* `/api/aegis/positions` resolves an operator truth day and then uses candidate lifecycle projection as a combined source for positions and candidates.

### Current Operator Confusion

* Operators cannot tell whether Positions is a holdings screen or a candidate workflow screen.
* Candidate blocker/capture language makes a position screen feel like a candidate review queue.
* Open positions, closed positions, candidate capture, and candidate readiness compete for attention.
* Missing mark/P&L information is less clear than candidate status, even though mark quality is central to current ownership.

## Target State

### Screen Purpose

Show current paper holdings, exposure, entry information, current values, P&L when available, and data-quality warnings.

The rebuilt screen must not display candidate workflow details as primary content.

### Operator Questions Answered

* What do we currently own?
* How many positions exist?
* What symbols/holdings are open?
* What is current exposure?
* When were positions entered?
* What is the current value?
* Is P&L available and trustworthy?
* Are marks stale or incomplete?
* Is anything concerning about risk or concentration?
* Do I need to inspect a position or evidence?

### Components Required

* Positions Status Summary.
* Open Positions Table.
* Holdings and Exposure Summary.
* Mark and P&L Quality Banner.
* Risk and Concentration Summary.
* Position Detail Drawer.
* Position Review Link.
* Positions Evidence Drawer, collapsed by default.

### Components Reused

| Component | Source | Reuse plan | Reason |
| --- | --- | --- | --- |
| Open Positions table structure | `renderPositionsOwnershipTable()` | Improve | Table already expresses core ownership rows. Remove candidate-adjacent assumptions and strengthen mark/P&L status. |
| Closed Positions table | `renderClosedPositionsTable()` | Improve | Useful if scoped as position history. Keep below open ownership, not as workflow competition. |
| Row detail pattern | `position-row-details` details blocks | Improve | Good secondary evidence affordance when collapsed. |
| Position number helpers | `positionExposure()`, `positionPnl()`, `positionHoldTime()` | Improve | Reusable transformations, but reliability must be explicit when source fields are missing. |
| Surface/day gating inputs | operator surface readiness / Today envelope pattern | Reuse as backend gate | Useful as safety input, not visible product vocabulary. |

### Components Bypassed

| Component | Source | Bypass plan | Reason |
| --- | --- | --- | --- |
| Today's Candidates section | `renderPositionsWorkspace()` | Do not render on rebuilt Positions | Candidate workflow belongs on Candidates. |
| Candidate capture confirmation panel | `renderCandidateCaptureConfirmationPanel()` | Do not call from Positions | Violates ownership boundary. |
| Candidate candidate columns | `positionsTodayCandidateColumns()` | Unreachable from Positions | Candidate details belong on Candidates. |
| Candidate action controls | candidate workflow handlers | Unreachable from Positions | Candidate actions are not position ownership. |
| Runtime/contract raw banners | shared legacy renderers | Bypass primary content | Raw governance vocabulary is not operator ownership language. |

### Components Removed From Positions

* Today's Candidates table.
* Output candidate capture status panel.
* Candidate readiness state widgets.
* Candidate blockers and repair commands.
* Candidate action buttons.
* Candidate generation diagnostics as primary content.

## Data Mapping

| Visible field | Data source | Current source location | Transformation required | Missing source | Reliability level |
| --- | --- | --- | --- | --- | --- |
| Requested day | route query / operator day | `routeParams.day`, API requested day | Preserve exact requested day; show if historical | None | High if explicit day is preserved |
| Source day | positions payload / surface readiness | `/api/aegis/positions.day_utc`, surface readiness row | Compare with requested day; block action if mismatch | None | High when gated |
| Open position count | `open_positions` | `_positions_lightweight_payload_v1().summary.open_positions` | Count open position rows only | None | High if lifecycle projection is current |
| Position id | open position row | `open_positions[].position_id` or candidate id fallback | Prefer position id; fallback only as row provenance | Stable position id may be missing on older rows | Medium |
| Symbol | open position row | `open_positions[].symbol` | Display as primary row label | None | High |
| Sleeve | open position row / sleeve attribution | `open_positions[].sleeve`, sleeve analytics | Recover from row/attribution; UNKNOWN only with reason | Some rows may lack sleeve attribution | Medium |
| Quantity | open position row / entry receipt | `open_positions[].quantity` | Numeric display; show unavailable if absent | None known | Medium-High |
| Entry price | open position row / entry receipt | `entry_price`, `paper_entry_price` | Prefer actual entry receipt value | None known | Medium-High |
| Entry timestamp/date | open position row / entry receipt | `entry_time`, `timestamp_utc`, receipt timestamp | Format as operator date/time | Some rows may lack timestamp | Medium |
| Current mark | canonical market marks / row mark fields | `mark_price`, `current_certified_mark`, `current_mark` | Prefer certified current mark; mark stale/missing explicitly | Current-day mark may be missing | Medium-High when certified |
| Current value | quantity x mark or row market value | row `market_value`/computed from quantity and mark | Compute only if quantity and mark are reliable; otherwise null with reason | Certified mark or quantity may be missing | Medium |
| Unrealized P&L | paper P&L report / row fields | `unrealized_pnl`, `pnl`, `paper_pnl_report` | Display only if report or row calculation is trustworthy | P&L report can be noncanonical | Medium |
| P&L status | paper P&L report / performance diagnostics | performance and P&L reports | Translate to complete, partial, unavailable | None | Medium |
| Mark coverage | market data / performance diagnostics | market data coverage and paper P&L diagnostics | Percent and missing symbol count | Coverage artifact may be missing | Medium |
| Stale/missing price warning | market marks | canonical market data / mark diagnostics | Show affected count and symbols only in details | None | High when diagnostics exist |
| Exposure by sleeve | open positions + marks + sleeve attribution | sleeve analytics / computed from rows | Compute from current value only when marks reliable | Sleeve attribution can be missing | Medium |
| Concentration warning | open positions + exposure summary | derived from marked positions | Flag largest symbol/sleeve; do not imply advice | Threshold policy may be absent | Medium |
| Position review availability | position review brief artifact | `aegis_position_review_brief_v1` | Link only if matching current-day review exists | Current-day briefs may be unavailable | Medium |
| Evidence references | position ledger, receipts, marks, P&L paths | payload source paths, row receipt ids | Collapse by default | Some source paths missing | Medium |
| Closed position count | `closed_positions` | `_positions_lightweight_payload_v1().summary.closed_positions` | Count closed rows | None | Medium |
| Closed position rows | closed positions projection | `closed_positions[]` | Keep below open positions; no candidate fields | None | Medium |

## State Matrix

### NORMAL

Visible message:

```text
Open paper positions are current for the selected day.
```

Components shown:

* Positions Status Summary
* Open Positions Table
* Holdings and Exposure Summary
* Mark and P&L Quality Banner, if useful
* Risk and Concentration Summary
* Position Detail Drawer
* Closed Positions below open positions
* Evidence collapsed

Components hidden:

* Candidate capture panel
* Today's Candidates table
* Candidate blockers
* Candidate qualification/readiness details

Operator expectation:

The operator can inspect current ownership and mark/P&L quality without seeing candidate workflow.

### NO_ACTIVITY

Visible message:

```text
No open paper positions are recorded for this day.
```

Components shown:

* Positions Status Summary
* Empty ownership state
* Evidence link if source evidence exists

Components hidden:

* Open Positions Table
* Exposure metrics
* Risk/concentration metrics
* Candidate workflow

Operator expectation:

No current holdings exist. The operator should not see candidate capture or candidate review content here.

### BLOCKED

Visible message:

```text
Position state is unavailable for this day because required ownership evidence is missing, stale, or inconsistent.
```

Components shown:

* Blocked state summary
* Reason
* Impact
* Next step
* Evidence collapsed

Components hidden:

* Open Positions Table
* Exposure/P&L cards
* Candidate workflow
* Any action-looking controls except evidence/navigation

Operator expectation:

Do not trust position counts or values until the named blocker is repaired.

### DEGRADED

Visible message:

```text
Positions are visible, but some values are incomplete.
```

Components shown:

* Open Positions Table with row-level unavailable fields
* Mark/P&L quality warning
* Holdings/exposure only for covered rows
* Evidence collapsed

Components hidden:

* Candidate workflow
* Any canonical-looking complete P&L claim for incomplete data

Operator expectation:

Position identity can be trusted, but marks/P&L/exposure may be partial.

### NEEDS_USER_ACTION

Visible message:

```text
A position needs review or evidence inspection.
```

Components shown:

* Position rows with View Details / View Review / Open Evidence
* Reason user review is needed

Components hidden:

* Candidate capture controls
* Broker/live/execution controls

Operator expectation:

The action is inspect/review only. It is not a trade or candidate action.

### MANUAL_IB_CAPTURE_READY

Visible message:

```text
Manual candidate capture does not belong on Positions. Open Candidates to review capture-ready candidates.
```

Components shown:

* Optional link to Candidates if Today or candidate summary says capture is ready

Components hidden:

* Confirm Captured
* Mark Not Captured
* Defer
* Candidate detail rows

Operator expectation:

Positions remains ownership-only; manual capture happens on Candidates.

## Screenshot Acceptance

A non-engineer must be able to answer:

### What do we currently own?

Acceptance:

* The first visible section is about open positions or no open positions.
* It does not begin with candidates, contracts, invariants, or diagnostics.

### How many positions exist?

Acceptance:

* Open position count is visible above the table.
* Closed/recent positions do not obscure open positions.

### Is position data complete?

Acceptance:

* Mark coverage and P&L availability are plainly stated.
* Missing or stale price warnings are visible if relevant.

### Is anything concerning?

Acceptance:

* Concentration, stale marks, missing sleeves, or missing P&L are summarized in operator language.
* No issue is buried only in raw evidence.

### Do I need to do anything?

Acceptance:

* The page shows no trade/broker/candidate actions.
* If action is needed, it is review/detail/evidence only.

## Dependencies

### API Dependencies

* Existing `/api/aegis/positions` can supply initial position rows, but it must be treated as a mixed legacy payload during implementation.
* Existing position ledger and lifecycle projection provide open/closed position rows.
* Existing market data and P&L reports provide marks and P&L reliability.
* Existing sleeve analytics can provide sleeve attribution and exposure, when canonical.
* Existing position review API/artifacts provide links only.

### Existing Reusable Components

* Table primitives.
* Metric cards with reduced use.
* Row details disclosure.
* Evidence drawer pattern.
* Navigation and dark shell.
* Position formatting helpers.

### Existing Problematic Components

* `renderPositionsWorkspace()` as currently structured.
* `renderPositionsPrimarySummary()` because it includes candidate counts.
* `renderCandidateCaptureConfirmationPanel()` when called from Positions.
* `positionsTodayCandidateColumns()` on Positions.
* Candidate workflow command handlers reachable from Positions.
* Raw surface/contract banners as primary page content.

### Risks

* Current `/api/aegis/positions` is useful but mixed; an implementation that consumes it directly will reintroduce candidate workflow into Positions.
* Open position ids may be missing for older rows, requiring careful provenance display.
* Mark/P&L reliability varies by day and must not be overstated.
* Sleeve attribution may still be partial; UNKNOWN must be explained without dominating the page.
* If Position Review artifacts are missing, links must degrade cleanly rather than showing stale reviews.

## Boundary Violations To Fix During Implementation

| Current behavior | Why it is wrong | Owning screen |
| --- | --- | --- |
| Positions summary shows `Today's Candidates`. | Positions should answer current ownership only. | Candidates; Today may show count summary. |
| Positions summary shows `Actionable Today`. | Candidate actionability is candidate workflow. | Candidates. |
| Positions renders `Today's Candidates` section. | Candidate rows compete with open holdings. | Candidates. |
| Positions renders candidate capture confirmation. | Manual IB capture is not current ownership. | Candidates. |
| Positions calls candidate readiness/signal evidence copy. | Candidate qualification belongs to candidate pipeline. | Candidates. |
| Positions API payload includes `today_candidates`. | Mixed read model encourages mixed UI. | Candidates endpoint/envelope. |
| Positions row actions can look like operational workflow controls. | Positions actions must be view/review/evidence only. | Positions, with strict read-only language. |
| Open Paper Positions legacy route duplicates Positions. | Duplicate ownership views reduce trust. | Positions should absorb useful table value. |

## Implementation Order Estimate

1. Define the operator-ready positions envelope from existing payload/artifacts without changing producers.
2. Build a new isolated Positions renderer against that envelope.
3. Render only Positions Status Summary, Open Positions Table, Mark/P&L Quality, Exposure, and collapsed Evidence.
4. Keep closed positions below open positions.
5. Verify screenshot against `docs/aegis_positions_screen_spec.md`.
6. Add tests forbidding candidate capture, candidate tables, candidate blockers, and P&L overstatement on Positions.

## Feasibility Conclusion

The Positions rebuild is feasible from existing backend systems. The required ownership data already exists in `open_positions`, position lifecycle/ledger projections, market mark/P&L artifacts, sleeve analytics, and position review artifacts. The main implementation risk is not missing data; it is preventing the current mixed positions/candidates payload and renderer from leaking candidate workflow back into the Positions screen.
