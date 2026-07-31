# Aegis Performance Implementation Plan

## Scope

This is an implementation plan only. It does not authorize UI, route, backend, trading, sleeve, candidate, research, canonical artifact, or safety-gate changes.

The rebuilt Performance screen must use existing evidence where possible and remain separate from System Health.

## Current State

Current screens/routes:

* Performance
* Paper Performance
* Sleeve Analytics
* Position P&L views
* Evidence/contract diagnostics embedded in analytics surfaces

Current components:

* Performance degraded banner
* total P&L metric cards
* mark coverage / data quality indicators
* position attribution rows
* sleeve performance summaries
* raw contract panel
* diagnostics/evidence drawer

Current problems:

* Performance can display analytics-looking cards while also saying data is noncanonical or degraded.
* Raw contract and invariant terminology leaks into the primary analytics experience.
* Performance and System Health overlap on data readiness and repair meaning.
* Position ownership detail can compete with performance attribution.
* Sleeve Analytics is adjacent but not identical; detailed sleeve analytics should not replace Performance.

Current coupling issues:

* Browser-side rendering may assemble meaning from multiple backend artifacts.
* Performance trust can depend on semantic invariants, surface readiness, P&L, marks, and sleeve analytics.
* Repair status sometimes appears on analytics pages instead of System Health.

Current operator confusion:

* Operators can see `NOT_CANONICAL`, `PASS`, unavailable metric cards, or raw contract terms without a clear performance answer.
* It is not always clear whether P&L is complete, partial, or unavailable.
* It is not always clear which values are realized versus unrealized.

## Target State

Screen purpose:

```text
Show portfolio and strategy performance, explain whether the data is complete, and identify contributors and trends.
```

Operator questions answered:

* How are we doing?
* Is performance data complete?
* What is working?
* What is not working?
* What trends matter?

Components required:

* Performance Summary
* Data Completeness
* Contributors
* Strategy / Sleeve Performance Summary
* Trend Summary
* Collapsed Evidence

Components reused:

* existing shell navigation and route chrome
* shared cards/tables/details primitives
* existing paper P&L report data
* existing daily paper performance data
* existing sleeve analytics summary data
* existing market mark coverage data

Components bypassed:

* raw contract panel as primary content
* raw semantic invariant display as primary content
* unavailable metric grids
* repair queue UI
* candidate/research/position workflow components

Components removed from primary Performance:

* surface contract vocabulary
* semantic invariant vocabulary
* system repair commands
* raw artifact paths
* candidate or research cards

## Proposed Operator API Boundary

Preferred endpoint:

```text
/api/aegis/operator/performance
```

Purpose:

Return an operator-ready Performance summary without requiring page-local interpretation of raw artifacts.

Suggested top-level fields:

```json
{
  "ok": true,
  "requested_day": "2026-05-30",
  "source_day": "2026-05-30",
  "state": "NORMAL|NO_DATA|PARTIAL_DATA|DEGRADED",
  "headline": "Performance is partially available.",
  "summary": "Plain-English performance and trust statement.",
  "data_complete": false,
  "metrics": {
    "total_pnl": null,
    "realized_pnl": null,
    "unrealized_pnl": null,
    "return_pct": null
  },
  "coverage": {
    "mark_coverage_pct": null,
    "missing_marks": null,
    "attribution_coverage_pct": null,
    "benchmark_available": false
  },
  "contributors": {
    "positive": [],
    "negative": []
  },
  "sleeves": [],
  "trends": [],
  "warnings": [],
  "evidence_available": true,
  "evidence_refs": []
}
```

## Data Mapping

| Visible field | Existing source | Current source location | Transformation required | Missing source | Reliability |
| --- | --- | --- | --- | --- | --- |
| Total P&L | paper P&L / daily paper performance | `aegis_paper_pnl_report_v1`, `aegis_daily_paper_performance_v1` | show only if complete or label partial | none known | High when canonical |
| Realized P&L | exit receipts / paper P&L | paper P&L report | null with reason if no exits | exit history may be absent | Medium |
| Unrealized P&L | open positions + marks | paper P&L report | require mark coverage caveat | marks can be missing | High when marks current |
| Mark coverage | market data coverage / paper P&L | `aegis_market_data_coverage_v1`, P&L report | translate to complete/partial/missing | none known | High |
| Attribution coverage | sleeve analytics / attribution recovery | `aegis_sleeve_analytics_v1`, sleeve performance truth | translate to performance trust | none known | Medium |
| Positive contributors | position attribution | daily performance / position attribution rows | rank by contribution | may need normalized contributor rows | Medium |
| Negative contributors | position attribution | daily performance / position attribution rows | rank by contribution | may need normalized contributor rows | Medium |
| Sleeve summary | sleeve analytics | `aegis_sleeve_analytics_v1` | summary only, no detailed sleeve page | none known | Medium |
| Benchmark comparison | benchmark report if present | advisor/benchmark artifacts | show unavailable if missing | benchmark may be absent | Low/Medium |
| Trend summary | performance history | daily paper performance history | summarize recent trend | history may be sparse | Medium |
| Trust statement | semantic invariants + surface readiness | invariant/readiness reports | translate to operator language | none known | High |

## State Matrix

### NORMAL

Visible message:

```text
Performance is available for this day.
```

Components shown: Performance Summary, Data Completeness, Contributors, Strategy / Sleeve Performance Summary, Trend Summary if available, Collapsed Evidence.

Components hidden: repair workflow, raw contracts, system health diagnostics, candidate/research workflows.

Operator expectation: Review analytics; no operational action is required.

### NO_DATA

Visible message:

```text
No performance data is available for this day.
```

Components shown: unavailable explanation, next expected performance run if known, Collapsed Evidence if available.

Components hidden: metric cards, contributor tables, strategy rankings, trends.

Operator expectation: Nothing can be evaluated yet.

### PARTIAL_DATA

Visible message:

```text
Performance is partially available.
```

Components shown: limited metrics, completeness statement, partial contributors if trustworthy, Collapsed Evidence.

Components hidden: complete-looking total P&L or rankings if coverage is incomplete.

Operator expectation: Use only the labeled values; inspect System Health if repair is needed.

### DEGRADED

Visible message:

```text
Performance is degraded and should not be treated as complete.
```

Components shown: reason, impact, next step, data completeness, Collapsed Evidence.

Components hidden: canonical-looking analytics cards, benchmark comparisons, unqualified totals.

Operator expectation: Treat analytics as limited or unavailable.

## Screenshot Acceptance

A non-engineer must be able to answer:

* How are we doing?
* Is performance data complete?
* What is working?
* What is not working?

Visible proof required:

* one clear performance headline
* P&L values only when trustworthy or clearly partial
* completeness statement near the top
* contributors visible when data allows
* no raw contract/invariant language as primary content
* no repair queue as primary content

## Dependencies

API dependencies:

* `/api/aegis/operator/performance` preferred
* existing paper performance API as source
* sleeve analytics API as source summary
* semantic invariant/surface readiness evidence as trust input

Reusable components:

* shell navigation
* dark theme
* summary cards
* compact tables
* collapsed evidence details

Problematic components:

* raw contract panel
* unavailable metric grid
* legacy performance dashboard variants
* repair/health panels inside analytics page

Risks:

* Current performance evidence may be noncanonical for current day.
* Position and sleeve attribution may be partial.
* Benchmark source may not exist.
* Existing UI tests may assert old raw contract text.

## Implementation Order

1. Define operator-ready performance envelope from existing artifacts.
2. Add state mapping tests for NORMAL, NO_DATA, PARTIAL_DATA, DEGRADED.
3. Replace Performance primary content with Performance Summary and Data Completeness.
4. Add Contributors from existing attribution rows when trustworthy.
5. Add Strategy / Sleeve Performance summary using sleeve analytics only as summary.
6. Add Trend Summary only if history exists.
7. Move raw evidence to collapsed Evidence.
8. Capture screenshot and review against spec before writing broad UI tests.

## Boundary Violations To Remove During Implementation

| Current behavior | Why it is wrong | Owning screen |
| --- | --- | --- |
| Raw contract panel appears on Performance. | Engineering-only metadata; not an analytics answer. | Evidence / Audit Detail or System Health. |
| Semantic invariant terms appear as visible analytics state. | Backend safety vocabulary, not operator language. | Evidence / Audit Detail. |
| Repair/status commands appear on analytics page. | Repair workflow belongs to System Health. | System Health. |
| Complete-looking metric cards render when noncanonical. | Misleads the operator. | Performance must block/degrade. |
| Position ownership detail dominates attribution. | Positions owns what is open; Performance owns contribution. | Positions. |

## Non-Implementation Note

This plan does not modify UI code, routes, backend logic, trading logic, sleeve logic, research logic, candidate generation, canonical artifacts, or safety gates.
