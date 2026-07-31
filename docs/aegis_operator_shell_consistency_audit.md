# Aegis Operator Shell Consistency Audit

Date: 2026-05-30
Scope: Today, Positions, Candidates, Research, Performance, System Health
Mode: audit only; no implementation changes

## Evidence Reviewed

Visible browser screenshots are treated as authoritative evidence:

| Screen | Screenshot reviewed |
| --- | --- |
| Today / Command Center | `docs/screenshots/aegis_today_2026-05-30-refined.png` |
| Positions | `docs/screenshots/aegis_positions_2026-05-30-final.png` |
| Candidates | `docs/screenshots/aegis_candidates_2026-05-30.png` |
| Research | `docs/screenshots/aegis_research_2026-05-30-refined.png` |
| Performance | `docs/screenshots/aegis_performance_2026-05-30-refined.png` |
| System Health | `docs/screenshots/aegis_system_health_2026-05-30-refined.png` |

Supporting product references reviewed:

* `docs/aegis_operator_screen_spec.md`
* `docs/aegis_operator_state_model.md`
* `docs/aegis_performance_system_health_boundary.md`
* `docs/aegis_today_screen_spec.md`
* `docs/aegis_system_health_screen_spec.md`

OCR was used to inspect visible text. Minor OCR spelling errors are ignored unless the underlying visible language appears ambiguous.

## Executive Summary

The rebuilt operator shell is materially more coherent than the pre-rebuild UI. The six main screens now mostly answer distinct operator questions, use plain-English summaries, and avoid exposing raw artifact names as primary content. Today, Candidates, Research, Performance, and System Health all now make user action state visible near the top of the screen.

The shell is close to an Operator Shell v1 freeze, but it still needs one final cleanup pass. The main blockers are not architecture problems; they are visible consistency issues: mixed labels for the same concepts, navigation identity leakage, inconsistent action language, and a few residual technical terms in primary content.

Recommendation: **Needs one final cleanup pass** before Operator Shell v1 Freeze.

Primary reasons:

* The left navigation still shows technical truth-owner subtitles such as `daily_operator_workspace`, `aegis_paper_position_ledger`, and `research_lab` on every screen.
* System Health still shows command syntax (`TARGET_DAY=... npm run aegis:audit`) in the primary top issue, even though the System Health refinement goal said engineering commands should not be the primary workflow.
* Research says “Research needs an operator decision” and “David action 2 research follow-ups,” while also showing rows where “Research action No” is visible. This is improved but still conceptually mixed.
* Today uses “Monitoring only,” Candidates uses “No candidate action today,” Performance uses “Performance partial,” Positions uses “Values incomplete,” and System Health uses “Runtime BLOCKED.” These are individually understandable, but the shell does not yet present a single consistent severity language.

## 1. Language Consistency

### What is consistent

The rebuilt screens consistently use operator-facing lead statements:

* Today: “No action required. Aegis is monitoring only.”
* Positions: “36 open paper positions; current values are incomplete.”
* Candidates: “No candidate action today.”
* Research: “Research needs an operator decision.”
* Performance: “Performance is partially available.”
* System Health: “Aegis is monitoring only because runtime evidence is incomplete.”

The phrase “David action” now appears on multiple screens and usually indicates user-action state:

* Today: “No operator action required.”
* Positions: “David action Monitor only.”
* Candidates: “David action No action required.”
* Research: “David action 2 research follow-ups.”
* System Health: “David action Review recovery plan only.”

### Inconsistencies

| Concept | Current visible variants | Issue |
| --- | --- | --- |
| No user action | “No action required,” “No operator action required,” “No candidate action today,” “Monitor only,” “Review recovery plan only” | These are not clearly part of one system. “Review recovery plan only” sounds like a user action, while Today says no action required. |
| Monitoring state | “Monitoring only,” “Paper monitoring,” “Monitor only,” “No candidate action today” | “Paper monitoring” appears in sidebar/status areas and is less clear than the page-level operator state. |
| Blocked state | “Candidate generation blocked,” “Candidate evaluation blocked,” “Runtime BLOCKED,” “BLOCKING,” “Blocked 1” | Blocked sometimes means a system/data blocker and sometimes a research blocker. The shell should distinguish “workflow blocked” from “item blocked.” |
| Partial/incomplete state | “Values incomplete,” “Performance partial,” “partially available,” “current values are incomplete,” “Price data incomplete” | Consistent enough in meaning, but not phrased consistently. |
| Next step | “Wait for the next evaluation,” “Use System Health,” “Review recovery plan only,” “Review research follow-ups,” “No future run confirmed” | Good concept coverage, but the label and placement vary. |

### Language consistency finding

Severity: **High**

The shell has good plain-English language at the page level, but shared concepts still use too many local variants. This is visible and operator-facing, not just implementation detail.

## 2. Status Consistency

### Current status presentation by screen

| Screen | Primary visible state | Secondary state signals |
| --- | --- | --- |
| Today | “No action required. Aegis is monitoring only.” | Runtime evidence incomplete; graph may be ready; no action buttons. |
| Positions | “36 open paper positions; current values are incomplete.” | Values incomplete; price coverage 0/36; monitor only. |
| Candidates | “No candidate action today.” | Candidate generation blocked; no actionable candidates; no future candidate evaluation confirmed. |
| Research | “Research needs an operator decision.” | Needs review; 2 research follow-ups; active hypotheses collecting evidence. |
| Performance | “Performance is partially available.” | Full portfolio P&L incomplete; benchmark stale; trend unavailable. |
| System Health | “Aegis is monitoring only because runtime evidence is incomplete.” | Evidence graph healthy; runtime readiness blocked; data availability blocked. |

### Contradictions or near-contradictions

1. **Today vs System Health action state**
   * Today says “No action required.”
   * System Health says “Review recovery plan only.”
   * This can be reconciled if System Health means optional/system-health review, not required action. The UI does not make that distinction explicit enough.

2. **Graph healthy vs Runtime blocked**
   * System Health now explains “Evidence graph healthy. Runtime verification incomplete.”
   * This is understandable and no longer a major contradiction.

3. **Research action ambiguity**
   * Research says “Research needs an operator decision” and “2 research follow-ups need review.”
   * A visible active research row says “RESEARCH ACTION No - Aegis is collecting evidence.”
   * This is not strictly contradictory because different rows have different states, but the above-fold summary could separate “follow-ups needing review” from “active research needing no action” more explicitly.

4. **Performance partial vs $0.00 values**
   * Performance clearly says “Marked positions only” and “Certified partial value; not full portfolio performance.”
   * This is now acceptable. The $0.00 is bounded by caveats.

### Status consistency finding

Severity: **High**

The screens are mostly non-contradictory, but action-state semantics still drift. “No action required,” “monitor only,” and “review recovery plan only” need a shared hierarchy: required action, optional review, system waiting, and monitoring only.

## 3. Navigation Consistency

### What works

The primary navigation labels are now closer to the rebuilt product model:

* Command Center
* Positions
* History
* Performance
* Position Review
* Research
* System Health

The System Health page identity now matches its purpose:

* Header: “System Health”
* Page title: “System Health”
* Primary question: “Can Aegis operate, what is healthy, what is degraded, and what is blocked?”

### Remaining legacy or technical navigation leakage

The left navigation still exposes truth-owner or backend labels beneath page names:

* `daily_operator_workspace`
* `aegis_paper_position_ledger`
* `aegis_daily_paper_performance`
* `aegis_paper_performance`
* `aegis_position_review`
* `research_lab`

These labels are visible on all screenshots and are engineering/value-source labels, not operator navigation copy.

The top chrome still shows “RUNTIME MODE READINESS” on every screen. This is not fatal, but it is architecture vocabulary and competes with the workflow-first page identity.

### Navigation consistency finding

Severity: **High**

Page titles and routes are largely consistent, but navigation subtitles still leak implementation truth-owner names. This is the most visible cross-screen architecture leakage remaining.

## 4. Workflow Consistency

### Distinct operator questions

| Screen | Distinct question | Audit result |
| --- | --- | --- |
| Today | Is Aegis okay today and do I need to do anything? | Mostly successful. It summarizes open positions, candidates, waiting, and trust without diving into detail. |
| Positions | What do we currently own? | Successful. It shows open positions, count, exposure, mark completeness, and no candidate workflow. |
| Candidates | What might we own next? | Successful. It shows no current candidates, why evaluation is blocked, and excludes holdings/P&L. |
| Research | What investigations exist and what is active? | Mostly successful. It shows investigations, active hypotheses, findings, blocker, and follow-ups. |
| Performance | How are we doing? | Mostly successful. It shows partial performance, completeness, attribution limitations, benchmark status, and trend status. |
| System Health | Can Aegis operate and what must be repaired? | Successful after refinement. It shows runtime evidence incomplete, recovery plan, and verification. |

### Overlap findings

1. Today summarizes all domains, but appropriately at high level.
2. Candidates refers to System Health for market-data repair; this is appropriate boundary handoff.
3. Performance avoids repair workflow and only states limitations; this is appropriate.
4. System Health avoids portfolio analytics; this is appropriate.
5. Research avoids positions/candidates/performance; this is appropriate.
6. Positions excludes candidate capture/readiness; this is appropriate.

### Workflow consistency finding

Severity: **Medium**

The screen boundaries are now mostly clean. The remaining overlap is mainly language/linkage, not workflow ownership.

## 5. Shell Leakage

### Remaining visible leakage

| Leakage | Where visible | Severity | Operator impact |
| --- | --- | --- | --- |
| Truth-owner subtitles such as `daily_operator_workspace`, `aegis_paper_position_ledger`, `research_lab` | Left navigation on all screens | High | Makes navigation feel like an engineering surface, not an operator shell. |
| `RUNTIME MODE READINESS` | Top chrome on all screens | Medium | Internal concept; acceptable only if translated nearby. |
| `BLOCKED`, `BLOCKING` | System Health and possibly Candidates/Research | Medium | Some raw status language remains; can be okay if paired with plain-English cause. |
| `TARGET_DAY=2026-05-30 npm run aegis:audit` | System Health top issue Verify row | High | Command syntax appears in primary workflow; conflicts with “do not add engineering commands as primary workflow.” |
| “Current-day data certification status” | System Health summary | Medium | Better than artifact names, but still technical. |
| “Certified partial value” | Performance | Low | Technical but useful for trust; acceptable if paired with “not full portfolio performance.” |
| “0/36 marked” | Positions | Low | Operator-relevant and understandable. |
| “2/20 observations” | Research | Low | Operator-relevant sample progress. |
| “P1” | System Health top issue | Medium | Priority code is engineering shorthand. Useful but not enough alone. |

### Leakage finding

Severity: **High**

The main content is much cleaner, but shell-level navigation subtitles and the System Health verify command still expose engineering concepts prominently.

## 6. Trust Consistency

### What works

The shell consistently provides trust caveats:

* Today: “Runtime evidence is incomplete... read-only summary and no action buttons.”
* Positions: “Current values are incomplete” and “36 positions are missing current marks.”
* Candidates: “today’s market data is missing.”
* Performance: “Full portfolio P&L incomplete” and “Marked positions only.”
* System Health: “Evidence graph healthy. Runtime verification incomplete.”
* Research: research-only action state and no trading/broker action.

The trust pattern is now visible before details on most pages.

### Trust inconsistencies

1. Trust labels are local rather than system-wide.
   * Positions says “Values incomplete.”
   * Performance says “Performance partial.”
   * System Health says “Runtime BLOCKED.”
   * Candidates says “Candidate generation blocked.”
   * These are all true but not obviously part of a common trust model.

2. Evidence availability differs by screen.
   * Today has a high-level trust statement but no visible evidence drawer in the screenshot region.
   * System Health exposes source artifacts and additional recovery items below the top issue.
   * Performance provides trust through completeness cards.
   * This is mostly acceptable because each screen has different depth, but the label for evidence/trust could be normalized.

3. “Graph ready but runtime blocked” is now explained well in System Health but only summarized in Today. This is acceptable as long as Today links to System Health for detail.

### Trust consistency finding

Severity: **Medium**

Trust is now generally handled well. The remaining need is language normalization, not a new trust architecture.

## 7. Top 20 Remaining Issues

| Rank | Severity | Issue | Evidence | Recommended disposition |
| --- | --- | --- | --- | --- |
| 1 | High | Left navigation exposes backend truth-owner names. | All screenshots show strings like `daily_operator_workspace`, `aegis_paper_position_ledger`, `research_lab`. | Final cleanup pass. |
| 2 | High | User-action language is inconsistent across screens. | Today: “No action required”; System Health: “Review recovery plan only”; Research: “2 research follow-ups need review.” | Final cleanup pass. |
| 3 | High | System Health primary top issue shows an npm verification command. | System Health screenshot shows `TARGET_DAY=2026-05-30 npm run aegis:audit`. | Replace primary command display with operator wording; keep command in details. |
| 4 | High | Research still mixes page-level “needs operator decision” with row-level “Research action No.” | Research screenshot shows both. | Clarify action grouping in copy. |
| 5 | Medium | Top chrome uses `RUNTIME MODE READINESS` on every operator screen. | All screenshots. | Consider plain-language top chrome label in final cleanup. |
| 6 | Medium | Status vocabulary varies: blocked, partial, incomplete, monitoring only, values incomplete. | All screenshots. | Create copy map and normalize labels. |
| 7 | Medium | “Paper monitoring” sidebar/status phrase is less clear than page-level states. | Multiple screenshots. | Replace or subordinate in final cleanup. |
| 8 | Medium | System Health uses `P1` and `BLOCKING` in primary card. | System Health screenshot. | Pair with or replace by plain severity label. |
| 9 | Medium | Candidates says “Candidate generation blocked” and “Candidate evaluation blocked”; distinction may be too fine for daily operation. | Candidates screenshot. | Normalize to one visible concept with detail below. |
| 10 | Medium | Performance “Aegis paper Available 0.00%” can still be misread as meaningful return despite partial state. | Performance benchmark section. | Label as source availability, not performance quality. |
| 11 | Medium | Positions action column repeats “View Review” and “Details” for every row, creating row density. | Positions screenshot. | Consider a single row action pattern later. |
| 12 | Medium | Today says “Last run” but global top timestamp says operational timestamps loading. | Today screenshot. | Fix top chrome loading state if persistent. |
| 13 | Medium | “No future run confirmed” appears on Today and Candidates but not consistently on Research/System Health. | Today/Candidates screenshots. | Normalize next scheduled activity language. |
| 14 | Low | “Certified partial value” is somewhat technical. | Performance screenshot. | Acceptable if kept with “not full portfolio performance.” |
| 15 | Low | “0/36 marked” may require slight explanation for non-engineers. | Positions screenshot. | Already explained nearby; acceptable. |
| 16 | Low | Research confidence shows “Not reported.” | Research screenshot. | Acceptable, but could be softened. |
| 17 | Low | “Strategies expected/completed/raw signals/output candidates” is a bit pipeline-oriented. | Candidates screenshot. | Acceptable as candidate screen detail. |
| 18 | Low | System Health “Current-day data certification status” is slightly technical. | System Health screenshot. | Acceptable but could be translated. |
| 19 | Low | Page subtitles sometimes use questions and sometimes state descriptions. | All screenshots. | Minor consistency pass. |
| 20 | Low | “Operational timestamps loading” appears in top chrome in screenshots. | Today, Positions, System Health screenshots. | Investigate only if it persists after load. |

## 8. Freeze Readiness

Recommendation: **Needs one final cleanup pass**.

### Evidence supporting freeze readiness

* Each rebuilt screen now answers a distinct operator question.
* The main screen boundaries are clean: Positions does not show candidate workflow, Candidates does not show holdings/P&L, Research does not show portfolio workflows, Performance does not show repair workflow, and System Health does not show portfolio analytics.
* Most primary messages are plain English and visible above the fold.
* Major stale/wrong-day/actionability failures are not visible in these screenshots.
* Performance now clearly labels partial values and incomplete marks.
* System Health now explains graph-ready/runtime-blocked without a major contradiction.

### Evidence against immediate freeze

* Navigation still visibly leaks backend names on every screen.
* Shared state/action language is not yet normalized enough for a polished v1.
* System Health still exposes an npm command in the primary recovery card.
* Research action state remains understandable but not fully clean.

## Final Audit Conclusion

The operator shell is **not in rollback/rework territory**. The rebuild direction is working, and the screens are now workflow-first rather than architecture-first.

However, the shell should not be frozen until one final cleanup pass resolves visible cross-screen copy and shell-level leakage. The cleanup should be limited to visible operator language and should not add new screens, backend gates, or product features.

Final recommendation: **Needs one final cleanup pass before Operator Shell v1 Freeze**.
