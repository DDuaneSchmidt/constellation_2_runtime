# Aegis Operator UI First-Pass Inventory

## Purpose

This inventory identifies current UI files and classifies them as keep, remove, bypass, or investigate candidates for the Option C operator shell rebuild.

This is not an implementation plan. It is the starting map for the rebuild.

## Runtime Truth Read Before Inventory

Pre-change audit for `TARGET_DAY=2026-05-30` reported:

* verified runtime graph: READY
* runtime truth classification: PARTIAL_CONTEXT
* highest readiness layer: BLOCKED
* missing/stale source count: 0
* trade advice allowed: false
* broker submit/transmit: disabled by policy
* autonomous execution: disabled by policy

The UI rebuild must preserve backend governance and safety gates while replacing visible operator presentation.

## Keep Candidates

### `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`

Keep as the current local dashboard server and API host.

Reasons:

* already serves the shell on `127.0.0.1:8787`
* already resolves truth root and requested day
* already exposes some operator endpoints
* already has route serving and health checks

Concerns:

* contains many legacy API paths and fallback behaviors
* some endpoints return backend-shaped projections rather than operator-ready summaries
* future work should add or wrap the new operator endpoints without exposing raw governance vocabulary to primary UI

### `constellation_2/phaseL/ui/static/index.html`

Keep initially as the static host shell.

Reasons:

* contains stable app mount points
* keeps deployment/restart path small

Concerns:

* existing topbar/sidebar structure may be too dense
* should be simplified only after the Today screen is screenshot-approved

### `constellation_2/phaseL/ui/static/operator_shell/main.js`

Keep only the minimal routing, navigation, fetch lifecycle, and event delegation needed to mount rebuilt screens.

Reasons:

* owns route loading and `workspaceContent`
* already handles navigation and URL changes

Bypass candidates inside this file:

* old engineering drawer behavior
* global Ask Aegis prominence before core status is clear
* legacy context rail assumptions
* command handlers tied to old page markup

### `constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js`

Keep as a route registry candidate.

Reasons:

* central route metadata is useful
* can map rebuilt screens to routes

Concerns:

* current route set is too broad for Option C first pass
* should be filtered to seven operator screens for rebuilt shell navigation

### `constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js`

Keep as API wrapper layer.

Reasons:

* already has query helpers
* already includes some preferred operator endpoints:
  * `/api/aegis/operator/today`
  * `/api/aegis/operator/health`
  * `/api/aegis/operator/tasks`
  * `/api/aegis/operator/diagnostics`
  * `/api/aegis/operator/what-changed`

Concerns:

* many functions expose raw backend-specific projections
* new UI should consume only new operator-ready endpoint wrappers for the seven screens

### `constellation_2/phaseL/ui/static/aegis.css`

Keep selectively.

Reasons:

* existing styles can supply typography, buttons, cards, and responsive rules

Concerns:

* current visual density and card patterns contributed to the failed UI
* new screens should use fewer components and lower density

## Bypass / Replace Candidates

### `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`

Bypass as the primary page implementation source for rebuilt screens.

Reasons:

* too large and multi-purpose
* contains many legacy render paths
* page-specific renderers assemble product meaning in the browser
* prior attempts to template-gate this file still produced unacceptable visible output

Short-term use:

* route current seven screen IDs to new small renderers
* leave old renderers unreachable for rebuilt routes

Do not patch this file screen-by-screen again except to bypass old routes.

### Existing Aegis workflow routes

Bypass from primary navigation for Option C first pass:

* `/aegis-opportunities`
* `/aegis-paper-performance`
* `/aegis-position-review`
* `/aegis-sleeve-analytics`
* `/research-lab/*`
* `/aegis-positions-diagnostics`
* `/aegis-runtime*`
* `/aegis-operator-cockpit`

These may remain available behind Evidence / Audit Detail or engineering links, but they should not define the new operator workflow.

### `renderTrustPanel`, raw context rails, and evidence drawers

Bypass as primary UI.

Reason:

* evidence detail is useful only after the workflow answer is clear
* evidence drawers must not appear if evidence does not exist
* raw trust text must live under Evidence / Audit Detail

### Current tests that assert DOM attributes only

Bypass as product proof.

Reason:

* visible browser output is authoritative
* tests can support regression after screenshot acceptance, but cannot prove product correctness alone

## Remove Candidates Later

Do not delete yet in first pass. Mark for later removal after new screens are screenshot-approved:

* obsolete page-specific empty states
* duplicated fallback renderers
* old performance unavailable shells
* candidate card layouts that are not current-day workflow rows
* engineering dashboard metric-first layouts
* raw backend status cards in primary screens

## Backend Endpoint Candidates

### Existing candidate endpoints worth preserving behind the boundary

* `/api/aegis/operator/today`
* `/api/aegis/operator/tasks`
* `/api/aegis/operator/diagnostics`
* `/api/aegis/operator/what-changed`
* `/api/aegis/operator/health`
* `/api/aegis/operator-cockpit`
* `/api/aegis/positions`
* `/api/aegis/performance-report`
* `/api/aegis/position-review/latest`
* `/api/aegis/sleeve-analytics/latest`
* `/api/aegis/operator-surface-contract/latest`

### Required new or wrapped endpoints

* `/api/aegis/operator/today`
* `/api/aegis/operator/positions`
* `/api/aegis/operator/candidates`
* `/api/aegis/operator/performance`
* `/api/aegis/operator/research`
* `/api/aegis/operator/health`
* `/api/aegis/operator/evidence`

The first endpoint may already exist but must be evaluated against the new screen spec before reuse.

## Recommendation For First Implementation Slice

Start with Today / Command Center only.

Recommended slice:

1. Build or wrap `/api/aegis/operator/today` into the envelope defined in `docs/aegis_operator_screen_spec.md`.
2. Add a small new renderer file for the rebuilt operator shell screen rather than patching the old renderer.
3. Route only `/aegis-command-center` to the new Today screen.
4. Keep existing navigation minimal and preserve requested `?day=`.
5. Render one screen state at a time:
   * current-day normal/no-activity
   * current-day blocked
   * waiting-for-next-run
   * user-action-required
6. Capture screenshot proof before writing product tests.
7. After screenshot acceptance, add tests that assert visible text, not hidden DOM attributes.

Do not rebuild Positions, Candidate Pipeline, Performance, Research, System Health, or Evidence / Audit Detail until Today / Command Center passes visible screenshot acceptance.

