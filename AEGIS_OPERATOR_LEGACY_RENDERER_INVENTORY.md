# Aegis Operator Legacy Renderer Inventory

Generated for the Operator UI cleanup after the shared template refresh.

## Scope

Routes reviewed:

* Command Center
* Positions
* Performance
* Position Review
* Research
* Engineering

## Inventory

| Page | Function | File | Visible text emitted | Allowed under shared template? | Delete / bypass plan |
|---|---|---|---|---|---|
| Command Center | `renderCommandCenterWorkspace` | `constellation_2/phaseL/ui/static/operator_shell/pages/index.js` | `Action Queue`, `Needs Attention`, command metrics, candidate review, monitor rows, local `renderTrustPanel` policy rail | Partially. Allowed only as `OperatorInboxTemplate` body after contract state; trust rail is disallowed as primary/context content. | Route through `OperatorInboxTemplate({ bodyHtml })`; rename legacy `Action Queue` heading to `Attention Queue`; remove page `contextHtml` trust rail. |
| Positions | `renderPositionsWorkspace` | `constellation_2/phaseL/ui/static/operator_shell/pages/index.js` | open positions, Today’s Candidates, candidate capture panel, local `renderTrustPanel` scope rail | Partially. Allowed only as `EntityListTemplate` body after contract state. | Route through `EntityListTemplate({ bodyHtml })`; remove page `contextHtml` trust rail; keep diagnostics behind contract/details. |
| Performance | `renderAegisPaperPerformancePage` | `constellation_2/phaseL/ui/static/operator_shell/pages/index.js` | performance metric grid, `Unavailable` metric cards, `Unavailable: aegis_sleeve_analytics_v1...`, `Unavailable: no position attribution...`, fallback `Performance unavailable` shell | Partially. Analytics are allowed only when contract status is READY and metrics are allowed. Current-day DEGRADED must show contract state only. | Route through `AnalyticsTemplate({ bodyHtml })`; remove separate banner concatenation; change fallback to contract-style unavailable copy; remove unavailable-shell strings from primary body. |
| Position Review | `renderAegisPositionReviewPage` | `constellation_2/phaseL/ui/static/operator_shell/pages/index.js` | `AEGIS_POSITION_REVIEW_BRIEF_V1`, `No position review briefs are available. Run npm run...`, fallback `Run npm run...`, local `renderTrustPanel` safety rail | No for primary operator view. Artifact IDs and commands may appear only in diagnostics/source references, not primary title/message. | Route through `ReviewTemplate({ bodyHtml })`; replace primary artifact title with operator-readable copy; replace npm empty/fallback text; remove page `contextHtml` trust rail. |
| Research | `renderResearchLabPage` | `constellation_2/phaseL/ui/static/operator_shell/pages/index.js` | hypothesis panels and local route bodies after contract banner | Allowed only as `EntityListTemplate` body after contract state. | Route through `EntityListTemplate({ bodyHtml })` single return path. |
| Engineering | `renderEngineeringDashboardWorkspace` | `constellation_2/phaseL/ui/static/operator_shell/pages/index.js` | top issue, Fix First, Ask Aegis, operator actions, status, day clarity, local `renderTrustPanel` policy rail | Partially. Troubleshooting content is allowed only as `TroubleshootingTemplate` body after contract state. | Route through `TroubleshootingTemplate({ bodyHtml })`; remove page `contextHtml` trust rail. |

## Specific string disposition

| String | Source | Disposition |
|---|---|---|
| `PAPER-2026-05-26-0950` | Not found in static renderer source; observed risk is artifact/session carry-forward. | Browser proof must confirm absent from current-day Positions primary view. |
| `SIGNAL_EVIDENCE_MISSING` | Not found in current operator route renderer source. | Browser proof must confirm absent from primary cards. |
| `AEGIS_POSITION_REVIEW_BRIEF_V1` | `renderAegisPositionReviewPage` primary eyebrow. | Remove from primary visible title; artifact remains diagnosable through source references if needed. |
| `No position review briefs are available. Run npm run...` | `renderAegisPositionReviewPage` table empty state. | Replace with operator-facing unavailable copy without npm command. |
| `Unavailable` metric shell strings | Performance and Position Review metrics. | Non-READY analytics render contract state only; ready bodies retain normal metric formatting but no unavailable shell as primary current-day view. |
| Old `Action Queue` / `Needs Attention` layout | `renderCommandCenterWorkspace`. | Rename to `Attention Queue` and route as `OperatorInboxTemplate` body only. |
| Raw trust drawer content | page-level `contextHtml: renderTrustPanel(...)`. | Remove from current-day operator route returns; contract diagnostics remain collapsed. |
