# Historical Short Research Review

## Scope

This is a deterministic local research review for Aegis Short Lab. It reviews assumptions about successful short strategies and generates candidate research ideas. It does not call APIs, fetch data, run a backtest, create candidates, promote candidates, provide trade advice, or change runtime readiness.

## Local Research Assumption Review

- Existing local factor assumptions already include earnings revisions, earnings quality, margin stability, capital efficiency, and financial strength as long-side families; the short lab should invert these only when deterioration is observable and current.
- The strongest short setup is multi-signal deterioration, not a single-factor rank. Revisions, accounting quality, profitability, momentum, and valuation should be combined as independent evidence layers.
- Runtime truth currently blocks trade advice and execution; this review is a static research synthesis and does not validate, promote, or recommend trades.
- No API calls, web calls, broker calls, Portfolio123 calls, or backtests are required for this review artifact.

## Most Common Successful Short Themes

| Rank | Theme | Why It Worked | Observable Signals | Aegis Candidate Idea | Best Use | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Collapsing earnings revisions | Successful shorts often had a transition from optimism to repeated estimate cuts. The edge was strongest when cuts broadened across revenue, margin, and cash-flow expectations. | negative one- and three-month EPS revision breadth; guidance cuts or withdrawn guidance; sell-side downgrade clusters after prior optimism; negative surprise followed by further estimate compression | Build a revision-collapse short watchlist that requires multi-horizon negative revisions, post-earnings estimate drift, and no offsetting balance-sheet strength. | primary short trigger and timing filter | HIGH |
| 2 | Accounting weakness | Accounting quality failures created delayed repricing when reported earnings were not backed by cash, working capital quality, or conservative recognition. | rising accruals relative to assets or earnings; cash flow lagging net income; receivables or inventory growing faster than sales; non-recurring adjustments becoming recurring | Create an accounting-fragility score that combines accruals, cash-conversion gaps, working-capital pressure, and adjustment persistence. | quality veto and fraud-risk proxy | HIGH |
| 3 | Deteriorating profitability | Durable short candidates usually showed worsening unit economics before the stock fully reflected the lower earnings power. | gross margin compression; operating margin compression; ROIC or ROA deterioration; negative operating leverage despite revenue growth | Test a profitability-breakdown screen requiring margin deterioration plus weak revisions or weak cash conversion. | fundamental confirmation layer | HIGH |
| 4 | Broken momentum | Shorts worked better when fundamental weakness coincided with failed price leadership, trend breaks, or repeated inability to recover after bad news. | price below medium-term moving averages; relative strength breakdown versus sector; failed post-earnings rebound; high-volume downside gaps | Use broken momentum as a timing filter only after revision, accounting, or profitability evidence exists. | entry timing and risk control | MEDIUM |
| 5 | Excessive valuation | Valuation mattered most when it amplified a fundamental disappointment. Expensive stocks with falling growth expectations had more room for multiple compression. | premium sales or earnings multiple versus history and peers; valuation unsupported by forward margin trajectory; long-duration cash-flow profile during discount-rate pressure; narrative valuation dependent on distant profitability | Treat excessive valuation as a severity multiplier, not a standalone short signal. | position sizing and downside convexity estimate | MEDIUM |

## Themes Likely To Fail

| Theme | Why It Failed | Warning Signs | Aegis Guardrail |
| --- | --- | --- | --- |
| Valuation-only shorts | High valuation alone often failed because expensive stocks can keep compounding or re-rate higher while fundamentals remain intact. | positive revisions; stable or expanding margins; strong cash conversion; sector leadership remains intact | Require a non-valuation failure signal before any short candidate can pass research review. |
| Crowded consensus shorts | Widely known bearish stories often failed when borrow, positioning, and squeeze risk dominated incremental fundamental information. | very high short interest; high borrow cost; positive catalyst risk; stock rallies on bad news | Add crowding and squeeze-risk checks before promoting any short theme to paper observation. |
| Low-quality companies without timing evidence | Weak businesses could survive for long periods when liquidity, narratives, or capital markets access delayed the recognition event. | no near-term catalyst; improving price momentum; available refinancing path; management still able to raise capital | Require timing evidence from revisions, price action, or financing stress before ranking. |
| Macro beta mistaken for company failure | Some apparent shorts were just high-beta exposure. They reversed sharply when the macro tape improved. | weakness shared by entire industry; company fundamentals better than peers; reversal follows rates, oil, or index beta; no idiosyncratic estimate damage | Separate sector and factor beta from company-specific deterioration before scoring. |

## Candidate Ideas For Aegis

| Candidate ID | Source Theme | Idea | Best Use | Priority |
| --- | --- | --- | --- | --- |
| aegis_short_candidate_earnings_revision_collapse_v1 | Collapsing earnings revisions | Build a revision-collapse short watchlist that requires multi-horizon negative revisions, post-earnings estimate drift, and no offsetting balance-sheet strength. | primary short trigger and timing filter | HIGH |
| aegis_short_candidate_accounting_weakness_v1 | Accounting weakness | Create an accounting-fragility score that combines accruals, cash-conversion gaps, working-capital pressure, and adjustment persistence. | quality veto and fraud-risk proxy | HIGH |
| aegis_short_candidate_deteriorating_profitability_v1 | Deteriorating profitability | Test a profitability-breakdown screen requiring margin deterioration plus weak revisions or weak cash conversion. | fundamental confirmation layer | HIGH |
| aegis_short_candidate_broken_momentum_v1 | Broken momentum | Use broken momentum as a timing filter only after revision, accounting, or profitability evidence exists. | entry timing and risk control | MEDIUM |
| aegis_short_candidate_excessive_valuation_v1 | Excessive valuation | Treat excessive valuation as a severity multiplier, not a standalone short signal. | position sizing and downside convexity estimate | MEDIUM |

## Research Design Implications

- Short candidates should require at least two independent deterioration signals before ranking.
- Valuation should increase severity only after earnings, accounting, profitability, or momentum evidence exists.
- Broken momentum is useful for timing, but weak as a standalone research thesis.
- Accounting and cash-conversion weakness deserve early warning status because recognition can lag reported earnings.
- Every future implementation should preserve the runtime truth invariant: consumers query verified truth and do not invent readiness.
