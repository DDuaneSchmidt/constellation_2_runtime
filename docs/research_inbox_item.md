# research_inbox_item.v1

`research_inbox_item.v1` captures raw research observations before they become formal hypotheses.

Inbox items are not hypotheses. They cannot authorize research execution, Lite promotion, trades, broker submission, or runtime mutation.

Allowed sources:
- `CHATGPT`
- `MANUAL`
- `LITE_FEEDBACK`
- `MARKET_OBSERVATION`
- `TRADE_REVIEW`

Lifecycle:
`NEW` -> `TRIAGED` -> `CONVERTED_TO_HYPOTHESIS` or `REJECTED` or `ARCHIVED`

Conversion to `research_hypothesis.v1` must be explicit and records the source inbox id in `related_research_refs`.
