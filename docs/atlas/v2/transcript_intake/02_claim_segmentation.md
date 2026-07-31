# Claim Segmentation

Segmentation splits supplied transcript text into sentence-like chunks and classifies each chunk as one of:

- `ENTRY_RULE`
- `EXIT_RULE`
- `FILTER_RULE`
- `RISK_RULE`
- `MARKET_CONTEXT`
- `PERFORMANCE_CLAIM`
- `OTHER`

Rule candidates require explicit entry detail and at least one supporting exit, risk, or filter rule. Vague descriptions become `INSUFFICIENT_DETAIL` candidates.

Repeated non-performance chunks are marked through `DUPLICATE_SEGMENT` candidates and excluded from downstream extraction.
