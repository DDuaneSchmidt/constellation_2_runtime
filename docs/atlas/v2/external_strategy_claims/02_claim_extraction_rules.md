# Claim Extraction Rules

The extractor only uses supplied text.

It may extract these fields when explicitly present:

- market context
- timeframe
- entry rule
- exit rule
- filter rule
- risk rule
- instrument
- claimed edge as a claim, not as truth

Statuses:

- `EXTRACTED`: enough rule detail exists for cheap research input
- `INSUFFICIENT_RULE_DETAIL`: the description is too vague for a cheap sanity test
- `TRANSCRIPT_REQUIRED`: no transcript or manual notes were supplied
- `UNSUPPORTED_SOURCE`: the source contract is unsupported by V1
- `DUPLICATE_CLAIM`: the claim matches prior claim evidence

No output may label a claim profitable, validated, proven, recommended, or tradeable.
