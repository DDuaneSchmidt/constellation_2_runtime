# Deduplication Rules

Deduplication is performed on two fingerprints:

- `claim_fingerprint`: normalized claim/rule text
- `mechanism_fingerprint`: normalized mechanism-level pattern with indicator terms reduced

Exact claim duplicates are marked with `CLAIM_FINGERPRINT_MATCH`.

Different indicators that express the same mechanism may be related through `MECHANISM_FINGERPRINT_MATCH`. This relationship does not prove the mechanism works; it only reduces repeated attention spend.
