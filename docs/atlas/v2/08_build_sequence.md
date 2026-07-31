# Atlas V2 Build Sequence

Build order:

1. Constitution docs.
2. Object schemas.
3. Ledgers.
4. State transitions.
5. Validation and audit tests.
6. No generation or discovery.

Current implementation scope:

- Append-only JSONL ledgers.
- Core object validation.
- Transition history preservation.
- Complete experience-chain audit.
- Forbidden artifact audit.

Deferred explicitly:

- Hypothesis generation.
- YouTube crawling.
- Broad discovery.
- Knowledge graph construction.
- Trading, validation, sleeve, candidate, paper-position, or capital-allocation authority.
