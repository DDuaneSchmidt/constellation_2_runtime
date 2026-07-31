# Parser Inventory

Coverage Expansion V1 adds source-specific parser support for these existing artifact families:

- ADR documents under `docs/aegis/adr/`.
- Investment Thesis Factory documents under `docs/aegis/investment_thesis_factory/`.
- Technical Strategy Factory documents under `docs/aegis/technical_strategy_factory/`.
- Research journal reports under `research_journal/reports/`.
- Failure reports, including structured research journal failure records.
- Review artifacts with explicit review, retrospective, result review, hostile review, or outcome review content.

The parser module is `ops/atlas/v2_historical_experience_parsers.py`. The Historical Experience Factory invokes it before legacy journal parsing and before inventory fallback extraction.
