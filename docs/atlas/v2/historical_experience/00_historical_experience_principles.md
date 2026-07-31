# Atlas V2 Historical Experience Principles

The Historical Experience Factory converts explicitly supplied historical Atlas and AEGIS artifact references into append-only Atlas V2 experience records. It exists to seed learning with outcome-linked examples, not to create new research, validation, recommendation, trading, or allocation authority.

Rules:

1. Preserve provenance on every derived record that can carry it.
2. Never overwrite source artifacts.
3. Every converted `ExperienceEvent` references the original source artifact and `HistoricalExperienceRecord`.
4. Unknown remains distinct from failed.
5. Historical conversion is append-only.
6. Conversion is read-only and audit-only.
7. No trading authority.
8. No sleeve authority.
9. No candidate authority.
10. No paper-position authority.
11. No validation authority.
12. No capital allocation authority.

The factory accepts curated historical source records that already contain explicit evidence. It does not scan for opportunities, generate hypotheses, ingest YouTube content, recommend actions, validate investment claims, or allocate capital.
