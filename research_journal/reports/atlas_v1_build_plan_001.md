# Atlas V1 Build Plan 001

Objective: convert `atlas_v1_prioritization_review_001.md` into an implementation-ready build plan for the top Atlas V1 capabilities.

Scope: build plan only. This report does not implement Atlas, add architecture, modify runtime truth logic, change sleeves, alter candidate rules, change validation rules, create trading logic, or create capital allocation behavior.

Evidence base:
- `atlas_v1_prioritization_review_001.md`
- `journal_review_002.md`
- `root_cause_review_001.md`
- `outcome_maturity_acceleration_review_001.md`
- `evidence_accumulation_forecast_review_001.md`

## 1. Executive Summary

Atlas V1 should start as a small set of read-only tools and reports that help researchers retrieve, compress, reuse, and compare existing evidence. The build-now scope is intentionally narrow because tonight's reviews found that AEGIS does not need more autonomous research architecture. It needs better use of the evidence it already has.

The four build-now capabilities are:

1. Research Evidence Librarian.
2. Research Synthesis / Evidence Compression.
3. Negative Knowledge / Failure Reuse.
4. Outcome Follow-Through Comparator.

These capabilities address the strongest bottlenecks from the Research Journal: underpowered outcome maturity, evidence concentration, candidate-funnel ambiguity, and repeated misdiagnosis risk. They must remain bounded to read-only analysis. They must not validate truth, recommend trades, allocate capital, mutate candidates, change sleeves, or create autonomous strategy behavior.

Recommended first implementation task: build a read-only Research Evidence Librarian CLI that indexes existing Research Journal objects and reports, then emits a plain Markdown evidence brief for one query or observation ID.

## 2. Build Now Scope

Build now means:
- Read existing Research Journal files and selected existing AEGIS artifacts.
- Produce human-readable Markdown or JSON summaries.
- Preserve source references and file paths.
- Keep outputs reviewable, Git-friendly, and non-authoritative.
- Add focused tests for parsing, retrieval, synthesis, and non-goal boundaries.

Build now does not mean:
- New schemas for journal objects.
- New UI.
- Databases.
- APIs.
- Autonomous agents.
- Workflow orchestration.
- Runtime truth mutation.
- Trading, execution, or capital allocation behavior.

The smallest acceptable implementation style is one or more `ops/tools/` scripts with tests in `constellation_2/common/tests/`, npm command wrappers only if needed, and module manifest updates only if commands or AEGIS capabilities are added.

## 3. Capability 1: Research Evidence Librarian

Purpose:
Help researchers find relevant existing journal entries, reports, and evidence references without repeated manual search.

Inputs:
- `research_journal/observations/*.yaml`
- `research_journal/knowledge/*.yaml`
- `research_journal/failures/*.yaml`
- `research_journal/reports/*.md`
- Optional explicit query text or object ID such as `OBS_0002`, `KNW_0017`, or `FAIL_0010`

Outputs:
- Markdown evidence brief listing matched objects and reports.
- Source file paths.
- Short snippets or summaries.
- Match reasons.
- No claims of validation or truth.

Minimum viable implementation:
- A read-only CLI such as `ops/tools/build_atlas_v1_evidence_librarian_brief.py`.
- Search by exact object ID, title terms, and simple keyword matching.
- Parse YAML journal objects with existing YAML parser behavior.
- Read Markdown reports as plain text.
- Emit a Markdown report to stdout or a specified file under `research_journal/reports/`.
- Include explicit "non-authoritative read-only brief" language in output.

Files likely touched:
- `ops/tools/build_atlas_v1_evidence_librarian_brief.py`
- `constellation_2/common/tests/test_atlas_v1_evidence_librarian.py`
- `package.json` only if adding an npm wrapper command
- `aegis/modules/**/aegis.module.yaml` only if adding a command or declared AEGIS capability

Tests required:
- Finds exact observation, knowledge, and failure IDs.
- Finds relevant report lines for keyword query.
- Handles missing query with a clear error.
- Does not write outside the requested output path.
- Does not infer readiness, validation, allocation, or trading claims.

What it must not do:
- Do not create journal objects.
- Do not classify observations.
- Do not mutate reports.
- Do not query live systems.
- Do not certify evidence.
- Do not recommend trades or allocations.

Success criteria:
- Given `OBS_0002`, output includes the observation context and related outcome-maturity reports.
- Given `candidate volume`, output includes candidate funnel learnings and failure references.
- Output contains source paths sufficient for a human reviewer to inspect the evidence.

## 4. Capability 2: Research Synthesis / Evidence Compression

Purpose:
Summarize existing evidence into support, contradiction, unresolved questions, and next evidence action.

Inputs:
- Evidence Librarian search output.
- Selected journal reports.
- Selected OBS/KNW/FAIL objects.
- Optional topic label, such as `outcome maturity`, `candidate quality`, or `capital review`.

Outputs:
- Markdown synthesis brief with:
  - Supported claims.
  - Contradicted claims.
  - Unresolved questions.
  - Reusable cautions.
  - Recommended next evidence check.
- Source references for each major claim.

Minimum viable implementation:
- A read-only CLI such as `ops/tools/build_atlas_v1_research_synthesis_brief.py`.
- Accept an input file or list of source files.
- Extract high-signal sections by headings and keyword matches.
- Require every synthesized claim to cite at least one source file.
- Emit deterministic Markdown.

Files likely touched:
- `ops/tools/build_atlas_v1_research_synthesis_brief.py`
- `constellation_2/common/tests/test_atlas_v1_research_synthesis.py`
- Shared helper module only if duplication between librarian and synthesis becomes meaningful
- `package.json` only if adding an npm wrapper command
- `aegis/modules/**/aegis.module.yaml` only if adding a command or declared AEGIS capability

Tests required:
- Produces all required sections.
- Includes source references.
- Refuses or warns on empty evidence input.
- Keeps unsupported or unresolved items separate from supported claims.
- Does not produce allocation, trading, validation, or architecture-expansion language.

What it must not do:
- Do not validate hypotheses.
- Do not mark observations supported as a journal object update.
- Do not create knowledge or failure entries automatically.
- Do not design new systems.
- Do not make capital-review or trade-readiness claims.

Success criteria:
- Given the outcome maturity reviews, output names closed outcomes as the binding constraint and cites the relevant reports.
- Given candidate-funnel evidence, output separates raw signal count from candidate quality.
- Human reviewer can use the brief to decide the next evidence check without treating it as authority.

## 5. Capability 3: Negative Knowledge / Failure Reuse

Purpose:
Surface prior failures and corrected misdiagnoses before a new review repeats them.

Inputs:
- `research_journal/failures/*.yaml`
- `research_journal/knowledge/*.yaml`
- `journal_review_001.md`
- `journal_review_002.md`
- Optional topic/query text

Outputs:
- Markdown caution brief with:
  - Relevant prior failures.
  - Corrected misdiagnoses.
  - Do-not-repeat cautions.
  - Related knowledge entries.
  - Review checklist questions.

Minimum viable implementation:
- A read-only CLI such as `ops/tools/build_atlas_v1_negative_knowledge_brief.py`.
- Search failures and knowledge by ID, statement, and keyword.
- Include caution language that distinguishes failure reuse from veto authority.
- Optionally use the Evidence Librarian helper once that exists.

Files likely touched:
- `ops/tools/build_atlas_v1_negative_knowledge_brief.py`
- `constellation_2/common/tests/test_atlas_v1_negative_knowledge.py`
- Shared research journal parser helper if introduced
- `package.json` only if adding an npm wrapper command
- `aegis/modules/**/aegis.module.yaml` only if adding a command or declared AEGIS capability

Tests required:
- Finds failures by exact ID and keyword.
- Links failures to related knowledge where text overlaps.
- Includes corrected misdiagnoses from Journal Review 002.
- Output explicitly says it is advisory and non-authoritative.
- Does not block, approve, allocate, validate, or recommend trades.

What it must not do:
- Do not create autonomous veto behavior.
- Do not stop research work.
- Do not mark hypotheses invalid.
- Do not change journal object status.
- Do not create new failure entries automatically.

Success criteria:
- Query `capital review` surfaces `FAIL_0010`-type cautions and the 0-capital-review-ready finding.
- Query `candidate volume` surfaces the candidate quality caution.
- Query `architecture maturity` surfaces the architecture-versus-evidence maturity failure.

## 6. Capability 4: Outcome Follow-Through Comparator

Purpose:
Compare outcome evidence across valid artifact days to determine whether included validation samples, closed outcomes, and zero-sample sleeve status improved under existing rules.

Inputs:
- Prior review baseline day.
- Current valid day.
- Existing paper position ledgers.
- Existing validation sample summaries.
- Existing sleeve performance truth.
- Existing outcome maturity artifacts.
- Existing exit recommendation artifacts where available.
- Existing verified graph status for the selected days.

Outputs:
- Markdown comparison report with:
  - Prior usable samples.
  - Current usable samples.
  - Sample delta.
  - New closed outcomes.
  - Sleeves contributing samples.
  - Outcome concentration status.
  - Forecast accuracy.
  - Next evidence action.

Minimum viable implementation:
- A read-only CLI such as `ops/tools/build_atlas_v1_outcome_follow_through_comparator.py`.
- Require explicit `--baseline-day` and `--current-day`.
- Check verified graph status and refuse to treat BLOCKED days as clean refresh evidence.
- Compare only existing artifacts.
- Emit a report; do not run outcome-generation commands.

Files likely touched:
- `ops/tools/build_atlas_v1_outcome_follow_through_comparator.py`
- `constellation_2/common/tests/test_atlas_v1_outcome_follow_through_comparator.py`
- `package.json` only if adding an npm wrapper command
- `aegis/modules/**/aegis.module.yaml` only if adding a command or declared AEGIS capability

Tests required:
- Computes sample delta from fixture validation summaries.
- Identifies concentration by sleeve when fixture sleeve truth provides closed positions.
- Refuses or clearly blocks when current-day verified graph is BLOCKED.
- Handles missing artifacts with a non-mutating diagnostic.
- Does not alter exits, positions, candidates, validation samples, or runtime truth.

What it must not do:
- Do not run paper outcome auto-closure.
- Do not run outcome validation.
- Do not close or manage positions.
- Do not change exit rules.
- Do not infer that more samples imply capital readiness.
- Do not issue trade recommendations.

Success criteria:
- Given 2026-06-03 baseline and a valid later fixture day, output correct included-sample delta.
- Given blocked 2026-06-04 fixture graph, output says the day is not a clean refresh cycle.
- Given zero-sample sleeve fixture data, identifies first closed sample contribution when present.

## 7. Explicit Non-Goals

Do not build:
- DES or DES-Lite systems.
- Mechanism Registry.
- Opportunity Registry.
- Search Space Evolution.
- Mutation Engine.
- New governance systems.
- New journal object types.
- Databases.
- APIs.
- Dashboards.
- Web UI.
- Autonomous agents.
- Runtime truth mutation.
- Autonomous validation authority.
- Capital allocation.
- Chief Scientist behavior.
- Trading recommendations.
- Candidate-generation changes.
- Sleeve changes.
- Exit-rule changes.
- Validation-rule changes.

Do not treat Atlas V1 output as:
- Verified truth.
- Hypothesis validation.
- Capital-review readiness.
- Trade advice.
- Execution advice.
- Evidence certification.

## 8. Implementation Order

1. Research Evidence Librarian.

Reason: all other capabilities need reliable retrieval and source references. This is the smallest useful piece and directly reduces manual repeated search.

2. Negative Knowledge / Failure Reuse.

Reason: it can reuse the librarian parser and immediately prevents repeated misdiagnoses.

3. Research Synthesis / Evidence Compression.

Reason: synthesis is more useful after retrieval and failure cautions are available. It should cite retrieved evidence and separate supported, contradicted, and unresolved claims.

4. Outcome Follow-Through Comparator.

Reason: it has the highest direct connection to the outcome-maturity bottleneck, but it depends on more careful artifact handling and valid-day checks.

5. Candidate Funnel Interpreter and Evidence Contract / Source Freshness Triage.

Reason: classified BUILD_NEXT, not in the first build-now implementation batch, but they are the natural next capabilities after the four build-now items.

## 9. Acceptance Criteria

Atlas V1 build-now acceptance criteria:

- All tools are read-only by default.
- All outputs are human-readable.
- Every material claim includes source references.
- Tools work from existing repo and truth artifact files.
- Tools do not require databases, APIs, UI, or external services.
- Tools do not create or modify OBS, KNW, FAIL, or schemas.
- Tools do not modify runtime truth.
- Tools do not run or change trading, sleeve, candidate, exit, validation, or allocation logic.
- Tools pass focused unit tests.
- Existing Research Journal validation continues to pass.
- Pinned verified graph and hydrate commands continue to pass for 2026-06-03.

Capability-specific acceptance:

- Librarian can retrieve exact object IDs and relevant reports by topic.
- Synthesis can produce support/contradiction/unresolved/next-action briefs from cited sources.
- Negative Knowledge can surface related failures and cautions for a topic.
- Outcome Comparator can compare baseline and current-day sample counts and reject blocked refresh days.

## 10. Risks

Risk: Atlas output is mistaken for validated truth.

Mitigation: include explicit non-authoritative language in every output and require source references.

Risk: Build expands into architecture before evidence supports it.

Mitigation: implement only small read-only CLIs and reports; defer UI, agents, orchestration, databases, and graph expansion.

Risk: Outcome comparator becomes hidden validation logic.

Mitigation: compare existing artifacts only; never run closure, validation, or certification producers.

Risk: Negative knowledge becomes autonomous veto authority.

Mitigation: output cautions and checklist questions only; do not block or approve work.

Risk: Synthesis over-compresses uncertain evidence into false certainty.

Mitigation: required sections for supported, contradicted, and unresolved items; cite sources for all material claims.

Risk: Future implementation misses module manifest requirements.

Mitigation: if implementation adds AEGIS commands, behavior, capabilities, evidence producers, policies, or UI surfaces, update the relevant `aegis/modules/**/aegis.module.yaml` files in the same change.

## 11. Recommended First Codex Implementation Task

Implement the Research Evidence Librarian as the first Atlas V1 task.

Concrete task:
- Create `ops/tools/build_atlas_v1_evidence_librarian_brief.py`.
- Add focused tests in `constellation_2/common/tests/test_atlas_v1_evidence_librarian.py`.
- Support exact ID lookup and simple keyword query over `research_journal/observations`, `research_journal/knowledge`, `research_journal/failures`, and `research_journal/reports`.
- Emit deterministic Markdown to stdout by default.
- Include source file paths and short match reasons.
- Include a non-authoritative read-only disclaimer.
- Do not add npm wrapper or module manifest changes unless the implementation explicitly needs a command surface.

Recommended first test cases:
- Query `OBS_0002` returns the observation and outcome maturity reports.
- Query `candidate volume` returns candidate-quality knowledge and failure references.
- Query `capital review` returns capital-review cautions and related journal reviews.
- Missing query exits non-zero with a clear message.
- Output contains no trade, allocation, validation-authority, or runtime mutation language.

The first implementation should stop there. Do not implement synthesis, negative knowledge, or outcome comparison until the librarian proves useful and remains small.
