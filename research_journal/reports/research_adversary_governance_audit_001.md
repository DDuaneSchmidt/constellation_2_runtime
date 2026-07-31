# Research Adversary Governance Audit 001

Date: 2026-06-05

Scope: audit of Research Adversary authority leakage across implementation, CLI wiring, tests, and generated markdown templates.

Reviewed files:

- `constellation_2/common/atlas_v2_research_os/research_adversary.py`
- `constellation_2/common/atlas_v2_research_os/research_adversary_review.py`
- `constellation_2/common/atlas_v2_research_os/research_adversary_evaluation.py`
- `constellation_2/common/atlas_v2_research_os/cli.py`
- `ops/tools/build_atlas_v2_research_adversary_v1.py`
- `constellation_2/common/tests/test_atlas_v2_research_os_research_adversary.py`
- `constellation_2/common/tests/test_atlas_v2_research_adversary_contracts.py`
- `constellation_2/common/tests/test_atlas_v2_research_adversary_governance.py`
- `constellation_2/common/tests/test_atlas_v2_research_adversary_evaluation.py`
- `constellation_2/common/tests/test_atlas_v2_research_adversary_evaluation_cli.py`
- `constellation_2/common/tests/test_atlas_v2_research_adversary_cli.py`
- `constellation_2/common/tests/test_atlas_v2_research_os_research_adversary_review.py`

Recommendation: PASS_WITH_WARNINGS

Rationale: no executable authority leakage was found. The implementation explicitly blocks trade, capital, sizing, candidate promotion, replay override, qualification override, governance override, automatic memory writes, and production pipeline integration. Warnings remain for approval-like or lifecycle-like wording in status names and markdown template text that could be misread by downstream consumers.

## Verification

Focused test command:

```text
pytest constellation_2/common/tests/test_atlas_v2_research_os_research_adversary.py constellation_2/common/tests/test_atlas_v2_research_adversary_contracts.py constellation_2/common/tests/test_atlas_v2_research_adversary_governance.py constellation_2/common/tests/test_atlas_v2_research_adversary_evaluation.py constellation_2/common/tests/test_atlas_v2_research_adversary_evaluation_cli.py constellation_2/common/tests/test_atlas_v2_research_adversary_cli.py constellation_2/common/tests/test_atlas_v2_research_os_research_adversary_review.py
```

Result: 31 passed.

## Authority Checklist

| Check | Result | Evidence |
| --- | --- | --- |
| Trade recommendation language | PASS | Forbidden pattern rejects trade recommendation, buy, and sell language in `research_adversary.py:22`; tests cover direct rejection in `test_atlas_v2_research_os_research_adversary.py:42` and governance parametrization in `test_atlas_v2_research_adversary_governance.py:24`. |
| Capital recommendation language | PASS | Authority boundary sets capital recommendation false in `research_adversary.py:36` and `research_adversary_evaluation.py:27`; forbidden pattern rejects capital allocation language in `research_adversary.py:23`. |
| Position sizing language | PASS | Authority boundary sets position sizing false in `research_adversary.py:37`; tests reject nested position sizing text in `test_atlas_v2_research_os_research_adversary.py:50`. |
| Candidate promotion authority | PASS_WITH_WARNING | Candidate promotion is denied in authority boundaries and summaries, but generated text still uses "promotion" in human-review context at `research_adversary_review.py:134`, `research_adversary_review.py:158`, and `research_adversary_review.py:172`. |
| Replay override authority | PASS | Replay override is denied in `research_adversary.py:38` and `research_adversary_evaluation.py:31`; boundary expansion is rejected in `test_atlas_v2_research_os_research_adversary.py:55` and `test_atlas_v2_research_adversary_governance.py:56`. |
| Qualification override authority | PASS | Qualification override is denied in `research_adversary.py:39` and `research_adversary_evaluation.py:32`; forbidden phrase coverage exists in `test_atlas_v2_research_adversary_governance.py:21`. |
| Governance override authority | PASS | Governance override is denied in `research_adversary.py:40` and `research_adversary_evaluation.py:33`; acknowledged forbidden action is asserted in `test_atlas_v2_research_os_research_adversary.py:30`. |
| Automatic memory writes | PASS | Evaluation boundary explicitly sets `automatic_memory_writes_authorized` false in `research_adversary_evaluation.py:25`; Research Adversary review/evaluation CLI paths only call write functions for selected output dirs at `cli.py:88` and `cli.py:100`. |
| Production pipeline integration | PASS | Review boundary sets production pipeline integration false in `research_adversary_review.py:24`; evaluation scope states no production pipeline integration in `research_adversary_evaluation.py:187`; CLI test asserts protected dirs are unchanged in `test_atlas_v2_research_adversary_evaluation_cli.py:60`. |
| Ambiguous status names that imply approval | PASS_WITH_WARNING | `APPROVED_FOR_EXPERIMENT_DESIGN` in `research_adversary.py:15`, `RETIRED` in `research_adversary.py:17`, `RETIRE_CANDIDATE_CAPABILITY` in `research_adversary_evaluation.py:18`, and recommendation `RETIRE` in `research_adversary_evaluation.py:21` are scoped in code but carry approval/lifecycle semantics that could confuse consumers. |

## Findings

### Finding 1: Approval-like Review Status

Severity: Warning

`research_adversary.py:15` allows status `APPROVED_FOR_EXPERIMENT_DESIGN`. The implementation does not grant candidate, replay, qualification, governance, capital, or trading authority, but the word `APPROVED` is stronger than needed for an adversarial research artifact. A downstream consumer could treat this as authority if it ignores the boundary object.

Minimal patch:

- Rename `APPROVED_FOR_EXPERIMENT_DESIGN` to `ELIGIBLE_FOR_HUMAN_REVIEWED_EXPERIMENT_DESIGN`.
- Keep a temporary backwards-compatible alias only if existing artifacts need migration.
- Add a test that rejects `APPROVED_FOR_CANDIDATE`, `APPROVED_FOR_REPLAY`, and `APPROVED_FOR_QUALIFICATION`.

### Finding 2: Lifecycle-like Evaluation Names

Severity: Warning

`research_adversary_evaluation.py:18` uses `RETIRE_CANDIDATE_CAPABILITY`, and `research_adversary_evaluation.py:21` allows recommendation `RETIRE`. These refer to evaluation disposition, not candidate retirement or production lifecycle mutation. The names are still ambiguous because they combine candidate and retirement vocabulary.

Minimal patch:

- Rename `RETIRE_CANDIDATE_CAPABILITY` to `DISABLE_ADVERSARY_CAPABILITY_REVIEW_REQUIRED`.
- Rename recommendation `RETIRE` to `STOP_ADVERSARY_EVALUATION`.
- Update `_summary_status`, `_recommendation`, and tests accordingly.

### Finding 3: Generated Markdown Uses Promotion/Approval Wording

Severity: Warning

The generated review template says no promotion evidence is produced at `research_adversary_review.py:134`, asks about "promotion discussion" at `research_adversary_review.py:158`, says the reviewer may "accept" a critique at `research_adversary_review.py:172`, and says output remains generated-only until a reviewer "approves it later" at `research_adversary_review.py:279`.

This is not executable leakage, and surrounding text denies automated promotion or pipeline action. The wording should still be tightened because markdown is what humans and future agents are likely to read.

Minimal patch:

- Change "promotion" to "later-stage review" where the text is not specifically denying authority.
- Change "accept" to "mark the critique as reviewed".
- Change "approves it later" to "marks it human-reviewed later".
- Add a markdown snapshot assertion that generated summaries do not contain approval-like words except inside explicit denied-authority boundary labels.

## Positive Controls Observed

- Review artifacts are `GENERATED_ONLY` and human review is required.
- Authority boundaries are explicit and false for prohibited authority.
- Forbidden authority language is recursively scanned across nested payload text.
- Review and evaluation CLI commands write only JSON/markdown artifacts to caller-selected output directories.
- Evaluation reports include an authority boundary verification block.
- Tests cover forbidden language, boundary override rejection, generated-only output, and no mutation of protected production-like directories.

## Residual Risk

The main residual risk is consumer misuse: a future consumer could ignore the authority boundary object and key only off ambiguous status or recommendation strings. The minimal patches above reduce that risk without changing behavior.

## Final Recommendation

PASS_WITH_WARNINGS.

No implementation authority leakage was found. Apply the wording/status patches before integrating Research Adversary output into any broader Atlas workflow.

