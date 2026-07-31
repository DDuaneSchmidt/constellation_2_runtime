from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_evidence_coverage_check.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_evidence_coverage_check import build_coverage_brief, build_coverage_check
from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_journal(root: Path) -> None:
    _write(
        root / "observations" / "OBS_0001.yaml",
        "id: OBS_0001\n"
        "observation: Capital review readiness is not established.\n"
        "why_interesting: Query fixture.\n"
        "source: Fixture review\n"
        "status: OPEN\n",
    )
    _write(
        root / "knowledge" / "KNW_0001.yaml",
        "id: KNW_0001\n"
        "statement: Outcome maturity requires direct closed samples.\n"
        "evidence:\n"
        "  - Fixture evidence\n"
        "limitations:\n"
        "  - Fixture only\n"
        "why_it_matters: Prevent readiness inference.\n"
        "status: SUPPORTED\n",
    )
    _write(
        root / "failures" / "FAIL_0010.yaml",
        "id: FAIL_0010\n"
        "what_we_expected: Capital allocation review would become useful.\n"
        "what_failed: Evidence remained underpowered.\n"
        "why_failed: Outcome samples were insufficient.\n"
        "what_to_try_next: Continue evidence follow-through.\n"
        "status: OPEN\n",
    )
    _write(
        root / "reports" / "capital_review.md",
        "# Capital Review\n\n"
        "capital review readiness remains unresolved. See FAIL_0010 and "
        "research_journal/knowledge/KNW_0001.yaml for adjacent journal context.\n",
    )


def test_coverage_output_is_deterministic(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = build_coverage_brief(tmp_path, "capital review readiness")
    second = build_coverage_brief(tmp_path, "capital review readiness")

    assert first == second
    assert "Atlas V1 Evidence Coverage Check" in first
    assert "NON_AUTHORITATIVE_READ_ONLY_COVERAGE_CHECK" in first


def test_required_sections_are_present(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_coverage_brief(tmp_path, "capital review readiness")

    assert "## Retrieved Source Paths" in brief
    assert "## Source Paths Referenced Inside Retrieved Sources" in brief
    assert "## Referenced-But-Not-Retrieved Source Paths" in brief
    assert "## Potential Coverage Gaps" in brief


def test_referenced_follow_through_removes_existing_object_from_gap_list(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    check = build_coverage_check(tmp_path, "capital review readiness")
    gap_paths = {gap.source_path for gap in check.referenced_but_not_retrieved}

    assert "reports/capital_review.md" in {path.removeprefix("research_journal/") for path in check.retrieved_paths}
    assert "research_journal/failures/FAIL_0010.yaml" in check.retrieved_paths
    assert "research_journal/failures/FAIL_0010.yaml" not in gap_paths
    assert "research_journal/knowledge/KNW_0001.yaml" in check.retrieved_paths
    assert "research_journal/knowledge/KNW_0001.yaml" not in gap_paths


def test_no_match_behavior_is_deterministic(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = build_coverage_brief(tmp_path, "not present anywhere")
    second = build_coverage_brief(tmp_path, "not present anywhere")

    assert first == second
    assert "None. No matching retrieved sources found." in first
    assert "Coverage cannot be checked for this query." in first


def test_cli_is_read_only_for_journal_root(tmp_path: Path) -> None:
    _seed_journal(tmp_path)
    before = {path.relative_to(tmp_path).as_posix(): path.read_text(encoding="utf-8") for path in tmp_path.rglob("*") if path.is_file()}

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--journal-root",
            str(tmp_path),
            "--query",
            "capital review readiness",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    after = {path.relative_to(tmp_path).as_posix(): path.read_text(encoding="utf-8") for path in tmp_path.rglob("*") if path.is_file()}
    assert before == after
    assert "Referenced-But-Not-Retrieved Source Paths" in result.stdout
    assert result.stderr == ""


def test_real_capital_review_follow_through_removes_fail_0010_gap() -> None:
    check = build_coverage_check(REPO_ROOT / "research_journal", "capital review readiness")
    gap_paths = {gap.source_path for gap in check.referenced_but_not_retrieved}

    assert "research_journal/failures/FAIL_0010.yaml" in check.retrieved_paths
    assert "research_journal/failures/FAIL_0010.yaml" not in gap_paths


def test_output_boundary_language_blocks_authority_claims(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_coverage_brief(tmp_path, "capital review readiness")

    assert "potential coverage gap" in brief
    assert "does not determine relevance, truth, readiness, capital allocation, trade quality, or evidence strength" in brief
    assert "No relevance ranking is applied." in brief


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []
