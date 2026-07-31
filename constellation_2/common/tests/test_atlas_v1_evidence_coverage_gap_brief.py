from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_evidence_coverage_gap_brief.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_evidence_coverage_gap_brief import build_coverage_gap_brief
from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_journal(root: Path) -> None:
    _write(
        root / "observations" / "OBS_0001.yaml",
        "id: OBS_0001\n"
        "observation: Candidate volume was not enough.\n"
        "why_interesting: Candidate volume can hide quality gaps.\n"
        "source: Fixture\n"
        "status: OPEN\n",
    )
    _write(
        root / "knowledge" / "KNW_0001.yaml",
        "id: KNW_0001\n"
        "statement: Candidate conversion needs governance context.\n"
        "evidence:\n"
        "  - Fixture\n"
        "limitations:\n"
        "  - Fixture only\n"
        "why_it_matters: Prevent false completeness.\n"
        "status: SUPPORTED\n",
    )
    _write(
        root / "failures" / "FAIL_0001.yaml",
        "id: FAIL_0001\n"
        "what_we_expected: Candidate conversion would be obvious.\n"
        "what_failed: Candidate conversion evidence was incomplete.\n"
        "why_failed: Missing direct objects.\n"
        "what_to_try_next: Check referenced sources.\n"
        "status: OPEN\n",
    )
    _write(
        root / "reports" / "review_001.md",
        "# Review\n\nCandidate volume references KNW_0001 and FAIL_0001.\n",
    )
    _write(
        root / "reports" / "atlas_evidence_coverage_review_001.md",
        "# Atlas Evidence Coverage Review 001\n\nCoverage was moderate and lexical gaps existed.\n",
    )


def _empty_journal(root: Path) -> None:
    for directory in ("observations", "knowledge", "failures", "reports"):
        (root / directory).mkdir(parents=True, exist_ok=True)


def _snapshot(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_coverage_gap_output_is_deterministic(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = build_coverage_gap_brief(tmp_path, "candidate volume")
    second = build_coverage_gap_brief(tmp_path, "candidate volume")

    assert first == second
    assert "# Atlas V1 Evidence Coverage Gap Brief" in first


def test_required_sections_are_present(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_coverage_gap_brief(tmp_path, "candidate volume")

    assert "# Coverage Summary" in brief
    assert "# Retrieved Source Paths" in brief
    assert "# Referenced But Not Retrieved" in brief
    assert "# Possible Coverage Gaps" in brief
    assert "# Noise / Duplicative Sources" in brief
    assert "# Source Paths" in brief


def test_source_citations_are_included(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_coverage_gap_brief(tmp_path, "candidate volume")

    assert "OBS_0001.yaml" in brief
    assert "review_001.md" in brief
    assert "atlas_evidence_coverage_review_001.md" in brief


def test_referenced_but_not_retrieved_section_flags_candidates(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_coverage_gap_brief(tmp_path, "candidate volume")

    assert "# Referenced But Not Retrieved" in brief
    assert "KNW_0001.yaml" in brief
    assert "FAIL_0001.yaml" in brief
    assert "coverage-gap candidates only" in brief


def test_no_match_behavior_is_deterministic(tmp_path: Path) -> None:
    _empty_journal(tmp_path)

    first = build_coverage_gap_brief(tmp_path, "candidate volume")
    second = build_coverage_gap_brief(tmp_path, "candidate volume")

    assert first == second
    assert "Retrieved source count: 0" in first
    assert "No sources retrieved for the query." in first
    assert "None." in first


def test_script_is_read_only(tmp_path: Path) -> None:
    _seed_journal(tmp_path)
    before = _snapshot(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--journal-root",
            str(tmp_path),
            "--query",
            "candidate volume",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    after = _snapshot(tmp_path)
    assert before == after
    assert "NON_AUTHORITATIVE_READ_ONLY_COVERAGE_GAP_BRIEF" in result.stdout
    assert "recommend trades" in result.stdout
    assert "allocate capital" in result.stdout
    assert "create journal objects" in result.stdout
    assert result.stderr == ""


def test_real_journal_capital_readiness_flags_known_failure_gap() -> None:
    brief = build_coverage_gap_brief(REPO_ROOT / "research_journal", "capital review readiness")

    assert "# Referenced But Not Retrieved" in brief
    assert "FAIL_0010.yaml" in brief


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []
