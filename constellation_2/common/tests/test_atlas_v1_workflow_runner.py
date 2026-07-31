from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_workflow_runner.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_workflow_runner import build_workflow_brief, classify_coverage
from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_journal(root: Path) -> None:
    _write(
        root / "observations" / "OBS_0001.yaml",
        "id: OBS_0001\n"
        "observation: Candidate volume did not imply quality.\n"
        "why_interesting: Candidate volume can be misleading.\n"
        "source: Fixture\n"
        "status: OPEN\n",
    )
    _write(
        root / "knowledge" / "KNW_0001.yaml",
        "id: KNW_0001\n"
        "statement: Candidate volume needs governance context.\n"
        "evidence:\n"
        "  - Candidate volume was blocked by validation gaps.\n"
        "limitations:\n"
        "  - Underpowered fixture only.\n"
        "why_it_matters: Prevent repeated misdiagnosis.\n"
        "status: SUPPORTED\n",
    )
    _write(
        root / "failures" / "FAIL_0001.yaml",
        "id: FAIL_0001\n"
        "what_we_expected: Candidate volume would prove quality.\n"
        "what_failed: Candidate volume failed as quality evidence.\n"
        "why_failed: Volume ignored validation blockers.\n"
        "what_to_try_next: Recheck candidate quality with closed outcomes.\n"
        "status: OPEN\n",
    )
    _write(
        root / "reports" / "review_001.md",
        "# Review\n\nCandidate volume references KNW_0001 and FAIL_0001 with unresolved gaps.\n",
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


def test_workflow_output_is_deterministic(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = build_workflow_brief(tmp_path, "candidate volume")
    second = build_workflow_brief(tmp_path, "candidate volume")

    assert first == second
    assert "# Atlas V1 Workflow Evidence Packet" in first


def test_all_sections_are_present(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_workflow_brief(tmp_path, "candidate volume")

    assert "# Evidence Packet Summary" in brief
    assert "# Retrieved Evidence" in brief
    assert "# Synthesized Evidence" in brief
    assert "# Negative Knowledge" in brief
    assert "# Contrarian Review" in brief
    assert "# Coverage Gaps" in brief
    assert "# Open Questions" in brief
    assert "# Source Paths" in brief


def test_source_path_deduplication_works(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_workflow_brief(tmp_path, "candidate volume")

    source_section = brief.split("# Source Paths", 1)[1]
    assert source_section.count("OBS_0001.yaml") == 1
    assert source_section.count("FAIL_0001.yaml") == 1


def test_coverage_classification_present(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_workflow_brief(tmp_path, "candidate volume")

    assert "Coverage classification:" in brief
    assert any(label in brief for label in ("HIGH_COVERAGE", "MODERATE_COVERAGE", "LOW_COVERAGE"))


def test_coverage_classification_helper() -> None:
    assert classify_coverage(retrieved_count=0, referenced_gap_count=0, possible_gap_count=1, noise_count=0) == "LOW_COVERAGE"
    assert classify_coverage(retrieved_count=2, referenced_gap_count=0, possible_gap_count=0, noise_count=1) == "HIGH_COVERAGE"
    assert classify_coverage(retrieved_count=2, referenced_gap_count=1, possible_gap_count=1, noise_count=3) == "MODERATE_COVERAGE"


def test_no_match_behavior_is_deterministic(tmp_path: Path) -> None:
    _empty_journal(tmp_path)

    first = build_workflow_brief(tmp_path, "candidate volume")
    second = build_workflow_brief(tmp_path, "candidate volume")

    assert first == second
    assert "Coverage classification: LOW_COVERAGE" in first
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
    assert "NON_AUTHORITATIVE_READ_ONLY_WORKFLOW_PACKET" in result.stdout
    assert "recommend trades" in result.stdout
    assert "allocate capital" in result.stdout
    assert "create journal objects" in result.stdout
    assert result.stderr == ""


def test_real_journal_candidate_volume_packet_includes_failure_and_coverage() -> None:
    brief = build_workflow_brief(REPO_ROOT / "research_journal", "candidate volume")

    assert "research_journal/failures/FAIL_0007.yaml" in brief
    assert "Coverage classification: MODERATE_COVERAGE" in brief
    assert "Referenced-but-not-retrieved coverage-gap candidates only" in brief


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []
