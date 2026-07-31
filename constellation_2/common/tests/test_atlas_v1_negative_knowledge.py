from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_negative_knowledge_brief.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_negative_knowledge_brief import build_negative_knowledge_brief
from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_journal(root: Path) -> None:
    _write(
        root / "failures" / "FAIL_0001.yaml",
        "id: FAIL_0001\n"
        "what_we_expected: Candidate volume would show quality.\n"
        "what_failed: Candidate volume was not sufficient evidence of quality.\n"
        "why_failed: Volume ignored validation blockers.\n"
        "what_to_try_next: Recheck candidate quality with closed outcomes.\n"
        "status: OPEN\n",
    )
    _write(
        root / "knowledge" / "KNW_0001.yaml",
        "id: KNW_0001\n"
        "statement: Candidate volume needs caution because unsupported volume can mislead.\n"
        "evidence:\n"
        "  - Candidate volume was not quality evidence.\n"
        "limitations:\n"
        "  - Underpowered sample.\n"
        "why_it_matters: Prevent repeated misdiagnosis.\n"
        "status: SUPPORTED\n",
    )
    _write(
        root / "observations" / "OBS_0001.yaml",
        "id: OBS_0001\n"
        "observation: Candidate volume increased.\n"
        "why_interesting: This neutral observation should not appear without negative language.\n"
        "source: Fixture\n"
        "status: OPEN\n",
    )
    _write(
        root / "reports" / "review.md",
        "# Review\n\nCandidate volume was a prior misdiagnosis and remains unsupported without validation.\n",
    )


def test_negative_knowledge_output_is_deterministic(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = build_negative_knowledge_brief(tmp_path, "candidate volume")
    second = build_negative_knowledge_brief(tmp_path, "candidate volume")

    assert first == second
    assert "Atlas V1 Negative Knowledge Brief" in first


def test_output_includes_source_paths(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_negative_knowledge_brief(tmp_path, "candidate volume")

    assert "## Source File Paths" in brief
    assert "failures/FAIL_0001.yaml" in brief
    assert "knowledge/KNW_0001.yaml" in brief
    assert "reports/review.md" in brief


def test_relevant_failure_entries_appear_for_known_query() -> None:
    brief = build_negative_knowledge_brief(REPO_ROOT / "research_journal", "candidate volume")

    assert "research_journal/failures/FAIL_0007.yaml" in brief
    assert "Candidate volume would be a useful proxy for candidate quality" in brief


def test_corrected_misdiagnoses_section_appears(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_negative_knowledge_brief(tmp_path, "candidate volume")

    assert "## Corrected Misdiagnoses" in brief
    assert "what_we_expected: Candidate volume would show quality." in brief
    assert "what_failed: Candidate volume was not sufficient evidence of quality." in brief


def test_no_match_behavior_is_deterministic(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = build_negative_knowledge_brief(tmp_path, "not present anywhere")
    second = build_negative_knowledge_brief(tmp_path, "not present anywhere")

    assert first == second
    assert "No matches found." in first
    assert "None." in first


def test_script_does_not_create_or_modify_journal_files(tmp_path: Path) -> None:
    _seed_journal(tmp_path)
    before = {path.relative_to(tmp_path).as_posix(): path.read_text(encoding="utf-8") for path in tmp_path.rglob("*") if path.is_file()}

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

    after = {path.relative_to(tmp_path).as_posix(): path.read_text(encoding="utf-8") for path in tmp_path.rglob("*") if path.is_file()}
    assert before == after
    assert "NON_AUTHORITATIVE_READ_ONLY_NEGATIVE_KNOWLEDGE_BRIEF" in result.stdout
    assert result.stderr == ""


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []
