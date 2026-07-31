from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_research_synthesis_brief.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_research_synthesis_brief import build_synthesis_brief
from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_journal(root: Path) -> None:
    _write(
        root / "knowledge" / "KNW_0001.yaml",
        "id: KNW_0001\n"
        "statement: Outcome maturity needs closed samples.\n"
        "evidence:\n"
        "  - Outcome maturity evidence exists.\n"
        "limitations:\n"
        "  - Underpowered sample remains.\n"
        "why_it_matters: Research claims need evidence.\n"
        "status: SUPPORTED\n",
    )
    _write(
        root / "failures" / "FAIL_0001.yaml",
        "id: FAIL_0001\n"
        "what_we_expected: Outcome maturity would be enough.\n"
        "what_failed: Outcome maturity was blocked by underpowered evidence.\n"
        "why_failed: Missing closed samples.\n"
        "what_to_try_next: Continue read-only follow-through.\n"
        "status: OPEN\n",
    )
    _write(
        root / "observations" / "OBS_0001.yaml",
        "id: OBS_0001\n"
        "date: 2026-06-03\n"
        "observation: Follow-through object was referenced by a retrieved report.\n"
        "why_interesting: Referenced journal objects should remain visible without direct query matching.\n"
        "source: Fixture review\n"
        "status: OPEN\n",
    )
    _write(
        root / "reports" / "review.md",
        "# Review\n\nOutcome maturity has unresolved gaps and missing distributed samples. OBS_0001 is referenced for follow-through.\n",
    )


def test_synthesis_output_is_deterministic(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = build_synthesis_brief(tmp_path, "outcome maturity")
    second = build_synthesis_brief(tmp_path, "outcome maturity")

    assert first == second
    assert "# Evidence Summary" in first


def test_output_includes_source_paths(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_synthesis_brief(tmp_path, "outcome maturity")

    assert "# Source Paths" in brief
    assert "knowledge/KNW_0001.yaml" in brief
    assert "failures/FAIL_0001.yaml" in brief
    assert "reports/review.md" in brief


def test_required_sections_are_present(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_synthesis_brief(tmp_path, "outcome maturity")

    assert "# Evidence Summary" in brief
    assert "## Direct Evidence" in brief
    assert "## Referenced Follow-Through" in brief
    assert "## Contradictory / Limiting Evidence" in brief
    assert "## Open Questions" in brief
    assert "## Source Paths" in brief


def test_evidence_tiers_are_visible_and_cited(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_synthesis_brief(tmp_path, "outcome maturity")
    direct_index = brief.index("## Direct Evidence")
    follow_index = brief.index("## Referenced Follow-Through")
    limiting_index = brief.index("## Contradictory / Limiting Evidence")
    direct_section = brief[direct_index:follow_index]
    follow_section = brief[follow_index:limiting_index]

    assert "knowledge/KNW_0001.yaml" in direct_section
    assert "reports/review.md" in direct_section
    assert "observations/OBS_0001.yaml" in follow_section
    assert "Follow-through object was referenced by a retrieved report." in follow_section


def test_no_match_case_is_deterministic(tmp_path: Path) -> None:
    empty_root = tmp_path / "empty_journal"
    (empty_root / "observations").mkdir(parents=True)
    (empty_root / "knowledge").mkdir()
    (empty_root / "failures").mkdir()
    (empty_root / "reports").mkdir()

    first = build_synthesis_brief(empty_root, "outcome maturity")
    second = build_synthesis_brief(empty_root, "outcome maturity")

    assert first == second
    assert "No matching evidence found." in first
    assert "None." in first


def test_contradictory_or_limiting_section_appears(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_synthesis_brief(tmp_path, "outcome maturity")

    assert "## Contradictory / Limiting Evidence" in brief
    assert "not an independent contradiction finding" in brief
    assert "Outcome maturity was blocked by underpowered evidence." in brief
    assert "unresolved gaps and missing distributed samples" in brief


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
            "outcome maturity",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    after = {path.relative_to(tmp_path).as_posix(): path.read_text(encoding="utf-8") for path in tmp_path.rglob("*") if path.is_file()}
    assert before == after
    assert "NON_AUTHORITATIVE_READ_ONLY_SYNTHESIS" in result.stdout
    assert result.stderr == ""


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []
