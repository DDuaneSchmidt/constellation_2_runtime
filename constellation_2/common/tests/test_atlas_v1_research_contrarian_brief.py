from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_research_contrarian_brief.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_research_contrarian_brief import build_contrarian_brief
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
        "source: Fixture review\n"
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
        root / "reports" / "review.md",
        "# Review\n\nCandidate volume has unresolved gaps and missing closed outcomes.\n",
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


def test_contrarian_output_is_deterministic(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = build_contrarian_brief(tmp_path, "candidate volume")
    second = build_contrarian_brief(tmp_path, "candidate volume")

    assert first == second
    assert "# Atlas V1 Research Contrarian Brief" in first


def test_required_sections_are_present(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_contrarian_brief(tmp_path, "candidate volume")

    assert "# Current Conclusion" in brief
    assert "# Supporting Evidence" in brief
    assert "# Strongest Contradictory Evidence" in brief
    assert "# Alternative Explanations" in brief
    assert "# What Would Falsify This?" in brief
    assert "# Source Paths" in brief


def test_output_includes_source_citations(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_contrarian_brief(tmp_path, "candidate volume")

    assert "OBS_0001.yaml" in brief
    assert "FAIL_0001.yaml" in brief
    assert "review.md" in brief


def test_contradictory_and_falsifier_sections_use_negative_knowledge(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_contrarian_brief(tmp_path, "candidate volume")

    assert "what_failed: Candidate volume failed as quality evidence." in brief
    assert "what_to_try_next: Recheck candidate quality with closed outcomes." in brief
    assert "Volume ignored validation blockers." in brief


def test_no_match_behavior_is_deterministic(tmp_path: Path) -> None:
    _empty_journal(tmp_path)

    first = build_contrarian_brief(tmp_path, "candidate volume")
    second = build_contrarian_brief(tmp_path, "candidate volume")

    assert first == second
    assert "No matching conclusion was retrieved." in first
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
    assert "NON_AUTHORITATIVE_READ_ONLY_CONTRARIAN_BRIEF" in result.stdout
    assert "recommend trades" in result.stdout
    assert "allocate capital" in result.stdout
    assert "create journal objects" in result.stdout
    assert result.stderr == ""


def test_real_journal_candidate_volume_surfaces_existing_failure() -> None:
    brief = build_contrarian_brief(REPO_ROOT / "research_journal", "candidate volume")

    assert "research_journal/failures/FAIL_0007.yaml" in brief
    assert "Candidate volume would be a useful proxy for candidate quality" in brief


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []
