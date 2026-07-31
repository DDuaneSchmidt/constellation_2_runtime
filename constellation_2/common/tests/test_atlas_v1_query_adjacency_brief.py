from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_query_adjacency_brief.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_query_adjacency_brief import build_query_adjacency, build_query_adjacency_brief
from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_journal(root: Path) -> None:
    _write(
        root / "observations" / "OBS_0001.yaml",
        "id: OBS_0001\n"
        "date: 2026-06-03\n"
        "observation: Capital review readiness remained unresolved.\n"
        "why_interesting: Capital allocation review should wait for outcome maturity.\n"
        "source: Fixture review\n"
        "status: OPEN\n",
    )
    _write(
        root / "knowledge" / "KNW_0001.yaml",
        "id: KNW_0001\n"
        "statement: Candidate volume needs evidence context.\n"
        "evidence:\n"
        "  - Candidate volume review\n"
        "limitations:\n"
        "  - Fixture only\n"
        "why_it_matters: Candidate quality cannot be inferred from volume.\n"
        "status: SUPPORTED\n",
    )
    _write(
        root / "failures" / "FAIL_0001.yaml",
        "id: FAIL_0001\n"
        "what_we_expected: Capital allocation review would be ready.\n"
        "what_failed: Capital review readiness lacked outcome evidence.\n"
        "why_failed: Outcome maturity was underpowered.\n"
        "what_to_try_next: Run evidence checks before capital review.\n"
        "status: OPEN\n",
    )
    _write(
        root / "reports" / "review_001.md",
        "# Review\n\nCapital review readiness depends on outcome maturity and evidence closure.\n",
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


def test_query_adjacency_output_is_deterministic(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = build_query_adjacency_brief(tmp_path, "capital review readiness")
    second = build_query_adjacency_brief(tmp_path, "capital review readiness")

    assert first == second
    assert "# Atlas V1 Query Adjacency Brief" in first


def test_required_sections_are_present(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_query_adjacency_brief(tmp_path, "capital review readiness")

    assert "# Query Adjacency Summary" in brief
    assert "# Original Query" in brief
    assert "# Adjacent Phrases Found" in brief
    assert "# Source Evidence" in brief
    assert "# Suggested Follow-Up Queries" in brief
    assert "# Source Paths" in brief


def test_source_citations_are_included(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_query_adjacency_brief(tmp_path, "capital review readiness")

    assert "observations/OBS_0001.yaml" in brief
    assert "failures/FAIL_0001.yaml" in brief
    assert "reports/review_001.md" in brief


def test_adjacent_phrases_come_from_source_text(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    adjacency = build_query_adjacency(tmp_path, "capital review readiness")
    source_text = " ".join(
        token
        for path in tmp_path.rglob("*")
        if path.is_file()
        for token in path.read_text(encoding="utf-8").casefold().replace(":", " ").split()
    )

    assert adjacency.adjacent_phrases
    assert any(item.phrase == "capital allocation" for item in adjacency.adjacent_phrases)
    for item in adjacency.adjacent_phrases:
        assert item.phrase in source_text
        assert item.source_path


def test_no_match_behavior_is_deterministic(tmp_path: Path) -> None:
    _empty_journal(tmp_path)

    first = build_query_adjacency_brief(tmp_path, "capital review readiness")
    second = build_query_adjacency_brief(tmp_path, "capital review readiness")

    assert first == second
    assert "Retrieved source count: 0" in first
    assert "Source-observed adjacent phrase count: 0" in first
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
            "capital review readiness",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    after = _snapshot(tmp_path)
    assert before == after
    assert "NON_AUTHORITATIVE_READ_ONLY_QUERY_ADJACENCY_BRIEF" in result.stdout
    assert "source-observed adjacent phrases" in result.stdout
    assert "recommend trades" in result.stdout
    assert "allocate capital" in result.stdout
    assert "create journal objects" in result.stdout
    assert result.stderr == ""


def test_real_journal_candidate_volume_returns_source_observed_phrases() -> None:
    brief = build_query_adjacency_brief(REPO_ROOT / "research_journal", "candidate volume")

    assert "source-observed adjacent phrase" in brief
    assert "research_journal/" in brief
    assert "relevance" not in brief.casefold()


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []
