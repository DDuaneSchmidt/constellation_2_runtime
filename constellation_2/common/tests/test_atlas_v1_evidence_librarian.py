from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_evidence_librarian_brief.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_evidence_librarian_brief import (
    build_brief,
    find_by_id,
    search_by_query,
    search_with_referenced_follow_through,
)
from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_journal(root: Path) -> None:
    _write(
        root / "observations" / "OBS_0001.yaml",
        "id: OBS_0001\n"
        "observation: Candidate volume was low.\n"
        "why_interesting: Candidate volume can be misleading.\n"
        "source: Fixture review\n"
        "status: OPEN\n",
    )
    _write(
        root / "knowledge" / "KNW_0017.yaml",
        "id: KNW_0017\n"
        "statement: Outcome maturity can improve while still remaining underpowered.\n"
        "evidence:\n"
        "  - Fixture evidence\n"
        "limitations:\n"
        "  - Fixture only\n"
        "why_it_matters: Outcome maturity must be checked directly.\n"
        "status: SUPPORTED\n",
    )
    _write(
        root / "failures" / "FAIL_0016.yaml",
        "id: FAIL_0016\n"
        "what_we_expected: Candidate volume would show quality.\n"
        "what_failed: Candidate volume was not sufficient evidence of quality.\n"
        "why_failed: Volume ignored governance and validation blockers.\n"
        "what_to_try_next: Track quality evidence first; do not recursively expand OBS_0001.\n"
        "status: OPEN\n",
    )
    _write(
        root / "reports" / "a_report.md",
        "# A Report\n\nOutcome maturity needs deterministic closure.\n",
    )
    _write(
        root / "reports" / "b_report.md",
        "# B Report\n\nCandidate volume is not candidate quality.\n",
    )
    _write(
        root / "reports" / "capital_report.md",
        "# Capital Report\n\nCapital review readiness is not established. "
        "Review FAIL_0016 and FAIL_9999 before making any claim.\n",
    )


def test_exact_observation_lookup_returns_context_and_related_reports() -> None:
    brief = build_brief(REPO_ROOT / "research_journal", object_id="OBS_0002")

    assert "research_journal/observations/OBS_0002.yaml" in brief
    assert "Paper-position observations accumulated faster" in brief
    assert "research_journal/reports/journal_review_001.md" in brief
    assert "research_journal/reports/outcome_maturity_acceleration_review_001.md" in brief
    assert "Match type: `related_report`" in brief


def test_exact_knowledge_and_failure_ids_resolve() -> None:
    knowledge = build_brief(REPO_ROOT / "research_journal", object_id="KNW_0017")
    failure = build_brief(REPO_ROOT / "research_journal", object_id="FAIL_0010")

    assert "research_journal/knowledge/KNW_0017.yaml" in knowledge
    assert "Outcome maturity can improve while still remaining underpowered" in knowledge
    assert "research_journal/failures/FAIL_0010.yaml" in failure
    assert "Capital allocation review would become useful" in failure


def test_find_by_id_does_not_fuzzy_match_ids(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    assert find_by_id(tmp_path, "OBS_000") == []
    assert find_by_id(tmp_path, "OBS_0001")[0].identifier == "OBS_0001"


def test_keyword_search_returns_deterministic_ordered_results(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = search_by_query(tmp_path, "candidate volume")
    second = search_by_query(tmp_path, "candidate volume")

    first_paths = [match.source_path.as_posix() for match in first]
    second_paths = [match.source_path.as_posix() for match in second]

    assert first_paths == second_paths
    assert first_paths == [
        (tmp_path / "observations" / "OBS_0001.yaml").as_posix(),
        (tmp_path / "failures" / "FAIL_0016.yaml").as_posix(),
        (tmp_path / "reports" / "b_report.md").as_posix(),
    ]


def test_no_match_result_is_deterministic(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    first = build_brief(tmp_path, query="no such research phrase")
    second = build_brief(tmp_path, query="no such research phrase")

    assert first == second
    assert "No matching evidence found." in first
    assert "None." in first


def test_output_includes_source_paths_for_keyword_search(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_brief(tmp_path, query="outcome maturity")

    assert "Status: NON_AUTHORITATIVE_READ_ONLY_BRIEF" in brief
    assert "Source path:" in brief
    assert "knowledge/KNW_0017.yaml" in brief
    assert "reports/a_report.md" in brief
    assert "No relevance ranking is applied." in brief


def test_output_boundary_language_blocks_authority_claims(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    brief = build_brief(tmp_path, query="candidate volume")

    assert "does not validate truth" in brief
    assert "recommend trades" in brief
    assert "allocate capital" in brief
    assert "create journal objects" in brief
    assert "### Result" not in brief


def test_cli_requires_id_or_query() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "one of the arguments --id --query is required" in result.stderr


def test_cli_is_read_only_for_journal_root(tmp_path: Path) -> None:
    _seed_journal(tmp_path)
    before = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))

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

    after = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    assert before == after
    assert "Atlas V1 Research Evidence Librarian Brief" in result.stdout
    assert result.stderr == ""


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []


def test_referenced_object_follow_through_adds_existing_object_ids(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    matches = search_with_referenced_follow_through(tmp_path, "capital review readiness")
    follow_through = [match for match in matches if match.match_type == "referenced_follow_through"]

    assert [match.identifier for match in follow_through] == ["FAIL_0016"]
    assert "referenced by retrieved source" in follow_through[0].reason


def test_referenced_object_follow_through_ignores_missing_ids(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    matches = search_with_referenced_follow_through(tmp_path, "capital review readiness")

    assert "FAIL_9999" not in {match.identifier for match in matches}


def test_referenced_object_follow_through_is_one_hop_only(tmp_path: Path) -> None:
    _seed_journal(tmp_path)

    matches = search_with_referenced_follow_through(tmp_path, "capital review readiness")

    assert "FAIL_0016" in {match.identifier for match in matches}
    assert "OBS_0001" not in {match.identifier for match in matches}


def test_real_capital_review_readiness_surfaces_fail_0010_as_follow_through() -> None:
    matches = search_with_referenced_follow_through(REPO_ROOT / "research_journal", "capital review readiness")
    fail_matches = [match for match in matches if match.identifier == "FAIL_0010"]

    assert len(fail_matches) == 1
    assert fail_matches[0].match_type == "referenced_follow_through"
