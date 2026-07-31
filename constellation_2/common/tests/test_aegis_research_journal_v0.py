from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_schemas(root: Path) -> None:
    for schema_name in ("observation", "knowledge", "failure"):
        source = REPO_ROOT / "research_journal" / f"{schema_name}.schema.yaml"
        target = root / f"{schema_name}.schema.yaml"
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")


def test_empty_journal_validates() -> None:
    errors = validate_journal(REPO_ROOT / "research_journal")

    assert errors == []


def test_missing_required_field_fails(tmp_path: Path) -> None:
    _seed_schemas(tmp_path)
    (tmp_path / "knowledge").mkdir()
    (tmp_path / "failures").mkdir()
    _write(
        tmp_path / "observations" / "OBS_0001.yaml",
        "id: OBS_0001\n"
        "date: 2026-06-03\n"
        "observation: Only 2 of 7 AEGIS sleeves generated candidates last week.\n"
        "source: Weekly sleeve review\n"
        "status: OPEN\n",
    )

    errors = validate_journal(tmp_path)

    assert any("missing required fields: why_interesting" in error for error in errors)


def test_duplicate_ids_fail_across_journal(tmp_path: Path) -> None:
    _seed_schemas(tmp_path)
    _write(
        tmp_path / "observations" / "OBS_0001.yaml",
        "id: DUP_0001\n"
        "date: 2026-06-03\n"
        "observation: Observation.\n"
        "why_interesting: Interesting.\n"
        "source: Review.\n"
        "status: OPEN\n",
    )
    _write(
        tmp_path / "knowledge" / "KNW_0001.yaml",
        "id: DUP_0001\n"
        "statement: Statement.\n"
        "evidence:\n"
        "  - Evidence.\n"
        "limitations:\n"
        "  - Limitation.\n"
        "why_it_matters: It matters.\n"
        "status: SUPPORTED\n",
    )
    (tmp_path / "failures").mkdir()

    errors = validate_journal(tmp_path)

    assert any("duplicate id: DUP_0001" in error for error in errors)


def test_invalid_yaml_fails(tmp_path: Path) -> None:
    _seed_schemas(tmp_path)
    (tmp_path / "knowledge").mkdir()
    (tmp_path / "failures").mkdir()
    _write(tmp_path / "observations" / "OBS_0001.yaml", "id: [unterminated\n")

    errors = validate_journal(tmp_path)

    assert errors
