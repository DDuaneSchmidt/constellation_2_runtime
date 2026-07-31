from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_research_adversary_cli_produces_json_and_markdown(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "ops/tools/build_atlas_v2_research_adversary_v1.py",
            "--report-root",
            str(tmp_path / "research_adversary"),
            "--day",
            "2026-06-05",
            "--created-at",
            "2026-06-05T00:00:00Z",
        ],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )

    paths = {key: Path(value) for key, value in json.loads(result.stdout).items()}
    assert paths["json"].exists()
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()

    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    markdown = paths["summary"].read_text(encoding="utf-8")

    assert payload["evidence_status"] == "GENERATED_ONLY"
    assert payload["taxonomy_integration"]["evidence_status"] == "GENERATED_ONLY"
    assert payload["taxonomy_integration"]["suspected_taxonomy_categories"]
    assert payload["authority_boundary"]["live_trading_authorized"] is False
    assert payload["authority_boundary"]["broker_execution_authorized"] is False
    assert payload["authority_boundary"]["capital_authorized"] is False
    assert payload["authority_boundary"]["candidate_promotion_authorized"] is False
    assert payload["authority_boundary"]["replay_override_authorized"] is False
    assert payload["authority_boundary"]["qualification_override_authorized"] is False
    assert "Supporting Evidence" in markdown
    assert "Weakening Evidence" in markdown
    assert "Falsification" in markdown
    assert "Taxonomy Integration" in markdown
    assert "Authority Boundaries" in markdown
    assert "GENERATED_ONLY" in markdown


def test_research_adversary_markdown_includes_required_lowercase_terms(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "ops/tools/build_atlas_v2_research_adversary_v1.py",
            "--report-root",
            str(tmp_path / "research_adversary"),
            "--day",
            "2026-06-05",
            "--created-at",
            "2026-06-05T00:00:00Z",
        ],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    paths = {key: Path(value) for key, value in json.loads(result.stdout).items()}
    markdown = paths["summary"].read_text(encoding="utf-8").lower()

    assert "supporting evidence" in markdown
    assert "weakening evidence" in markdown
    assert "falsification" in markdown
    assert "taxonomy integration" in markdown
    assert "authority boundaries" in markdown
    assert "generated_only" in markdown
