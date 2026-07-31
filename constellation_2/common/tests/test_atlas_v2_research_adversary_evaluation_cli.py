from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


def test_research_adversary_evaluation_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    reviews = tmp_path / "research_adversary"
    failures = tmp_path / "failures"
    out = tmp_path / "reports" / "atlas_v2_research_os" / "research_adversary_evaluation" / "2026-06-05"
    reviews.mkdir(parents=True)
    failures.mkdir(parents=True)
    (reviews / "research_adversary_review.json").write_text(
        json.dumps(
            {
                "review_id": "review-cli",
                "assumptions": [{"statement": "Regime mismatch and duplicate observations are controlled."}],
                "constraints": [{"description": "Source lineage must reconstruct the mechanism."}],
                "falsification_tests": [{"description": "Reject if regime controls remove the effect."}],
            }
        ),
        encoding="utf-8",
    )
    (failures / "FAIL_REGIME.yaml").write_text(
        "id: FAIL_REGIME\nwhat_failed: Regime mismatch and duplicate observations weakened the hypothesis.\nwhy_failed: Source lineage could not reconstruct the mechanism.\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "constellation_2.common.atlas_v2_research_os.cli",
            "research-adversary-evaluation",
            "--reviews",
            str(reviews),
            "--failures",
            str(failures),
            "--out",
            str(out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    paths = json.loads(result.stdout)
    json_path = Path(paths["json"])
    markdown_path = Path(paths["markdown"])
    assert json_path.exists()
    assert markdown_path.exists()
    report = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = markdown_path.read_text(encoding="utf-8")
    assert report["summary"]["cases_evaluated"] == 1
    assert "## Authority boundary verification" in markdown


def test_evaluation_cli_does_not_modify_production_artifact_dirs(tmp_path: Path) -> None:
    reviews = tmp_path / "research_adversary"
    failures = tmp_path / "failures"
    out = tmp_path / "research_adversary_evaluation" / "2026-06-05"
    reviews.mkdir(parents=True)
    failures.mkdir(parents=True)
    protected_dirs = [
        "historical_replay",
        "edge_qualification",
        "paper_trade_candidates",
        "paper_forward_observation",
        "portfolio",
    ]
    sentinels = {}
    for dirname in protected_dirs:
        path = tmp_path / dirname
        path.mkdir()
        sentinel = path / "sentinel.json"
        sentinel.write_text('{"untouched": true}\n', encoding="utf-8")
        sentinels[sentinel] = sentinel.read_text(encoding="utf-8")
    (reviews / "research_adversary_review.json").write_text(
        json.dumps(
            {
                "review_id": "review-cli",
                "assumptions": [{"statement": "Regime mismatch and duplicate observations are controlled."}],
                "constraints": [{"description": "Source lineage must reconstruct the mechanism."}],
                "falsification_tests": [{"description": "Reject if regime controls remove the effect."}],
            }
        ),
        encoding="utf-8",
    )
    (failures / "FAIL_REGIME.yaml").write_text(
        "id: FAIL_REGIME\nwhat_failed: Regime mismatch and duplicate observations weakened the hypothesis.\nwhy_failed: Source lineage could not reconstruct the mechanism.\n",
        encoding="utf-8",
    )

    subprocess.run(
        [
            sys.executable,
            "-m",
            "constellation_2.common.atlas_v2_research_os.cli",
            "research-adversary-evaluation",
            "--reviews",
            str(reviews),
            "--failures",
            str(failures),
            "--out",
            str(out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    for sentinel, original_text in sentinels.items():
        assert sentinel.read_text(encoding="utf-8") == original_text
