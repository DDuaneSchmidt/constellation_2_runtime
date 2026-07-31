from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_research_adversary_review_cli_writes_generated_only_artifacts(tmp_path: Path) -> None:
    observation_path = tmp_path / "observation.json"
    observation_path.write_text(
        json.dumps(
            {
                "observation_id": "obs-test-001",
                "mechanism": "BREAKOUT",
                "regime": "TRENDING",
                "observation": "Breakout follow-through appeared after range compression.",
                "confidence": 0.61,
            }
        ),
        encoding="utf-8",
    )
    out_dir = tmp_path / "reports" / "research_adversary" / "2026-06-05"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "constellation_2.common.atlas_v2_research_os.cli",
            "research-adversary-review",
            "--observation",
            str(observation_path),
            "--out",
            str(out_dir),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    paths = json.loads(result.stdout)
    report_path = Path(paths["json"])
    summary_path = Path(paths["summary"])
    report = json.loads(report_path.read_text(encoding="utf-8"))
    summary = summary_path.read_text(encoding="utf-8")

    assert report["artifact_status"] == "GENERATED_ONLY"
    assert report["summary"]["external_api_calls_made"] is False
    assert report["summary"]["trading_recommendations_made"] is False
    assert report["summary"]["candidate_promotions_made"] is False
    assert report["summary"]["replay_or_qualification_changes_made"] is False
    assert report["authority_boundary"]["production_pipeline_integration_authorized"] is False
    assert report["review_items"][0]["source_id"] == "obs-test-001"
    assert "## Authority Boundary" in summary
    assert "`artifact_status`: `GENERATED_ONLY`" in summary
    assert "Trading recommendations: none" in summary


def test_research_adversary_review_cli_accepts_markdown_claim(tmp_path: Path) -> None:
    claim_path = tmp_path / "claim.md"
    claim_path.write_text("# Claim\n\nOpening range behavior may repeat in high-volatility sessions.\n", encoding="utf-8")
    out_dir = tmp_path / "out"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "constellation_2.common.atlas_v2_research_os.cli",
            "research-adversary-review",
            "--claim",
            str(claim_path),
            "--out",
            str(out_dir),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    report = json.loads((out_dir / "research_adversary_review.json").read_text(encoding="utf-8"))
    assert report["source_artifacts"][0]["format"] == "markdown"
    assert report["review_items"][0]["source_type"] == "claim"
    assert report["review_items"][0]["evidence_level"] == "GENERATED_ONLY"
