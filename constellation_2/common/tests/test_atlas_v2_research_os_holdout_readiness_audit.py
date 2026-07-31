from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.holdout_readiness_audit import (
    build_holdout_readiness_audit,
    run_holdout_readiness_audit,
    write_holdout_readiness_audit,
)

NOW = "2026-06-06T00:00:00Z"


def test_holdout_readiness_audits_all_9_families_and_classifies_blockers(tmp_path: Path) -> None:
    _seed_sources(tmp_path)

    report = build_holdout_readiness_audit(root=tmp_path, created_at=NOW)

    assert report["summary"]["families_audited"] == 9
    assert report["summary"]["readiness_counts"]["BLOCKED_DATA"] == 9
    assert report["summary"]["ready_count"] == 0
    assert report["summary"]["blocked_count"] == 9
    assert report["summary"]["confidence_impact"] == "NONE"
    assert report["summary"]["promotion_authority_emitted"] is False
    assert any(row["priority"] == "P0" for row in report["blockers"])
    assert all(row["confidence_impact"] == "NONE" for row in report["blockers"])
    assert {row["readiness_status"] for row in report["readiness_matrix"]} == {"BLOCKED_DATA"}


def test_holdout_readiness_writes_required_reports(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    report = run_holdout_readiness_audit(root=tmp_path, created_at=NOW)
    paths = write_holdout_readiness_audit(report, root=tmp_path)

    required = [
        tmp_path / "holdout_readiness_audit" / "latest.json",
        tmp_path / "holdout_readiness_audit" / "latest_summary.md",
        tmp_path / "holdout_readiness_audit" / "holdout_readiness_matrix.csv",
        tmp_path / "holdout_readiness_audit" / "holdout_blockers.csv",
        tmp_path / "holdout_readiness_audit" / "holdout_repair_plan.md",
    ]
    for path in required:
        assert path.exists(), path
    assert paths["matrix"].name == "holdout_readiness_matrix.csv"
    rows = list(csv.DictReader((tmp_path / "holdout_readiness_audit" / "holdout_readiness_matrix.csv").open()))
    assert len(rows) == 9
    assert "confidence_impact" not in rows[0]


def test_holdout_readiness_cli_emits_no_validation_or_promotion_authority(tmp_path: Path, capsys) -> None:
    _seed_sources(tmp_path)

    rc = main(["--root", str(tmp_path), "--holdout-readiness-audit"])
    captured = capsys.readouterr()

    assert rc == 0
    assert "holdout_readiness_audit/latest.json" in captured.out
    payload = json.loads((tmp_path / "holdout_readiness_audit" / "latest.json").read_text(encoding="utf-8"))
    assert payload["target_selection_policy"]["holdout_validation_performed"] is False
    assert payload["authority_boundary"]["candidate_production_promotion_authorized"] is False
    assert payload["confidence_impact"] == "NONE"


def _seed_sources(root: Path) -> None:
    families = []
    reviews = []
    campaign = []
    for index in range(9):
        family_id = f"family_{index:02d}"
        candidate_id = f"candidate_{index:02d}"
        family = {
            "family_id": family_id,
            "family_name": f"MEAN_REVERSION / TRENDING / {index + 1}M / TEST",
            "candidate_ids": [candidate_id],
            "mechanism": "MEAN_REVERSION",
            "regime": "TRENDING",
            "timeframes": [f"{index + 1}M"],
            "source_types": ["TEST"],
            "symbols": ["SPY"],
            "best_rank": index + 1,
        }
        families.append(family)
        reviews.append(
            {
                **family,
                "classification": "ROBUST_ENOUGH_TO_OBSERVE" if index < 2 else "PROMISING_BUT_DATA_BLOCKED",
            }
        )
        if index >= 7:
            campaign.insert(0, {"candidate_id": candidate_id})
        else:
            campaign.append({"candidate_id": candidate_id})
    _write_json(root / "candidate_family_discovery" / "latest.json", {"families": families, "report_type": "CANDIDATE_FAMILY_DISCOVERY"})
    _write_json(root / "family_robustness_review" / "latest.json", {"family_reviews": reviews, "report_type": "FAMILY_ROBUSTNESS_REVIEW"})
    _write_json(root / "focused_observation_campaign" / "latest.json", {"campaign_candidates": campaign, "report_type": "FOCUSED_OBSERVATION_CAMPAIGN"})
    _write_json(root / "final_candidate_ranking" / "latest.json", {"campaign_candidate_preview": campaign, "report_type": "FINAL_CANDIDATE_RANKING"})
    _write_json(root / "search_overfit_guardrail" / "latest.json", {"report_type": "SEARCH_OVERFIT_GUARDRAIL"})
    _write_json(root / "backtest_aware_final_qualification" / "latest.json", {"report_type": "BACKTEST_AWARE_FINAL_QUALIFICATION"})
    _write_json(root / "historical_replay" / "latest.json", {"results": [], "report_type": "HISTORICAL_REPLAY"})


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
