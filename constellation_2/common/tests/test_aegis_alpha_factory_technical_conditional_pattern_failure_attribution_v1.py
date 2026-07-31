from __future__ import annotations

import json
from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_technical_conditional_pattern_failure_attribution_v1 import (
    FAILURE_MODES,
    build_alpha_factory_technical_conditional_pattern_failure_attribution_v1,
    write_alpha_factory_technical_conditional_pattern_failure_attribution_v1,
)


DAY = "2026-06-03"
PILOT_FAMILY = "aegis_alpha_factory_technical_conditional_pattern_pilot_v1"
PILOT_FILENAME = "aegis_alpha_factory_technical_conditional_pattern_pilot_v1.json"


def _seed_pilot(root: Path) -> None:
    features = []
    evidence = []
    supported = []
    groups = []
    for asset in ("SPY", "QQQ"):
        for condition in ("oversold_reversal", "trend_continuation"):
            for idx in range(3):
                feature_id = f"TCP-FEAT-{asset}-{condition}-2026-01-{idx + 10:02d}"
                features.append(
                    {
                        "feature_id": feature_id,
                        "asset": asset,
                        "condition_type": condition,
                        "day": f"2026-01-{idx + 10:02d}",
                        "source_observations": [f"OBS-{asset}-{idx}"],
                        "source_series": [asset],
                        "calculation_version": "alpha_factory_technical_conditional_pattern_pilot.v1",
                        "known_at_rule": "technical_condition_known_after_all_source_observations_known_at",
                        "lineage": [f"OBS-{asset}-{idx}"],
                    }
                )
            for horizon in (1, 5):
                row = {
                    "evidence_id": f"TCP-EV-{asset}-{condition}-{horizon}D",
                    "target_asset": asset,
                    "condition_type": condition,
                    "horizon_days": horizon,
                    "sample_count": 12 + horizon,
                    "effect_size": 0.003 + horizon * 0.0001,
                    "direction_consistency": 0.65,
                    "baseline_recoverable": False,
                    "technical_condition_non_pairwise": True,
                    "support_verdict": "SUPPORTED",
                    "support_blockers": [],
                    "lineage": [f"TCP-FEAT-{asset}-{condition}-2026-01-10", f"OBS-{asset}-0"],
                }
                evidence.append(row)
                supported.append(row)
            blocked = {
                "evidence_id": f"TCP-EV-{asset}-{condition}-20D",
                "target_asset": asset,
                "condition_type": condition,
                "horizon_days": 20,
                "sample_count": 2,
                "effect_size": 0.0001,
                "direction_consistency": 0.4,
                "baseline_recoverable": True,
                "technical_condition_non_pairwise": True,
                "support_verdict": "BLOCKED",
                "support_blockers": ["LOW_SAMPLE_COUNT", "WEAK_EFFECT_SIZE", "BASELINE_RECOVERABLE_SUPPORT"],
                "lineage": [f"TCP-FEAT-{asset}-{condition}-2026-01-10", f"OBS-{asset}-0"],
            }
            evidence.append(blocked)
            groups.append(
                {
                    "technical_pattern_candidate_group_id": f"TCP-GROUP-{asset}-{condition}",
                    "proposed_name": f"Observed technical conditional pattern: {asset} {condition}",
                    "target_asset": asset,
                    "condition_type": condition,
                    "supporting_evidence_ids": [
                        f"TCP-EV-{asset}-{condition}-1D",
                        f"TCP-EV-{asset}-{condition}-5D",
                    ],
                    "unique_non_naive_support_count": 2,
                    "qualifies_as_pilot_group": True,
                    "research_asset_candidate_qualification": {"qualifies": False},
                    "limiting_evidence": [],
                    "contradictory_evidence": [],
                    "lineage": [f"TCP-FEAT-{asset}-{condition}-2026-01-10", f"OBS-{asset}-0"],
                }
            )

    artifact = {
        "schema_id": "aegis_alpha_factory_technical_conditional_pattern_pilot",
        "schema_version": "v1",
        "artifact_id": PILOT_FAMILY,
        "day_utc": DAY,
        "content_hash": "seeded-technical-pilot",
        "input_universe": {"loaded_symbols": ["SPY", "QQQ", "GLD", "SLV", "TLT", "UUP", "VIX"]},
        "technical_features": features,
        "pattern_evidence": evidence,
        "supported_technical_evidence": supported,
        "naive_baseline_comparison": {
            "top_candidate_relationship_clusters": [
                {"cluster_id": "NBC-SPY-TO-QQQ", "support_count": 3, "total_sample_count": 90},
                {"cluster_id": "NBC-QQQ-TO-SPY", "support_count": 3, "total_sample_count": 88},
                {"cluster_id": "NBC-GLD-TO-SLV", "support_count": 2, "total_sample_count": 80},
                {"cluster_id": "NBC-TLT-TO-UUP", "support_count": 2, "total_sample_count": 70},
            ],
            "baseline_recoverable_relationship_pairs": ["SPY:QQQ", "QQQ:SPY"],
        },
        "technical_pattern_candidate_groups": groups,
        "valid_research_asset_candidates": [],
        "limiting_or_contradictory_evidence": [
            {
                "limitation_id": "TCP-LIM-BLOCKED-EVIDENCE",
                "blocked_evidence_count": 4,
                "blocker_counts": {"BASELINE_RECOVERABLE_SUPPORT": 4},
            }
        ],
        "hostile_checks": {
            "technical_condition_lineage_complete": True,
            "deterministic_replay": True,
        },
        "verdicts": {
            "execution": "TECHNICAL_CONDITIONAL_PATTERN_PILOT_EXECUTION_VALID",
            "real_data_technical_pattern": "REAL_DATA_TECHNICAL_PATTERN_FOUND",
            "technical_conditional_advantage": "ABSENT",
            "pipeline_vs_baseline": "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE",
            "real_market_technical_discovery_claim": "PROHIBITED",
            "minimum_next_action": "Run an out-of-sample technical conditional pattern replay.",
        },
        "summary": {
            "technical_feature_count": len(features),
            "pattern_evidence_count": len(evidence),
            "supported_evidence_count": len(supported),
            "candidate_group_count": len(groups),
            "valid_rac_count": 0,
            "baseline_cluster_count": 4,
            "lineage_complete": True,
            "output_reproducible": True,
        },
    }
    path = root / "reports" / PILOT_FAMILY / DAY / PILOT_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_technical_failure_attribution_artifact_generation(tmp_path: Path) -> None:
    _seed_pilot(tmp_path)
    payload = build_alpha_factory_technical_conditional_pattern_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_technical_conditional_pattern_failure_attribution"
    assert payload["verdicts"]["execution"] == "TECHNICAL_ATTRIBUTION_EXECUTION_VALID"
    assert payload["verdicts"]["primary_technical_failure_mode"] == "BASELINE_TOO_STRONG"
    paths = write_alpha_factory_technical_conditional_pattern_failure_attribution_v1(
        truth_root=tmp_path, day_utc=DAY, payload=payload
    )
    assert Path(paths["json"]).exists()


def test_technical_failure_attribution_is_deterministic(tmp_path: Path) -> None:
    _seed_pilot(tmp_path)
    first = build_alpha_factory_technical_conditional_pattern_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_technical_conditional_pattern_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_technical_failure_attribution_required_verdicts(tmp_path: Path) -> None:
    _seed_pilot(tmp_path)
    verdicts = build_alpha_factory_technical_conditional_pattern_failure_attribution_v1(
        truth_root=tmp_path, day_utc=DAY
    )["verdicts"]

    assert verdicts["execution"] == "TECHNICAL_ATTRIBUTION_EXECUTION_VALID"
    assert verdicts["primary_technical_failure_mode"] in FAILURE_MODES
    assert verdicts["branch_decision"] in {"TECHNICAL_BRANCH_CONTINUE", "PIVOT_RECOMMENDED", "STOP_BRANCH"}
    assert verdicts["minimum_next_action"] in {
        "stronger regime features",
        "broader universe",
        "better technical pattern definitions",
        "out-of-period/walk-forward validation",
        "stop technical branch",
    }


def test_technical_failure_attribution_hostile_checks(tmp_path: Path) -> None:
    _seed_pilot(tmp_path)
    payload = build_alpha_factory_technical_conditional_pattern_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    hostile = payload["hostile_checks"]

    assert hostile["baseline_dominates_because_pattern_alone_sufficient"] is True
    assert hostile["condition_merely_restates_pattern"] is True
    assert hostile["candidate_was_overnamed"] is False
    assert hostile["deterministic_replay"] is True
    assert hostile["complete_lineage"] is True
    assert hostile["limiting_or_contradictory_evidence_captured"] is True


def test_technical_failure_attribution_compares_against_source_pilot(tmp_path: Path) -> None:
    _seed_pilot(tmp_path)
    payload = build_alpha_factory_technical_conditional_pattern_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    counts = payload["inspected_surfaces"]["counts"]

    assert counts["technical_feature_count"] == 12
    assert counts["pattern_evidence_count"] == 12
    assert counts["supported_evidence_count"] == 8
    assert counts["candidate_group_count"] == 4
    assert counts["baseline_cluster_count"] == 4
    assert payload["input_pilot_verdicts"]["pipeline_vs_baseline"] == "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE"
