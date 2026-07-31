from __future__ import annotations

import json
from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_cross_asset_etf_failure_attribution_v1 import (
    build_alpha_factory_cross_asset_etf_failure_attribution_v1,
    write_alpha_factory_cross_asset_etf_failure_attribution_v1,
)


DAY = "2026-06-03"
PILOT_FAMILY = "aegis_alpha_factory_cross_asset_etf_discovery_pilot_v1"
PILOT_FILENAME = "aegis_alpha_factory_cross_asset_etf_discovery_pilot_v1.json"


def _seed_pilot(root: Path) -> None:
    pilot = {
        "schema_id": "aegis_alpha_factory_cross_asset_etf_discovery_pilot",
        "schema_version": "v1",
        "artifact_id": PILOT_FAMILY,
        "day_utc": DAY,
        "content_hash": "seeded-cross-asset-pilot",
        "input_universe": {"loaded_symbols": ["GLD", "QQQ", "SLV", "SPY", "TLT", "UUP", "VIX"]},
        "verdicts": {
            "execution": "CROSS_ASSET_ETF_PILOT_EXECUTION_VALID",
            "real_data_rac": "REAL_DATA_RAC_FOUND",
            "discovery_advantage": "ABSENT",
            "pipeline_vs_baseline": "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE",
            "macro_claim_leakage": "MACRO_CLAIM_LEAKAGE_ABSENT",
            "real_market_cross_asset_discovery_claim": "PROHIBITED",
            "minimum_next_action": "Keep claims prohibited.",
        },
        "summary": {
            "question_count": 3,
            "relationship_feature_count": 6,
            "evidence_count": 3,
            "supported_evidence_count": 2,
            "unique_non_naive_support_count": 2,
            "baseline_cluster_count": 12,
            "valid_rac_count": 1,
            "lineage_complete": True,
            "output_reproducible": True,
        },
        "generated_questions": [
            {
                "question_id": "QDV2-005",
                "source_type": "RELATIONSHIP_INSTABILITY",
                "question": "When does the observed SLV relationship with QQQ become unstable relative to its recent window?",
                "source_relationship_refs": ["AF-FSR-SLV-QQQ-rolling_beta_20d-2026-05-20"],
                "source_feature_refs": ["AF-FSR-SLV-QQQ-rolling_beta_20d-2026-05-20"],
            },
            {
                "question_id": "QDV2-007",
                "source_type": "RELATIONSHIP_INSTABILITY",
                "question": "When does the observed SLV relationship with QQQ become unstable relative to its recent window?",
                "source_relationship_refs": ["AF-FSR-SLV-QQQ-rolling_beta_20d-2026-05-22"],
                "source_feature_refs": ["AF-FSR-SLV-QQQ-rolling_beta_20d-2026-05-22"],
            },
            {
                "question_id": "QDV2-009",
                "source_type": "CORRELATION_BREAKDOWN",
                "question": "When does the observed QQQ relationship with SLV break down?",
                "source_relationship_refs": ["AF-FSR-QQQ-SLV-rolling_correlation_20d-2026-05-22"],
                "source_feature_refs": ["AF-FSR-QQQ-SLV-rolling_correlation_20d-2026-05-22"],
            },
        ],
        "relationship_features": [
            {
                "feature_id": "AF-FSR-SLV-QQQ-rolling_beta_20d-2026-05-20",
                "feature_type": "rolling_beta_20d",
                "relationship_key": "SLV:QQQ",
            },
            {
                "feature_id": "AF-FSR-SLV-QQQ-rolling_beta_20d-2026-05-22",
                "feature_type": "rolling_beta_20d",
                "relationship_key": "SLV:QQQ",
            },
            {
                "feature_id": "AF-FSR-QQQ-SLV-rolling_correlation_20d-2026-05-22",
                "feature_type": "rolling_correlation_20d",
                "relationship_key": "QQQ:SLV",
            },
        ],
        "evidence_support": [
            {
                "evidence_id": "ETF-HIST-EV-QDV2-005",
                "question_id": "QDV2-005",
                "relationship_key": "SLV:QQQ:RELATIONSHIP_INSTABILITY",
                "support_verdict": "SUPPORTED",
                "baseline_recoverable": False,
                "baseline_separated_support": True,
                "non_naive_relationship_feature_support": True,
                "relationship_specific_support": True,
                "nonzero_support": True,
                "sample_count": 8,
            },
            {
                "evidence_id": "ETF-HIST-EV-QDV2-007",
                "question_id": "QDV2-007",
                "relationship_key": "SLV:QQQ:RELATIONSHIP_INSTABILITY",
                "support_verdict": "SUPPORTED",
                "baseline_recoverable": False,
                "baseline_separated_support": True,
                "non_naive_relationship_feature_support": True,
                "relationship_specific_support": True,
                "nonzero_support": True,
                "sample_count": 8,
            },
            {
                "evidence_id": "ETF-HIST-EV-QDV2-009",
                "question_id": "QDV2-009",
                "relationship_key": "QQQ:SLV:RELATIONSHIP_INSTABILITY",
                "support_verdict": "BLOCKED",
                "baseline_recoverable": True,
                "baseline_separated_support": False,
                "non_naive_relationship_feature_support": True,
                "relationship_specific_support": True,
                "nonzero_support": True,
                "support_blockers": ["BASELINE_RECOVERABLE_SUPPORT"],
            },
        ],
        "unique_non_naive_support": [
            {
                "evidence_id": "ETF-HIST-EV-QDV2-005",
                "relationship_key": "SLV:QQQ:RELATIONSHIP_INSTABILITY",
            },
            {
                "evidence_id": "ETF-HIST-EV-QDV2-007",
                "relationship_key": "SLV:QQQ:RELATIONSHIP_INSTABILITY",
            },
        ],
        "naive_baseline_comparison": {
            "baseline_recoverable_relationship_pairs": ["QQQ:SLV", "SLV:QQQ"],
            "top_candidate_relationship_clusters": [
                {"cluster_id": "QQQ:SLV", "relationship_pair": "QQQ:SLV", "sample_count": 20},
                {"cluster_id": "GLD:TLT", "relationship_pair": "GLD:TLT", "sample_count": 18},
            ]
            + [{"cluster_id": f"PAIR-{idx}", "relationship_pair": f"A{idx}:B{idx}", "sample_count": 10} for idx in range(10)],
        },
        "candidate_groups": [
            {
                "research_asset_candidate_id": "ETF-RAC-SLV-QQQ-RELATIONSHIP_INSTABILITY",
                "proposed_name": "Observed cross-asset ETF/VIX relationship structure: SLV:QQQ",
                "relationship_key": "SLV:QQQ:RELATIONSHIP_INSTABILITY",
                "supporting_evidence_ids": ["ETF-HIST-EV-QDV2-005", "ETF-HIST-EV-QDV2-007"],
                "related_question_ids": ["QDV2-005", "QDV2-007"],
                "unique_non_naive_support_count": 2,
                "research_asset_candidate_qualification": {"qualifies": True},
                "lineage": ["QDV2-005", "QDV2-007", "AF-FSR-SLV-QQQ-rolling_beta_20d-2026-05-20"],
            }
        ],
        "valid_research_asset_candidates": [
            {
                "research_asset_candidate_id": "ETF-RAC-SLV-QQQ-RELATIONSHIP_INSTABILITY",
                "proposed_name": "Observed cross-asset ETF/VIX relationship structure: SLV:QQQ",
                "relationship_key": "SLV:QQQ:RELATIONSHIP_INSTABILITY",
                "supporting_evidence_ids": ["ETF-HIST-EV-QDV2-005", "ETF-HIST-EV-QDV2-007"],
                "related_question_ids": ["QDV2-005", "QDV2-007"],
                "unique_non_naive_support_count": 2,
                "research_asset_candidate_qualification": {"qualifies": True},
                "lineage": ["QDV2-005", "QDV2-007", "AF-FSR-SLV-QQQ-rolling_beta_20d-2026-05-20"],
            }
        ],
    }
    out = root / "reports" / PILOT_FAMILY / DAY / PILOT_FILENAME
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(pilot, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_cross_asset_etf_failure_attribution_artifact_generation(tmp_path: Path) -> None:
    _seed_pilot(tmp_path)
    payload = build_alpha_factory_cross_asset_etf_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_cross_asset_etf_failure_attribution"
    assert payload["verdicts"]["execution"] == "CROSS_ASSET_FAILURE_ATTRIBUTION_EXECUTION_VALID"
    paths = write_alpha_factory_cross_asset_etf_failure_attribution_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        payload=payload,
    )
    assert Path(paths["json"]).exists()


def test_cross_asset_etf_failure_attribution_is_deterministic(tmp_path: Path) -> None:
    _seed_pilot(tmp_path)
    first = build_alpha_factory_cross_asset_etf_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_cross_asset_etf_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_cross_asset_etf_failure_attribution_required_verdict_fields(tmp_path: Path) -> None:
    _seed_pilot(tmp_path)
    verdicts = build_alpha_factory_cross_asset_etf_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "CROSS_ASSET_FAILURE_ATTRIBUTION_EXECUTION_VALID"
    assert verdicts["primary_failure_mode"] in {
        "BASELINE_REDUNDANCY",
        "QUESTION_TOO_CLOSE_TO_BASELINE",
        "EVIDENCE_TEST_TOO_CLOSE_TO_BASELINE",
        "FEATURE_SURFACE_TOO_CONVENTIONAL",
        "CANDIDATE_GROUPING_TOO_PERMISSIVE",
        "DATA_UNIVERSE_TOO_SMALL",
        "INCONCLUSIVE",
    }
    assert verdicts["fault_class"] in {
        "QUESTION_GENERATION_FAULT",
        "EVIDENCE_TEST_FAULT",
        "FEATURE_SURFACE_FAULT",
        "UNIVERSE_FAULT",
        "CANDIDATE_GROUPING_FAULT",
        "INCONCLUSIVE",
    }
    assert verdicts["branch_decision"] in {"CURRENT_BRANCH_CONTINUE", "PIVOT_RECOMMENDED", "STOP_BRANCH"}
    assert verdicts["minimum_next_action"] in {
        "broader ETF universe",
        "technical-analysis conditional pattern pilot",
        "long-term fundamental thesis branch",
        "alternative feature representation",
        "stop current cross-asset branch",
    }


def test_cross_asset_etf_failure_attribution_hostile_checks(tmp_path: Path) -> None:
    _seed_pilot(tmp_path)
    payload = build_alpha_factory_cross_asset_etf_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert checks["rac_support_entirely_baseline_recoverable"] is False
    assert checks["rac_relationship_recoverable_by_naive_baseline"] is True
    assert checks["non_naive_evidence_adds_any_information"] is True
    assert checks["non_naive_evidence_adds_material_information_beyond_baseline"] is False
    assert checks["questions_are_mostly_restated_pairwise_relationships"] is True
    assert checks["feature_surface_only_obvious_pairwise_relationships"] is True
    assert checks["universe_size_prevents_discovery_advantage"] is True
    assert checks["candidate_naming_remains_valid"] is True


def test_cross_asset_etf_failure_attribution_classifies_seeded_baseline_redundancy(tmp_path: Path) -> None:
    _seed_pilot(tmp_path)
    payload = build_alpha_factory_cross_asset_etf_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["verdicts"]["primary_failure_mode"] == "BASELINE_REDUNDANCY"
    assert payload["verdicts"]["fault_class"] == "FEATURE_SURFACE_FAULT"
    assert payload["verdicts"]["branch_decision"] == "PIVOT_RECOMMENDED"
    assert payload["verdicts"]["minimum_next_action"] == "alternative feature representation"
