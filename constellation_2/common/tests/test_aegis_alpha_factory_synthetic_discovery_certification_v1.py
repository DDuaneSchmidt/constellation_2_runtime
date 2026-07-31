from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_synthetic_discovery_certification_v1 import (
    MINIMUM_NEXT_ACTION,
    build_alpha_factory_synthetic_discovery_certification_v1,
    write_alpha_factory_synthetic_discovery_certification_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1


DAY = "2026-06-03"
SOURCE_FAMILY = "aegis_alpha_factory_fixture_suite_retest_with_repaired_features_v1"
SOURCE_FILENAME = "aegis_alpha_factory_fixture_suite_retest_with_repaired_features_v1.json"


def _seed_retest_artifact(root: Path) -> None:
    payload = {
        "schema_id": "aegis_alpha_factory_fixture_suite_retest_with_repaired_features",
        "schema_version": "v1",
        "artifact_id": SOURCE_FAMILY,
        "content_hash": "seed-fixture-suite-retest-hash",
        "verdicts": {
            "execution": "REPAIRED_FEATURE_FIXTURE_RETEST_EXECUTION_VALID",
            "spy_vix_injected_structure_detection": "SPY_VIX_INJECTED_STRUCTURE_DETECTED",
            "qqq_real_yield_false_positive": "QQQ_REAL_YIELD_FALSE_POSITIVE_BLOCKED",
            "false_positive_control": "FALSE_POSITIVE_CONTROL_ACCEPTABLE",
            "discovery_generalization": "DISCOVERY_GENERALIZATION_PRESENT",
            "research_asset_candidate": "RESEARCH_ASSET_CANDIDATE_VALID",
        },
        "summary": {
            "fixture_count": 5,
            "valid_rac_fixture_ids": [
                "SYNTHETIC_INJECTED_QQQ_REAL_YIELD_INSTABILITY",
                "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY",
            ],
            "spy_vix_detected_fixture_ids": ["SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY"],
            "qqq_real_yield_valid_fixture_ids": ["SYNTHETIC_INJECTED_QQQ_REAL_YIELD_INSTABILITY"],
            "output_reproducible": True,
        },
        "hostile_checks": {
            "spy_vix_detected_only_where_injected": True,
            "qqq_real_yield_does_not_qualify_in_no_qqq_control": True,
            "no_rac_from_baseline_recoverable_only_support": True,
            "no_zero_sample_support_counted": True,
            "repaired_features_used_in_question_evidence_lineage": True,
            "deterministic_replay": True,
            "lineage_complete": True,
        },
    }
    write_json_v1(root / "reports" / SOURCE_FAMILY / DAY / SOURCE_FILENAME, payload)


def test_synthetic_discovery_certification_artifact_generation(tmp_path: Path) -> None:
    _seed_retest_artifact(tmp_path)
    payload = build_alpha_factory_synthetic_discovery_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_synthetic_discovery_certification"
    assert payload["verdicts"]["certification"] == "SYNTHETIC_DISCOVERY_CERTIFICATION_VALID"
    paths = write_alpha_factory_synthetic_discovery_certification_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_synthetic_discovery_certification_is_deterministic(tmp_path: Path) -> None:
    _seed_retest_artifact(tmp_path)
    first = build_alpha_factory_synthetic_discovery_certification_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_synthetic_discovery_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_synthetic_discovery_certification_required_verdicts(tmp_path: Path) -> None:
    _seed_retest_artifact(tmp_path)
    verdicts = build_alpha_factory_synthetic_discovery_certification_v1(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["certification"] in {"SYNTHETIC_DISCOVERY_CERTIFICATION_VALID", "SYNTHETIC_DISCOVERY_CERTIFICATION_INVALID"}
    assert verdicts["synthetic_discovery_advantage"] in {"SYNTHETIC_DISCOVERY_ADVANTAGE_CERTIFIED", "NOT_CERTIFIED"}
    assert verdicts["false_positive_control"] in {"FALSE_POSITIVE_CONTROL_CERTIFIED", "NOT_CERTIFIED"}
    assert verdicts["real_market_discovery_claim"] == "PROHIBITED"
    assert verdicts["minimum_next_action"] == MINIMUM_NEXT_ACTION


def test_synthetic_discovery_certification_limits_claim_scope(tmp_path: Path) -> None:
    _seed_retest_artifact(tmp_path)
    payload = build_alpha_factory_synthetic_discovery_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["certified_scope"]["claim_scope"] == "DETERMINISTIC_SYNTHETIC_FIXTURE_DISCOVERY_ONLY"
    assert "real-market alpha" in payload["certified_scope"]["forbidden_claims"]
    assert "live trading readiness" in payload["certified_scope"]["forbidden_claims"]
    assert "capital allocation readiness" in payload["certified_scope"]["forbidden_claims"]
    assert "broad Alpha Factory success" in payload["certified_scope"]["forbidden_claims"]


def test_synthetic_discovery_certification_conditions_from_retest(tmp_path: Path) -> None:
    _seed_retest_artifact(tmp_path)
    conditions = build_alpha_factory_synthetic_discovery_certification_v1(truth_root=tmp_path, day_utc=DAY)["certification_conditions"]

    assert conditions["injected_spy_vix_structure_detected"] is True
    assert conditions["detected_only_in_injected_fixture"] is True
    assert conditions["qqq_real_yield_false_positive_blocked"] is True
    assert conditions["no_zero_sample_support_counted"] is True
    assert conditions["no_baseline_recoverable_only_rac"] is True
    assert conditions["repaired_feature_lineage_present"] is True
    assert conditions["deterministic_replay_passed"] is True
    assert conditions["real_market_claims_forbidden"] is True
