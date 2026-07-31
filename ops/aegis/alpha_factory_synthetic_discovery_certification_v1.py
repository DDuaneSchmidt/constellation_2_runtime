from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_synthetic_discovery_certification_v1"
SOURCE_FAMILY = "aegis_alpha_factory_fixture_suite_retest_with_repaired_features_v1"
SOURCE_FILENAME = "aegis_alpha_factory_fixture_suite_retest_with_repaired_features_v1.json"
SCHEMA_ID = "aegis_alpha_factory_synthetic_discovery_certification"
SCHEMA_VERSION = "v1"
SPY_VIX_FIXTURE_ID = "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY"
MINIMUM_NEXT_ACTION = (
    "Run a real historical data pilot with the same repaired feature/evidence pipeline, "
    "but retain benchmark and hostile review gates."
)


def build_alpha_factory_synthetic_discovery_certification_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    source_path = root / "reports" / SOURCE_FAMILY / day_utc / SOURCE_FILENAME
    source = read_json_v1(source_path)
    checks = source.get("hostile_checks", {}) if source else {}
    summary = source.get("summary", {}) if source else {}
    source_verdicts = source.get("verdicts", {}) if source else {}

    conditions = _certification_conditions(source=source, checks=checks, summary=summary, source_verdicts=source_verdicts)
    verdicts = _verdicts(conditions, source_verdicts)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_synthetic_discovery_certification_v1",
        "constraints": {
            "question_discovery_v2_modified": False,
            "evidence_test_v2_modified": False,
            "feature_surface_repair_v1_modified": False,
            "fixture_suite_retest_with_repaired_features_modified": False,
            "candidate_qualification_rules_modified": False,
            "real_market_alpha_claimed": False,
            "live_trading_readiness_claimed": False,
            "capital_allocation_readiness_claimed": False,
            "broad_alpha_factory_success_claimed": False,
            "trading_allowed": False,
        },
        "source_artifacts": {
            "fixture_suite_retest_with_repaired_features_v1": {
                "status": "AVAILABLE" if source else "NOT_FOUND",
                "path": str(source_path),
                "content_hash": source.get("content_hash") if source else None,
            }
        },
        "certification_conditions": conditions,
        "certified_scope": {
            "claim_scope": "DETERMINISTIC_SYNTHETIC_FIXTURE_DISCOVERY_ONLY",
            "allowed_claims": [
                "synthetic SPY:VIX fixture structure was detected by the repaired feature/evidence pipeline",
                "the synthetic claim is limited to deterministic fixture behavior and benchmarked hostile checks",
            ],
            "forbidden_claims": [
                "real-market alpha",
                "live trading readiness",
                "capital allocation readiness",
                "broad Alpha Factory success",
            ],
        },
        "hostile_checks": {
            "source_artifact_present": bool(source),
            "spy_vix_detected_only_where_injected": bool(checks.get("spy_vix_detected_only_where_injected")),
            "qqq_real_yield_false_positive_blocked": bool(checks.get("qqq_real_yield_does_not_qualify_in_no_qqq_control")),
            "zero_sample_support_blocked": bool(checks.get("no_zero_sample_support_counted")),
            "baseline_recoverable_only_rac_blocked": bool(checks.get("no_rac_from_baseline_recoverable_only_support")),
            "repaired_feature_lineage_present": bool(checks.get("repaired_features_used_in_question_evidence_lineage"))
            and bool(checks.get("lineage_complete")),
            "deterministic_replay_passed": bool(checks.get("deterministic_replay")) and bool(summary.get("output_reproducible")),
            "real_market_claims_prohibited": True,
        },
        "source_summary": {
            "fixture_count": summary.get("fixture_count", 0),
            "spy_vix_detected_fixture_ids": list(summary.get("spy_vix_detected_fixture_ids", [])),
            "qqq_real_yield_valid_fixture_ids": list(summary.get("qqq_real_yield_valid_fixture_ids", [])),
            "valid_rac_fixture_ids": list(summary.get("valid_rac_fixture_ids", [])),
            "source_verdicts": source_verdicts,
        },
        "verdicts": verdicts,
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_synthetic_discovery_certification_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_synthetic_discovery_certification_v1.json", payload)
    return {"json": str(path)}


def _certification_conditions(
    *,
    source: dict[str, Any],
    checks: dict[str, Any],
    summary: dict[str, Any],
    source_verdicts: dict[str, Any],
) -> dict[str, bool]:
    spy_detected = source_verdicts.get("spy_vix_injected_structure_detection") == "SPY_VIX_INJECTED_STRUCTURE_DETECTED"
    spy_only = list(summary.get("spy_vix_detected_fixture_ids", [])) == [SPY_VIX_FIXTURE_ID]
    qqq_blocked = source_verdicts.get("qqq_real_yield_false_positive") == "QQQ_REAL_YIELD_FALSE_POSITIVE_BLOCKED"
    return {
        "source_artifact_loaded": bool(source),
        "injected_spy_vix_structure_detected": spy_detected,
        "detected_only_in_injected_fixture": spy_only and bool(checks.get("spy_vix_detected_only_where_injected")),
        "qqq_real_yield_false_positive_blocked": qqq_blocked and bool(checks.get("qqq_real_yield_does_not_qualify_in_no_qqq_control")),
        "no_zero_sample_support_counted": bool(checks.get("no_zero_sample_support_counted")),
        "no_baseline_recoverable_only_rac": bool(checks.get("no_rac_from_baseline_recoverable_only_support")),
        "repaired_feature_lineage_present": bool(checks.get("repaired_features_used_in_question_evidence_lineage"))
        and bool(checks.get("lineage_complete")),
        "deterministic_replay_passed": bool(checks.get("deterministic_replay")) and bool(summary.get("output_reproducible")),
        "valid_research_asset_candidate_present": source_verdicts.get("research_asset_candidate") == "RESEARCH_ASSET_CANDIDATE_VALID",
        "real_market_claims_forbidden": True,
    }


def _verdicts(conditions: dict[str, bool], source_verdicts: dict[str, Any]) -> dict[str, str]:
    certification_valid = all(conditions.values())
    synthetic_advantage = (
        certification_valid
        and source_verdicts.get("discovery_generalization") == "DISCOVERY_GENERALIZATION_PRESENT"
        and source_verdicts.get("research_asset_candidate") == "RESEARCH_ASSET_CANDIDATE_VALID"
    )
    false_positive_control = (
        conditions["detected_only_in_injected_fixture"]
        and conditions["qqq_real_yield_false_positive_blocked"]
        and conditions["no_zero_sample_support_counted"]
        and conditions["no_baseline_recoverable_only_rac"]
    )
    return {
        "certification": "SYNTHETIC_DISCOVERY_CERTIFICATION_VALID" if certification_valid else "SYNTHETIC_DISCOVERY_CERTIFICATION_INVALID",
        "synthetic_discovery_advantage": "SYNTHETIC_DISCOVERY_ADVANTAGE_CERTIFIED" if synthetic_advantage else "NOT_CERTIFIED",
        "false_positive_control": "FALSE_POSITIVE_CONTROL_CERTIFIED" if false_positive_control else "NOT_CERTIFIED",
        "real_market_discovery_claim": "PROHIBITED",
        "minimum_next_action": MINIMUM_NEXT_ACTION,
    }


def _hash(payload: dict[str, Any]) -> str:
    material = {key: value for key, value in payload.items() if key != "content_hash"}
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
