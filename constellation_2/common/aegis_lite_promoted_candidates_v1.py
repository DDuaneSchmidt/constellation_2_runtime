from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.aegis_lite_eod_v1 import normalize_trade_candidate_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
PROMOTED_CANDIDATE_SET_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/promoted_candidate_set.v1.schema.json"
PROMOTED_SLEEVE_LIBRARY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/promoted_sleeve_library.v1.schema.json"


def promoted_candidate_set_path_v1(*, truth_root: Path, day_utc: str, run_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "promoted_candidate_set_v1"
        / day_utc
        / _safe_run_id(run_id)
        / "promoted_candidate_set.v1.json"
    )


def promoted_sleeve_library_path_v1(*, truth_root: Path, day_utc: str, run_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "promoted_sleeve_library_v1"
        / day_utc
        / _safe_run_id(run_id)
        / "promoted_sleeve_library.v1.json"
    )


def build_promoted_candidate_set_v1(
    *,
    day_utc: str,
    run_id: str,
    generated_at_utc: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    rows = []
    for idx, raw in enumerate(candidates, start=1):
        normalized = normalize_trade_candidate_v1(raw, ordinal=idx)
        promoted = str(raw.get("promotion_status") or "promoted").lower() == "promoted"
        rows.append(
            {
                "candidate_id": normalized["candidate_id"],
                "sleeve_id": normalized["sleeve_id"],
                "edge_cluster_id": str(raw.get("edge_cluster_id") or ""),
                "promotion_status": "promoted" if promoted else str(raw.get("promotion_status") or "unknown"),
                "executable_status": normalized["executable_status"],
                "governance_status": "PASS" if promoted and normalized["executable_status"] == "EXECUTABLE" else "BLOCKED",
                "source_promotion_refs": raw.get("source_promotion_refs") if isinstance(raw.get("source_promotion_refs"), list) else [],
                "demo_mode": bool(raw.get("demo_mode", False)),
                "dry_run_only": bool(raw.get("dry_run_only", False)),
            }
        )
    payload = {
        "schema_id": "promoted_candidate_set",
        "schema_version": "v1",
        "artifact_id": "promoted_candidate_set_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "candidate_count": len(rows),
        "promoted_candidate_count": sum(1 for row in rows if row["promotion_status"] == "promoted"),
        "candidates": rows,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_demo_promoted_sleeve_library_v1(*, generated_at_utc: str) -> dict[str, Any]:
    sleeve = {
        "sleeve_id": "C2_DEMO_SUPERVISED_LITE",
        "source_hypothesis_id": "RH_DEMO_SUPERVISED_LITE",
        "research_hypothesis_id": "RH_DEMO_SUPERVISED_LITE",
        "sleeve_name": "Demo Supervised Lite Sleeve",
        "promotion_status": "promoted",
        "approved_by_human": True,
        "approved_for_lite_implementation": True,
        "approved_edge_families": ["DEMO_TREND_CONTINUATION"],
        "approved_trade_classes": ["LONG_EQUITY"],
        "expected_regimes": ["DRY_RUN"],
        "operational_constraints": ["DEMO_ONLY", "DRY_RUN_ONLY", "MAX_ONE_TRADE"],
        "archived": False,
        "human_approval_status": "approved",
        "implementation_status": "approved",
    }
    payload = {
        "schema_id": "promoted_sleeve_library",
        "schema_version": "v1",
        "artifact_id": "promoted_sleeve_library_v1",
        "generated_at_utc": generated_at_utc,
        "promoted_sleeves": [sleeve],
        "sleeves": [sleeve],
        "only_promoted_sleeves_allowed": True,
        "research_lab_artifacts_directly_executable": False,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_demo_candidate_input_v1(*, generated_at_utc: str) -> dict[str, Any]:
    return {
        "candidates": [
            {
                "candidate_id": "DEMO_SPY_SUPERVISED_001",
                "sleeve_id": "C2_DEMO_SUPERVISED_LITE",
                "source_hypothesis_id": "RH_DEMO_SUPERVISED_LITE",
                "research_hypothesis_id": "RH_DEMO_SUPERVISED_LITE",
                "symbol": "SPY",
                "direction": "LONG",
                "instrument_type": "LONG_EQUITY",
                "entry_reference_price": "520.10",
                "suggested_quantity": 1,
                "sizing_guidance": "Demo dry-run: 1 share maximum for UI/operator validation only.",
                "stop_price": "514.90",
                "stop_logic": "DEMO_STOP: fixed protective stop below entry reference.",
                "risk_per_trade": "5.20",
                "sleeve_ownership": "C2_DEMO_SUPERVISED_LITE",
                "confidence": "DEMO",
                "conviction": "DEMO",
                "reason_codes": ["DEMO_ONLY", "DRY_RUN_ONLY", "SUPERVISED_WORKFLOW_PROOF"],
                "edge_family": "DEMO_TREND_CONTINUATION",
                "thesis_id": "DEMO_SPY_SUPERVISED_001",
                "shared_risk_tags": ["DEMO_US_EQUITY_BETA"],
                "correlated_symbols": ["QQQ"],
                "regime_dependency": "DRY_RUN",
                "macro_sensitivity": "DEMO",
                "volatility_liquidity_dependency": "DEMO",
                "promotion_status": "promoted",
                "demo_mode": True,
                "dry_run_only": True,
                "execution_confidence_badges": ["GOVERNED_READY", "DEMO_ONLY", "DRY_RUN_ONLY"],
                "source_promotion_refs": [{"artifact_type": "promoted_sleeve_library_v1", "path": "DEMO_GENERATED"}],
                "source_artifact_refs": [{"artifact_type": "demo_promoted_candidate_set_v1", "path": "DEMO_GENERATED"}],
            }
        ],
        "data_freshness_status": {"status": "PASS", "reason_codes": ["DEMO_DRY_RUN_DATA"]},
        "governance_status": {"status": "PASS", "reason_codes": ["DEMO_DRY_RUN_GOVERNANCE"]},
        "market_regime_state": {"status": "DRY_RUN", "reason_codes": ["DEMO_DRY_RUN_REGIME"]},
        "operator_notes": f"Demo promoted candidate input generated at {generated_at_utc}; not broker executable.",
    }


def validate_promoted_candidate_set_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, PROMOTED_CANDIDATE_SET_SCHEMA)


def validate_promoted_sleeve_library_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, PROMOTED_SLEEVE_LIBRARY_SCHEMA)


def write_promoted_candidate_set_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_promoted_candidate_set_v1(payload)
    path = promoted_candidate_set_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]), run_id=str(payload["run_id"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def write_promoted_sleeve_library_v1(*, truth_root: Path, day_utc: str, run_id: str, payload: dict[str, Any]) -> Path:
    validate_promoted_sleeve_library_v1(payload)
    path = promoted_sleeve_library_path_v1(truth_root=truth_root, day_utc=day_utc, run_id=run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def _safe_run_id(run_id: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(run_id or "").strip())
    return cleaned or "aegis_lite_promoted_candidate_set_v1"
