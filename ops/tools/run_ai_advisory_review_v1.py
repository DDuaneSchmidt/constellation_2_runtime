#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, read_json_object_v1, resolve_fact_plane_truth_root_v1
from constellation_2.common.safety_state_authority_v1 import safety_state_authority_output_path
from constellation_2.common.trading_day_readiness_authority_v1 import trading_day_readiness_authority_output_path
from ops.tools.run_decision_ledger_v1 import decision_ledger_path
from ops.tools.run_decision_consistency_v1 import decision_consistency_path
from ops.tools.run_edge_attribution_v1 import edge_attribution_path
from ops.tools.run_insight_engine_v1 import insight_engine_path
from ops.tools.run_intent_lifecycle_state_v1 import intent_lifecycle_state_path
from ops.tools.run_missed_opportunity_v1 import missed_opportunity_path
from ops.tools.run_portfolio_activation_gate_v1 import portfolio_activation_gate_path
from ops.tools.run_portfolio_scoring_v1 import portfolio_scoring_path
from ops.tools.run_position_lifecycle_state_v1 import position_lifecycle_state_path
from ops.tools.run_regime_confidence_v1 import regime_confidence_path
from ops.tools.run_selection_quality_v1 import selection_quality_path
from ops.tools.run_trade_outcome_v1 import trade_outcome_path
from ops.tools.run_advisory_evidence_gateway_v1 import advisory_evidence_packet_path
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1, git_commit_v1, git_dirty_status_v1

PAPER_MODE = "PAPER"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/ai_advisory_review.v1.schema.json"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _validate_ai_advisory_schema(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def _validate_ai_advisory_schema(payload: dict[str, Any]) -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError("AI_ADVISORY_REVIEW_SCHEMA_INVALID:" + ";".join(str(err.message) for err in errors[:3]))


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def ai_advisory_review_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "ai_advisory_review_v1" / day_utc / "ai_advisory_review.v1.json"


def _artifact_map(truth_root: Path, day_utc: str) -> dict[str, Path]:
    return {
        "decision_ledger_v1": decision_ledger_path(truth_root=truth_root, day_utc=day_utc),
        "selection_quality_v1": selection_quality_path(truth_root=truth_root, day_utc=day_utc),
        "edge_attribution_v1": edge_attribution_path(truth_root=truth_root, day_utc=day_utc),
        "regime_confidence_v1": regime_confidence_path(truth_root=truth_root, day_utc=day_utc),
        "trade_outcome_v1": trade_outcome_path(truth_root=truth_root, day_utc=day_utc),
        "decision_consistency_v1": decision_consistency_path(truth_root=truth_root, day_utc=day_utc),
        "missed_opportunity_v1": missed_opportunity_path(truth_root=truth_root, day_utc=day_utc),
        "portfolio_scoring_v1": portfolio_scoring_path(truth_root=truth_root, day_utc=day_utc),
        "portfolio_activation_gate_v1": portfolio_activation_gate_path(truth_root=truth_root, day_utc=day_utc),
        "intent_lifecycle_state_v1": intent_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc),
        "position_lifecycle_state_v1": position_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc),
        "safety_state_authority_v1": safety_state_authority_output_path(truth_root=truth_root, day_utc=day_utc),
        "trading_day_readiness_authority_v1": trading_day_readiness_authority_output_path(truth_root=truth_root, target_day=day_utc),
    }


def build_ai_advisory_review_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    runtime_root = truth_root
    out_path = ai_advisory_review_path(truth_root=truth_root, day_utc=day_utc)
    packet_path = advisory_evidence_packet_path(truth_root=truth_root, day_utc=day_utc)
    packet = _read_json(packet_path)
    packet_status = str(packet.get("status") or "").strip().upper()
    consumption_allowed = bool(packet.get("ai_consumption_allowed") is True)
    generated_at = _now_iso()
    base_contract = {
        "artifact_id": "ai_advisory_review_v1",
        "generated_at": generated_at,
        "git_commit": git_commit_v1(),
        "git_dirty_status": git_dirty_status_v1(),
        "truth_root": str(truth_root),
        "runtime_root": str(runtime_root),
        "producer": "ops/tools/run_ai_advisory_review_v1.py",
        "authority": "ADVISORY_ONLY",
        "readiness_authority": "aegis_control_plane_v1",
        "submit_authority": "aegis_submit_enforcement_v1",
        "operator_action_authority": "CONTROL_PLANE_OR_GOVERNED_RECOVERY_ONLY",
    }
    if not packet or packet_status != "PASS" or not consumption_allowed:
        payload = {
            "schema_id": "ai_advisory_review",
            "schema_version": "v1",
            **base_contract,
            "day_utc": day_utc,
            "environment": environment,
            "status": "NO_GOVERNED_ADVISORY_INPUT",
            "advisory_evidence_packet_path": str(packet_path),
            "advisory_evidence_packet_status": packet_status or "MISSING",
            "control_plane_final_status_observed": str(packet.get("control_plane_final_status") or "UNKNOWN"),
            "control_plane_current_domain_observed": str(packet.get("control_plane_current_domain") or ""),
            "control_plane_canonical_blocker_observed": str(packet.get("control_plane_canonical_blocker") or ""),
            "control_plane_submit_allowed_observed": bool(packet.get("control_plane_submit_allowed") is True),
            "decisions_reviewed": 0,
            "anomaly_flags": ["NO_GOVERNED_ADVISORY_INPUT"],
            "recommendations": [],
            "warnings": ["NO_GOVERNED_ADVISORY_INPUT"],
            "confidence": "NONE",
            "requires_human_review": True,
            "prohibited_actions_attempted": False,
            "evidence_paths": [str(packet_path)] if packet_path.exists() else [],
            "produced_at_utc": generated_at,
            "artifact_path": str(out_path),
        }
        attach_producer_contract_v1(
            payload,
            producer_name="ops/tools/run_ai_advisory_review_v1.py",
            producer_command=f"python3 ops/tools/run_ai_advisory_review_v1.py --day_utc {day_utc} --environment {environment}",
            input_artifacts=[packet_path],
            output_artifacts=[out_path],
            schema_versions={"ai_advisory_review": "v1"},
        )
        _write_json(out_path, payload)
        return payload

    included = packet.get("included_artifacts") if isinstance(packet.get("included_artifacts"), list) else []
    excluded = packet.get("excluded_artifacts") if isinstance(packet.get("excluded_artifacts"), list) else []
    deferred = packet.get("non_actionable_deferred_evidence") if isinstance(packet.get("non_actionable_deferred_evidence"), list) else []
    payload = {
        "schema_id": "ai_advisory_review",
        "schema_version": "v1",
        **base_contract,
        "day_utc": day_utc,
        "environment": environment,
        "status": "PASS",
        "advisory_evidence_packet_path": str(packet_path),
        "advisory_evidence_packet_status": packet_status,
        "control_plane_final_status_observed": str(packet.get("control_plane_final_status") or "UNKNOWN"),
        "control_plane_current_domain_observed": str(packet.get("control_plane_current_domain") or ""),
        "control_plane_canonical_blocker_observed": str(packet.get("control_plane_canonical_blocker") or ""),
        "control_plane_submit_allowed_observed": bool(packet.get("control_plane_submit_allowed") is True),
        "decisions_reviewed": len(included),
        "included_artifact_count": len(included),
        "excluded_artifact_count": len(excluded),
        "non_actionable_deferred_evidence_count": len(deferred),
        "advisory_observations": [
            {
                "summary": "AI consumed only the governed advisory evidence packet.",
                "included_artifacts": [row.get("artifact_type") for row in included if isinstance(row, dict)],
                "excluded_artifacts": [
                    {"artifact_type": row.get("artifact_type"), "reason": row.get("reason")}
                    for row in excluded
                    if isinstance(row, dict)
                ],
            }
        ],
        "anomaly_flags": [str(item) for item in packet.get("warnings", []) if str(item)],
        "recommendations": [],
        "warnings": [str(item) for item in packet.get("warnings", []) if str(item)],
        "confidence": "LOW" if str(packet.get("control_plane_final_status") or "").upper() != "READY" else "MEDIUM",
        "requires_human_review": True,
        "prohibited_actions_attempted": False,
        "evidence_paths": [str(packet_path)],
        "produced_at_utc": generated_at,
        "artifact_path": str(out_path),
    }
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_ai_advisory_review_v1.py",
        producer_command=f"python3 ops/tools/run_ai_advisory_review_v1.py --day_utc {day_utc} --environment {environment}",
        input_artifacts=[packet_path],
        output_artifacts=[out_path],
        schema_versions={"ai_advisory_review": "v1"},
    )
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_ai_advisory_review_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_ai_advisory_review_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "recommendation_count": len(payload["recommendations"]), "path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "DEGRADED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
