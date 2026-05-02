#!/usr/bin/env python3
"""
run_capital_authority_allocation_day_v1.py

Bundle B: canonical candidate -> trade_intent -> authorized_trade_intent writer
for the active paper-day path.

Writes:
  constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1/<DAY>/capital_authority_allocation.v1.json

Canonical authority:
  - decision_chain.candidate_actions
  - decision_chain.trade_intents
  - decision_chain.authorized_trade_intents
  - allocation_state
  - sleeve_account_authority_state

Compatibility bridge (explicit, non-authoritative):
  - portfolio / per_sleeve / per_intent legacy views remain materialized for
    downstream consumers that have not yet migrated off capital_authority_allocation_v1.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root
from constellation_2.common.sleeve_edge_measurement_v1 import (
    allocator_action_from_qualification_v1,
    load_sleeve_edge_policy_v1,
    read_sleeve_edge_snapshot_for_day_v1,
)
from constellation_2.common.governed_evaluation_control_v1 import (
    resolve_capital_authority_runtime_control_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
SLEEVE_EDGE_POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_SLEEVE_EDGE_QUALIFICATION_POLICY_V1.json").resolve()
GOVERNED_EVALUATION_POLICY_PATH = (
    REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_GOVERNED_EVALUATION_POLICY_V1.json"
).resolve()
SLEEVE_REGISTRY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json").resolve()
ACCOUNT_REGISTRY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
POSITIONS_SNAPSHOT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/capital_authority_allocation.v1.schema.json"
CANONICAL_SEQUENCE_OWNER = "ops/tools/run_c2_paper_day_orchestrator_v2.py"
WEIGHT_QUANT = Decimal("0.000001")
ECONOMIC_BUILD_FAMILY = "economic_state_build_v1"
ECONOMIC_SIGNAL_PRIORITY = {"INCREASE": 0, "MAINTAIN": 1, "DECREASE": 2, "UNKNOWN": 3}
ECONOMIC_SIGNAL_MULTIPLIER_BP = {"INCREASE": 10000, "MAINTAIN": 10000, "DECREASE": 5000, "UNKNOWN": 10000}

ACTION_OPEN = "OPEN"
ACTION_ADD = "ADD"
ACTION_REDUCE = "REDUCE"
ACTION_CLOSE = "CLOSE"
ACTION_HOLD = "HOLD"

OUTCOME_APPROVED = "APPROVED"
OUTCOME_RESIZED = "RESIZED"
OUTCOME_REJECTED = "REJECTED"
OUTCOME_BLOCKED = "BLOCKED"
PAPER_BOOTSTRAP_MODE = "PAPER_BOOTSTRAP"
PAPER_DISCOVERY_MODE_ACTIVE = "PAPER_DISCOVERY_MODE_ACTIVE"
SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED = "SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED"
PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED = "PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED"
PAPER_DISCOVERY_MIN_EXECUTABLE_HEADROOM_ALLOWED = "PAPER_DISCOVERY_MIN_EXECUTABLE_HEADROOM_ALLOWED"


@dataclass(frozen=True)
class ExecutionBinding:
    execution_sleeve_id: str
    mode: str
    enabled: bool
    account_id: str
    allowed_engine_ids: Tuple[str, ...]
    allowed_execution_sleeve_ids: Tuple[str, ...]


def _resolve_truth_root(truth_root_arg: Optional[str]) -> Path:
    if truth_root_arg:
        return Path(truth_root_arg).resolve()
    return resolve_truth_root(REPO_ROOT)


def _require_canonical_sequence_owner(raw_owner: str) -> str:
    owner = str(raw_owner or "").strip()
    if not owner:
        raise SystemExit(
            f"FAIL: SLEEVE_EDGE_CANONICAL_SEQUENCE_OWNER_REQUIRED:expected={CANONICAL_SEQUENCE_OWNER}"
        )
    if owner != CANONICAL_SEQUENCE_OWNER:
        raise SystemExit(
            f"FAIL: SLEEVE_EDGE_CANONICAL_SEQUENCE_OWNER_MISMATCH:"
            f"expected={CANONICAL_SEQUENCE_OWNER}:actual={owner}"
        )
    return owner


def _load_sleeve_edge_allocator_meta(
    *,
    truth_root: Path,
    day_utc: str,
    sleeve_id: str,
    sleeve_edge_policy: Dict[str, Any],
    canonical_sequence_owner: str,
    environment: str = "PAPER",
    paper_discovery_policy: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _require_canonical_sequence_owner(canonical_sequence_owner)
    expected_policy_version = str(sleeve_edge_policy.get("policy_version") or "v1").strip() or "v1"
    compatibility = sleeve_edge_policy.get("allocator_compatibility")
    if not isinstance(compatibility, dict):
        raise SystemExit("FAIL: SLEEVE_EDGE_ALLOCATOR_COMPATIBILITY_INVALID")
    allowed_calculation_versions = [
        str(item).strip()
        for item in compatibility.get("allowed_calculation_versions") or []
        if str(item).strip()
    ]
    if not allowed_calculation_versions:
        raise SystemExit("FAIL: SLEEVE_EDGE_ALLOCATOR_ALLOWED_CALCULATION_VERSIONS_INVALID")
    try:
        snapshot = read_sleeve_edge_snapshot_for_day_v1(
            truth_root=truth_root,
            sleeve_id=sleeve_id,
            day_utc=day_utc,
            expected_policy_version=expected_policy_version,
            allowed_calculation_versions=allowed_calculation_versions,
        )
    except ValueError as exc:
        detail = str(exc)
        if "SLEEVE_EDGE_SNAPSHOT_MISSING" in detail:
            prior_snapshot_days = _prior_edge_snapshot_day_count(
                truth_root=truth_root,
                sleeve_id=sleeve_id,
                day_utc=day_utc,
            )
            discovery = _paper_discovery_activation(
                policy=dict(paper_discovery_policy or {}),
                environment=environment,
                trigger="SLEEVE_EDGE_SNAPSHOT_MISSING",
                prior_edge_snapshot_days=prior_snapshot_days,
            )
            if bool(discovery.get("active") is True):
                reason_codes = _dedupe_reason_codes(
                    [detail, "SLEEVE_EDGE_MEASUREMENT_INVALID"]
                    + list(discovery.get("reason_codes") or [])
                )
                return {
                    "qualification_state": "MEASUREMENT_INVALID",
                    "edge_band": "",
                    "execution_health_band": "",
                    "sample_sufficiency_band": "",
                    "drift_band": "",
                    "reason_codes": reason_codes,
                    "action_reason_code": SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED,
                    "capital_multiplier_bp": int(discovery.get("headroom_multiplier_bp") or 0),
                    "snapshot_path": "",
                    "snapshot_sha256": "",
                    "snapshot_policy_version": expected_policy_version,
                    "snapshot_calculation_version": "",
                    "discovery_mode_active": True,
                    "discovery_max_headroom_cents": int(discovery.get("max_sleeve_headroom_cents") or 0),
                    "discovery_policy_id": str(discovery.get("policy_id") or ""),
                    "discovery_sleeve_governance_multiplier_bp": int(
                        discovery.get("sleeve_governance_multiplier_bp") or 0
                    ),
                }
            # Fail-closed default: missing sleeve-edge snapshots force zero allocation.
            return {
                "qualification_state": "MEASUREMENT_INVALID",
                "edge_band": "",
                "execution_health_band": "",
                "sample_sufficiency_band": "",
                "drift_band": "",
                "reason_codes": [detail, "SLEEVE_EDGE_MEASUREMENT_INVALID"],
                "action_reason_code": "SLEEVE_EDGE_MEASUREMENT_INVALID",
                "capital_multiplier_bp": 0,
                "snapshot_path": "",
                "snapshot_sha256": "",
                "snapshot_policy_version": expected_policy_version,
                "snapshot_calculation_version": "",
                "discovery_mode_active": False,
                "discovery_max_headroom_cents": 0,
                "discovery_policy_id": "",
                "discovery_sleeve_governance_multiplier_bp": 0,
            }
        raise SystemExit(
            f"FAIL: SLEEVE_EDGE_SNAPSHOT_DEPENDENCY:sleeve_id={sleeve_id}:day_utc={day_utc}:detail={exc}"
        ) from exc
    qualification = dict(snapshot.get("qualification") or {})
    allocator_action = allocator_action_from_qualification_v1(sleeve_edge_policy, qualification)
    qualification_state = str(qualification.get("qualification_state") or "")
    reason_codes = list(qualification.get("reason_codes") or [])
    action_reason_code = str(allocator_action.get("reason_code") or "")
    capital_multiplier_bp = int(allocator_action.get("capital_multiplier_bp") or 0)
    discovery_max_headroom_cents = 0
    discovery_policy_id = ""
    discovery_mode_active = False
    if qualification_state in {"MEASUREMENT_INVALID", "INSUFFICIENT_DATA"}:
        prior_snapshot_days = _prior_edge_snapshot_day_count(
            truth_root=truth_root,
            sleeve_id=sleeve_id,
            day_utc=day_utc,
        )
        discovery = _paper_discovery_activation(
            policy=dict(paper_discovery_policy or {}),
            environment=environment,
            trigger="SLEEVE_EDGE_INSUFFICIENT_EVIDENCE",
            prior_edge_snapshot_days=prior_snapshot_days,
        )
        if bool(discovery.get("active") is True):
            reason_codes = _dedupe_reason_codes(reason_codes + list(discovery.get("reason_codes") or []))
            action_reason_code = SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED
            capital_multiplier_bp = int(discovery.get("headroom_multiplier_bp") or 0)
            discovery_max_headroom_cents = int(discovery.get("max_sleeve_headroom_cents") or 0)
            discovery_policy_id = str(discovery.get("policy_id") or "")
            discovery_mode_active = True
    snapshot_path = str(snapshot.get("artifact_path") or "").strip()
    if not snapshot_path:
        raise SystemExit(f"FAIL: SLEEVE_EDGE_SNAPSHOT_ARTIFACT_PATH_MISSING:sleeve_id={sleeve_id}:day_utc={day_utc}")
    return {
        "qualification_state": qualification_state,
        "edge_band": str(qualification.get("edge_band") or ""),
        "execution_health_band": str(qualification.get("execution_health_band") or ""),
        "sample_sufficiency_band": str(qualification.get("sample_sufficiency_band") or ""),
        "drift_band": str(qualification.get("drift_band") or ""),
        "reason_codes": reason_codes,
        "action_reason_code": action_reason_code,
        "capital_multiplier_bp": capital_multiplier_bp,
        "snapshot_path": snapshot_path,
        "snapshot_sha256": str(snapshot.get("artifact_sha256") or ""),
        "snapshot_policy_version": str(snapshot.get("policy_version") or ""),
        "snapshot_calculation_version": str(snapshot.get("calculation_version") or ""),
        "discovery_mode_active": discovery_mode_active,
        "discovery_max_headroom_cents": discovery_max_headroom_cents,
        "discovery_policy_id": discovery_policy_id,
        "discovery_sleeve_governance_multiplier_bp": int(
            discovery.get("sleeve_governance_multiplier_bp") if discovery_mode_active else 0
        ),
    }


def _bootstrap_zero_baseline_qualification_meta() -> Dict[str, Any]:
    return {
        "qualification_state": "",
        "edge_band": "",
        "execution_health_band": "",
        "sample_sufficiency_band": "",
        "drift_band": "",
        "reason_codes": ["PAPER_BOOTSTRAP_ZERO_RISK_BASELINE"],
        "action_reason_code": "PAPER_BOOTSTRAP_ZERO_RISK_BASELINE",
        "capital_multiplier_bp": 0,
        "snapshot_path": "",
        "snapshot_sha256": "",
        "snapshot_policy_version": "",
        "snapshot_calculation_version": "",
        "discovery_mode_active": False,
        "discovery_max_headroom_cents": 0,
        "discovery_policy_id": "",
        "discovery_sleeve_governance_multiplier_bp": 0,
    }


def _authority_head_path(truth_root: Path) -> Path:
    return (truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()


def _out_root(truth_root: Path) -> Path:
    return (truth_root / "allocation_v1" / "capital_authority_allocation_v1").resolve()


def _require_passing_authority_verdict_for_day(day: str, verdict_path: Path) -> Dict[str, Any]:
    verdict = _read_json_obj(verdict_path)
    verdict_schema_id = str(verdict.get("schema_id") or "").strip()
    verdict_schema_version = str(verdict.get("schema_version") or "").strip()
    verdict_day = str(verdict.get("day_utc") or "").strip()
    verdict_status = str(verdict.get("status") or "").strip().upper()

    if verdict_day != day:
        raise SystemExit("FAIL: AUTHORITY_HEAD_POINTS_TO_DAY_MISMATCH")
    if verdict_status not in ("PASS", "BOOTSTRAP_PASS"):
        raise SystemExit("FAIL: AUTHORITY_HEAD_POINTS_TO_NOT_EXECUTION_AUTHORIZED")

    verdict_name = verdict_path.name
    verdict_path_text = str(verdict_path)
    if verdict_name == "authorization_gate_verdict.v1.json" or "authorization_gate_verdict_v1" in verdict_path_text:
        if verdict_schema_id != "authorization_gate_verdict_v1" or verdict_schema_version not in {"1", "v1"}:
            raise SystemExit("FAIL: AUTHORIZATION_VERDICT_SCHEMA_MISMATCH")
    elif verdict_name == "gate_stack_verdict.v1.json" or "gate_stack_verdict_v1" in verdict_path_text:
        if verdict_schema_id != "gate_stack_verdict" or verdict_schema_version != "v1":
            raise SystemExit("FAIL: GATE_STACK_VERDICT_SCHEMA_MISMATCH")
    else:
        raise SystemExit("FAIL: AUTHORITY_HEAD_POINTS_TO_UNSUPPORTED_VERDICT")
    return verdict


def _require_authority_head_pass_authoritative(day: str, truth_root: Path) -> Dict[str, Any]:
    p = _authority_head_path(truth_root)
    ah = _read_json_obj(p)
    schema_id = str(ah.get("schema_id") or "").strip()
    schema_ver = str(ah.get("schema_version") or "").strip()
    status = str(ah.get("status") or "").strip().upper()
    authoritative = bool(ah.get("authoritative") is True)
    day_utc = str(ah.get("day_utc") or "").strip()

    if schema_id != "c2_run_pointer_canonical_authority_head" or schema_ver != "v1":
        raise SystemExit("FAIL: AUTHORITY_HEAD_SCHEMA_MISMATCH")
    if day_utc != day:
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_DAY_MISMATCH head_day={day_utc!r} expected_day={day!r}")
    if status not in ("PASS", "BOOTSTRAP_PASS"):
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_NOT_EXECUTION_AUTHORIZED status={status!r}")
    if not authoritative:
        raise SystemExit("FAIL: AUTHORITY_HEAD_NOT_AUTHORITATIVE")
    points_to = str(ah.get("points_to") or "").strip()
    verdict_path = Path(points_to).resolve() if Path(points_to).is_absolute() else (truth_root / points_to).resolve()
    _require_passing_authority_verdict_for_day(day, verdict_path)
    return ah


def _require_explicit_authority_verdict_pass_authoritative(day: str, raw_path: str) -> Dict[str, Any]:
    path_text = str(raw_path or "").strip()
    if not path_text:
        raise SystemExit("FAIL: AUTHORITY_VERDICT_PATH_REQUIRED")
    verdict_path = Path(path_text).expanduser().resolve()
    if not verdict_path.is_absolute():
        raise SystemExit("FAIL: AUTHORITY_VERDICT_PATH_NOT_ABSOLUTE")
    return _require_passing_authority_verdict_for_day(day, verdict_path)



def _parse_day(day_utc: str) -> str:
    d = str(day_utc).strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {d!r}")
    return d


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise SystemExit(f"FAIL: missing_or_not_file: {str(path)}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: json_parse_failed: {str(path)}: {e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _dedupe_reason_codes(values: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for raw in values:
        code = str(raw or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _load_paper_discovery_mode_policy() -> Dict[str, Any]:
    payload = _read_json_obj(GOVERNED_EVALUATION_POLICY_PATH)
    runtime_policy = payload.get("runtime_control_policy")
    if not isinstance(runtime_policy, dict):
        raise SystemExit("FAIL: GOVERNED_EVALUATION_RUNTIME_CONTROL_POLICY_MISSING")
    allocation_policy = runtime_policy.get("capital_authority_allocation_v1")
    if not isinstance(allocation_policy, dict):
        raise SystemExit("FAIL: GOVERNED_EVALUATION_CAPITAL_AUTHORITY_CONTROL_POLICY_MISSING")
    paper_discovery = allocation_policy.get("paper_discovery_mode")
    if paper_discovery is None:
        return {
            "policy_id": "BOOTSTRAP_PAPER_TRADING",
            "enabled": False,
            "environments": [],
            "activation_triggers": [],
            "min_prior_edge_snapshot_days_for_edge_governed": 0,
            "discovery_headroom_multiplier_bp": 0,
            "max_sleeve_headroom_cents": 0,
            "discovery_sleeve_governance_multiplier_bp": 0,
            "discovery_portfolio_governance_multiplier_bp": 0,
            "minimum_executable_trade_risk_cents": 0,
            "reason_codes": [],
        }
    if not isinstance(paper_discovery, dict):
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_POLICY_INVALID")
    policy_id = str(paper_discovery.get("policy_id") or "").strip() or "BOOTSTRAP_PAPER_TRADING"
    enabled = bool(paper_discovery.get("enabled") is True)
    environments_raw = paper_discovery.get("environments") or []
    if not isinstance(environments_raw, list):
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_ENVIRONMENTS_INVALID")
    environments = [
        str(item).strip().upper()
        for item in environments_raw
        if str(item).strip()
    ]
    activation_triggers_raw = paper_discovery.get("activation_triggers") or []
    if not isinstance(activation_triggers_raw, list):
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_ACTIVATION_TRIGGERS_INVALID")
    activation_triggers = [
        str(item).strip().upper()
        for item in activation_triggers_raw
        if str(item).strip()
    ]
    min_prior_days = paper_discovery.get("min_prior_edge_snapshot_days_for_edge_governed")
    if not isinstance(min_prior_days, int) or int(min_prior_days) < 0:
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_MIN_PRIOR_EDGE_DAYS_INVALID")
    multiplier_bp = paper_discovery.get("discovery_headroom_multiplier_bp")
    if not isinstance(multiplier_bp, int) or int(multiplier_bp) < 0 or int(multiplier_bp) > 10000:
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_MULTIPLIER_BP_INVALID")
    max_sleeve_headroom_cents = paper_discovery.get("max_sleeve_headroom_cents")
    if not isinstance(max_sleeve_headroom_cents, int) or int(max_sleeve_headroom_cents) < 0:
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_MAX_SLEEVE_HEADROOM_INVALID")
    sleeve_relax_multiplier_bp = paper_discovery.get("discovery_sleeve_governance_multiplier_bp")
    if not isinstance(sleeve_relax_multiplier_bp, int) or int(sleeve_relax_multiplier_bp) < 0 or int(
        sleeve_relax_multiplier_bp
    ) > 10000:
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_SLEEVE_GOVERNANCE_MULTIPLIER_BP_INVALID")
    portfolio_relax_multiplier_bp = paper_discovery.get("discovery_portfolio_governance_multiplier_bp", 0)
    if not isinstance(portfolio_relax_multiplier_bp, int) or int(portfolio_relax_multiplier_bp) < 0 or int(
        portfolio_relax_multiplier_bp
    ) > 10000:
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_PORTFOLIO_GOVERNANCE_MULTIPLIER_BP_INVALID")
    minimum_executable_trade_risk_cents = paper_discovery.get("minimum_executable_trade_risk_cents", 0)
    if not isinstance(minimum_executable_trade_risk_cents, int) or int(minimum_executable_trade_risk_cents) < 0:
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_MIN_EXECUTABLE_TRADE_RISK_CENTS_INVALID")
    reason_codes = _dedupe_reason_codes(paper_discovery.get("reason_codes") or [])
    required_codes = {PAPER_DISCOVERY_MODE_ACTIVE, SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED}
    if enabled and (int(multiplier_bp) == 0 or int(max_sleeve_headroom_cents) == 0):
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_CAPS_INVALID_WHEN_ENABLED")
    if enabled and not required_codes.issubset(set(reason_codes)):
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_REASON_CODES_INCOMPLETE")
    if enabled and int(sleeve_relax_multiplier_bp) > 0 and PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED not in reason_codes:
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_REASON_CODES_MISSING_SLEEVE_RELAXATION")
    if enabled and int(portfolio_relax_multiplier_bp) > 0 and PAPER_DISCOVERY_MIN_EXECUTABLE_HEADROOM_ALLOWED not in reason_codes:
        raise SystemExit("FAIL: PAPER_DISCOVERY_MODE_REASON_CODES_MISSING_MIN_EXECUTABLE_HEADROOM")
    return {
        "policy_id": policy_id,
        "enabled": enabled,
        "environments": environments,
        "activation_triggers": activation_triggers,
        "min_prior_edge_snapshot_days_for_edge_governed": int(min_prior_days),
        "discovery_headroom_multiplier_bp": int(multiplier_bp),
        "max_sleeve_headroom_cents": int(max_sleeve_headroom_cents),
        "discovery_sleeve_governance_multiplier_bp": int(sleeve_relax_multiplier_bp),
        "discovery_portfolio_governance_multiplier_bp": int(portfolio_relax_multiplier_bp),
        "minimum_executable_trade_risk_cents": int(minimum_executable_trade_risk_cents),
        "reason_codes": reason_codes,
    }


def _prior_edge_snapshot_day_count(*, truth_root: Path, sleeve_id: str, day_utc: str) -> int:
    root = (truth_root / "reports" / "sleeve_edge_snapshot_v1").resolve()
    if not root.exists() or not root.is_dir():
        return 0
    days: set[str] = set()
    for day_dir in root.iterdir():
        if (not day_dir.is_dir()) or (len(day_dir.name) != 10) or (day_dir.name[4] != "-") or (day_dir.name[7] != "-"):
            continue
        if day_dir.name >= day_utc:
            continue
        sleeve_root = (day_dir / sleeve_id).resolve()
        if not sleeve_root.exists() or not sleeve_root.is_dir():
            continue
        if any(sleeve_root.glob("*/sleeve_edge_snapshot.v1.json")):
            days.add(day_dir.name)
    return len(days)


def _paper_discovery_activation(
    *,
    policy: Dict[str, Any],
    environment: str,
    trigger: str,
    prior_edge_snapshot_days: int,
) -> Dict[str, Any]:
    env = str(environment or "").strip().upper()
    normalized_trigger = str(trigger or "").strip().upper()
    if not bool(policy.get("enabled") is True):
        return {"active": False}
    if env not in {str(item).strip().upper() for item in (policy.get("environments") or []) if str(item).strip()}:
        return {"active": False}
    if normalized_trigger not in {
        str(item).strip().upper()
        for item in (policy.get("activation_triggers") or [])
        if str(item).strip()
    }:
        return {"active": False}
    min_prior = int(policy.get("min_prior_edge_snapshot_days_for_edge_governed") or 0)
    if int(prior_edge_snapshot_days) >= min_prior:
        return {"active": False}
    return {
        "active": True,
        "policy_id": str(policy.get("policy_id") or "").strip(),
        "headroom_multiplier_bp": int(policy.get("discovery_headroom_multiplier_bp") or 0),
        "max_sleeve_headroom_cents": int(policy.get("max_sleeve_headroom_cents") or 0),
        "sleeve_governance_multiplier_bp": int(policy.get("discovery_sleeve_governance_multiplier_bp") or 0),
        "portfolio_governance_multiplier_bp": int(policy.get("discovery_portfolio_governance_multiplier_bp") or 0),
        "minimum_executable_trade_risk_cents": int(policy.get("minimum_executable_trade_risk_cents") or 0),
        "reason_codes": _dedupe_reason_codes(policy.get("reason_codes") or []),
    }


def _effective_sleeve_governance_multiplier_bp(
    *,
    governed_multiplier_bp: int,
    environment: str,
    qualification_meta: Dict[str, Any],
) -> Tuple[int, List[str]]:
    effective_multiplier_bp = int(governed_multiplier_bp)
    reason_codes: List[str] = []
    env = str(environment or "").strip().upper()
    discovery_mode_active = bool(qualification_meta.get("discovery_mode_active") is True)
    discovery_override_multiplier_bp = int(qualification_meta.get("discovery_sleeve_governance_multiplier_bp") or 0)
    missing_or_invalid_action = str(qualification_meta.get("governance_artifact_status") or "").strip().upper() in {
        "MISSING",
        "INVALID",
    }
    if discovery_mode_active and env == "PAPER" and discovery_override_multiplier_bp > 0 and not missing_or_invalid_action:
        effective_multiplier_bp = int(discovery_override_multiplier_bp)
        reason_codes = [PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED]
    return effective_multiplier_bp, reason_codes


def _effective_portfolio_governance_multiplier_bp(
    *,
    governed_multiplier_bp: int,
    environment: str,
    discovery_mode_active: bool,
    discovery_override_multiplier_bp: int,
) -> Tuple[int, List[str]]:
    effective_multiplier_bp = int(governed_multiplier_bp)
    reason_codes: List[str] = []
    env = str(environment or "").strip().upper()
    if discovery_mode_active and env == "PAPER" and int(discovery_override_multiplier_bp) > 0:
        effective_multiplier_bp = max(effective_multiplier_bp, int(discovery_override_multiplier_bp))
        if effective_multiplier_bp != int(governed_multiplier_bp):
            reason_codes = [PAPER_DISCOVERY_MIN_EXECUTABLE_HEADROOM_ALLOWED]
    return effective_multiplier_bp, reason_codes


def _prior_day_utc(day_utc: str) -> str:
    return (date.fromisoformat(day_utc) - timedelta(days=1)).isoformat()


def _target_day_admission_path(truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "target_day_admission_v1" / f"{day_utc}.json").resolve()


def _load_target_day_bootstrap_admission(truth_root: Path, day_utc: str) -> Dict[str, Any] | None:
    path = _target_day_admission_path(truth_root, day_utc)
    if not path.exists() or not path.is_file():
        return None
    payload = _read_json_obj(path)
    mode = str(payload.get("mode") or "").strip().upper()
    admission_status = str(payload.get("admission_status") or "").strip().upper()
    if mode != PAPER_BOOTSTRAP_MODE or admission_status != "ADMIT":
        return None
    return {
        "path": str(path),
        "sha256": _sha256_file(path),
    }


def _previous_day_economic_state_complete(truth_root: Path, day_utc: str) -> bool:
    prev_day_utc = _prior_day_utc(day_utc)
    prev_root = (truth_root / "reports" / ECONOMIC_BUILD_FAMILY / prev_day_utc).resolve()
    if not prev_root.exists() or not prev_root.is_dir():
        return False
    for candidate in sorted(prev_root.glob("*/economic_state_build.v1.json")):
        payload = _read_optional_json_obj(candidate)
        if not isinstance(payload, dict):
            continue
        if str(payload.get("closure_status") or "").strip().upper() == "COMPLETE":
            return True
    return False


def _read_optional_json_obj(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    return obj


def _load_previous_day_economic_reallocation_state(truth_root: Path, day_utc: str) -> Dict[str, Any]:
    prev_day_utc = _prior_day_utc(day_utc)
    prev_root = (truth_root / "reports" / ECONOMIC_BUILD_FAMILY / prev_day_utc).resolve()
    unknown = {
        "status": "UNKNOWN",
        "source_day_utc": prev_day_utc,
        "artifact_path": "",
        "artifact_sha256": "",
        "signals_by_sleeve": {},
        "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_STATE_MISSING"],
    }
    if not prev_root.exists() or not prev_root.is_dir():
        return dict(unknown)

    candidates = sorted(prev_root.glob("*/economic_state_build.v1.json"))
    if not candidates:
        return dict(unknown)
    if len(candidates) != 1:
        raise SystemExit(
            "FAIL: BUNDLE_C_PREVIOUS_DAY_ECONOMIC_STATE_AMBIGUOUS:"
            f"day_utc={prev_day_utc}:count={len(candidates)}"
        )

    path = candidates[0].resolve()
    sha256 = _sha256_file(path)
    try:
        build_obj = _read_json_obj(path)
    except SystemExit:
        return {
            "status": "UNKNOWN",
            "source_day_utc": prev_day_utc,
            "artifact_path": str(path),
            "artifact_sha256": sha256,
            "signals_by_sleeve": {},
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_STATE_UNREADABLE"],
        }

    if str(build_obj.get("schema_id") or "").strip() != "economic_state_build":
        return {
            "status": "UNKNOWN",
            "source_day_utc": prev_day_utc,
            "artifact_path": str(path),
            "artifact_sha256": sha256,
            "signals_by_sleeve": {},
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_STATE_SCHEMA_INVALID"],
        }
    if str(build_obj.get("day_utc") or "").strip() != prev_day_utc:
        return {
            "status": "UNKNOWN",
            "source_day_utc": prev_day_utc,
            "artifact_path": str(path),
            "artifact_sha256": sha256,
            "signals_by_sleeve": {},
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_STATE_DAY_MISMATCH"],
        }
    if str(build_obj.get("closure_status") or "").strip().upper() != "COMPLETE":
        return {
            "status": "UNKNOWN",
            "source_day_utc": prev_day_utc,
            "artifact_path": str(path),
            "artifact_sha256": sha256,
            "signals_by_sleeve": {},
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_STATE_NOT_COMPLETE"],
        }

    evaluation = build_obj.get("economic_evaluation")
    if not isinstance(evaluation, dict):
        return {
            "status": "UNKNOWN",
            "source_day_utc": prev_day_utc,
            "artifact_path": str(path),
            "artifact_sha256": sha256,
            "signals_by_sleeve": {},
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_EVALUATION_MISSING"],
        }

    signal_state = evaluation.get("reallocation_signal_state")
    if not isinstance(signal_state, dict):
        return {
            "status": "UNKNOWN",
            "source_day_utc": prev_day_utc,
            "artifact_path": str(path),
            "artifact_sha256": sha256,
            "signals_by_sleeve": {},
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_REALLOCATION_SIGNAL_MISSING"],
        }

    signals_by_sleeve: Dict[str, Dict[str, Any]] = {}
    for row in signal_state.get("signals") or []:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or "").strip()
        signal = str(row.get("signal") or "UNKNOWN").strip().upper() or "UNKNOWN"
        if not sleeve_id:
            continue
        signals_by_sleeve[sleeve_id] = {
            "signal": signal,
            "reason_code": str(row.get("reason_code") or "").strip(),
            "observed_return": row.get("observed_return"),
            "portfolio_return": row.get("portfolio_return"),
            "capital_multiplier_bp": int(ECONOMIC_SIGNAL_MULTIPLIER_BP.get(signal, 10000)),
            "priority_rank": int(ECONOMIC_SIGNAL_PRIORITY.get(signal, ECONOMIC_SIGNAL_PRIORITY["UNKNOWN"])),
        }

    return {
        "status": "OK",
        "source_day_utc": prev_day_utc,
        "artifact_path": str(path),
        "artifact_sha256": sha256,
        "signals_by_sleeve": signals_by_sleeve,
        "reason_codes": [],
    }


def _atomic_write_replace_if_changed(path: Path, data: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    candidate_sha = _sha256_bytes(data)
    if path.exists():
        if not path.is_file():
            raise SystemExit(f"FAIL: TARGET_NOT_FILE: {str(path)}")
        existing = path.read_bytes()
        existing_sha = _sha256_bytes(existing)
        if existing == data:
            print(
                f"OK: CAPITAL_AUTHORITY_ALLOCATION_V1_WRITTEN path={path} "
                f"sha256={existing_sha} action=EXISTS_IDENTICAL"
            )
            return existing_sha
        tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
        tmp.write_bytes(data)
        fd = os.open(str(tmp), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(str(tmp), str(path))
        dfd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
        print(
            f"OK: CAPITAL_AUTHORITY_ALLOCATION_V1_WRITTEN path={path} "
            f"sha256={candidate_sha} action=REPLACED_STALE existing_sha={existing_sha}"
        )
        return candidate_sha
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    tmp.write_bytes(data)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp), str(path))
    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
    print(
        f"OK: CAPITAL_AUTHORITY_ALLOCATION_V1_WRITTEN path={path} "
        f"sha256={candidate_sha} action=WROTE"
    )
    return candidate_sha


def _git_sha_failclosed() -> str:
    """
    Deterministic git sha resolution without shelling out:
      - Read .git/HEAD
      - If it is a ref, read that ref file
      - Return the hash string (must be 7..40 lowercase hex in schema)
    """
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        s = out.decode("utf-8").strip()
    except Exception:
        # Clean runtime roots can be source-derived without .git metadata.
        s = "0" * 40
    if len(s) < 7:
        raise SystemExit(f"FAIL: GIT_SHA_INVALID_FAILCLOSED: {s!r}")
    return s


def EXPOSURE_NET_PATH(truth_root: Path, day: str) -> Path:
    return (truth_root / "risk_v1" / "exposure_net_v1" / day / "exposure_net.v1.json").resolve()


def ENVELOPE_V2_PATH(truth_root: Path, day: str) -> Path:
    return (truth_root / "reports" / "capital_risk_envelope_v2" / day / "capital_risk_envelope.v2.json").resolve()


def INTENTS_DAY_DIR(truth_root: Path, day: str) -> Path:
    return (truth_root / "intents_v1" / "snapshots" / day).resolve()

def CORRELATION_ENVELOPE_GATE_PATH(truth_root: Path, day: str) -> Path:
    return (truth_root / "reports" / "correlation_envelope_gate_v1" / day / "correlation_envelope_gate.v1.json").resolve()

@dataclass(frozen=True)
class SleeveLimit:
    sleeve_id: str
    display_name: str
    priority_rank: int
    engine_ids: List[str]
    max_capital_at_risk_cents: int


def _load_policy() -> Dict[str, Any]:
    return _read_json_obj(POLICY_PATH)


def _headroom_from_envelope_v2(truth_root: Path, day: str) -> int:
    p = ENVELOPE_V2_PATH(truth_root, day)
    env = _read_json_obj(p)
    envelope = env.get("envelope")
    if not isinstance(envelope, dict):
        raise SystemExit("FAIL: ENVELOPE_V2_MISSING_envelope_OBJECT")
    headroom = envelope.get("headroom_cents")
    if not isinstance(headroom, int):
        raise SystemExit("FAIL: ENVELOPE_V2_MISSING_headroom_cents_INT")
    return int(headroom)


def _status_is_passing(status: Any) -> bool:
    value = str(status or "").strip().upper()
    return value in {"PASS", "OK", "BOOTSTRAP_PASS", "AUTHORIZED"}


def _unique_reason_codes(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _control_rows_by_sleeve(control_state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in control_state.get("sleeve_controls") or []:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("scope_id") or "").strip()
        if sleeve_id:
            out[sleeve_id] = row
    return out


def _decimal_usd_to_cents(value: Any, *, field_name: str) -> int:
    try:
        dec = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise SystemExit(f"FAIL: INVALID_DECIMAL_{field_name}: {value!r}") from exc
    if dec <= 0:
        raise SystemExit(f"FAIL: NONPOSITIVE_DECIMAL_{field_name}: {value!r}")
    cents = (dec * Decimal("100")).to_integral_value(rounding=ROUND_CEILING)
    return int(cents)


def _positions_snapshot_path(truth_root: Path, day: str) -> Path:
    return (truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json").resolve()


def _load_positions_snapshot(truth_root: Path, day: str) -> Tuple[Path, Dict[str, Any], str]:
    path = _positions_snapshot_path(truth_root, day)
    if not path.exists():
        raise SystemExit(f"FAIL: POSITIONS_SNAPSHOT_V5_MISSING: {path}")
    obj = _read_json_obj(path)
    validate_against_repo_schema_v1(obj, REPO_ROOT, POSITIONS_SNAPSHOT_SCHEMA_RELPATH)
    return path, obj, _sha256_file(path)


def _fraction_text(value: Decimal) -> str:
    return f"{value.quantize(WEIGHT_QUANT, rounding=ROUND_HALF_UP):.6f}"


def _fraction_from_any(value: Any, *, default: str = "0") -> Decimal:
    text = str(value if value is not None else default).strip() or default
    try:
        dec = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise SystemExit(f"FAIL: INVALID_DECIMAL_FRACTION:{value!r}") from exc
    return dec.quantize(WEIGHT_QUANT, rounding=ROUND_HALF_UP)


def _position_value_basis_cents(item: Dict[str, Any]) -> int:
    qty = abs(int(item.get("qty") or 0))
    avg_cost_cents = int(item.get("avg_cost_cents") or 0)
    return qty * avg_cost_cents


def _portfolio_nav_basis_cents(positions_obj: Dict[str, Any]) -> int:
    accounts = positions_obj.get("accounts")
    if not isinstance(accounts, list) or not accounts:
        raise SystemExit("FAIL: POSITIONS_SNAPSHOT_ACCOUNTS_INVALID_OR_EMPTY")
    cash_total_cents = sum(int((account or {}).get("cash_total_cents") or 0) for account in accounts if isinstance(account, dict))
    items = positions_obj.get("items")
    if not isinstance(items, list):
        raise SystemExit("FAIL: POSITIONS_SNAPSHOT_ITEMS_NOT_LIST")
    deployed_cents = sum(_position_value_basis_cents(item) for item in items if isinstance(item, dict) and int(item.get("qty") or 0) != 0)
    nav_basis = int(cash_total_cents) + int(deployed_cents)
    return int(max(nav_basis, 0))


def _resolve_intent_symbol(intent_obj: Dict[str, Any]) -> str:
    underlying = intent_obj.get("underlying")
    if isinstance(underlying, dict):
        symbol = str(underlying.get("symbol") or "").strip().upper()
        if symbol:
            return symbol
    return str(underlying or "").strip().upper()


def _resolve_intent_target_notional_pct(intent_obj: Dict[str, Any]) -> Optional[Decimal]:
    raw = intent_obj.get("target_notional_pct")
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return Decimal(text).quantize(WEIGHT_QUANT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None


def _load_execution_bindings() -> Tuple[List[ExecutionBinding], str, str]:
    sleeve_registry = _read_json_obj(SLEEVE_REGISTRY_PATH)
    account_registry = _read_json_obj(ACCOUNT_REGISTRY_PATH)
    sleeve_registry_sha = _sha256_file(SLEEVE_REGISTRY_PATH)
    account_registry_sha = _sha256_file(ACCOUNT_REGISTRY_PATH)

    accounts_raw = account_registry.get("accounts")
    if not isinstance(accounts_raw, list) or not accounts_raw:
        raise SystemExit("FAIL: ACCOUNT_REGISTRY_ACCOUNTS_INVALID_OR_EMPTY")
    accounts_by_id: Dict[str, Dict[str, Any]] = {}
    for row in accounts_raw:
        if not isinstance(row, dict):
            continue
        account_id = str(row.get("account_id") or "").strip()
        if account_id:
            accounts_by_id[account_id] = row

    sleeves_raw = sleeve_registry.get("sleeves")
    if not isinstance(sleeves_raw, list) or not sleeves_raw:
        raise SystemExit("FAIL: SLEEVE_REGISTRY_SLEEVES_INVALID_OR_EMPTY")

    bindings: List[ExecutionBinding] = []
    for sleeve in sleeves_raw:
        if not isinstance(sleeve, dict):
            continue
        account_id = str(sleeve.get("ib_account") or "").strip()
        if not account_id:
            raise SystemExit("FAIL: SLEEVE_REGISTRY_ACCOUNT_ID_MISSING")
        account_row = accounts_by_id.get(account_id)
        if not isinstance(account_row, dict):
            raise SystemExit(f"FAIL: ACCOUNT_REGISTRY_ENTRY_MISSING_FOR_SLEEVE_ACCOUNT:{account_id}")
        bindings.append(
            ExecutionBinding(
                execution_sleeve_id=str(sleeve.get("sleeve_id") or "").strip(),
                mode=str(sleeve.get("mode") or "").strip().upper(),
                enabled=bool(sleeve.get("enabled") is True),
                account_id=account_id,
                allowed_engine_ids=tuple(
                    sorted(
                        str(value).strip()
                        for value in (account_row.get("allowed_engine_ids") or [])
                        if str(value).strip()
                    )
                ),
                allowed_execution_sleeve_ids=tuple(
                    sorted(
                        str(value).strip()
                        for value in (account_row.get("allowed_sleeve_ids") or [])
                        if str(value).strip()
                    )
                ),
            )
        )
    bindings.sort(key=lambda row: (row.execution_sleeve_id, row.account_id))
    return bindings, sleeve_registry_sha, account_registry_sha


def _resolve_execution_binding(
    *,
    bindings: List[ExecutionBinding],
    engine_id: str,
    requested_mode: str,
) -> Tuple[Optional[ExecutionBinding], List[str]]:
    matching_mode = [
        binding for binding in bindings
        if binding.mode == requested_mode and engine_id in binding.allowed_engine_ids
    ]
    if not matching_mode:
        return None, ["BUNDLE_B_NO_EXECUTION_BINDING_FOR_ENGINE_MODE"]
    allowed = [
        binding for binding in matching_mode
        if (not binding.allowed_execution_sleeve_ids) or binding.execution_sleeve_id in binding.allowed_execution_sleeve_ids
    ]
    if not allowed:
        return None, ["BUNDLE_B_EXECUTION_SLEEVE_NOT_ALLOWED_FOR_ACCOUNT"]
    allowed.sort(key=lambda row: (row.execution_sleeve_id, row.account_id))
    binding = allowed[0]
    reason_codes: List[str] = []
    if len(allowed) > 1:
        reason_codes.append("BUNDLE_B_MULTIPLE_EXECUTION_BINDINGS_DETERMINISTIC_FIRST")
    return binding, reason_codes


def _positions_by_symbol_account(positions_obj: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    items = positions_obj.get("items")
    if not isinstance(items, list):
        raise SystemExit("FAIL: POSITIONS_SNAPSHOT_ITEMS_INVALID")
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        instrument = item.get("instrument") if isinstance(item.get("instrument"), dict) else {}
        symbol = str(instrument.get("symbol") or instrument.get("underlying") or "").strip().upper()
        account_id = str(item.get("account_id") or "").strip()
        if not symbol or not account_id:
            continue
        out[(account_id, symbol)] = item
    return out


def _strategy_sleeve_actuals(
    *,
    positions_obj: Dict[str, Any],
    nav_basis_cents: int,
    engine_to_sleeve: Dict[str, str],
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    items = positions_obj.get("items")
    if not isinstance(items, list):
        return result
    denominator = Decimal(str(max(nav_basis_cents, 1)))
    for item in items:
        if not isinstance(item, dict):
            continue
        qty = int(item.get("qty") or 0)
        if qty == 0:
            continue
        engine_id = str(item.get("engine_id") or "").strip()
        sleeve_id = engine_to_sleeve.get(engine_id, "UNASSIGNED_IMPORTED")
        value_cents = _position_value_basis_cents(item)
        entry = result.setdefault(
            sleeve_id,
            {
                "strategy_sleeve_id": sleeve_id,
                "actual_value_cents": 0,
                "actual_notional_pct": Decimal("0.000000"),
            },
        )
        entry["actual_value_cents"] = int(entry["actual_value_cents"]) + int(value_cents)
    for entry in result.values():
        entry["actual_notional_pct"] = (
            Decimal(str(entry["actual_value_cents"])) / denominator
        ).quantize(WEIGHT_QUANT, rounding=ROUND_HALF_UP)
    return result


def _infer_action_type(*, current_qty: int, target_pct: Optional[Decimal], actual_pct: Decimal) -> str:
    if target_pct is None:
        return ACTION_OPEN if current_qty == 0 else ACTION_ADD
    if target_pct <= Decimal("0"):
        return ACTION_CLOSE if current_qty != 0 else ACTION_HOLD
    if current_qty == 0:
        return ACTION_OPEN
    if target_pct > actual_pct:
        return ACTION_ADD
    if target_pct < actual_pct:
        return ACTION_REDUCE
    return ACTION_HOLD


def _requested_reduce_quantity(*, current_qty_abs: int, target_pct: Decimal, actual_pct: Decimal) -> int:
    if current_qty_abs <= 0:
        return 0
    if actual_pct <= 0:
        return current_qty_abs
    delta_pct = actual_pct - target_pct
    if delta_pct <= 0:
        return 0
    requested = int((Decimal(str(current_qty_abs)) * delta_pct / actual_pct).to_integral_value(rounding=ROUND_DOWN))
    if requested <= 0:
        requested = 1
    return int(min(requested, current_qty_abs))


def _outcome_priority(outcome: str) -> int:
    return {
        OUTCOME_BLOCKED: 0,
        OUTCOME_REJECTED: 1,
        OUTCOME_RESIZED: 2,
        OUTCOME_APPROVED: 3,
    }.get(str(outcome or "").strip().upper(), -1)


def _latest_market_close_cents(*, truth_root: Optional[Path], day_utc: str, symbol: str) -> Optional[int]:
    if truth_root is None:
        return None
    day = str(day_utc or "").strip()
    sym = str(symbol or "").strip().upper()
    if not day or not sym:
        return None
    snapshot_path = (
        Path(truth_root)
        / "market_data_snapshot_v1"
        / "snapshots"
        / day
        / f"{sym}.market_data_snapshot.v1.json"
    ).resolve()
    if snapshot_path.exists() and snapshot_path.is_file():
        payload = _read_json_obj(snapshot_path)
        if str(payload.get("day_utc") or "").strip() != day:
            return None
        if str(payload.get("symbol") or "").strip().upper() != sym:
            return None
        close = payload.get("close")
        if close is not None:
            cents = (Decimal(str(close).strip()) * Decimal("100")).to_integral_value(rounding=ROUND_CEILING)
            return int(cents) if cents > 0 else None
    jsonl_path = (Path(truth_root) / "market_data_snapshot_v1" / sym / f"{day[:4]}.jsonl").resolve()
    if not jsonl_path.exists() or not jsonl_path.is_file():
        return None
    cutoff = f"{day}T23:59:59Z"
    latest_close: Optional[Decimal] = None
    try:
        for line in jsonl_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            ts = str(row.get("timestamp_utc") or "").strip()
            if not ts or ts > cutoff:
                continue
            close = row.get("close")
            if close is None:
                continue
            latest_close = Decimal(str(close).strip())
    except Exception:
        return None
    if latest_close is None or latest_close <= 0:
        return None
    return int((latest_close * Decimal("100")).to_integral_value(rounding=ROUND_CEILING))


def _long_equity_stop_risk_per_unit_cents(
    intent_obj: Dict[str, Any],
    *,
    day_utc: str,
    truth_root: Optional[Path],
    stop_loss_bps: Any,
) -> Optional[int]:
    try:
        stop_bps = int(stop_loss_bps)
    except (TypeError, ValueError):
        return None
    if stop_bps <= 0:
        return None
    underlying = intent_obj.get("underlying") if isinstance(intent_obj.get("underlying"), dict) else {}
    symbol = str(underlying.get("symbol") or "").strip().upper()
    close_cents = _latest_market_close_cents(truth_root=truth_root, day_utc=day_utc, symbol=symbol)
    if close_cents is None or close_cents <= 0:
        return None
    risk_per_unit = (Decimal(close_cents) * Decimal(stop_bps) / Decimal("10000")).to_integral_value(rounding=ROUND_CEILING)
    return int(risk_per_unit) if risk_per_unit > 0 else None


def _extract_quantity_and_risk_per_unit_cents(
    intent_obj: Dict[str, Any],
    *,
    nav_total_cents: int,
    day_utc: str,
    intent_hash: str,
    truth_root: Optional[Path],
    environment: str,
) -> Optional[Tuple[int, int]]:
    schema_id = str(intent_obj.get("schema_id") or "").strip()
    schema_version = str(intent_obj.get("schema_version") or "").strip()
    if schema_id == "options_intent" and schema_version == "v2":
        risk = intent_obj.get("risk")
        if not isinstance(risk, dict):
            return None
        max_contracts = risk.get("max_contracts")
        if not isinstance(max_contracts, int) or max_contracts <= 0:
            return None
        max_risk_cents = _decimal_usd_to_cents(risk.get("max_risk_usd"), field_name="MAX_RISK_USD")
        risk_per_unit_cents = (max_risk_cents + max_contracts - 1) // max_contracts
        if risk_per_unit_cents <= 0:
            return None
        return int(max_contracts), int(risk_per_unit_cents)
    if schema_id == "equity_intent" and schema_version == "v1":
        sizing = intent_obj.get("sizing")
        if not isinstance(sizing, dict):
            return None
        target_notional_pct = Decimal(str(sizing.get("target_notional_pct") or "").strip())
        max_risk_pct = Decimal(str(sizing.get("max_risk_pct") or "").strip())
        if target_notional_pct <= 0 or max_risk_pct <= 0:
            return None
        if nav_total_cents <= 0:
            return None
        risk_per_unit_cents = int((Decimal(nav_total_cents) * max_risk_pct).to_integral_value(rounding=ROUND_CEILING))
        if risk_per_unit_cents <= 0:
            return None
        # EquityIntent v1 does not carry a deterministic share count. Authorize at most one unit when caps are positive.
        return 1, risk_per_unit_cents
    if schema_id == "exposure_intent" and schema_version == "v1":
        exposure_type = str(intent_obj.get("exposure_type") or "").strip().upper()
        if exposure_type == "SHORT_VOL_DEFINED":
            return _extract_short_vol_defined_risk_from_phasec(
                truth_root=truth_root,
                day_utc=day_utc,
                intent_hash=intent_hash,
                environment=environment,
            )
        if exposure_type != "LONG_EQUITY":
            return None
        constraints = intent_obj.get("constraints")
        if not isinstance(constraints, dict):
            return None
        target_notional_pct = Decimal(str(intent_obj.get("target_notional_pct") or "").strip())
        max_risk_pct = Decimal(str(constraints.get("max_risk_pct") or "").strip())
        if target_notional_pct <= 0 or max_risk_pct <= 0:
            return None
        if nav_total_cents <= 0:
            return None
        stop_loss_bps = constraints.get("stop_loss_bps")
        if stop_loss_bps is not None:
            risk_per_unit_cents = _long_equity_stop_risk_per_unit_cents(
                intent_obj,
                day_utc=day_utc,
                truth_root=truth_root,
                stop_loss_bps=stop_loss_bps,
            )
            if risk_per_unit_cents is None:
                return None
            return 1, int(risk_per_unit_cents)
        risk_per_unit_cents = int((Decimal(nav_total_cents) * max_risk_pct).to_integral_value(rounding=ROUND_CEILING))
        if risk_per_unit_cents <= 0:
            return None
        # ExposureIntent v1 is pre-transform sizing; keep authorization conservative at one unit.
        return 1, risk_per_unit_cents
    return None


def _specific_unproven_requested_quantity_reason_codes(
    intent_obj: Dict[str, Any],
    *,
    nav_total_cents: int,
    day_utc: str = "",
    intent_hash: str = "",
    truth_root: Optional[Path] = None,
    environment: str = "",
) -> List[str]:
    schema_id = str(intent_obj.get("schema_id") or "").strip()
    schema_version = str(intent_obj.get("schema_version") or "").strip()
    codes: List[str] = []

    if schema_id == "options_intent" and schema_version == "v2":
        risk = intent_obj.get("risk")
        if not isinstance(risk, dict):
            return ["AUTHZ_MISSING_SIZING_INPUT"]
        max_contracts = risk.get("max_contracts")
        if not isinstance(max_contracts, int) or max_contracts <= 0:
            return ["AUTHZ_INTENT_MISSING_EXECUTABLE_CONTRACT"]
        try:
            max_risk_cents = _decimal_usd_to_cents(risk.get("max_risk_usd"), field_name="MAX_RISK_USD")
        except SystemExit:
            return ["AUTHZ_MISSING_RISK_PER_UNIT"]
        risk_per_unit_cents = (max_risk_cents + max_contracts - 1) // max_contracts
        if risk_per_unit_cents <= 0:
            return ["AUTHZ_MISSING_RISK_PER_UNIT"]
        return []

    if schema_id == "equity_intent" and schema_version == "v1":
        sizing = intent_obj.get("sizing")
        if not isinstance(sizing, dict):
            return ["AUTHZ_MISSING_SIZING_INPUT"]
        try:
            target_notional_pct = Decimal(str(sizing.get("target_notional_pct") or "").strip())
            max_risk_pct = Decimal(str(sizing.get("max_risk_pct") or "").strip())
        except (InvalidOperation, ValueError):
            return ["AUTHZ_MISSING_SIZING_INPUT"]
        if target_notional_pct <= 0:
            codes.append("AUTHZ_INTENT_NOT_EXECUTABLE")
        if max_risk_pct <= 0:
            codes.append("AUTHZ_MISSING_RISK_PER_UNIT")
        if nav_total_cents <= 0:
            codes.append("AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS")
        if (not codes) and int((Decimal(nav_total_cents) * max_risk_pct).to_integral_value(rounding=ROUND_CEILING)) <= 0:
            codes.append("AUTHZ_MISSING_RISK_PER_UNIT")
        return _unique_reason_codes(codes)

    if schema_id == "exposure_intent" and schema_version == "v1":
        exposure_type = str(intent_obj.get("exposure_type") or "").strip().upper()
        if exposure_type == "SHORT_VOL_DEFINED":
            sizing = _extract_short_vol_defined_risk_from_phasec(
                truth_root=truth_root,
                day_utc=day_utc,
                intent_hash=intent_hash,
                environment=environment,
            )
            if sizing is not None:
                return []
            return ["AUTHZ_MISSING_DEFINED_RISK_EVIDENCE"]
        if exposure_type != "LONG_EQUITY":
            return ["AUTHZ_INTENT_NOT_EXECUTABLE"]
        constraints = intent_obj.get("constraints")
        if not isinstance(constraints, dict):
            return ["AUTHZ_MISSING_SIZING_INPUT"]
        try:
            target_notional_pct = Decimal(str(intent_obj.get("target_notional_pct") or "").strip())
            max_risk_pct = Decimal(str(constraints.get("max_risk_pct") or "").strip())
        except (InvalidOperation, ValueError):
            return ["AUTHZ_MISSING_SIZING_INPUT"]
        if target_notional_pct <= 0:
            codes.append("AUTHZ_INTENT_NOT_EXECUTABLE")
        if max_risk_pct <= 0:
            codes.append("AUTHZ_MISSING_RISK_PER_UNIT")
        if nav_total_cents <= 0:
            codes.append("AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS")
        if constraints.get("stop_loss_bps") is not None:
            stop_risk = _long_equity_stop_risk_per_unit_cents(
                intent_obj,
                day_utc=day_utc,
                truth_root=truth_root,
                stop_loss_bps=constraints.get("stop_loss_bps"),
            )
            if stop_risk is None:
                codes.append("AUTHZ_MISSING_EQUITY_STOP_RISK_EVIDENCE")
        if (not codes) and int((Decimal(nav_total_cents) * max_risk_pct).to_integral_value(rounding=ROUND_CEILING)) <= 0:
            codes.append("AUTHZ_MISSING_RISK_PER_UNIT")
        return _unique_reason_codes(codes)

    return ["AUTHZ_INTENT_NOT_EXECUTABLE"]


def _parse_sleeves(policy: Dict[str, Any]) -> List[SleeveLimit]:
    sleeves_raw = policy.get("sleeves")
    if not isinstance(sleeves_raw, list) or not sleeves_raw:
        raise SystemExit("FAIL: POLICY_SLEEVES_INVALID_OR_EMPTY")

    out: List[SleeveLimit] = []
    for s in sleeves_raw:
        if not isinstance(s, dict):
            continue
        sleeve_id = str(s.get("sleeve_id") or "").strip()
        if not sleeve_id:
            raise SystemExit("FAIL: POLICY_SLEEVE_ID_MISSING")
        display_name = str(s.get("display_name") or sleeve_id).strip() or sleeve_id
        pr = s.get("priority_rank")
        if not isinstance(pr, int):
            raise SystemExit(f"FAIL: POLICY_PRIORITY_RANK_NOT_INT sleeve_id={sleeve_id}")

        eids = s.get("engine_ids")
        if not isinstance(eids, list) or not eids:
            raise SystemExit(f"FAIL: POLICY_ENGINE_IDS_INVALID_OR_EMPTY sleeve_id={sleeve_id}")
        engine_ids = [str(x).strip() for x in eids if str(x).strip()]
        if not engine_ids:
            raise SystemExit(f"FAIL: POLICY_ENGINE_IDS_EMPTY_AFTER_STRIP sleeve_id={sleeve_id}")

        limits = s.get("limits")
        if not isinstance(limits, dict):
            raise SystemExit(f"FAIL: POLICY_LIMITS_MISSING_OR_INVALID sleeve_id={sleeve_id}")
        mcar = limits.get("max_capital_at_risk_cents")
        if not isinstance(mcar, int) or mcar < 0:
            raise SystemExit(f"FAIL: POLICY_MAX_CAPITAL_AT_RISK_CENTS_INVALID sleeve_id={sleeve_id}")

        out.append(
            SleeveLimit(
                sleeve_id=sleeve_id,
                display_name=display_name,
                priority_rank=int(pr),
                engine_ids=engine_ids,
                max_capital_at_risk_cents=int(mcar),
            )
        )

    if not out:
        raise SystemExit("FAIL: POLICY_SLEEVES_EMPTY_AFTER_PARSE")

    out.sort(key=lambda x: (x.priority_rank, x.sleeve_id))
    return out


def _build_engine_to_sleeve(sleeves: List[SleeveLimit]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for s in sleeves:
        for eid in s.engine_ids:
            m[eid] = s.sleeve_id
    return m


def _select_effective_intents(intents_dir: Path) -> List[Path]:
    # Same-day corrected snapshots may coexist with stale prior snapshots for the
    # same intent_id. Keep exactly one effective file per intent_id by selecting
    # the latest file mtime; break ties by lexicographically larger filename.
    by_intent_id: Dict[str, Tuple[int, str, Path]] = {}
    passthrough: List[Path] = []
    for p in sorted([p for p in intents_dir.iterdir() if p.is_file() and p.name.endswith(".json")], key=lambda p: p.name):
        try:
            obj = _read_json_obj(p)
        except Exception:
            passthrough.append(p)
            continue
        intent_id = str(obj.get("intent_id") or "").strip()
        if not intent_id:
            passthrough.append(p)
            continue
        stat = p.stat()
        candidate = (int(stat.st_mtime_ns), p.name, p)
        prior = by_intent_id.get(intent_id)
        if prior is None or candidate[:2] >= prior[:2]:
            by_intent_id[intent_id] = candidate
    return sorted(passthrough + [item[2] for item in by_intent_id.values()], key=lambda p: p.name)


def _intent_hash_from_snapshot_path(intent_path: Path) -> str:
    name = str(intent_path.name or "").strip()
    suffix = ".exposure_intent.v1.json"
    if name.endswith(suffix):
        candidate = name[: -len(suffix)].strip().lower()
        if len(candidate) == 64 and all(ch in "0123456789abcdef" for ch in candidate):
            return candidate
    return _sha256_file(intent_path)


def _latest_phasec_execution_identity_path(*, truth_root: Path, day_utc: str, intent_hash: str) -> Optional[Path]:
    normalized_intent_hash = str(intent_hash or "").strip().lower()
    if not normalized_intent_hash:
        return None
    candidate_roots: List[Path] = [truth_root.resolve()]
    sleeve_execution_root = (truth_root.resolve().parent / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    if sleeve_execution_root not in candidate_roots and sleeve_execution_root.exists() and sleeve_execution_root.is_dir():
        candidate_roots.append(sleeve_execution_root)
    candidates: List[Path] = []
    for base_root in candidate_roots:
        phasec_day_root = (base_root / "phaseC_preflight_v1" / day_utc).resolve()
        if not phasec_day_root.exists() or not phasec_day_root.is_dir():
            continue
        candidates.extend(phasec_day_root.glob(f"attempt_*/{normalized_intent_hash}/execution_identity_record.v1.json"))
    candidates = sorted(candidates, key=lambda p: (int(p.stat().st_mtime_ns), str(p)))
    if not candidates:
        return None
    return candidates[-1].resolve()


def _extract_short_vol_defined_risk_from_phasec(
    *,
    truth_root: Optional[Path],
    day_utc: str,
    intent_hash: str,
    environment: str,
) -> Optional[Tuple[int, int]]:
    if truth_root is None:
        return None
    identity_path = _latest_phasec_execution_identity_path(
        truth_root=truth_root,
        day_utc=day_utc,
        intent_hash=intent_hash,
    )
    if identity_path is None:
        return None
    identity_obj = _read_optional_json_obj(identity_path)
    if not isinstance(identity_obj, dict):
        return None
    if str(identity_obj.get("day_utc") or "").strip() != day_utc:
        return None
    identity_env = str(identity_obj.get("environment") or "").strip().upper()
    if identity_env and identity_env != str(environment or "").strip().upper():
        return None
    if str(identity_obj.get("intent_hash") or "").strip().lower() != str(intent_hash or "").strip().lower():
        return None
    order_plan_path: Optional[Path] = None
    for ref in identity_obj.get("source_refs") or []:
        if not isinstance(ref, dict):
            continue
        if str(ref.get("ref_type") or "").strip() != "order_plan_ref":
            continue
        raw_path = str(ref.get("path") or "").strip()
        if not raw_path:
            continue
        path = Path(raw_path).resolve()
        if path.exists() and path.is_file():
            order_plan_path = path
            break
    if order_plan_path is None:
        fallback = (identity_path.parent / "order_plan.v1.json").resolve()
        if fallback.exists() and fallback.is_file():
            order_plan_path = fallback
    if order_plan_path is None:
        return None
    order_plan_obj = _read_optional_json_obj(order_plan_path)
    if not isinstance(order_plan_obj, dict):
        return None
    risk_proof = order_plan_obj.get("risk_proof")
    if not isinstance(risk_proof, dict):
        return None
    if risk_proof.get("defined_risk_proven") is not True:
        return None
    contracts = risk_proof.get("contracts")
    if not isinstance(contracts, int) or contracts <= 0:
        return None
    max_loss_usd = risk_proof.get("max_loss_usd")
    if max_loss_usd is None:
        return None
    try:
        max_loss_cents = _decimal_usd_to_cents(max_loss_usd, field_name="MAX_DEFINED_LOSS_USD")
    except SystemExit:
        return None
    risk_per_unit_cents = (max_loss_cents + contracts - 1) // contracts
    if risk_per_unit_cents <= 0:
        return None
    return int(contracts), int(risk_per_unit_cents)


def _allocate_sleeve_headroom(
    portfolio_headroom_cents: int,
    sleeves: List[SleeveLimit],
    *,
    economic_signals_by_sleeve: Dict[str, Dict[str, Any]],
) -> Dict[str, int]:
    remaining = int(max(portfolio_headroom_cents, 0))
    allowed_by_sleeve: Dict[str, int] = {}
    ordered_sleeves = sorted(
        sleeves,
        key=lambda s: (
            int((economic_signals_by_sleeve.get(s.sleeve_id) or {}).get("priority_rank", ECONOMIC_SIGNAL_PRIORITY["UNKNOWN"])),
            s.priority_rank,
            s.sleeve_id,
        ),
    )
    for s in ordered_sleeves:
        cap = int(max(s.max_capital_at_risk_cents, 0))
        allow = min(cap, remaining)
        allowed_by_sleeve[s.sleeve_id] = int(allow)
        remaining -= int(allow)
    return allowed_by_sleeve


def _headroom_authorization_metrics(
    *,
    requested_quantity: int,
    risk_per_unit_cents: int,
    available_sleeve_headroom_cents: int,
    available_portfolio_headroom_cents: int,
) -> Dict[str, int]:
    requested_qty = int(max(requested_quantity, 0))
    risk_per_unit = int(max(risk_per_unit_cents, 0))
    sleeve_available = int(max(available_sleeve_headroom_cents, 0))
    portfolio_available = int(max(available_portfolio_headroom_cents, 0))
    available_headroom = int(min(sleeve_available, portfolio_available))
    required_risk = int(requested_qty * risk_per_unit) if requested_qty > 0 and risk_per_unit > 0 else 0
    if risk_per_unit <= 0:
        max_by_sleeve = 0
        max_by_portfolio = 0
        authorized_qty = 0
    else:
        max_by_sleeve = int(sleeve_available // risk_per_unit)
        max_by_portfolio = int(portfolio_available // risk_per_unit)
        authorized_qty = int(min(requested_qty, max_by_sleeve, max_by_portfolio))
    return {
        "requested_quantity": requested_qty,
        "risk_per_unit_cents": risk_per_unit,
        "required_risk_cents": required_risk,
        "available_sleeve_headroom_cents": sleeve_available,
        "available_portfolio_headroom_cents": portfolio_available,
        "available_headroom_cents": available_headroom,
        "max_by_sleeve": max_by_sleeve,
        "max_by_portfolio": max_by_portfolio,
        "authorized_quantity": authorized_qty,
    }


def _decision_chain_for_intent(
    *,
    intent_obj: Dict[str, Any],
    intent_path: Path,
    intent_sha: str,
    engine_to_sleeve: Dict[str, str],
    execution_bindings: List[ExecutionBinding],
    positions_lookup: Dict[Tuple[str, str], Dict[str, Any]],
    nav_basis_cents: int,
    nav_total_cents: int,
    day_utc: str,
    truth_root: Path,
    env_status_ok: bool,
    corr_status_ok: bool,
    remaining_by_strategy_sleeve: Dict[str, int],
    remaining_portfolio_headroom_cents_ref: Dict[str, int],
    economic_signals_by_sleeve: Dict[str, Dict[str, Any]],
    economic_signal_source_day_utc: str,
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], Dict[str, Any]]:
    engine = intent_obj.get("engine")
    if not isinstance(engine, dict):
        raise SystemExit(f"FAIL: INTENT_ENGINE_INVALID:{intent_path}")
    engine_id = str(engine.get("engine_id") or "").strip()
    requested_mode = str(engine.get("mode") or "PAPER").strip().upper() or "PAPER"
    intent_id = str(intent_obj.get("intent_id") or "").strip()
    if not engine_id or not intent_id:
        raise SystemExit(f"FAIL: INTENT_MISSING_ENGINE_OR_INTENT_ID: {intent_path}")

    strategy_sleeve_id = engine_to_sleeve.get(engine_id, "UNKNOWN_STRATEGY_SLEEVE")
    economic_signal_meta = dict(economic_signals_by_sleeve.get(strategy_sleeve_id) or {})
    economic_signal = str(economic_signal_meta.get("signal") or "UNKNOWN").strip().upper() or "UNKNOWN"
    economic_signal_reason_code = str(economic_signal_meta.get("reason_code") or "").strip()
    economic_signal_multiplier_bp = int(economic_signal_meta.get("capital_multiplier_bp") or 10000)
    binding, binding_reason_codes = _resolve_execution_binding(
        bindings=execution_bindings,
        engine_id=engine_id,
        requested_mode=requested_mode,
    )
    execution_sleeve_id = binding.execution_sleeve_id if binding is not None else ""
    account_id = binding.account_id if binding is not None else ""

    target_pct = _resolve_intent_target_notional_pct(intent_obj)
    symbol = _resolve_intent_symbol(intent_obj)
    current_position = positions_lookup.get((account_id, symbol)) if account_id and symbol else None
    current_qty = int((current_position or {}).get("qty") or 0)
    current_qty_abs = abs(current_qty)
    current_value_cents = _position_value_basis_cents(current_position or {})
    actual_pct = (
        (Decimal(str(current_value_cents)) / Decimal(str(max(nav_basis_cents, 1))))
        .quantize(WEIGHT_QUANT, rounding=ROUND_HALF_UP)
    )
    resolved_action = _infer_action_type(current_qty=current_qty, target_pct=target_pct, actual_pct=actual_pct)
    lifecycle_intent = {
        ACTION_OPEN: ACTION_OPEN,
        ACTION_ADD: ACTION_ADD,
        ACTION_REDUCE: ACTION_REDUCE,
        ACTION_CLOSE: ACTION_CLOSE,
        ACTION_HOLD: ACTION_HOLD,
    }[resolved_action]

    drift_pct = None if target_pct is None else (target_pct - actual_pct).quantize(WEIGHT_QUANT, rounding=ROUND_HALF_UP)
    candidate_scope = {
        "intent_hash": intent_sha,
        "engine_id": engine_id,
        "action_type": resolved_action,
        "account_id": account_id,
        "execution_sleeve_id": execution_sleeve_id,
        "position_id": str((current_position or {}).get("position_id") or ""),
    }
    candidate_id = _sha256_bytes(canonical_json_bytes_v1(candidate_scope))
    candidate_reason_codes = list(binding_reason_codes)

    if binding is None:
        candidate_reason_codes.append("BUNDLE_B_ACCOUNT_MAPPING_BLOCKED")
    elif not binding.enabled:
        candidate_reason_codes.append("BUNDLE_B_EXECUTION_SLEEVE_DISABLED")
    elif binding.mode == "INACTIVE":
        candidate_reason_codes.append("BUNDLE_B_EXECUTION_SLEEVE_INACTIVE")
    elif binding.mode != requested_mode:
        candidate_reason_codes.append("BUNDLE_B_EXECUTION_SLEEVE_MODE_MISMATCH")

    if resolved_action == ACTION_HOLD:
        if current_qty_abs == 0 and (target_pct is None or target_pct <= Decimal("0")):
            candidate_reason_codes.append("BUNDLE_B_NO_POSITION_FOR_EXIT_INTENT")
        else:
            candidate_reason_codes.append("BUNDLE_B_NO_ACTION_REQUIRED")
    elif resolved_action in {ACTION_OPEN, ACTION_ADD} and economic_signal == "DECREASE" and economic_signal_multiplier_bp < 10000:
        candidate_reason_codes.append("BUNDLE_C_REALLOCATION_SIGNAL_DECREASE_CAP_APPLIED")

    candidate_row = {
        "candidate_id": candidate_id,
        "intent_hash": intent_sha,
        "intent_id": intent_id,
        "engine_id": engine_id,
        "requested_mode": requested_mode,
        "strategy_sleeve_id": strategy_sleeve_id,
        "execution_sleeve_id": execution_sleeve_id,
        "account_id": account_id,
        "position_id": str((current_position or {}).get("position_id") or ""),
        "symbol": symbol,
        "action_type": resolved_action,
        "lifecycle_intent": lifecycle_intent,
        "target_notional_pct": None if target_pct is None else _fraction_text(target_pct),
        "actual_notional_pct": _fraction_text(actual_pct),
        "drift_notional_pct": None if drift_pct is None else _fraction_text(drift_pct),
        "economic_signal": economic_signal,
        "economic_signal_reason_code": economic_signal_reason_code,
        "economic_signal_source_day_utc": economic_signal_source_day_utc,
        "reason_codes": _unique_reason_codes(candidate_reason_codes),
    }

    if resolved_action == ACTION_HOLD:
        authorized_row = {
            "authorized_trade_intent_id": _sha256_bytes(canonical_json_bytes_v1({"candidate_id": candidate_id, "outcome": OUTCOME_REJECTED})),
            "trade_intent_id": "",
            "candidate_id": candidate_id,
            "intent_hash": intent_sha,
            "intent_id": intent_id,
            "engine_id": engine_id,
            "symbol": symbol,
            "strategy_sleeve_id": strategy_sleeve_id,
            "execution_sleeve_id": execution_sleeve_id,
            "account_id": account_id,
            "position_id": str((current_position or {}).get("position_id") or ""),
            "action_type": resolved_action,
            "lifecycle_intent": lifecycle_intent,
            "requested_quantity": 0,
            "requested_quantity_basis": "NO_ACTION",
            "authorized_quantity": 0,
            "authorization_outcome": OUTCOME_REJECTED,
            "reason_codes": _unique_reason_codes(candidate_reason_codes),
        }
        return candidate_row, None, authorized_row

    requested_quantity = 0
    requested_quantity_basis = ""
    risk_per_unit_cents = 0
    trade_reason_codes = list(candidate_reason_codes)
    if resolved_action in {ACTION_OPEN, ACTION_ADD}:
        sizing = _extract_quantity_and_risk_per_unit_cents(
            intent_obj,
            nav_total_cents=nav_total_cents,
            day_utc=day_utc,
            intent_hash=intent_sha,
            truth_root=truth_root,
            environment=requested_mode,
        )
        if sizing is None:
            trade_reason_codes.extend(
                _specific_unproven_requested_quantity_reason_codes(
                    intent_obj,
                    nav_total_cents=nav_total_cents,
                    day_utc=day_utc,
                    intent_hash=intent_sha,
                    truth_root=truth_root,
                    environment=requested_mode,
                )
            )
            trade_reason_codes.append("BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN")
            requested_quantity_basis = "UNPROVEN_INTENT_RISK_BUDGET"
        else:
            requested_quantity, risk_per_unit_cents = sizing
            requested_quantity_basis = "INTENT_RISK_BUDGET"
    elif resolved_action == ACTION_CLOSE:
        requested_quantity = current_qty_abs
        requested_quantity_basis = "CURRENT_POSITION_QTY"
    elif resolved_action == ACTION_REDUCE and target_pct is not None:
        requested_quantity = _requested_reduce_quantity(
            current_qty_abs=current_qty_abs,
            target_pct=target_pct,
            actual_pct=actual_pct,
        )
        requested_quantity_basis = "ACTUAL_TO_TARGET_DRIFT"

    trade_scope = {
        "candidate_id": candidate_id,
        "requested_quantity": requested_quantity,
        "action_type": resolved_action,
    }
    trade_intent_id = _sha256_bytes(canonical_json_bytes_v1(trade_scope))
    trade_intent = {
        "trade_intent_id": trade_intent_id,
        "candidate_id": candidate_id,
        "intent_hash": intent_sha,
        "intent_id": intent_id,
        "engine_id": engine_id,
        "symbol": symbol,
        "strategy_sleeve_id": strategy_sleeve_id,
        "execution_sleeve_id": execution_sleeve_id,
        "account_id": account_id,
        "position_id": str((current_position or {}).get("position_id") or ""),
        "action_type": resolved_action,
        "lifecycle_intent": lifecycle_intent,
        "requested_quantity": int(requested_quantity),
        "requested_quantity_basis": requested_quantity_basis,
        "target_notional_pct": None if target_pct is None else _fraction_text(target_pct),
        "actual_notional_pct": _fraction_text(actual_pct),
        "drift_notional_pct": None if drift_pct is None else _fraction_text(drift_pct),
        "economic_signal": economic_signal,
        "economic_signal_reason_code": economic_signal_reason_code,
        "economic_signal_source_day_utc": economic_signal_source_day_utc,
        "reason_codes": _unique_reason_codes(trade_reason_codes),
    }

    available_sleeve_headroom_cents = int(max(remaining_by_strategy_sleeve.get(strategy_sleeve_id, 0), 0))
    available_portfolio_headroom_cents = int(max(remaining_portfolio_headroom_cents_ref["value"], 0))
    headroom_metrics = _headroom_authorization_metrics(
        requested_quantity=int(requested_quantity),
        risk_per_unit_cents=int(risk_per_unit_cents),
        available_sleeve_headroom_cents=available_sleeve_headroom_cents,
        available_portfolio_headroom_cents=available_portfolio_headroom_cents,
    )

    authorized_quantity = 0
    authorization_outcome = OUTCOME_REJECTED
    authorization_reason_codes = list(trade_reason_codes)
    if binding is None or (binding is not None and (not binding.enabled or binding.mode == "INACTIVE" or binding.mode != requested_mode)):
        authorization_outcome = OUTCOME_BLOCKED
        authorization_reason_codes.append("BUNDLE_B_EXECUTION_BINDING_BLOCKED")
    elif requested_quantity <= 0:
        authorization_outcome = OUTCOME_REJECTED
        authorization_reason_codes.append("BUNDLE_B_REQUESTED_QUANTITY_ZERO")
    elif resolved_action in {ACTION_REDUCE, ACTION_CLOSE}:
        authorized_quantity = int(requested_quantity)
        authorization_outcome = OUTCOME_APPROVED
        authorization_reason_codes.append("BUNDLE_B_EXIT_ACTION_APPROVED")
    elif not env_status_ok or not corr_status_ok:
        authorization_outcome = OUTCOME_BLOCKED
        authorization_reason_codes.append("BUNDLE_B_GATE_BLOCKED")
    elif strategy_sleeve_id not in remaining_by_strategy_sleeve:
        authorization_outcome = OUTCOME_BLOCKED
        authorization_reason_codes.append("BUNDLE_B_STRATEGY_SLEEVE_UNKNOWN")
    elif risk_per_unit_cents <= 0:
        authorization_outcome = OUTCOME_REJECTED
        authorization_reason_codes.append("BUNDLE_B_RISK_PER_UNIT_UNPROVEN")
    else:
        authorized_quantity = int(headroom_metrics["authorized_quantity"])
        if authorized_quantity <= 0:
            authorization_outcome = OUTCOME_REJECTED
            authorization_reason_codes.append("BUNDLE_B_HEADROOM_REJECTED")
        else:
            used_cents = int(authorized_quantity * risk_per_unit_cents)
            remaining_by_strategy_sleeve[strategy_sleeve_id] = int(remaining_by_strategy_sleeve.get(strategy_sleeve_id, 0)) - used_cents
            remaining_portfolio_headroom_cents_ref["value"] = int(remaining_portfolio_headroom_cents_ref["value"]) - used_cents
            if resolved_action in {ACTION_OPEN, ACTION_ADD} and economic_signal == "DECREASE" and economic_signal_multiplier_bp < 10000:
                authorization_reason_codes.append("BUNDLE_C_REALLOCATION_SIGNAL_DECREASE_CAP_APPLIED")
            if authorized_quantity < requested_quantity:
                authorization_outcome = OUTCOME_RESIZED
                authorization_reason_codes.append("BUNDLE_B_RESIZED")
            else:
                authorization_outcome = OUTCOME_APPROVED
                authorization_reason_codes.append("BUNDLE_B_APPROVED")

    authorized_scope = {
        "trade_intent_id": trade_intent_id,
        "authorized_quantity": authorized_quantity,
        "authorization_outcome": authorization_outcome,
    }
    authorized_row = {
        "authorized_trade_intent_id": _sha256_bytes(canonical_json_bytes_v1(authorized_scope)),
        "trade_intent_id": trade_intent_id,
        "candidate_id": candidate_id,
        "intent_hash": intent_sha,
        "intent_id": intent_id,
        "engine_id": engine_id,
        "symbol": symbol,
        "strategy_sleeve_id": strategy_sleeve_id,
        "execution_sleeve_id": execution_sleeve_id,
        "account_id": account_id,
        "position_id": str((current_position or {}).get("position_id") or ""),
        "action_type": resolved_action,
        "lifecycle_intent": lifecycle_intent,
        "requested_quantity": int(requested_quantity),
        "requested_quantity_basis": requested_quantity_basis,
        "authorized_quantity": int(authorized_quantity),
        "authorization_outcome": authorization_outcome,
        "risk_per_unit_cents": int(headroom_metrics["risk_per_unit_cents"]),
        "required_risk_cents": int(headroom_metrics["required_risk_cents"]),
        "available_sleeve_headroom_cents": int(headroom_metrics["available_sleeve_headroom_cents"]),
        "available_portfolio_headroom_cents": int(headroom_metrics["available_portfolio_headroom_cents"]),
        "allowed_capital_at_risk_cents": int(headroom_metrics["available_headroom_cents"]),
        "headroom_cents": int(headroom_metrics["available_headroom_cents"]),
        "rejected_quantity": int(max(int(requested_quantity) - int(authorized_quantity), 0)),
        "economic_signal": economic_signal,
        "economic_signal_reason_code": economic_signal_reason_code,
        "economic_signal_source_day_utc": economic_signal_source_day_utc,
        "reason_codes": _unique_reason_codes(authorization_reason_codes),
    }
    return candidate_row, trade_intent, authorized_row


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_capital_authority_allocation_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=False, default=None)
    ap.add_argument("--canonical_sequence_owner", required=False, default="")
    ap.add_argument("--authority_verdict_path", required=False, default="")
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"
    truth_root = _resolve_truth_root(args.truth_root)
    canonical_truth_root = resolve_canonical_truth_root().resolve()
    canonical_sequence_owner = _require_canonical_sequence_owner(args.canonical_sequence_owner)
    authority_verdict_path = str(args.authority_verdict_path or "").strip()

    intents_dir = INTENTS_DAY_DIR(truth_root, day)
    if not intents_dir.exists() or not intents_dir.is_dir():
        raise SystemExit(f"FAIL: INTENTS_DIR_MISSING: {str(intents_dir)}")
    intents = _select_effective_intents(intents_dir)
    bootstrap_admission = _load_target_day_bootstrap_admission(canonical_truth_root, day)
    prior_day_complete = _previous_day_economic_state_complete(canonical_truth_root, day)
    bootstrap_zero_baseline = False

    # Fail-closed authority check:
    # Required when there is intent activity; no-intent/no-submission clean roots can still emit
    # deterministic no-activity allocation evidence without an authority-day head.
    if intents:
        try:
            if authority_verdict_path:
                _require_explicit_authority_verdict_pass_authoritative(day, authority_verdict_path)
            else:
                _require_authority_head_pass_authoritative(day, truth_root)
        except SystemExit as exc:
            detail = str(exc)
            if (
                bootstrap_admission is not None
                and not prior_day_complete
                and "NOT_EXECUTION_AUTHORIZED" in detail
            ):
                bootstrap_zero_baseline = True
                intents = []
            else:
                raise

    # Required inputs
    p_ex = EXPOSURE_NET_PATH(truth_root, day)
    exposure_present = p_ex.exists() and p_ex.is_file()
    if not exposure_present and not bootstrap_zero_baseline:
        raise SystemExit(f"FAIL: EXPOSURE_NET_MISSING: {str(p_ex)}")
    ex_sha = _sha256_file(p_ex) if exposure_present else ""

    envp = ENVELOPE_V2_PATH(truth_root, day)
    if not envp.exists():
        raise SystemExit(f"FAIL: ENVELOPE_V2_MISSING: {str(envp)}")
    env_sha = _sha256_file(envp)
    env_obj = _read_json_obj(envp)
    env_status_ok = _status_is_passing(env_obj.get("status"))

    cegp = CORRELATION_ENVELOPE_GATE_PATH(truth_root, day)
    if not cegp.exists():
        raise SystemExit(f"FAIL: CORRELATION_ENVELOPE_GATE_MISSING: {str(cegp)}")
    ceg_sha = _sha256_file(cegp)
    ceg_obj = _read_json_obj(cegp)
    caps = ceg_obj.get("caps")
    if not isinstance(caps, dict):
        raise SystemExit("FAIL: CORRELATION_ENVELOPE_GATE_MISSING_caps_OBJECT")
    mult = caps.get("multiplier_bp_by_sleeve")
    if not isinstance(mult, dict):
        raise SystemExit("FAIL: CORRELATION_ENVELOPE_GATE_MISSING_multiplier_bp_by_sleeve_OBJECT")
    corr_status_ok = _status_is_passing(ceg_obj.get("status"))

    policy = _load_policy()
    pol_sha = _sha256_file(POLICY_PATH)

    sleeves = _parse_sleeves(policy)
    engine_to_sleeve = _build_engine_to_sleeve(sleeves)
    governed_control_state = resolve_capital_authority_runtime_control_v1(
        truth_root=truth_root,
        day_utc=day,
        sleeve_ids=[s.sleeve_id for s in sleeves],
    )
    governed_control_by_sleeve = _control_rows_by_sleeve(governed_control_state)
    economic_reallocation_state = _load_previous_day_economic_reallocation_state(truth_root, day)
    economic_signals_by_sleeve = dict(economic_reallocation_state.get("signals_by_sleeve") or {})
    positions_path, positions_obj, positions_sha = _load_positions_snapshot(truth_root, day)
    positions_items = positions_obj.get("items")
    positions_count = len(positions_items) if isinstance(positions_items, list) else 0
    if bootstrap_zero_baseline and positions_count != 0:
        raise SystemExit("FAIL: PAPER_BOOTSTRAP_BASELINE_REQUIRES_ZERO_POSITIONS")
    nav_basis_cents = _portfolio_nav_basis_cents(positions_obj)
    positions_lookup = _positions_by_symbol_account(positions_obj)
    execution_bindings, sleeve_registry_sha, account_registry_sha = _load_execution_bindings()

    try:
        sleeve_edge_policy = load_sleeve_edge_policy_v1(REPO_ROOT)
    except ValueError as exc:
        raise SystemExit(f"FAIL: {exc}") from exc
    paper_discovery_policy = _load_paper_discovery_mode_policy()
    sleeve_edge_policy_sha = _sha256_file(SLEEVE_EDGE_POLICY_PATH)
    qualification_by_sleeve: Dict[str, Dict[str, Any]] = {}
    discovery_mode_active_for_day = False
    discovery_portfolio_environment = "PAPER"
    for sleeve_limit in sleeves:
        sid = sleeve_limit.sleeve_id
        governed_row = dict(governed_control_by_sleeve.get(sid) or {})
        sleeve_environment = str(governed_row.get("mode") or "PAPER").strip().upper() or "PAPER"
        if bootstrap_zero_baseline:
            qualification_meta = _bootstrap_zero_baseline_qualification_meta()
        else:
            qualification_meta = _load_sleeve_edge_allocator_meta(
                truth_root=truth_root,
                day_utc=day,
                sleeve_id=sid,
                sleeve_edge_policy=sleeve_edge_policy,
                canonical_sequence_owner=canonical_sequence_owner,
                environment=sleeve_environment,
                paper_discovery_policy=paper_discovery_policy,
            )
        qualification_by_sleeve[sid] = dict(qualification_meta)
        qualification_rc = set(str(code).strip() for code in list(qualification_meta.get("reason_codes") or []))
        if PAPER_DISCOVERY_MODE_ACTIVE in qualification_rc:
            discovery_mode_active_for_day = True
            discovery_portfolio_environment = sleeve_environment

    raw_portfolio_headroom_cents = _headroom_from_envelope_v2(truth_root, day)
    portfolio_multiplier_bp = int(governed_control_state.get("portfolio_headroom_multiplier_bp") or 0)
    discovery_portfolio_multiplier_bp = int(
        paper_discovery_policy.get("discovery_portfolio_governance_multiplier_bp") or 0
    )
    portfolio_multiplier_bp, portfolio_discovery_reason_codes = _effective_portfolio_governance_multiplier_bp(
        governed_multiplier_bp=portfolio_multiplier_bp,
        environment=discovery_portfolio_environment,
        discovery_mode_active=discovery_mode_active_for_day,
        discovery_override_multiplier_bp=discovery_portfolio_multiplier_bp,
    )
    governed_control_state["portfolio_headroom_multiplier_bp"] = int(portfolio_multiplier_bp)
    portfolio_control = dict(governed_control_state.get("portfolio_control") or {})
    portfolio_control["headroom_multiplier_bp"] = int(portfolio_multiplier_bp)
    if portfolio_discovery_reason_codes:
        portfolio_control["reason_codes"] = _unique_reason_codes(
            list(portfolio_control.get("reason_codes") or []) + list(portfolio_discovery_reason_codes)
        )
        for sid, meta in list(qualification_by_sleeve.items()):
            meta_codes = set(str(code).strip() for code in list(meta.get("reason_codes") or []))
            if PAPER_DISCOVERY_MODE_ACTIVE not in meta_codes:
                continue
            qualification_by_sleeve[sid] = {
                **meta,
                "reason_codes": _unique_reason_codes(
                    list(meta.get("reason_codes") or []) + list(portfolio_discovery_reason_codes)
                ),
            }
    governed_control_state["portfolio_control"] = portfolio_control

    portfolio_headroom_cents = (int(raw_portfolio_headroom_cents) * int(portfolio_multiplier_bp)) // 10000
    if bootstrap_zero_baseline:
        raw_portfolio_headroom_cents = 0
        portfolio_headroom_cents = 0
    allowed_by_sleeve_raw = _allocate_sleeve_headroom(
        portfolio_headroom_cents,
        sleeves,
        economic_signals_by_sleeve=economic_signals_by_sleeve,
    )

    # HARD BINDING: apply correlation envelope cap multipliers (basis points) per sleeve
    allowed_by_sleeve: Dict[str, int] = {}
    governed_control_audit_by_sleeve: Dict[str, Dict[str, Any]] = {}
    for sid, allow in sorted(allowed_by_sleeve_raw.items(), key=lambda kv: kv[0]):
        bp_any = mult.get(sid)
        if not isinstance(bp_any, int):
            raise SystemExit(f"FAIL: CORRELATION_GATE_MISSING_MULTIPLIER_FOR_SLEEVE: {sid}")
        bp = int(bp_any)
        if bp < 0 or bp > 10000:
            raise SystemExit(f"FAIL: CORRELATION_GATE_MULTIPLIER_OUT_OF_RANGE sleeve={sid} bp={bp}")
        sleeve_limit = next((item for item in sleeves if item.sleeve_id == sid), None)
        if sleeve_limit is None:
            raise SystemExit(f"FAIL: POLICY_SLEEVE_NOT_FOUND_FOR_QUALIFICATION: {sid}")
        governed_row = dict(governed_control_by_sleeve.get(sid) or {})
        sleeve_environment = str(governed_row.get("mode") or "PAPER").strip().upper() or "PAPER"
        qualification_meta = dict(
            qualification_by_sleeve.get(sleeve_limit.sleeve_id) or _bootstrap_zero_baseline_qualification_meta()
        )
        qualification_meta["governance_artifact_status"] = str(governed_row.get("artifact_status") or "").strip().upper()
        qualified_allow = (int(allow) * bp) // 10000
        allowed_after_qualification = (qualified_allow * int(qualification_meta.get("capital_multiplier_bp") or 0)) // 10000
        discovery_headroom_cap_cents = int(qualification_meta.get("discovery_max_headroom_cents") or 0)
        if discovery_headroom_cap_cents > 0 and allowed_after_qualification > discovery_headroom_cap_cents:
            allowed_after_qualification = discovery_headroom_cap_cents
            qualification_meta = {
                **qualification_meta,
                "reason_codes": _unique_reason_codes(
                    list(qualification_meta.get("reason_codes") or [])
                    + ["PAPER_DISCOVERY_MODE_HEADROOM_CAP_APPLIED"]
                ),
            }
        economic_multiplier_bp = int((economic_signals_by_sleeve.get(sid) or {}).get("capital_multiplier_bp") or 10000)
        allowed_before_governance = (allowed_after_qualification * economic_multiplier_bp) // 10000
        qualification_by_sleeve[sid] = qualification_meta
        raw_governance_multiplier_bp = governed_row.get("headroom_multiplier_bp")
        governance_multiplier_bp = 10000 if raw_governance_multiplier_bp is None else int(raw_governance_multiplier_bp)
        governance_multiplier_bp, discovery_relax_reason_codes = _effective_sleeve_governance_multiplier_bp(
            governed_multiplier_bp=governance_multiplier_bp,
            environment=sleeve_environment,
            qualification_meta=qualification_meta,
        )
        if discovery_relax_reason_codes:
            qualification_meta = {
                **qualification_meta,
                "reason_codes": _unique_reason_codes(
                    list(qualification_meta.get("reason_codes") or []) + list(discovery_relax_reason_codes)
                ),
            }
        allowed_by_sleeve[sid] = (allowed_before_governance * governance_multiplier_bp) // 10000
        governed_control_audit_by_sleeve[sid] = {
            **governed_row,
            "headroom_multiplier_bp": int(governance_multiplier_bp),
            "reason_codes": _unique_reason_codes(
                list(governed_row.get("reason_codes") or []) + list(discovery_relax_reason_codes)
            ),
            "pre_governance_headroom_cents": int(allowed_before_governance),
            "effective_headroom_cents": int(allowed_by_sleeve[sid]),
        }


    sleeve_priority = {s.sleeve_id: s.priority_rank for s in sleeves}
    initial_allowed_by_sleeve = {sid: int(v) for sid, v in allowed_by_sleeve.items()}
    remaining_by_sleeve = {sid: int(v) for sid, v in allowed_by_sleeve.items()}
    remaining_portfolio_headroom_cents_ref = {"value": int(max(portfolio_headroom_cents, 0))}

    candidate_actions: List[Dict[str, Any]] = []
    trade_intents: List[Dict[str, Any]] = []
    authorized_trade_intents: List[Dict[str, Any]] = []
    allocation_targets: List[Dict[str, Any]] = []
    per_intent_rows: List[Tuple[int, str, Dict[str, Any]]] = []
    for p in intents:
        o = _read_json_obj(p)
        intent_sha = _intent_hash_from_snapshot_path(p)
        candidate_row, trade_intent_row, authorized_row = _decision_chain_for_intent(
            intent_obj=o,
            intent_path=p,
            intent_sha=intent_sha,
            engine_to_sleeve=engine_to_sleeve,
            execution_bindings=execution_bindings,
            positions_lookup=positions_lookup,
            nav_basis_cents=nav_basis_cents,
            nav_total_cents=int(env_obj.get("envelope", {}).get("nav_total_cents") or 0),
            day_utc=day,
            truth_root=truth_root,
            env_status_ok=env_status_ok,
            corr_status_ok=corr_status_ok,
            remaining_by_strategy_sleeve=remaining_by_sleeve,
            remaining_portfolio_headroom_cents_ref=remaining_portfolio_headroom_cents_ref,
            economic_signals_by_sleeve=economic_signals_by_sleeve,
            economic_signal_source_day_utc=str(economic_reallocation_state.get("source_day_utc") or ""),
        )
        candidate_actions.append(candidate_row)
        if trade_intent_row is not None:
            trade_intents.append(trade_intent_row)
        authorized_trade_intents.append(authorized_row)

        strategy_sleeve_id = str(authorized_row.get("strategy_sleeve_id") or "UNKNOWN_STRATEGY_SLEEVE")
        qualification_meta = qualification_by_sleeve.get(strategy_sleeve_id, {})
        qualification_rc = _unique_reason_codes(
            [str(qualification_meta.get("action_reason_code") or "")] + list(qualification_meta.get("reason_codes") or [])
        )
        if PAPER_DISCOVERY_MODE_ACTIVE in qualification_rc:
            authorized_row["reason_codes"] = _unique_reason_codes(
                list(authorized_row.get("reason_codes") or [])
                + [
                    PAPER_DISCOVERY_MODE_ACTIVE,
                    SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED,
                    PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED,
                    PAPER_DISCOVERY_MIN_EXECUTABLE_HEADROOM_ALLOWED,
                ]
            )
        legacy_reason_codes = _unique_reason_codes(list(authorized_row.get("reason_codes") or []) + qualification_rc)
        legacy_decision = "AUTHORIZED" if int(authorized_row.get("authorized_quantity") or 0) > 0 else "REJECTED"
        per_intent_rows.append(
            (
                int(sleeve_priority.get(strategy_sleeve_id, 999999)),
                intent_sha,
                {
                    "intent_hash": intent_sha,
                    "intent_id": str(authorized_row.get("intent_id") or ""),
                    "engine_id": str(authorized_row.get("engine_id") or ""),
                    "sleeve_id": strategy_sleeve_id,
                    "decision": legacy_decision,
                    "authorized_quantity": int(authorized_row.get("authorized_quantity") or 0),
                    "reason_codes": legacy_reason_codes,
                    "qualification_state": str(qualification_meta.get("qualification_state") or ""),
                },
            )
        )
        allocation_targets.append(
            {
                "intent_hash": intent_sha,
                "intent_id": str(candidate_row.get("intent_id") or ""),
                "engine_id": str(candidate_row.get("engine_id") or ""),
                "symbol": str(candidate_row.get("symbol") or ""),
                "strategy_sleeve_id": strategy_sleeve_id,
                "execution_sleeve_id": str(candidate_row.get("execution_sleeve_id") or ""),
                "account_id": str(candidate_row.get("account_id") or ""),
                "target_notional_pct": candidate_row.get("target_notional_pct"),
                "actual_notional_pct": candidate_row.get("actual_notional_pct"),
                "drift_notional_pct": candidate_row.get("drift_notional_pct"),
                "action_type": str(candidate_row.get("action_type") or ""),
                "position_id": str(candidate_row.get("position_id") or ""),
            }
        )

    per_intent_rows.sort(key=lambda row: (row[0], row[1]))
    per_intent = [row[2] for row in per_intent_rows]
    per_intent.sort(key=lambda r: (r["sleeve_id"], r["intent_hash"]))

    strategy_actuals = _strategy_sleeve_actuals(
        positions_obj=positions_obj,
        nav_basis_cents=nav_basis_cents,
        engine_to_sleeve=engine_to_sleeve,
    )
    per_sleeve = []
    for s in sorted(sleeves, key=lambda x: x.sleeve_id):
        initial_allow = int(initial_allowed_by_sleeve.get(s.sleeve_id, 0))
        remaining_allow = int(remaining_by_sleeve.get(s.sleeve_id, 0))
        used_allow = int(initial_allow - remaining_allow)
        qualification_meta = qualification_by_sleeve.get(s.sleeve_id, {})
        actual_meta = strategy_actuals.get(
            s.sleeve_id,
            {
                "actual_value_cents": 0,
                "actual_notional_pct": Decimal("0.000000"),
            },
        )
        per_sleeve.append(
            {
                "sleeve_id": s.sleeve_id,
                "engine_ids": list(s.engine_ids),
                "allowed_capital_at_risk_cents": initial_allow,
                "used_capital_at_risk_cents": used_allow,
                "headroom_cents": remaining_allow,
                "actual_value_cents": int(actual_meta.get("actual_value_cents") or 0),
                "actual_notional_pct": _fraction_text(_fraction_from_any(actual_meta.get("actual_notional_pct"))),
                "qualification_state": str(qualification_meta.get("qualification_state") or ""),
                "edge_band": str(qualification_meta.get("edge_band") or ""),
                "execution_health_band": str(qualification_meta.get("execution_health_band") or ""),
                "sample_sufficiency_band": str(qualification_meta.get("sample_sufficiency_band") or ""),
                "drift_band": str(qualification_meta.get("drift_band") or ""),
                "qualification_reason_codes": list(qualification_meta.get("reason_codes") or []),
                "qualification_snapshot_path": str(qualification_meta.get("snapshot_path") or ""),
                "qualification_snapshot_sha256": str(qualification_meta.get("snapshot_sha256") or ""),
                "qualification_action_multiplier_bp": int(qualification_meta.get("capital_multiplier_bp") or 0),
                "qualification_action_reason_code": str(qualification_meta.get("action_reason_code") or ""),
                "economic_signal": str((economic_signals_by_sleeve.get(s.sleeve_id) or {}).get("signal") or "UNKNOWN"),
                "economic_signal_reason_code": str((economic_signals_by_sleeve.get(s.sleeve_id) or {}).get("reason_code") or ""),
                "economic_signal_source_day_utc": str(economic_reallocation_state.get("source_day_utc") or ""),
                "economic_signal_capital_multiplier_bp": int((economic_signals_by_sleeve.get(s.sleeve_id) or {}).get("capital_multiplier_bp") or 10000),
            }
        )

    candidate_actions.sort(key=lambda row: (str(row.get("execution_sleeve_id") or ""), str(row.get("intent_hash") or "")))
    trade_intents.sort(key=lambda row: (str(row.get("execution_sleeve_id") or ""), str(row.get("intent_hash") or "")))
    authorized_trade_intents.sort(
        key=lambda row: (
            str(row.get("execution_sleeve_id") or ""),
            _outcome_priority(str(row.get("authorization_outcome") or "")),
            str(row.get("intent_hash") or ""),
        )
    )
    allocation_targets.sort(key=lambda row: (str(row.get("execution_sleeve_id") or ""), str(row.get("intent_hash") or "")))

    total_target_pct = sum(
        (
            _fraction_from_any(item.get("target_notional_pct"))
            for item in allocation_targets
            if item.get("target_notional_pct") is not None
        ),
        Decimal("0"),
    ).quantize(WEIGHT_QUANT, rounding=ROUND_HALF_UP)
    total_actual_pct = sum(
        (
            _fraction_from_any(item.get("actual_notional_pct"))
            for item in allocation_targets
            if item.get("actual_notional_pct") is not None
        ),
        Decimal("0"),
    ).quantize(WEIGHT_QUANT, rounding=ROUND_HALF_UP)
    max_abs_drift_pct = Decimal("0.000000")
    for item in allocation_targets:
        if item.get("drift_notional_pct") is None:
            continue
        max_abs_drift_pct = max(max_abs_drift_pct, abs(_fraction_from_any(item.get("drift_notional_pct"))))

    sleeve_account_authority_state = {
        "mode_source": "C2_SLEEVE_REGISTRY_V1",
        "bindings": [
            {
                "execution_sleeve_id": binding.execution_sleeve_id,
                "mode": binding.mode,
                "enabled": bool(binding.enabled),
                "account_id": binding.account_id,
                "allowed_engine_ids": list(binding.allowed_engine_ids),
                "allowed_execution_sleeve_ids": list(binding.allowed_execution_sleeve_ids),
            }
            for binding in execution_bindings
        ],
    }
    allocation_state = {
        "target_basis": "INTENT_TARGET_NOTIONAL_PCT",
        "actual_basis": "POSITIONS_COST_BASIS_ABS_OVER_NAV_BASIS",
        "portfolio_nav_basis_cents": int(nav_basis_cents),
        "portfolio_target_notional_pct": _fraction_text(total_target_pct),
        "portfolio_actual_notional_pct": _fraction_text(total_actual_pct),
        "max_abs_drift_notional_pct": _fraction_text(max_abs_drift_pct),
        "reallocation_state": {
            "status": (
                "ACTION_NEEDED"
                if (
                    any(row.get("action_type") in {ACTION_OPEN, ACTION_ADD, ACTION_REDUCE, ACTION_CLOSE} for row in candidate_actions)
                    or any(
                        str((economic_signals_by_sleeve.get(s.sleeve_id) or {}).get("signal") or "UNKNOWN") in {"INCREASE", "DECREASE"}
                        for s in sleeves
                    )
                )
                else "STABLE"
            ),
            "reason_codes": _unique_reason_codes(
                (["BUNDLE_B_DRIFT_DETECTED"] if max_abs_drift_pct > Decimal("0") else ["BUNDLE_B_NO_DRIFT"])
                + list(economic_reallocation_state.get("reason_codes") or [])
                + (
                    ["BUNDLE_C_PREVIOUS_DAY_REALLOCATION_SIGNAL_APPLIED"]
                    if str(economic_reallocation_state.get("status") or "").strip().upper() == "OK"
                    else []
                )
            ),
            "bundle_c_signal_status": str(economic_reallocation_state.get("status") or "UNKNOWN"),
            "bundle_c_signal_source_day_utc": str(economic_reallocation_state.get("source_day_utc") or ""),
            "bundle_c_signal_artifact_path": str(economic_reallocation_state.get("artifact_path") or ""),
            "bundle_c_signal_artifact_sha256": str(economic_reallocation_state.get("artifact_sha256") or ""),
            "per_sleeve_signals": [
                {
                    "sleeve_id": sleeve_id,
                    "signal": str(meta.get("signal") or "UNKNOWN"),
                    "reason_code": str(meta.get("reason_code") or ""),
                    "capital_multiplier_bp": int(meta.get("capital_multiplier_bp") or 10000),
                    "observed_return": meta.get("observed_return"),
                    "portfolio_return": meta.get("portfolio_return"),
                }
                for sleeve_id, meta in sorted(economic_signals_by_sleeve.items(), key=lambda item: item[0])
            ],
        },
        "target_rows": allocation_targets,
    }
    governed_control_state = {
        **governed_control_state,
        "portfolio_control": {
            **dict(governed_control_state.get("portfolio_control") or {}),
            "raw_headroom_cents": int(raw_portfolio_headroom_cents),
            "effective_headroom_cents": int(max(portfolio_headroom_cents, 0)),
        },
        "sleeve_controls": [
            dict(governed_control_audit_by_sleeve.get(s.sleeve_id) or {})
            for s in sorted(sleeves, key=lambda item: item.sleeve_id)
        ],
    }
    governed_control_reason_codes = _unique_reason_codes(
        ["GOVERNED_EVALUATION_RUNTIME_CONTROL_APPLIED"]
        + list((governed_control_state.get("portfolio_control") or {}).get("reason_codes") or [])
        + [
            code
            for row in governed_control_state.get("sleeve_controls") or []
            if isinstance(row, dict)
            for code in (row.get("reason_codes") or [])
        ]
    )
    discovery_mode_active = any(
        PAPER_DISCOVERY_MODE_ACTIVE in set(str(code).strip() for code in list(meta.get("reason_codes") or []))
        for meta in qualification_by_sleeve.values()
    )
    discovery_sleeve_relaxation_active = any(
        PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED in set(str(code).strip() for code in list(meta.get("reason_codes") or []))
        for meta in qualification_by_sleeve.values()
    )

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_CAPITAL_AUTHORITY_ALLOCATION_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {
            "repo": "constellation_2_runtime",
            "git_sha": _git_sha_failclosed(),
            "module": "ops/tools/run_capital_authority_allocation_day_v1.py",
        },
        "status": "OK",
        "reason_codes": [
            "BUNDLE_B_CANONICAL_DECISION_CHAIN_V1",
            "CAPITAL_AUTHORITY_ALLOCATION_V1_COMPATIBILITY_VIEW_PRESENT",
        ]
        + (["PAPER_BOOTSTRAP_ZERO_RISK_BASELINE"] if bootstrap_zero_baseline else [])
        + ([PAPER_DISCOVERY_MODE_ACTIVE] if discovery_mode_active else [])
        + ([PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED] if discovery_sleeve_relaxation_active else [])
        + governed_control_reason_codes,
        "input_manifest": (
            (
                [{"type": "exposure_net", "path": str(p_ex), "sha256": ex_sha, "day_utc": day, "producer": "risk_v1"}]
                if exposure_present
                else []
            )
            + [
                {"type": "positions_snapshot", "path": str(positions_path), "sha256": positions_sha, "day_utc": day, "producer": "positions_snapshot_v5"},
                {"type": "policy_manifest", "path": str(POLICY_PATH), "sha256": pol_sha, "day_utc": None, "producer": "governance"},
                {"type": "qualification_policy_manifest", "path": str(SLEEVE_EDGE_POLICY_PATH), "sha256": sleeve_edge_policy_sha, "day_utc": None, "producer": "governance"},
                {"type": "other", "path": str(governed_control_state["policy_registry_ref"]["path"]), "sha256": str(governed_control_state["policy_registry_ref"]["sha256"]), "day_utc": None, "producer": "C2_GOVERNED_EVALUATION_POLICY_V1"},
                {"type": "other", "path": str(SLEEVE_REGISTRY_PATH), "sha256": sleeve_registry_sha, "day_utc": None, "producer": "governance"},
                {"type": "other", "path": str(ACCOUNT_REGISTRY_PATH), "sha256": account_registry_sha, "day_utc": None, "producer": "governance"},
                {"type": "other", "path": str(envp), "sha256": env_sha, "day_utc": day, "producer": "capital_risk_envelope_v2"},
                {"type": "other", "path": str(cegp), "sha256": ceg_sha, "day_utc": day, "producer": "correlation_envelope_gate_v1"},
            ]
            + (
                [
                    {
                        "type": "other",
                        "path": str(bootstrap_admission.get("path") or ""),
                        "sha256": str(bootstrap_admission.get("sha256") or ""),
                        "day_utc": day,
                        "producer": "target_day_admission_v1",
                    }
                ]
                if bootstrap_zero_baseline and bootstrap_admission is not None
                else []
            )
            + (
                [
                    {
                        "type": "other",
                        "path": str(economic_reallocation_state.get("artifact_path") or ""),
                        "sha256": str(economic_reallocation_state.get("artifact_sha256") or ""),
                        "day_utc": str(economic_reallocation_state.get("source_day_utc") or ""),
                        "producer": ECONOMIC_BUILD_FAMILY,
                    }
                ]
                if str(economic_reallocation_state.get("status") or "").strip().upper() == "OK"
                and str(economic_reallocation_state.get("artifact_path") or "").strip()
                and str(economic_reallocation_state.get("artifact_sha256") or "").strip()
                else []
            )
            + [
                {
                    "type": "sleeve_edge_snapshot",
                    "path": str(meta["snapshot_path"]),
                    "sha256": str(meta["snapshot_sha256"]),
                    "day_utc": day,
                    "producer": "sleeve_edge_snapshot_v1",
                }
                for _, meta in sorted(qualification_by_sleeve.items(), key=lambda item: item[0])
                if str(meta.get("snapshot_path") or "").strip() and str(meta.get("snapshot_sha256") or "").strip()
            ]
            + [
                {"type": "intents_snapshot", "path": str(p), "sha256": _sha256_file(p), "day_utc": day, "producer": "intents_v1"}
                for p in intents
            ]
            + (
                [
                    {
                        "type": "other",
                        "path": str((governed_control_state.get("portfolio_control") or {}).get("action_ref", {}).get("path") or ""),
                        "sha256": str((governed_control_state.get("portfolio_control") or {}).get("action_ref", {}).get("sha256") or ""),
                        "day_utc": day,
                        "producer": "portfolio_governance_action_state_v1",
                    }
                ]
                if str((governed_control_state.get("portfolio_control") or {}).get("artifact_status") or "") == "OK"
                else []
            )
            + [
                {
                    "type": "other",
                    "path": str((row.get("action_ref") or {}).get("path") or ""),
                    "sha256": str((row.get("action_ref") or {}).get("sha256") or ""),
                    "day_utc": day,
                    "producer": "sleeve_governance_action_state_v1",
                }
                for row in governed_control_state.get("sleeve_controls") or []
                if isinstance(row, dict) and str(row.get("artifact_status") or "") == "OK"
            ]
        ),
        "portfolio": {
            "allowed_capital_at_risk_cents": int(max(portfolio_headroom_cents, 0)),
            "used_capital_at_risk_cents": int(max(portfolio_headroom_cents, 0)) - int(remaining_portfolio_headroom_cents_ref["value"]),
            "headroom_cents": int(remaining_portfolio_headroom_cents_ref["value"]),
        },
        "governed_evaluation_control_state": governed_control_state,
        "correlation_gate_binding": {
            "gate_artifact_path": str(cegp),
            "gate_artifact_sha256": ceg_sha,
            "policy_id": "C2_CORRELATION_ENVELOPE_POLICY_V1",
            "policy_sha256": _sha256_file(REPO_ROOT / "governance/02_REGISTRIES/C2_CORRELATION_ENVELOPE_POLICY_V1.json"),
            "binding_mode": "ALLOCATION_CAP_CONSTRAINT",
            "applied": True
        },
        "allocation_state": allocation_state,
        "sleeve_account_authority_state": sleeve_account_authority_state,
        "decision_chain": {
            "candidate_actions": candidate_actions,
            "trade_intents": trade_intents,
            "authorized_trade_intents": authorized_trade_intents,
        },
        "per_sleeve": per_sleeve,
        "per_intent": per_intent,
    }

    validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

    payload = canonical_json_bytes_v1(out_obj) + b"\n"
    out_path = (_out_root(truth_root) / day / "capital_authority_allocation.v1.json").resolve()
    wrote_sha = _atomic_write_replace_if_changed(out_path, payload)

    print(f"OK: CAPITAL_AUTHORITY_ALLOCATION_V1_DAY day_utc={day} path={out_path} sha256={wrote_sha} status={out_obj['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
