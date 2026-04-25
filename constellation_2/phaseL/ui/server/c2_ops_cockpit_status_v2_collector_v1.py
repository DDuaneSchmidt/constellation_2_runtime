#!/usr/bin/env python3
"""
Constellation 2.0 — Ops Cockpit UI V2 — Status Collector (Read-Only, Deterministic)

Contract:
- Reads ONLY canonical truth artifacts under /home/node/constellation_runtime_data/truth and instance config JSON (if present).
- Produces a single deterministic payload for Operations + Engines.
- Fail-closed: missing/parse errors are explicit, never inferred.
- No nondeterministic ordering: all lists are sorted by stable keys.
- No trading logic changes.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.common.accounting_authority_v1 import read_accounting_authority_state
from constellation_2.common.execution_day_authority_v1 import read_execution_day_authority_state
from constellation_2.common.runtime_base_v1 import advisor_runtime_root
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root

# -------------------------
# Deterministic helpers
# -------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_day_str(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except Exception:
        return False


def _safe_read_json(path: Path) -> Tuple[Optional[Any], Optional[str]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except FileNotFoundError:
        return None, "FILE_NOT_FOUND"
    except json.JSONDecodeError:
        return None, "JSON_DECODE_ERROR"
    except Exception:
        return None, "READ_ERROR"


def _mtime(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except Exception:
        return None


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> Optional[str]:
    try:
        return _sha256_bytes(path.read_bytes())
    except Exception:
        return None


def _stable_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _coerce_state(s: Optional[str]) -> str:
    if not isinstance(s, str) or not s:
        return "UNKNOWN"
    u = s.upper()
    if u in ("PASS", "DEGRADED", "FAIL", "ABORTED", "UNKNOWN", "MISSING"):
        return u
    if u in ("OK", "PRESENT", "SUCCESS"):
        return "PASS"
    return "UNKNOWN"


def _top2_reason_codes(x: Any) -> List[str]:
    if isinstance(x, list):
        out = [str(a) for a in x if isinstance(a, (str, int, float)) and str(a).strip()]
        return out[:2]
    return []


THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[4]
SLEEVE_POLICY_REGISTRY = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
SLEEVE_REGISTRY = (REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
IB_ACCOUNT_REGISTRY = (REPO_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
PLATFORM_READINESS_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_PLATFORM_READINESS_POLICY_V1.json").resolve()
GLOBAL_RUNTIME_TRUTH_ROOT = resolve_canonical_truth_root().resolve()
RUNTIME_STATE_PATH = (GLOBAL_RUNTIME_TRUTH_ROOT / "system_snapshot/constellation_runtime_state.v1.json").resolve()
BOND_OPERATOR_INPUT_ROOT = (REPO_ROOT / "constellation_2/operator_inputs/bond_sleeve").resolve()
BOND_POSITIONS_INPUT_PATH = (BOND_OPERATOR_INPUT_ROOT / "bond_positions_v1.json").resolve()
BOND_POLICY_INPUT_PATH = (BOND_OPERATOR_INPUT_ROOT / "bond_sleeve_policy_v1.json").resolve()
BOND_MACRO_POLICY_INPUT_PATH = (BOND_OPERATOR_INPUT_ROOT / "bond_macro_policy_v1.json").resolve()
BOND_FAMILY_SPECS: List[Tuple[str, str]] = [
    ("bond_candidate_coverage_v1", "bond_candidate_coverage.v1.json"),
    ("bond_yield_curve_snapshot_v1", "bond_yield_curve_snapshot.v1.json"),
    ("bond_credit_stress_proxy_v1", "bond_credit_stress_proxy.v1.json"),
    ("bond_candidate_classification_v1", "bond_candidate_classification.v1.json"),
    ("bond_sleeve_policy_snapshot_v1", "bond_sleeve_policy_snapshot.v1.json"),
    ("bond_ladder_recommendation_v1", "bond_ladder_recommendation.v1.json"),
    ("bond_duration_report_v1", "bond_duration_report.v1.json"),
    ("bond_yield_report_v1", "bond_yield_report.v1.json"),
    ("bond_withdrawal_coverage_report_v1", "bond_withdrawal_coverage_report.v1.json"),
    ("bond_sleeve_capital_posture_v1", "bond_sleeve_capital_posture.v1.json"),
    ("bond_purchase_recommendation_v1", "bond_purchase_recommendation.v1.json"),
    ("bond_macro_regime_snapshot_v1", "bond_macro_regime_snapshot.v1.json"),
    ("bond_sleeve_recommendation_v2", "bond_sleeve_recommendation.v2.json"),
    ("bond_ladder_shape_v2", "bond_ladder_shape.v2.json"),
    ("bond_sleeve_explanation_v1", "bond_sleeve_explanation.v1.json"),
]
STANDARD_TRADING_SLEEVE_IDS = {
    "C2_CROSS_ASSET_TREND",
    "C2_DEFENSIVE_TAIL",
    "C2_EVENT_DISLOCATION",
    "C2_MARKET_NEUTRAL_SPREAD",
    "C2_MEAN_REVERSION_EQ",
    "C2_TREND_EQ_PRIMARY",
    "C2_VOL_INCOME_DEFINED_RISK",
}
ADVISOR_RUNTIME_ROOT = advisor_runtime_root()


# -------------------------
# Tile model
# -------------------------

@dataclass(frozen=True)
class Tile:
    tile_id: str
    state: str
    last_updated_utc: Optional[str]
    reason_codes: List[str]
    reason_human: List[str]
    artifact_path: Optional[str]
    artifact_sha256: Optional[str]


def _tile_dict(t: Tile) -> Dict[str, Any]:
    return {
        "tile_id": t.tile_id,
        "state": t.state,
        "last_updated_utc": t.last_updated_utc,
        "reason_codes": t.reason_codes,
        "reason_human": t.reason_human,
        "artifact_ref": {
            "path": t.artifact_path,
            "sha256": t.artifact_sha256,
        },
    }


def _tone_from_state(state: str) -> str:
    st = _coerce_state(str(state or "UNKNOWN"))
    if st in {"PASS", "OK"}:
        return "positive"
    if st in {"DEGRADED", "MISSING_INPUTS", "WARN", "WARNING"}:
        return "warning"
    if st in {"FAIL", "ABORTED", "MISSING"}:
        return "negative"
    return "info"


def _governed_surface_row(
    label: str,
    state: str,
    detail: str,
    artifact_path: Optional[str],
    authoritative: bool = True,
) -> Dict[str, Any]:
    return {
        "label": label,
        "state": _coerce_state(state),
        "tone": _tone_from_state(state),
        "detail": detail,
        "artifact_path": artifact_path,
        "authoritative": authoritative,
    }


# -------------------------
# Candidate roots
# -------------------------

def _candidate_replay_roots(truth_root: Path) -> List[Path]:
    return [
        (truth_root / "reports" / "replay_certification_gate_v1"),
        (truth_root / "reports" / "replay_certification_bundle_v1"),
        (truth_root / "reports" / "replay_integrity_v2"),
        (truth_root / "reports" / "replay_integrity_day_v2"),
        (truth_root / "reports" / "replay_integrity_day_v1"),
    ]


def _day_dirs(root: Path) -> List[str]:
    if not root.exists() or not root.is_dir():
        return []
    out: List[str] = []
    for p in root.iterdir():
        if p.is_dir() and _is_day_str(p.name):
            out.append(p.name)
    return sorted(set(out))


def _parse_decimal_text(v: Any) -> Optional[Decimal]:
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    if isinstance(v, str) and v.strip():
        try:
            return Decimal(v.strip())
        except InvalidOperation:
            return None
    return None


def _collect_operator_holdings_view() -> Tuple[Dict[str, Any], List[str], List[str]]:
    warnings: List[str] = []
    missing_paths: List[str] = []
    out: Dict[str, Any] = {
        "positions_path": str(BOND_POSITIONS_INPUT_PATH),
        "positions_sha256": _sha256_file(BOND_POSITIONS_INPUT_PATH),
        "policy_path": str(BOND_POLICY_INPUT_PATH),
        "policy_sha256": _sha256_file(BOND_POLICY_INPUT_PATH),
        "macro_policy_path": str(BOND_MACRO_POLICY_INPUT_PATH),
        "macro_policy_sha256": _sha256_file(BOND_MACRO_POLICY_INPUT_PATH),
        "positions_count": 0,
        "market_value_total": None,
        "positions": [],
        "present": False,
    }

    positions_obj, p_err = _safe_read_json(BOND_POSITIONS_INPUT_PATH)
    if not isinstance(positions_obj, dict):
        missing_paths.append(str(BOND_POSITIONS_INPUT_PATH))
        warnings.append(f"BOND_POSITIONS_INPUT_UNREADABLE:{p_err}")
        return out, sorted(set(warnings)), sorted(set(missing_paths))

    policy_obj, pol_err = _safe_read_json(BOND_POLICY_INPUT_PATH)
    if not isinstance(policy_obj, dict):
        missing_paths.append(str(BOND_POLICY_INPUT_PATH))
        warnings.append(f"BOND_POLICY_INPUT_UNREADABLE:{pol_err}")

    items = positions_obj.get("positions") if isinstance(positions_obj.get("positions"), list) else []
    rows: List[Dict[str, Any]] = []
    total_mv = Decimal("0")
    for idx, raw in enumerate(items):
        if not isinstance(raw, dict):
            warnings.append(f"BOND_POSITION_ROW_INVALID:{idx}")
            continue
        mv = _parse_decimal_text(raw.get("market_value"))
        if mv is None:
            warnings.append(f"BOND_POSITION_MARKET_VALUE_INVALID:{idx}")
            continue
        total_mv += mv
        rows.append(
            {
                "instrument_id": raw.get("instrument_id"),
                "maturity_date": raw.get("maturity_date"),
                "issuer_type": raw.get("issuer_type"),
                "credit_type": raw.get("credit_type"),
                "market_value": str(mv),
                "yield_to_maturity": raw.get("yield_to_maturity"),
                "duration": raw.get("duration"),
            }
        )
    rows.sort(key=lambda x: str(x.get("instrument_id") or ""))

    out["positions_count"] = len(rows)
    out["positions"] = rows
    out["present"] = len(rows) > 0
    if rows:
        out["market_value_total"] = str(total_mv)

    return out, sorted(set(warnings)), sorted(set(missing_paths))


def _read_bond_family_head(*, day_utc: str, family: str, artifact_name: str) -> Tuple[Dict[str, Any], List[str], List[str], List[str], Dict[str, float]]:
    warnings: List[str] = []
    missing_paths: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}

    day_root = (GLOBAL_RUNTIME_TRUTH_ROOT / "reports" / family / day_utc).resolve()
    display_path = (day_root / "display_head_pointer.v1.json").resolve()
    authority_path = (day_root / "authority_head_pointer.v1.json").resolve()
    pointer_index_path = (day_root / "canonical_pointer_index.v1.jsonl").resolve()

    out: Dict[str, Any] = {
        "family": family,
        "artifact_name": artifact_name,
        "day_utc": day_utc,
        "day_root": str(day_root),
        "display_head_path": str(display_path),
        "authority_head_path": str(authority_path),
        "pointer_index_path": str(pointer_index_path),
        "present": False,
        "status": "MISSING",
        "state_label": "NO_CURRENT_ARTIFACT",
        "head_mode_label": "NO_CURRENT_ARTIFACT",
        "points_to": None,
        "points_to_sha256": None,
        "produced_utc": None,
        "artifact": None,
    }

    if not pointer_index_path.exists():
        missing_paths.append(str(pointer_index_path))
    else:
        source_paths.append(str(pointer_index_path))
        mt = _mtime(pointer_index_path)
        if mt is not None:
            source_mtimes[str(pointer_index_path)] = mt

    if authority_path.exists():
        source_paths.append(str(authority_path))
        mt = _mtime(authority_path)
        if mt is not None:
            source_mtimes[str(authority_path)] = mt

    if not display_path.exists():
        missing_paths.append(str(display_path))
        out["state_label"] = "NO_CURRENT_ARTIFACT"
        out["head_mode_label"] = "NO_CURRENT_ARTIFACT"
        return out, sorted(set(warnings)), sorted(set(missing_paths)), sorted(set(source_paths)), source_mtimes

    source_paths.append(str(display_path))
    mt = _mtime(display_path)
    if mt is not None:
        source_mtimes[str(display_path)] = mt

    head_obj, head_err = _safe_read_json(display_path)
    if not isinstance(head_obj, dict):
        warnings.append(f"BOND_DISPLAY_HEAD_UNREADABLE:{family}:{head_err}")
        out["state_label"] = "NO_CURRENT_ARTIFACT"
        out["head_mode_label"] = "NO_CURRENT_ARTIFACT"
        return out, sorted(set(warnings)), sorted(set(missing_paths)), sorted(set(source_paths)), source_mtimes

    points_to = head_obj.get("points_to")
    out["status"] = _coerce_state(str(head_obj.get("status") or "UNKNOWN"))
    out["points_to"] = points_to if isinstance(points_to, str) and points_to.strip() else None
    out["points_to_sha256"] = head_obj.get("points_to_sha256") if isinstance(head_obj.get("points_to_sha256"), str) else None
    out["produced_utc"] = head_obj.get("produced_utc") if isinstance(head_obj.get("produced_utc"), str) else None

    authority_exists = authority_path.exists()
    out["head_mode_label"] = "DISPLAY_HEAD_AVAILABLE_ONLY" if not authority_exists else "AUTHORITY_HEAD_AVAILABLE"
    if not authority_exists:
        warnings.append(f"AUTHORITY_HEAD_MISSING:{family}")

    if not isinstance(points_to, str) or not points_to.strip():
        warnings.append(f"BOND_DISPLAY_HEAD_POINTS_TO_MISSING:{family}")
        out["state_label"] = "NO_CURRENT_ARTIFACT"
        return out, sorted(set(warnings)), sorted(set(missing_paths)), sorted(set(source_paths)), source_mtimes

    artifact_path = Path(points_to).resolve()
    if not artifact_path.exists():
        missing_paths.append(str(artifact_path))
        warnings.append(f"BOND_ARTIFACT_MISSING:{family}")
        out["state_label"] = "NO_CURRENT_ARTIFACT"
        return out, sorted(set(warnings)), sorted(set(missing_paths)), sorted(set(source_paths)), source_mtimes

    source_paths.append(str(artifact_path))
    mt = _mtime(artifact_path)
    if mt is not None:
        source_mtimes[str(artifact_path)] = mt

    artifact_obj, art_err = _safe_read_json(artifact_path)
    if not isinstance(artifact_obj, dict):
        warnings.append(f"BOND_ARTIFACT_UNREADABLE:{family}:{art_err}")
        out["state_label"] = "NO_CURRENT_ARTIFACT"
        return out, sorted(set(warnings)), sorted(set(missing_paths)), sorted(set(source_paths)), source_mtimes

    out["present"] = True
    out["state_label"] = "DISPLAY_HEAD_AVAILABLE_ONLY" if not authority_exists else "AUTHORITY_HEAD_AVAILABLE"
    out["artifact"] = artifact_obj
    return out, sorted(set(warnings)), sorted(set(missing_paths)), sorted(set(source_paths)), source_mtimes


def _collect_bond_sleeve_view(selected_day: str) -> Tuple[Dict[str, Any], List[str], List[str], List[str], Dict[str, float]]:
    warnings: List[str] = []
    missing_paths: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}

    operator_holdings, warn_holdings, miss_holdings = _collect_operator_holdings_view()
    warnings.extend(warn_holdings)
    missing_paths.extend(miss_holdings)
    source_paths.extend([str(BOND_POSITIONS_INPUT_PATH), str(BOND_POLICY_INPUT_PATH), str(BOND_MACRO_POLICY_INPUT_PATH)])
    for p in [BOND_POSITIONS_INPUT_PATH, BOND_POLICY_INPUT_PATH, BOND_MACRO_POLICY_INPUT_PATH]:
        mt = _mtime(p)
        if mt is not None:
            source_mtimes[str(p)] = mt

    rollup_path = (GLOBAL_RUNTIME_TRUTH_ROOT / "reports" / "sleeve_rollup_v1" / selected_day / "sleeve_rollup.v1.json").resolve()
    run_metadata: Dict[str, Any] = {
        "day_utc": selected_day,
        "rollup_path": str(rollup_path),
        "present": False,
        "status": "UNKNOWN",
        "reason_codes": [],
        "truth_root_used": None,
        "runner": None,
        "rc": None,
    }
    if not rollup_path.exists():
        missing_paths.append(str(rollup_path))
        warnings.append("BOND_ROLLUP_MISSING")
    else:
        source_paths.append(str(rollup_path))
        mt = _mtime(rollup_path)
        if mt is not None:
            source_mtimes[str(rollup_path)] = mt
        rollup_obj, roll_err = _safe_read_json(rollup_path)
        if not isinstance(rollup_obj, dict):
            warnings.append(f"BOND_ROLLUP_UNREADABLE:{roll_err}")
        else:
            bond_run = rollup_obj.get("bond_sleeve_run")
            if not isinstance(bond_run, dict):
                warnings.append("BOND_ROLLUP_BOND_RUN_MISSING")
            else:
                run_metadata["present"] = True
                run_metadata["status"] = str(bond_run.get("status") or "UNKNOWN").upper()
                run_metadata["reason_codes"] = [str(x) for x in (bond_run.get("reason_codes") or []) if isinstance(x, str)]
                run_metadata["truth_root_used"] = (
                    str(bond_run.get("truth_root_used"))
                    if isinstance(bond_run.get("truth_root_used"), str)
                    else None
                )
                run_metadata["runner"] = str(bond_run.get("runner")) if isinstance(bond_run.get("runner"), str) else None
                run_metadata["rc"] = int(bond_run.get("rc")) if isinstance(bond_run.get("rc"), int) else None

    selected_has_any = False
    for family, _artifact in BOND_FAMILY_SPECS:
        d = (GLOBAL_RUNTIME_TRUTH_ROOT / "reports" / family / selected_day).resolve()
        if d.exists() and d.is_dir():
            selected_has_any = True
            break

    used_day = selected_day
    last_available_day: Optional[str] = None
    current_day_unavailable_reason = "CURRENT_DAY_AVAILABLE"
    if not selected_has_any:
        latest_days: List[str] = []
        for family, _artifact in BOND_FAMILY_SPECS:
            latest_days.extend(_day_dirs((GLOBAL_RUNTIME_TRUTH_ROOT / "reports" / family).resolve()))
        if latest_days:
            last_available_day = sorted(set(latest_days))[-1]
            used_day = last_available_day
            warnings.append("BOND_USING_LATEST_AVAILABLE_DAY")
            if run_metadata.get("present") is True:
                run_status = str(run_metadata.get("status") or "UNKNOWN").upper()
                if run_status == "SKIP":
                    current_day_unavailable_reason = "BOND_RUN_SKIPPED"
                elif run_status == "FAIL":
                    current_day_unavailable_reason = "BOND_RUN_FAILED"
                else:
                    current_day_unavailable_reason = "NO_CURRENT_ARTIFACT"
            else:
                current_day_unavailable_reason = "NO_CURRENT_ARTIFACT"
        else:
            warnings.append("BOND_REPORTS_NOT_FOUND")
            current_day_unavailable_reason = "NO_CURRENT_ARTIFACT"
    else:
        last_available_day = selected_day

    families_out: Dict[str, Any] = {}
    merged_artifacts: Dict[str, Any] = {}
    for family, artifact_name in BOND_FAMILY_SPECS:
        fam_obj, warn_f, miss_f, sp_f, sm_f = _read_bond_family_head(
            day_utc=used_day,
            family=family,
            artifact_name=artifact_name,
        )
        families_out[family] = fam_obj
        warnings.extend(warn_f)
        missing_paths.extend(miss_f)
        source_paths.extend(sp_f)
        source_mtimes.update(sm_f)
        art = fam_obj.get("artifact")
        if isinstance(art, dict):
            merged_artifacts[family] = art

    candidate_coverage = merged_artifacts.get("bond_candidate_coverage_v1", {})
    yield_curve = merged_artifacts.get("bond_yield_curve_snapshot_v1", {})
    credit_proxy = merged_artifacts.get("bond_credit_stress_proxy_v1", {})
    classification = merged_artifacts.get("bond_candidate_classification_v1", {})
    policy = merged_artifacts.get("bond_sleeve_policy_snapshot_v1", {})
    ladder = merged_artifacts.get("bond_ladder_recommendation_v1", {})
    duration = merged_artifacts.get("bond_duration_report_v1", {})
    yld = merged_artifacts.get("bond_yield_report_v1", {})
    withdrawal = merged_artifacts.get("bond_withdrawal_coverage_report_v1", {})
    posture = merged_artifacts.get("bond_sleeve_capital_posture_v1", {})
    purchase = merged_artifacts.get("bond_purchase_recommendation_v1", {})
    macro_regime = merged_artifacts.get("bond_macro_regime_snapshot_v1", {})
    overlay = merged_artifacts.get("bond_sleeve_recommendation_v2", {})
    ladder_shape = merged_artifacts.get("bond_ladder_shape_v2", {})
    explanation = merged_artifacts.get("bond_sleeve_explanation_v1", {})

    coverage_missing_buckets = (
        candidate_coverage.get("missing_buckets")
        if isinstance(candidate_coverage.get("missing_buckets"), list)
        else []
    )
    curve_missing_buckets = (
        yield_curve.get("missing_buckets")
        if isinstance(yield_curve.get("missing_buckets"), list)
        else []
    )
    classification_missing_dims = (
        classification.get("classification_dimensions_missing")
        if isinstance(classification.get("classification_dimensions_missing"), list)
        else []
    )
    candidate_eligible = candidate_coverage.get("eligible_count")
    candidate_quality_label = "NOT AVAILABLE"
    if isinstance(candidate_eligible, int) and candidate_eligible > 0:
        candidate_quality_label = "GOOD" if not coverage_missing_buckets else "PARTIAL"

    curve_status = str(yield_curve.get("coverage_status") or "UNAVAILABLE").upper()
    curve_quality_label = {
        "COMPLETE": "GOOD",
        "PARTIAL": "PARTIAL",
        "INSUFFICIENT": "NOT AVAILABLE",
    }.get(curve_status, "NOT AVAILABLE")

    credit_status = str(credit_proxy.get("coverage_quality") or "UNAVAILABLE").upper()
    credit_quality_label = "AVAILABLE" if credit_status == "AVAILABLE" else "NOT AVAILABLE"

    classified_count = classification.get("classified_count")
    classification_quality_label = "NOT AVAILABLE"
    if isinstance(classified_count, int) and classified_count > 0:
        classification_quality_label = "PARTIAL" if classification_missing_dims else "GOOD"

    input_quality = {
        "candidate_coverage": {
            "label": candidate_quality_label,
            "status": candidate_coverage.get("status"),
            "eligible_count": candidate_eligible,
            "missing_buckets": coverage_missing_buckets,
            "reason_codes": candidate_coverage.get("reason_codes") if isinstance(candidate_coverage.get("reason_codes"), list) else [],
        },
        "yield_curve_coverage": {
            "label": curve_quality_label,
            "coverage_status": curve_status,
            "missing_buckets": curve_missing_buckets,
            "fallback_used": bool(yield_curve.get("fallback_used")),
            "reason_codes": yield_curve.get("reason_codes") if isinstance(yield_curve.get("reason_codes"), list) else [],
        },
        "credit_proxy": {
            "label": credit_quality_label,
            "coverage_quality": credit_status,
            "spread_regime": credit_proxy.get("spread_regime"),
            "fallback_used": bool(credit_proxy.get("fallback_used")),
            "reason_codes": credit_proxy.get("reason_codes") if isinstance(credit_proxy.get("reason_codes"), list) else [],
        },
        "classification_coverage": {
            "label": classification_quality_label,
            "status": classification.get("status"),
            "available_dimensions": classification.get("classification_dimensions_available") if isinstance(classification.get("classification_dimensions_available"), list) else [],
            "missing_dimensions": classification_missing_dims,
            "reason_codes": classification.get("reason_codes") if isinstance(classification.get("reason_codes"), list) else [],
        },
        "fallback": {
            "label": "BASELINE POLICY ACTIVE" if bool(macro_regime.get("fallback_used")) else "NONE",
            "message": str(macro_regime.get("fallback_message") or "Macro overlay active."),
        },
    }

    summary = {
        "bond_sleeve_allocation_pct": policy.get("bond_sleeve_allocation_pct"),
        "bond_sleeve_target_pct": policy.get("bond_sleeve_target_pct"),
        "bond_sleeve_capital": posture.get("bond_sleeve_capital"),
        "trading_sleeve_allocation_pct": posture.get("trading_sleeve_allocation_pct"),
        "gap_to_target_pct": posture.get("gap_to_target_pct"),
        "weighted_duration": duration.get("weighted_duration"),
        "weighted_yield": yld.get("weighted_yield"),
        "treasury_pct": yld.get("treasury_pct"),
        "credit_pct": yld.get("credit_pct"),
        "years_of_withdrawal_coverage": withdrawal.get("years_of_withdrawal_coverage"),
        "coverage_shortfall_amount": withdrawal.get("coverage_shortfall_amount"),
        "first_uncovered_year": withdrawal.get("first_uncovered_year"),
        "rollover_concentration_max_pct": ladder.get("rollover_concentration_max_pct"),
        "top_policy_recommendations": ladder.get("top_policy_recommendations") if isinstance(ladder.get("top_policy_recommendations"), list) else [],
        "warnings": policy.get("warnings") if isinstance(policy.get("warnings"), list) else [],
        "purchase_action_required": purchase.get("action_required"),
        "purchase_recommendation_state": purchase.get("recommendation_state"),
        "purchase_target_rung": purchase.get("target_rung"),
        "purchase_reason_codes": purchase.get("reason_codes") if isinstance(purchase.get("reason_codes"), list) else [],
        "recommended_trade": purchase.get("recommended_trade") if isinstance(purchase.get("recommended_trade"), dict) else None,
        "candidate_rankings": purchase.get("candidate_rankings") if isinstance(purchase.get("candidate_rankings"), list) else [],
        "purchase_warnings": purchase.get("warnings") if isinstance(purchase.get("warnings"), list) else [],
        "purchase_portfolio_context": purchase.get("portfolio_context") if isinstance(purchase.get("portfolio_context"), dict) else {},
        "macro_regime_label": macro_regime.get("regime_label"),
        "bond_strategy_label": macro_regime.get("bond_strategy_label") or overlay.get("bond_strategy_label"),
        "portfolio_role": macro_regime.get("portfolio_role") or overlay.get("portfolio_role"),
        "macro_fallback_used": macro_regime.get("fallback_used"),
        "macro_fallback_message": macro_regime.get("fallback_message"),
        "macro_reason_codes": macro_regime.get("reason_codes") if isinstance(macro_regime.get("reason_codes"), list) else [],
        "proxy_metrics": macro_regime.get("proxy_metrics") if isinstance(macro_regime.get("proxy_metrics"), dict) else {},
        "action_state": overlay.get("action_state"),
        "mismatch_severity": overlay.get("mismatch_severity"),
        "duration_target_years_min": overlay.get("duration_target_years_min"),
        "duration_target_years_max": overlay.get("duration_target_years_max"),
        "treasury_target_weight": overlay.get("treasury_target_weight"),
        "ig_target_weight": overlay.get("ig_target_weight"),
        "liquidity_reserve_horizon_years": overlay.get("liquidity_reserve_horizon_years"),
        "rollover_cap_target_pct": overlay.get("rollover_cap_target_pct"),
        "duration_mismatch": overlay.get("duration_mismatch"),
        "credit_mix_mismatch": overlay.get("credit_mix_mismatch") if isinstance(overlay.get("credit_mix_mismatch"), list) else [],
        "liquidity_mismatch": overlay.get("liquidity_mismatch"),
        "rollover_concentration_mismatch": overlay.get("rollover_concentration_mismatch"),
        "ladder_shape_mismatch": overlay.get("ladder_shape_mismatch") if isinstance(overlay.get("ladder_shape_mismatch"), list) else [],
        "top_mismatch_reasons": overlay.get("top_mismatch_reasons") if isinstance(overlay.get("top_mismatch_reasons"), list) else [],
        "mismatch_summary": overlay.get("mismatch_summary") if isinstance(overlay.get("mismatch_summary"), list) else [],
        "why_now": overlay.get("why_now") if isinstance(overlay.get("why_now"), list) else [],
        "current_vs_target": overlay.get("current_vs_target") if isinstance(overlay.get("current_vs_target"), dict) else {},
        "ladder_shape_label": ladder_shape.get("ladder_shape_label"),
        "current_ladder": ladder_shape.get("current_maturity_buckets") if isinstance(ladder_shape.get("current_maturity_buckets"), list) else [],
        "recommended_ladder": ladder_shape.get("target_maturity_buckets") if isinstance(ladder_shape.get("target_maturity_buckets"), list) else [],
        "ladder_bucket_deltas": ladder_shape.get("bucket_deltas") if isinstance(ladder_shape.get("bucket_deltas"), list) else [],
        "explanation_headline": explanation.get("headline"),
        "explanation_summary": explanation.get("plain_language_summary"),
        "operator_next_step": explanation.get("operator_next_step"),
        "input_quality": input_quality,
    }

    family_statuses = [str(v.get("status") or "MISSING") for v in families_out.values() if isinstance(v, dict)]
    authority_head_present = all(
        isinstance(v, dict) and Path(str(v.get("authority_head_path") or "")).exists()
        for v in families_out.values()
    ) if families_out else False
    overall_state = "MISSING"
    if family_statuses:
        if any(s in ("FAIL", "ABORTED") for s in family_statuses):
            overall_state = "FAIL"
        elif any(s in ("UNKNOWN", "MISSING") for s in family_statuses):
            overall_state = "DEGRADED"
        else:
            overall_state = "PASS"

    holdings_state_label = "OPERATOR_HOLDINGS_LOADED" if bool(operator_holdings.get("present")) else "OPERATOR_HOLDINGS_MISSING"
    recommendation_state_label = "NO_CURRENT_ARTIFACT"
    if bool(merged_artifacts):
        recommendation_state_label = "CURRENT_DAY_RECOMMENDATION_AVAILABLE" if used_day == selected_day else "USING_LAST_AVAILABLE_RECOMMENDATION"
    authority_state_label = "AUTHORITY_HEAD_AVAILABLE" if authority_head_present else "DISPLAY_HEAD_AVAILABLE_ONLY"
    authority_missing_label = "AUTHORITY_HEAD_PRESENT" if authority_head_present else "AUTHORITY_HEAD_MISSING"
    if not authority_head_present:
        warnings.append("AUTHORITY_HEAD_MISSING")

    out: Dict[str, Any] = {
        "selected_day": selected_day,
        "last_available_day": last_available_day,
        "used_day": used_day,
        "used_selected_day": used_day == selected_day,
        "using_fallback_day": used_day != selected_day,
        "current_day_unavailable_reason": current_day_unavailable_reason,
        "status": overall_state,
        "state_labels": sorted(
            set(
                [
                    recommendation_state_label,
                    holdings_state_label,
                    authority_state_label,
                    authority_missing_label,
                    "OPERATOR_HOLDINGS_LOADED" if holdings_state_label == "OPERATOR_HOLDINGS_LOADED" else "OPERATOR_HOLDINGS_MISSING",
                ]
            )
        ),
        "recommendation_state_label": recommendation_state_label,
        "holdings_state_label": holdings_state_label,
        "authority_state_label": authority_state_label,
        "operator_holdings": operator_holdings,
        "run_metadata": run_metadata,
        "families": families_out,
        "summary": summary,
        "available": bool(merged_artifacts),
    }
    return out, sorted(set(warnings)), sorted(set(missing_paths)), sorted(set(source_paths)), source_mtimes


# -------------------------
# Attempts (V2) discovery
# -------------------------

# -------------------------
# Orchestrator attempt mode/account
# -------------------------

def _attempt_mode_and_account(truth_root: Path, day: str, attempt_id: Optional[str]) -> Tuple[Optional[str], Optional[str], List[str], List[str]]:
    """
    Reads orchestrator_attempt_manifest.v2.json for selected attempt.
    Returns (mode, ib_account, warnings, missing_paths).
    """
    warnings: List[str] = []
    missing: List[str] = []

    if not isinstance(attempt_id, str) or not attempt_id.strip():
        warnings.append("ATTEMPT_ID_MISSING_FOR_MODE_ACCOUNT")
        return None, None, warnings, missing

    p = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day / attempt_id.strip() / "orchestrator_attempt_manifest.v2.json").resolve()
    if not p.exists():
        missing.append(str(p))
        warnings.append("ATTEMPT_MANIFEST_MISSING_FOR_MODE_ACCOUNT")
        return None, None, warnings, missing

    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        warnings.append(f"ATTEMPT_MANIFEST_UNREADABLE:{err}")
        return None, None, warnings, missing

    mode_raw = obj.get("mode")
    acct_raw = obj.get("ib_account")

    mode = str(mode_raw).upper().strip() if isinstance(mode_raw, str) else None
    acct = str(acct_raw).strip() if isinstance(acct_raw, str) else None

    if mode not in ("PAPER", "LIVE"):
        warnings.append("ATTEMPT_MANIFEST_MODE_INVALID")
        mode = None
    if not acct:
        warnings.append("ATTEMPT_MANIFEST_IB_ACCOUNT_MISSING")
        acct = None

    return mode, acct, warnings, missing


def _attempt_stage_status(doc: Optional[Dict[str, Any]], stage_id: str) -> Optional[str]:
    if not isinstance(doc, dict):
        return None
    stages = doc.get("stages")
    if not isinstance(stages, list):
        return None
    for s in stages:
        if not isinstance(s, dict):
            continue
        if str(s.get("stage_id") or "") != stage_id:
            continue
        st = s.get("status")
        if isinstance(st, str) and st.strip():
            return st.strip().upper()
        return None
    return None


def _attempt_stage_view(doc: Optional[Dict[str, Any]], stage_id: str) -> Dict[str, Any]:
    if not isinstance(doc, dict):
        return {"present": False, "status": None, "reason_codes": []}
    stages = doc.get("stages")
    if not isinstance(stages, list):
        return {"present": False, "status": None, "reason_codes": []}
    for s in stages:
        if not isinstance(s, dict):
            continue
        if str(s.get("stage_id") or "") != stage_id:
            continue
        status = s.get("status")
        return {
            "present": True,
            "status": str(status).upper() if isinstance(status, str) and status.strip() else None,
            "reason_codes": s.get("reason_codes") if isinstance(s.get("reason_codes"), list) else [],
        }
    return {"present": False, "status": None, "reason_codes": []}


def _discover_phasec_identity_dirs(truth_root: Path, day: str) -> List[Path]:
    root = (truth_root / "phaseC_preflight_v1" / day).resolve()
    if not root.exists() or not root.is_dir():
        return []
    out: List[Path] = []
    for p in sorted(root.iterdir(), key=lambda x: x.name):
        if not p.is_dir():
            continue
        supported = (
            ((p / "equity_order_plan.v2.json").exists() and (p / "mapping_ledger_record.v2.json").exists() and (p / "binding_record.v2.json").exists())
            or ((p / "equity_order_plan.v1.json").exists() and (p / "mapping_ledger_record.v2.json").exists() and (p / "binding_record.v2.json").exists())
            or ((p / "order_plan.v1.json").exists() and (p / "mapping_ledger_record.v1.json").exists() and (p / "binding_record.v1.json").exists())
        )
        if supported:
            out.append(p.resolve())
    return out


def _jsonl_same_day_presence(path: Path, day: str) -> bool:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                row_day = str(
                    obj.get("day_utc")
                    or obj.get("date")
                    or obj.get("date_utc")
                    or obj.get("session_date")
                    or obj.get("timestamp_utc")
                    or ""
                ).strip()
                if row_day == day:
                    return True
                if len(row_day) >= 10 and row_day[:10] == day:
                    return True
    except FileNotFoundError:
        return False
    except Exception:
        return False
    return False


def _compute_signal_frequency_30d(truth_root: Path, day: str) -> Dict[str, Any]:
    try:
        ref_day = datetime.strptime(day, "%Y-%m-%d").date()
    except Exception:
        return {
            "window_days": 30,
            "avg_signals_per_day": None,
            "last_signal_date": None,
            "last_submit_date": None,
            "status": "UNKNOWN",
            "label": "Signal frequency unavailable",
        }

    window_days: List[str] = []
    for offset in range(29, -1, -1):
        window_days.append((ref_day - timedelta(days=offset)).isoformat())

    total_signals = 0
    last_signal_date: Optional[str] = None
    last_submit_date: Optional[str] = None
    for day_key in window_days:
        intents_count, _ = _count_intents(truth_root, day_key)
        sub_counts, _ = _count_submissions_and_fills(truth_root, day_key)
        total_signals += intents_count
        if intents_count > 0:
            last_signal_date = day_key
        if isinstance(sub_counts, dict) and int(sub_counts.get("submitted", 0) or 0) > 0:
            last_submit_date = day_key

    avg_signals = total_signals / 30.0
    days_since_signal: Optional[int] = None
    if isinstance(last_signal_date, str):
        days_since_signal = (ref_day - datetime.strptime(last_signal_date, "%Y-%m-%d").date()).days

    status = "positive"
    label = "Within expected signal band"
    if last_signal_date is None or days_since_signal is None or days_since_signal > 14:
        status = "negative"
        label = "No recent signals in the last two weeks"
    elif avg_signals < 0.20 or days_since_signal > 7:
        status = "warning"
        label = "Signals are light but still within tolerance"

    return {
        "window_days": 30,
        "avg_signals_per_day": round(avg_signals, 2),
        "last_signal_date": last_signal_date,
        "last_submit_date": last_submit_date,
        "status": status,
        "label": label,
    }


def _load_gate_stack_day_state(truth_root: Path, day: str) -> Dict[str, Any]:
    path = (truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json").resolve()
    obj, err = _safe_read_json(path)
    if not isinstance(obj, dict):
        return {
            "status": "UNKNOWN",
            "reason_codes": ["ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE"],
            "path": str(path),
        }
    return {
        "status": _coerce_state(str(obj.get("status") or "UNKNOWN")),
        "reason_codes": _top2_reason_codes(obj.get("reason_codes")),
        "path": str(path),
    }


def _load_kill_switch_day_state(truth_root: Path, day: str) -> Dict[str, Any]:
    path = (truth_root / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json").resolve()
    obj, err = _safe_read_json(path)
    if not isinstance(obj, dict):
        return {
            "state": "UNKNOWN",
            "allow_entries": None,
            "allow_exits": None,
            "reason_codes": ["ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE"],
            "path": str(path),
        }
    return {
        "state": str(obj.get("state") or "UNKNOWN").upper(),
        "allow_entries": obj.get("allow_entries") if isinstance(obj.get("allow_entries"), bool) else None,
        "allow_exits": obj.get("allow_exits") if isinstance(obj.get("allow_exits"), bool) else None,
        "reason_codes": _top2_reason_codes(obj.get("reason_codes")),
        "path": str(path),
    }


def _derive_trading_day_outcome(
    *,
    day: str,
    run_doc: Optional[Dict[str, Any]],
    gate_stack: Dict[str, Any],
    kill_switch: Dict[str, Any],
    intent_count: int,
    veto_count: int,
    released_identity_dir_count: int,
    submit_stage: Dict[str, Any],
    upstream_rows: List[Dict[str, Any]],
    expected_heartbeat_count: int,
    present_heartbeat_count: int,
) -> Dict[str, Any]:
    gate_status = str(gate_stack.get("status") or "UNKNOWN").upper()
    gate_pass = gate_status == "PASS"
    kill_state = str(kill_switch.get("state") or "UNKNOWN").upper()
    allow_entries = kill_switch.get("allow_entries") is True
    upstream_ready = all(bool(row.get("same_day_present")) for row in upstream_rows) if upstream_rows else False
    submit_status = str(submit_stage.get("status") or "UNKNOWN").upper()
    submit_reason_codes = [str(x).upper() for x in (submit_stage.get("reason_codes") if isinstance(submit_stage.get("reason_codes"), list) else [])]
    orchestrator_status = str((run_doc or {}).get("status") or "UNKNOWN").upper()
    execution_path = "HEALTHY" if orchestrator_status in {"PASS", "DEGRADED"} and gate_pass and kill_state == "INACTIVE" and allow_entries else "BLOCKED"

    classification = "EXECUTION_FAILURE_DAY"
    label = "EXECUTION BLOCKED"
    subtitle = "See active blocker below"
    tone = "negative"

    if gate_pass and kill_state == "INACTIVE" and allow_entries and upstream_ready and intent_count == 0 and released_identity_dir_count == 0 and submit_status == "SKIP" and submit_reason_codes == ["SKIP_NOT_REQUIRED_NO_ACTIVITY"]:
        classification = "HEALTHY_NO_SIGNAL_DAY"
        label = "NO SIGNAL DAY"
        subtitle = "Healthy execution path; no submit was required"
        tone = "positive"
    elif intent_count > 0 and veto_count > 0 and released_identity_dir_count == 0:
        classification = "VETO_ONLY_DAY"
        label = "VETO-ONLY DAY"
        subtitle = "Signals were produced but filtered by governed preflight"
        tone = "warning"
    elif released_identity_dir_count > 0 and submit_stage.get("present"):
        classification = "SUBMITTABLE_DAY"
        label = "SUBMIT PATH REACHED"
        subtitle = "Identities were released and governed submit was reached"
        tone = "info"
    elif gate_status == "FAIL" or kill_state == "ACTIVE" or (kill_switch.get("allow_entries") is False) or not upstream_ready or orchestrator_status in {"ABORTED", "FAIL"}:
        classification = "EXECUTION_FAILURE_DAY"
        label = "EXECUTION BLOCKED"
        subtitle = "See active blocker below"
        tone = "negative"

    if classification == "VETO_ONLY_DAY" and submit_status == "FAIL":
        tone = "negative"
    facts = [
        {"label": "Gates", "value": gate_status},
        {"label": "Data", "value": "READY" if upstream_ready else "MISSING"},
        {"label": "Heartbeats", "value": f"{present_heartbeat_count} / {expected_heartbeat_count}"},
        {"label": "Intents", "value": str(intent_count)},
        {"label": "Phase C identities", "value": str(released_identity_dir_count)},
        {"label": "Submit", "value": submit_status if submit_status != "UNKNOWN" else "NOT_REACHED"},
    ]
    if classification == "HEALTHY_NO_SIGNAL_DAY":
        facts[5]["value"] = "SKIPPED"
    elif classification == "VETO_ONLY_DAY":
        facts[5]["value"] = "NOT REACHED"

    return {
        "day_utc": day,
        "classification": classification,
        "label": label,
        "subtitle": subtitle,
        "tone": tone,
        "execution_path": execution_path,
        "facts": facts,
        "gate_stack": gate_stack,
        "kill_switch": kill_switch,
        "orchestrator_status": orchestrator_status,
        "submit_reason_codes": submit_reason_codes,
    }



def _derive_operational_readiness(
    day: str,
    platform_readiness: Dict[str, Any],
    signal_activity: Dict[str, Any],
    trading_day_state: Dict[str, Any],
    day_start_blocked: Dict[str, Any],
) -> Dict[str, Any]:
    readiness = platform_readiness if isinstance(platform_readiness, dict) else {}
    signal = signal_activity if isinstance(signal_activity, dict) else {}
    trading = trading_day_state if isinstance(trading_day_state, dict) else {}
    blocked = day_start_blocked if isinstance(day_start_blocked, dict) else {}

    heartbeats = signal.get("engine_heartbeats") if isinstance(signal.get("engine_heartbeats"), dict) else {}
    upstream = signal.get("upstream_data_status") if isinstance(signal.get("upstream_data_status"), dict) else {}
    outcome = signal.get("trading_day_outcome") if isinstance(signal.get("trading_day_outcome"), dict) else {}
    gate_stack = outcome.get("gate_stack") if isinstance(outcome.get("gate_stack"), dict) else {}
    kill_switch = outcome.get("kill_switch") if isinstance(outcome.get("kill_switch"), dict) else {}

    expected_heartbeats = int(heartbeats.get("expected_count") or 0)
    present_heartbeats = int(heartbeats.get("present_count") or 0)
    upstream_rows = upstream.get("symbols") if isinstance(upstream.get("symbols"), list) else []
    upstream_ready = bool(upstream_rows) and all(bool(row.get("same_day_present")) for row in upstream_rows if isinstance(row, dict))
    same_day_structural_artifact = (
        readiness.get("requested_day_present") is True
        and readiness.get("authoritative_for_family") is True
        and str(readiness.get("resolved_day") or "") == day
    )
    gate_pass = str(gate_stack.get("status") or "UNKNOWN").upper() == "PASS"
    kill_switch_inactive = str(kill_switch.get("state") or "UNKNOWN").upper() == "INACTIVE"
    allow_entries = kill_switch.get("allow_entries") is True
    trading_heartbeat_pass = str(trading.get("heartbeat_status") or "UNKNOWN").upper() == "PASS"
    day_blocked = blocked.get("blocked") is True

    reasons: List[str] = []
    if not same_day_structural_artifact:
        if readiness.get("resolved_via_latest_pointer") is True:
            reasons.append("STRUCTURAL_READINESS_STALE_LATEST_POINTER")
        else:
            reasons.append("STRUCTURAL_READINESS_SELECTED_DAY_MISSING")
    if not upstream_ready:
        reasons.append("UPSTREAM_DATA_SELECTED_DAY_MISSING")
    if expected_heartbeats <= 0:
        reasons.append("ENGINE_HEARTBEAT_EXPECTATION_MISSING")
    elif present_heartbeats != expected_heartbeats:
        reasons.append(f"ENGINE_HEARTBEATS_INCOMPLETE:{present_heartbeats}/{expected_heartbeats}")
    if not gate_pass:
        reasons.append(f"GATE_STACK_{str(gate_stack.get('status') or 'UNKNOWN').upper()}")
    if not kill_switch_inactive:
        reasons.append(f"KILL_SWITCH_{str(kill_switch.get('state') or 'UNKNOWN').upper()}")
    if not allow_entries:
        reasons.append("KILL_SWITCH_ENTRIES_NOT_ALLOWED")
    if not trading_heartbeat_pass:
        reasons.append(f"TRADING_DAY_HEARTBEAT_{str(trading.get('heartbeat_status') or 'UNKNOWN').upper()}")
    if day_blocked:
        reasons.append("DAY_START_BLOCKED")

    state = "READY" if not reasons else "NOT_READY"
    summary = (
        "Selected-day operational readiness confirmed from same-day authoritative inputs."
        if state == "READY"
        else "Selected-day operational readiness failed closed because one or more same-day authoritative inputs are missing or failing."
    )
    return {
        "day_utc": day,
        "state": state,
        "label": "Operational Readiness",
        "summary": summary,
        "same_day_authoritative_inputs_only": True,
        "checks": {
            "selected_day_structural_artifact_present": same_day_structural_artifact,
            "selected_day_structural_artifact_requested_day_present": readiness.get("requested_day_present") is True,
            "selected_day_structural_artifact_resolved_day": readiness.get("resolved_day"),
            "selected_day_structural_artifact_resolution_mode": readiness.get("resolution_mode"),
            "selected_day_upstream_ready": upstream_ready,
            "selected_day_heartbeat_complete": expected_heartbeats > 0 and present_heartbeats == expected_heartbeats,
            "selected_day_gate_stack_pass": gate_pass,
            "selected_day_kill_switch_inactive": kill_switch_inactive,
            "selected_day_entries_allowed": allow_entries,
            "selected_day_trading_heartbeat_pass": trading_heartbeat_pass,
            "selected_day_blocked": day_blocked,
        },
        "reason_codes": reasons,
    }


def _summary_scalar(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        text = str(value).strip()
        return text or None
    return None


def _artifact_display_state(obj: Dict[str, Any]) -> str:
    for key in (
        "status",
        "state",
        "publication_status",
        "candidate_status",
        "review_status",
        "overall_status",
        "decision",
    ):
        value = _summary_scalar(obj.get(key))
        if value:
            return value.upper()
    return "PRESENT"


def _artifact_produced_utc(obj: Dict[str, Any]) -> Optional[str]:
    for key in ("produced_utc", "created_at_utc", "created_at", "event_time_utc", "day_utc"):
        value = _summary_scalar(obj.get(key))
        if value:
            return value
    return None


def _artifact_summary_rows(
    obj: Dict[str, Any],
    *,
    preferred_fields: Tuple[Tuple[str, str], ...],
    list_count_fields: Tuple[Tuple[str, str], ...] = (),
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen: set[str] = set()
    for field, label in preferred_fields:
        value = _summary_scalar(obj.get(field))
        if value and label not in seen:
            rows.append({"label": label, "value": value})
            seen.add(label)
    for field, label in list_count_fields:
        value = obj.get(field)
        if isinstance(value, list) and label not in seen:
            rows.append({"label": label, "value": str(len(value))})
            seen.add(label)
    for fallback_field, fallback_label in (("schema_id", "schema_id"), ("schema_version", "schema_version")):
        value = _summary_scalar(obj.get(fallback_field))
        if value and fallback_label not in seen:
            rows.append({"label": fallback_label, "value": value})
            seen.add(fallback_label)
    return rows[:6]


def _load_json_artifact_summary(
    path: Path,
    *,
    label: str,
    preferred_fields: Tuple[Tuple[str, str], ...],
    list_count_fields: Tuple[Tuple[str, str], ...] = (),
) -> Dict[str, Any]:
    resolved = path.resolve()
    base = {
        "label": label,
        "present": False,
        "path": str(resolved),
        "sha256": _sha256_file(resolved) if resolved.exists() and resolved.is_file() else None,
        "display_state": "MISSING",
        "produced_utc": None,
        "reason_codes": ["FILE_NOT_FOUND"] if not resolved.exists() else [],
        "summary_rows": [],
    }
    obj, err = _safe_read_json(resolved)
    if not isinstance(obj, dict):
        if resolved.exists():
            return {
                **base,
                "display_state": "UNREADABLE",
                "reason_codes": [err or "READ_ERROR"],
            }
        return base
    return {
        **base,
        "present": True,
        "display_state": _artifact_display_state(obj),
        "produced_utc": _artifact_produced_utc(obj),
        "reason_codes": _top2_reason_codes(obj.get("reason_codes")),
        "summary_rows": _artifact_summary_rows(
            obj,
            preferred_fields=preferred_fields,
            list_count_fields=list_count_fields,
        ),
    }


def _latest_day_file(day_root: Path, pattern: str) -> Optional[Path]:
    if not day_root.exists() or not day_root.is_dir():
        return None
    matches = sorted((p.resolve() for p in day_root.glob(pattern) if p.is_file()), key=lambda p: str(p))
    return matches[-1] if matches else None


def _select_decision_plan_path(mode_root: Path, day: str) -> Optional[Path]:
    if not mode_root.exists() or not mode_root.is_dir():
        return None
    candidates: List[Tuple[int, str, str, Path]] = []
    for path in sorted(mode_root.rglob("decision_plan.v1.json"), key=lambda p: str(p)):
        if "canonical" not in path.parts:
            continue
        obj, _err = _safe_read_json(path)
        day_match = 1 if day in path.parts else 0
        produced_utc = ""
        if isinstance(obj, dict):
            produced_utc = str(obj.get("produced_utc") or obj.get("created_at") or "").strip()
            if str(obj.get("day_utc") or "").strip() == day or produced_utc.startswith(f"{day}T"):
                day_match = 1
        candidates.append((day_match, produced_utc, str(path), path.resolve()))
    return candidates[-1][3] if candidates else None


def _build_advisor_visibility(
    *,
    truth_root: Path,
    day: str,
    mode: str,
    platform_readiness: Dict[str, Any],
    operational_readiness: Dict[str, Any],
) -> Dict[str, Any]:
    mode_root = (ADVISOR_RUNTIME_ROOT / mode).resolve()
    decision_plan_path = _select_decision_plan_path(mode_root, day)
    official_path = (mode_root / "official_recommendation_set_v1" / day / "official_recommendation_set.v1.json").resolve()
    promotion_candidate_path = (mode_root / "promotion_candidate_v1" / day / "promotion_candidate.v1.json").resolve()
    publication_path = (mode_root / "publication_gate_result_v1" / day / "publication_gate_result.v1.json").resolve()
    authority_registry_path = (mode_root / "reports" / "authority_registry_v1" / day / "authority_registry.v1.json").resolve()
    replay_manifest_path = _latest_day_file((truth_root / "reports" / "replay_manifest_v1" / day).resolve(), "*.replay_manifest.v1.json")
    runtime_trace_bundle_path = _latest_day_file((truth_root / "reports" / "runtime_trace_bundle_v1" / day).resolve(), "*.runtime_trace_bundle.v1.json")
    execution_truth_path = (truth_root / "reports" / "execution_completion_gap_report_v1" / day / "execution_completion_gap_report.v1.json").resolve()
    gate_authority_path = (truth_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json").resolve()
    canonical_head_path = (truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()

    decision_plan = _load_json_artifact_summary(
        decision_plan_path if decision_plan_path is not None else mode_root / "canonical" / "decision_plan.v1.json",
        label="decision_plan",
        preferred_fields=(("plan_id", "plan_id"), ("planning_snapshot_id", "planning_snapshot_id"), ("mode", "mode")),
        list_count_fields=(("actions", "actions"), ("blocked_actions", "blocked_actions")),
    )
    official_recommendation_set = _load_json_artifact_summary(
        official_path,
        label="official_recommendation_set",
        preferred_fields=(("advisory_packet_id", "advisory_packet_id"), ("version", "version")),
        list_count_fields=(("recommendations", "recommendations"),),
    )
    promotion_candidate = _load_json_artifact_summary(
        promotion_candidate_path,
        label="promotion_candidate",
        preferred_fields=(
            ("candidate_id", "candidate_id"),
            ("candidate_status", "candidate_status"),
            ("candidate_class", "candidate_class"),
            ("decision_plan_id", "decision_plan_id"),
        ),
    )
    publication_gate_result = _load_json_artifact_summary(
        publication_path,
        label="publication_gate_result",
        preferred_fields=(
            ("publication_status", "publication_status"),
            ("decision", "decision"),
            ("decision_plan_id", "decision_plan_id"),
        ),
        list_count_fields=(("results", "results"),),
    )
    authority_registry = _load_json_artifact_summary(
        authority_registry_path,
        label="authority_registry",
        preferred_fields=(("day_utc", "day_utc"), ("mode", "mode")),
        list_count_fields=(("artifact_families", "artifact_families"), ("rows", "rows"), ("registry_rows", "registry_rows")),
    )

    execution_truth = _load_json_artifact_summary(
        execution_truth_path,
        label="execution_truth",
        preferred_fields=(("status", "status"), ("day_utc", "day_utc")),
    )
    gate_authority = _load_json_artifact_summary(
        gate_authority_path,
        label="gate_authority",
        preferred_fields=(("status", "status"), ("day_utc", "day_utc")),
    )
    replay_manifest = _load_json_artifact_summary(
        replay_manifest_path if replay_manifest_path is not None else (truth_root / "reports" / "replay_manifest_v1" / day / "missing.replay_manifest.v1.json"),
        label="replay_manifest",
        preferred_fields=(("status", "status"), ("day_utc", "day_utc")),
    )
    runtime_trace_bundle = _load_json_artifact_summary(
        runtime_trace_bundle_path if runtime_trace_bundle_path is not None else (truth_root / "reports" / "runtime_trace_bundle_v1" / day / "missing.runtime_trace_bundle.v1.json"),
        label="runtime_trace_bundle",
        preferred_fields=(("status", "status"), ("day_utc", "day_utc")),
        list_count_fields=(("stages", "stages"),),
    )

    canonical_head_obj, canonical_head_err = _safe_read_json(canonical_head_path)
    canonical_head_present = isinstance(canonical_head_obj, dict)
    canonical_head_authoritative = bool(canonical_head_obj.get("authoritative")) if canonical_head_present else False
    canonical_head_status = str(canonical_head_obj.get("status") or "MISSING").strip().upper() if canonical_head_present else "MISSING"
    gate_authority_detail = (
        f"authorization={gate_authority['display_state']} • canonical_head={canonical_head_status}"
        if canonical_head_present
        else f"authorization={gate_authority['display_state']} • canonical_head={canonical_head_err or 'FILE_NOT_FOUND'}"
    )
    publication_detail = (
        f"publication={publication_gate_result['display_state']} • promotion_candidate={promotion_candidate['display_state']}"
    )
    diagnostics_state = str(platform_readiness.get("platform_readiness_state") or operational_readiness.get("state") or "UNKNOWN").upper()
    diagnostics_detail = str(platform_readiness.get("readiness_summary") or operational_readiness.get("summary") or "No diagnostics summary available.")

    evidence_spine = [
        {
            "id": "advisorEvidenceExecutionTruth",
            "label": "execution truth",
            "state": execution_truth["display_state"],
            "detail": execution_truth["reason_codes"][0] if execution_truth["reason_codes"] else "Execution truth artifact-backed",
            "path": execution_truth["path"],
            "sha256": execution_truth["sha256"],
        },
        {
            "id": "advisorEvidenceGateAuthority",
            "label": "gate authority",
            "state": "PASS" if gate_authority["present"] and canonical_head_present and canonical_head_authoritative else gate_authority["display_state"],
            "detail": gate_authority_detail,
            "path": gate_authority["path"],
            "sha256": gate_authority["sha256"],
        },
        {
            "id": "advisorEvidencePublicationPromotion",
            "label": "publication / promotion",
            "state": publication_gate_result["display_state"] if publication_gate_result["present"] else promotion_candidate["display_state"],
            "detail": publication_detail,
            "path": publication_gate_result["path"] if publication_gate_result["present"] else promotion_candidate["path"],
            "sha256": publication_gate_result["sha256"] if publication_gate_result["present"] else promotion_candidate["sha256"],
        },
        {
            "id": "advisorEvidenceReplayIntegrity",
            "label": "replay integrity",
            "state": replay_manifest["display_state"],
            "detail": replay_manifest["reason_codes"][0] if replay_manifest["reason_codes"] else "Replay manifest artifact-backed",
            "path": replay_manifest["path"],
            "sha256": replay_manifest["sha256"],
        },
        {
            "id": "advisorEvidenceTraceIntegrity",
            "label": "trace integrity",
            "state": runtime_trace_bundle["display_state"],
            "detail": runtime_trace_bundle["reason_codes"][0] if runtime_trace_bundle["reason_codes"] else "Runtime trace bundle artifact-backed",
            "path": runtime_trace_bundle["path"],
            "sha256": runtime_trace_bundle["sha256"],
        },
        {
            "id": "advisorEvidenceDiagnostics",
            "label": "diagnostics",
            "state": diagnostics_state,
            "detail": diagnostics_detail,
            "path": str(platform_readiness.get("path") or ""),
            "sha256": _sha256_file(Path(str(platform_readiness.get("path") or "")).resolve()) if str(platform_readiness.get("path") or "").strip() else None,
        },
    ]

    advisor_artifacts = {
        "decision_plan": decision_plan,
        "official_recommendation_set": official_recommendation_set,
        "promotion_candidate": promotion_candidate,
        "publication_gate_result": publication_gate_result,
        "authority_registry": authority_registry,
    }
    has_any_artifacts = any(artifact["present"] for artifact in advisor_artifacts.values())
    last_validated_candidates = [
        artifact.get("produced_utc")
        for artifact in advisor_artifacts.values()
        if isinstance(artifact.get("produced_utc"), str) and artifact.get("produced_utc")
    ] + [
        produced
        for produced in (
            execution_truth.get("produced_utc"),
            gate_authority.get("produced_utc"),
            replay_manifest.get("produced_utc"),
            runtime_trace_bundle.get("produced_utc"),
            platform_readiness.get("produced_utc"),
        )
        if isinstance(produced, str) and produced
    ]
    last_validated = max(last_validated_candidates) if last_validated_candidates else "n/a"
    system_state = str(operational_readiness.get("state") or ("ARTIFACTS_PRESENT" if has_any_artifacts else "NO_ADVISOR_ARTIFACTS")).upper()
    why = (
        "No advisor artifacts found"
        if not has_any_artifacts
        else publication_detail
    )
    shell = {
        "system_state": system_state,
        "allowed_action": "READ_ONLY_REVIEW" if has_any_artifacts else "NO_OPERATOR_ACTION",
        "why": why,
        "last_validated": last_validated,
    }
    chain_sha256 = _sha256_bytes(_stable_json_bytes({"shell": shell, "evidence_spine": evidence_spine, "artifacts": advisor_artifacts}))
    return {
        "mode": mode,
        "day_utc": day,
        "chain_sha256": chain_sha256,
        "has_any_artifacts": has_any_artifacts,
        "shell": shell,
        "evidence_spine": evidence_spine,
        "artifacts": advisor_artifacts,
        "empty_state": {
            "title": "No advisor artifacts found",
            "detail": "Advisor panel is read-only and artifact-backed",
        },
    }


# -------------------------
# Tile readers
# -------------------------

def _parse_simple_gate_tile(path: Path, tile_id: str) -> Tuple[Tile, List[str], List[str]]:
    """
    Deterministic read of a single gate artifact.
    Returns (tile, warnings, missing_paths)
    """
    warnings: List[str] = []
    missing: List[str] = []

    if not path.exists():
        missing.append(str(path))
        return Tile(
            tile_id=tile_id,
            state="MISSING",
            last_updated_utc=None,
            reason_codes=["MISSING_GATE_ARTIFACT"],
            reason_human=[],
            artifact_path=str(path),
            artifact_sha256=None,
        ), warnings, missing

    obj, err = _safe_read_json(path)
    if not isinstance(obj, dict):
        warnings.append(f"GATE_UNREADABLE:{err}")
        return Tile(
            tile_id=tile_id,
            state="UNKNOWN",
            last_updated_utc=None,
            reason_codes=[f"GATE_UNREADABLE:{err}"],
            reason_human=[],
            artifact_path=str(path),
            artifact_sha256=_sha256_file(path),
        ), warnings, missing

    # Common fields
    st = obj.get("state") or obj.get("status") or obj.get("verdict") or obj.get("run_verdict")
    if isinstance(st, dict):
        st = st.get("state") or st.get("status") or st.get("run_verdict")
    state = _coerce_state(str(st) if st is not None else "UNKNOWN")

    rc = obj.get("reason_codes") or obj.get("reason_codes_top") or []
    if isinstance(obj.get("verdict"), dict) and not rc:
        v = obj.get("verdict")
        rc = v.get("reason_codes") or v.get("reason_codes_top") or []

    last = obj.get("generated_at_utc") or obj.get("generated_utc") or obj.get("produced_utc") or obj.get("asof_utc") or None

    return Tile(
        tile_id=tile_id,
        state=state,
        last_updated_utc=str(last) if isinstance(last, str) and last else None,
        reason_codes=_top2_reason_codes(rc),
        reason_human=[],
        artifact_path=str(path),
        artifact_sha256=_sha256_file(path),
    ), warnings, missing


def _parse_gate_stack_verdict_tile(truth_root: Path, day: str) -> Tuple[Optional[Tile], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    p = (truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json").resolve()
    if not p.exists():
        missing.append(str(p))
        warnings.append("GATE_STACK_VERDICT_MISSING")
        return None, missing, [], {}, warnings

    obj, err = _safe_read_json(p)
    source_paths.append(str(p))
    mt = _mtime(p)
    if mt is not None:
        source_mtimes[str(p)] = mt

    if not isinstance(obj, dict):
        warnings.append(f"GATE_STACK_UNREADABLE:{err}")
        return None, missing, source_paths, source_mtimes, warnings

    v = obj.get("verdict") if isinstance(obj.get("verdict"), dict) else obj
    state = _coerce_state(str(v.get("state") or v.get("overall_state") or "UNKNOWN"))
    rc = v.get("reason_codes_top") or v.get("reason_codes") or []
    last = obj.get("generated_at_utc") or obj.get("generated_utc") or obj.get("asof_utc") or None

    tile = Tile(
        tile_id="gate_stack_verdict_v1",
        state=state,
        last_updated_utc=str(last) if isinstance(last, str) and last else None,
        reason_codes=_top2_reason_codes(rc),
        reason_human=[],
        artifact_path=str(p),
        artifact_sha256=_sha256_file(p),
    )
    return tile, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def _parse_orchestrator_run_verdict_v2(truth_root: Path, day: str, attempt_id: Optional[str]) -> Tuple[Optional[Tile], List[str], List[str], Dict[str, float], List[str]]:
    """
    Deterministic best-of selection:
    - Prefer attempt-scoped verdict file under .../day/<attempt_id>/orchestrator_run_verdict.v2.json
    - Else search day directory for any orchestrator_run_verdict.v2.json and choose best by:
      (has_attempt_fields, attempt_seq, produced_utc, path)
    """
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    day_dir = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day).resolve()
    if not day_dir.exists() or not day_dir.is_dir():
        missing.append(str(day_dir))
        warnings.append("RUN_VERDICT_NOT_FOUND")
        return None, missing, [], {}, warnings

    # 1) exact attempt file if attempt_id present
    if isinstance(attempt_id, str) and attempt_id.strip():
        p = (day_dir / attempt_id.strip() / "orchestrator_run_verdict.v2.json").resolve()
        if p.exists():
            obj, err = _safe_read_json(p)
            source_paths.append(str(p))
            mt = _mtime(p)
            if mt is not None:
                source_mtimes[str(p)] = mt
            if isinstance(obj, dict):
                state = _coerce_state(str(obj.get("status") or obj.get("state") or "UNKNOWN"))
                rc = obj.get("reason_codes") or obj.get("reason_codes_top") or []
                last = obj.get("run_completed_utc") or obj.get("produced_utc") or obj.get("generated_at_utc") or obj.get("generated_utc") or None
                tile = Tile(
                    tile_id="orchestrator_run_verdict_v2",
                    state=state,
                    last_updated_utc=str(last) if isinstance(last, str) and last else None,
                    reason_codes=_top2_reason_codes(rc),
                    reason_human=[],
                    artifact_path=str(p),
                    artifact_sha256=_sha256_file(p),
                )
                return tile, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))
            warnings.append(f"RUN_VERDICT_UNREADABLE:{err}")

    # 2) scan all attempt dirs for verdict files
    candidates: List[Path] = []
    for d in sorted([x for x in day_dir.iterdir() if x.is_dir()], key=lambda x: x.name):
        fp = (d / "orchestrator_run_verdict.v2.json").resolve()
        if fp.exists():
            candidates.append(fp)

    candidates = sorted(set(candidates), key=lambda p: str(p))

    best_tile: Optional[Tile] = None
    best_key: Optional[Tuple[int, int, str, str]] = None

    for p in candidates:
        obj, err = _safe_read_json(p)
        source_paths.append(str(p))
        mt = _mtime(p)
        if mt is not None:
            source_mtimes[str(p)] = mt

        if not isinstance(obj, dict):
            warnings.append(f"RUN_VERDICT_UNREADABLE:{p}:{err}")
            continue

        attempt_seq = obj.get("attempt_seq")
        attempt_id2 = obj.get("attempt_id")
        produced2 = obj.get("produced_utc")

        has_attempt = isinstance(attempt_seq, int) and attempt_seq > 0 and isinstance(attempt_id2, str) and attempt_id2.strip()
        produced_s = produced2 if isinstance(produced2, str) else ""

        key = (
            1 if has_attempt else 0,
            int(attempt_seq) if isinstance(attempt_seq, int) else -1,
            produced_s,
            str(p),
        )

        state = _coerce_state(str(obj.get("status") or obj.get("state") or "UNKNOWN"))
        rc = obj.get("reason_codes") or obj.get("reason_codes_top") or []
        last = obj.get("run_completed_utc") or obj.get("produced_utc") or obj.get("generated_at_utc") or obj.get("generated_utc") or None

        tile = Tile(
            tile_id="orchestrator_run_verdict_v2",
            state=state,
            last_updated_utc=str(last) if isinstance(last, str) and last else None,
            reason_codes=_top2_reason_codes(rc),
            reason_human=[],
            artifact_path=str(p),
            artifact_sha256=_sha256_file(p),
        )

        if best_key is None or key > best_key:
            best_key = key
            best_tile = tile

    if best_tile is None:
        warnings.append("RUN_VERDICT_NOT_FOUND")
    return best_tile, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def _parse_replay_tile(truth_root: Path, day: str, attempt_id: Optional[str]) -> Tuple[Optional[Tile], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    roots = _candidate_replay_roots(truth_root)
    candidates: List[Path] = []

    for r in roots:
        ddir = (r / day).resolve()
        if not ddir.exists() or not ddir.is_dir():
            missing.append(str(ddir))
            continue

        # attempt-scoped files first
        if isinstance(attempt_id, str) and attempt_id.strip():
            adir = (ddir / attempt_id.strip()).resolve()
            if adir.exists() and adir.is_dir():
                candidates.extend(sorted([p for p in adir.iterdir() if p.is_file() and p.suffix == ".json"], key=lambda p: p.name))

        candidates.extend(sorted([p for p in ddir.iterdir() if p.is_file() and p.suffix == ".json"], key=lambda p: p.name))

    # deterministic unique
    uniq: List[Path] = []
    seen = set()
    for p in candidates:
        sp = str(p)
        if not p.exists() or sp in seen:
            continue
        seen.add(sp)
        uniq.append(p)
    candidates = uniq

    for p in candidates:
        obj, err = _safe_read_json(p)
        source_paths.append(str(p))
        mt = _mtime(p)
        if mt is not None:
            source_mtimes[str(p)] = mt

        if not isinstance(obj, dict):
            warnings.append(f"REPLAY_UNREADABLE:{p}:{err}")
            continue

        st = obj.get("state") or obj.get("status") or obj.get("verdict") or obj.get("run_verdict")
        if isinstance(st, dict):
            st = st.get("state") or st.get("status")
        state = _coerce_state(str(st) if st is not None else "UNKNOWN")

        rc = obj.get("reason_codes") or obj.get("reason_codes_top") or []
        last = obj.get("generated_at_utc") or obj.get("generated_utc") or obj.get("asof_utc") or obj.get("produced_utc") or None

        tile = Tile(
            tile_id="replay_certification",
            state=state,
            last_updated_utc=str(last) if isinstance(last, str) and last else None,
            reason_codes=_top2_reason_codes(rc),
            reason_human=[],
            artifact_path=str(p),
            artifact_sha256=_sha256_file(p),
        )
        return tile, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))

    warnings.append("REPLAY_NOT_FOUND")
    return None, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


# -------------------------
# Engines from truth (no runtime config dependency)
# -------------------------

def _engine_ids_deep_scan(obj: Any) -> List[str]:
    eids: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "engine_id" and isinstance(v, str) and v.strip():
                    eids.append(v.strip())
                else:
                    walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(obj)
    return sorted(set(eids))


def _engine_ids_from_active_engine_set(truth_root: Path, day: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Primary engine list source (truth): reports/active_engine_set_v1/<day>/active_engine_set.v1.json
    Returns (engine_ids, missing_paths, warnings)
    """
    missing: List[str] = []
    warnings: List[str] = []

    p = (truth_root / "reports" / "active_engine_set_v1" / day / "active_engine_set.v1.json").resolve()
    if not p.exists():
        missing.append(str(p))
        warnings.append("ACTIVE_ENGINE_SET_MISSING")
        return [], missing, warnings

    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        warnings.append(f"ACTIVE_ENGINE_SET_UNREADABLE:{err}")
        return [], missing, warnings

    # common shapes
    for k in ("engine_ids", "active_engine_ids", "engines"):
        v = obj.get(k)
        if isinstance(v, list):
            out: List[str] = []
            for it in v:
                if isinstance(it, str) and it.strip():
                    out.append(it.strip())
                elif isinstance(it, dict):
                    eid = it.get("engine_id")
                    if isinstance(eid, str) and eid.strip():
                        out.append(eid.strip())
            out = sorted(set(out))
            if out:
                return out, missing, warnings

    # deep scan fallback
    out = _engine_ids_deep_scan(obj)
    if not out:
        warnings.append("ACTIVE_ENGINE_SET_EMPTY")
    return out, missing, warnings


def _engine_ids_from_engine_linkage(truth_root: Path, day: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Fallback engine list source (truth): engine_linkage_v1/snapshots/<day>/engine_linkage.v1.json
    Schema-agnostic deep scan for engine_id.
    """
    missing: List[str] = []
    warnings: List[str] = []

    p = (truth_root / "engine_linkage_v1" / "snapshots" / day / "engine_linkage.v1.json").resolve()
    if not p.exists():
        missing.append(str(p))
        warnings.append("ENGINE_LINKAGE_MISSING")
        return [], missing, warnings

    obj, err = _safe_read_json(p)
    if not isinstance(obj, (dict, list)):
        warnings.append(f"ENGINE_LINKAGE_UNREADABLE:{err}")
        return [], missing, warnings

    out = _engine_ids_deep_scan(obj)
    if not out:
        warnings.append("ENGINE_LINKAGE_EMPTY")
    return out, missing, warnings


def _engine_ids_from_heartbeat(truth_root: Path, day: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Fallback engine list from heartbeat directory names:
    monitoring_v1/engine_heartbeat_v1/<day>/<ENGINE_ID>/...
    """
    missing: List[str] = []
    warnings: List[str] = []
    root = (truth_root / "monitoring_v1" / "engine_heartbeat_v1" / day).resolve()
    if not root.exists() or not root.is_dir():
        missing.append(str(root))
        warnings.append("ENGINE_HEARTBEAT_MISSING")
        return [], missing, warnings
    out: List[str] = []
    for p in sorted([x for x in root.iterdir() if x.is_dir()], key=lambda x: x.name):
        n = p.name.strip()
        if n:
            out.append(n)
    if not out:
        warnings.append("ENGINE_HEARTBEAT_EMPTY")
    return sorted(set(out)), missing, warnings


# -------------------------
# Counts / flow
# -------------------------

def _intents_root(truth_root: Path) -> Path:
    return truth_root / "intents_v1" / "snapshots"


def _submissions_root(truth_root: Path) -> Path:
    return truth_root / "execution_evidence_v1" / "submissions"


def _count_intents(truth_root: Path, day: str) -> Tuple[int, List[str]]:
    root = (_intents_root(truth_root) / day).resolve()
    if not root.exists() or not root.is_dir():
        return 0, [str(root)]
    files = sorted([p for p in root.iterdir() if p.is_file()])
    return len(files), []


def _count_submissions_and_fills(truth_root: Path, day: str) -> Tuple[Dict[str, int], List[str]]:
    miss: List[str] = []
    root = (_submissions_root(truth_root) / day).resolve()
    if not root.exists() or not root.is_dir():
        miss.append(str(root))
        return {"submitted": 0, "filled": 0}, miss

    submitted = 0
    filled = 0

    items = sorted(list(root.rglob("*.json")), key=lambda p: str(p))
    for p in items:
        obj, _ = _safe_read_json(p)
        if not isinstance(obj, dict):
            continue
        sid = str(obj.get("schema_id") or "")
        if "broker_submission_record" in sid:
            submitted += 1
        if "execution_event_record" in sid:
            st = obj.get("status")
            if isinstance(st, str) and "FILL" in st.upper():
                filled += 1

    return {"submitted": submitted, "filled": filled}, miss


def _collect_submission_order_flow(truth_root: Path, day: str) -> Dict[str, Any]:
    root = (_submissions_root(truth_root) / day).resolve()
    out: Dict[str, Any] = {
        "summary": {
            "submitted_records": 0,
            "pending_orders": 0,
            "filled_orders": 0,
            "rejected_orders": 0,
            "not_executed_orders": 0,
        },
        "records": [],
        "pending_orders": [],
        "sources": {"submissions_root": str(root)},
        "missing_paths": [],
    }
    if not root.exists() or not root.is_dir():
        out["missing_paths"] = [str(root)]
        return out

    def _load(path: Path) -> Optional[Dict[str, Any]]:
        if not path.exists():
            return None
        obj, _err = _safe_read_json(path)
        return obj if isinstance(obj, dict) else None

    recs: List[Dict[str, Any]] = []
    for intent_dir in sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name):
        plan_path = (intent_dir / "equity_order_plan.v1.json").resolve()
        sub_path = (intent_dir / "broker_submission_record.v2.json").resolve()
        evt_path = (intent_dir / "execution_event_record.v1.json").resolve()
        plan = _load(plan_path)
        sub = _load(sub_path)
        evt = _load(evt_path)
        if plan is None and sub is None and evt is None:
            continue

        status_sub = str((sub or {}).get("status") or "").upper()
        status_evt = str((evt or {}).get("status") or "").upper()
        filled_qty_raw = (evt or {}).get("filled_qty")
        filled_qty = float(filled_qty_raw) if isinstance(filled_qty_raw, (int, float)) else 0.0
        qty_raw = (plan or {}).get("qty_shares")
        qty = float(qty_raw) if isinstance(qty_raw, (int, float)) else None

        if "FILL" in status_evt or filled_qty > 0:
            lifecycle = "FILLED"
        elif status_sub in {"PRESUBMITTED", "SUBMITTED", "PENDING", "API_PENDING"}:
            lifecycle = "PENDING"
        elif "REJECT" in status_sub or "REJECT" in status_evt:
            lifecycle = "REJECTED"
        elif any(x in status_sub for x in ("CANCEL", "INACTIVE", "EXPIRE")) or any(
            x in status_evt for x in ("CANCEL", "INACTIVE", "EXPIRE")
        ):
            lifecycle = "NOT_EXECUTED"
        elif sub is not None:
            lifecycle = "PENDING"
        else:
            lifecycle = "NOT_EXECUTED"

        rec = {
            "intent_hash": (plan or {}).get("intent_hash") or intent_dir.name,
            "engine_id": (plan or {}).get("engine_id"),
            "symbol": (plan or {}).get("symbol"),
            "side": (plan or {}).get("action"),
            "qty": qty,
            "submitted_status": status_sub or None,
            "execution_status": status_evt or None,
            "filled_qty": filled_qty,
            "submitted_at_utc": (sub or {}).get("submitted_at_utc"),
            "event_time_utc": (evt or {}).get("event_time_utc"),
            "lifecycle_state": lifecycle,
            "evidence_paths": [str(x) for x in [plan_path, sub_path, evt_path] if x.exists()],
        }
        recs.append(rec)

    for rec in recs:
        if rec.get("submitted_status"):
            out["summary"]["submitted_records"] += 1
        st = str(rec.get("lifecycle_state") or "")
        if st == "PENDING":
            out["summary"]["pending_orders"] += 1
            out["pending_orders"].append(rec)
        elif st == "FILLED":
            out["summary"]["filled_orders"] += 1
        elif st == "REJECTED":
            out["summary"]["rejected_orders"] += 1
        elif st == "NOT_EXECUTED":
            out["summary"]["not_executed_orders"] += 1
    out["records"] = recs
    return out

# -------------------------
# Accounting / portfolio
# -------------------------


def _flow_drilldown(truth_root: Path, day: str, counts: Dict[str, Any]) -> Dict[str, Any]:
    intents_dir = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    auth_dir = (truth_root / "engine_activity_v1" / "authorization_v1" / day).resolve()
    veto_dir = (truth_root / "phaseC_preflight_v1" / day).resolve()
    sub_dir = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    recon_path = (truth_root / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json").resolve()

    def top_paths(paths: List[str]) -> List[str]:
        return sorted(paths)[:20]

    intent_paths = top_paths([str(p) for p in intents_dir.glob("*.json")]) if intents_dir.exists() else []
    auth_paths = top_paths([str(p) for p in auth_dir.glob("*.authorization.v1.json")]) if auth_dir.exists() else []
    rejected_paths: List[str] = []
    authorized_paths: List[str] = []
    for p in sorted([Path(x) for x in auth_paths], key=lambda x: str(x)):
        obj, _err = _safe_read_json(p)
        if not isinstance(obj, dict):
            continue
        st = str(obj.get("status") or obj.get("decision") or "").upper()
        if st == "REJECTED":
            rejected_paths.append(str(p))
        elif st == "AUTHORIZED":
            authorized_paths.append(str(p))

    veto_paths = top_paths([str(p) for p in veto_dir.rglob("*.veto_record.v1.json")]) if veto_dir.exists() else []

    submitted_paths: List[str] = []
    filled_paths: List[str] = []
    if sub_dir.exists():
        for p in sorted(sub_dir.rglob("*.json"), key=lambda x: str(x)):
            obj, _err = _safe_read_json(p)
            if not isinstance(obj, dict):
                continue
            sid = str(obj.get("schema_id") or "")
            if "broker_submission_record" in sid:
                submitted_paths.append(str(p))
            if "execution_event_record" in sid:
                st = str(obj.get("status") or "").upper()
                if "FILL" in st:
                    filled_paths.append(str(p))

    return {
        "intents": {
            "count": counts.get("intents"),
            "summary": "Intent artifacts emitted for selected day.",
            "evidence_paths": top_paths(intent_paths),
        },
        "rejected_or_vetoed": {
            "count": counts.get("rejected"),
            "summary": "Rejected authorizations and/or submit vetoes blocked before broker submission.",
            "evidence_paths": top_paths(rejected_paths + veto_paths),
        },
        "authorized": {
            "count": counts.get("authorized"),
            "summary": "Authorization records allowed by capital authority.",
            "evidence_paths": top_paths(authorized_paths),
        },
        "submitted": {
            "count": counts.get("submitted"),
            "summary": "Broker submission records written.",
            "evidence_paths": top_paths(submitted_paths),
        },
        "filled": {
            "count": counts.get("filled"),
            "summary": "Execution records with fill state.",
            "evidence_paths": top_paths(filled_paths),
        },
        "reconciled": {
            "count": counts.get("reconciled"),
            "summary": "Execution reconciliation status for day.",
            "evidence_paths": [str(recon_path)] if recon_path.exists() else [],
        },
        "vetoed": {
            "count": counts.get("vetoed"),
            "summary": "PhaseC submit veto records observed for day.",
            "evidence_paths": top_paths(veto_paths),
        },
    }


def _count_authorization_rejected(truth_root: Path, day: str) -> Tuple[int, List[str]]:
    root = (truth_root / "engine_activity_v1" / "authorization_v1" / day).resolve()
    if not root.exists() or not root.is_dir():
        return 0, [str(root)]
    seen = set()
    cnt = 0
    for p in sorted(root.glob("*.authorization.v1.json"), key=lambda x: x.name):
        obj, _err = _safe_read_json(p)
        if not isinstance(obj, dict):
            continue
        ih = str(obj.get("intent_hash") or "").strip() or p.name.split(".")[0]
        if ih in seen:
            continue
        seen.add(ih)
        st = str(obj.get("status") or obj.get("decision") or "").strip().upper()
        if st == "REJECTED":
            cnt += 1
    return cnt, []


def _count_phasec_veto_records(truth_root: Path, day: str) -> Tuple[int, List[str]]:
    root = (truth_root / "phaseC_preflight_v1" / day).resolve()
    if not root.exists() or not root.is_dir():
        return 0, [str(root)]
    seen = set()
    cnt = 0
    for p in sorted(root.rglob("*.veto_record.v1.json"), key=lambda x: str(x)):
        ih = p.name.split(".")[0]
        if ih in seen:
            continue
        seen.add(ih)
        cnt += 1
    return cnt, []


def _active_engine_ids_for_day(truth_root: Path, day: str) -> List[str]:
    eids = set()

    # Heartbeat surface
    hb_day = (truth_root / "monitoring_v1" / "engine_heartbeat_v1" / day).resolve()
    if hb_day.exists() and hb_day.is_dir():
        for p in hb_day.iterdir():
            if p.is_dir() and p.name.strip():
                eids.add(p.name.strip())

    # Intent surface
    intents_day = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    if intents_day.exists() and intents_day.is_dir():
        for p in sorted([x for x in intents_day.iterdir() if x.is_file() and x.suffix == ".json"], key=lambda x: x.name):
            obj, _err = _safe_read_json(p)
            if not isinstance(obj, dict):
                continue
            eng = obj.get("engine") if isinstance(obj.get("engine"), dict) else None
            eid = eng.get("engine_id") if isinstance(eng, dict) else None
            if isinstance(eid, str) and eid.strip():
                eids.add(eid.strip())

    # Authorization surface
    auth_day = (truth_root / "engine_activity_v1" / "authorization_v1" / day).resolve()
    if auth_day.exists() and auth_day.is_dir():
        for p in sorted(auth_day.glob("*.authorization.v1.json"), key=lambda x: x.name):
            obj, _err = _safe_read_json(p)
            if not isinstance(obj, dict):
                continue
            eid = obj.get("engine_id")
            if isinstance(eid, str) and eid.strip():
                eids.add(eid.strip())

    return sorted(eids)


def _load_sleeve_policy() -> List[Dict[str, Any]]:
    obj, _err = _safe_read_json(SLEEVE_POLICY_REGISTRY)
    if not isinstance(obj, dict):
        return []
    sleeves = obj.get("sleeves")
    if not isinstance(sleeves, list):
        return []
    out: List[Dict[str, Any]] = []
    for s in sleeves:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("sleeve_id") or "").strip()
        engine_ids = s.get("engine_ids") if isinstance(s.get("engine_ids"), list) else []
        limits = s.get("limits") if isinstance(s.get("limits"), dict) else {}
        if not sid or not engine_ids:
            continue
        out.append(
            {
                "sleeve_id": sid,
                "display_name": str(s.get("display_name") or sid),
                "engine_ids": [str(e).strip() for e in engine_ids if isinstance(e, str) and str(e).strip()],
                "priority_rank": int(s.get("priority_rank")) if isinstance(s.get("priority_rank"), int) else 9999,
                "max_capital_at_risk_cents": int(limits.get("max_capital_at_risk_cents")) if isinstance(limits.get("max_capital_at_risk_cents"), int) else 0,
            }
        )
    out.sort(key=lambda x: (x.get("priority_rank", 9999), x.get("sleeve_id", "")))
    return out


def _build_sleeve_strip_rows(
    *,
    truth_root: Path,
    day: str,
    mode_from_attempt: Optional[str],
    primary_account: Optional[str],
    fallback_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    warnings: List[str] = []
    sleeves = _load_sleeve_policy()
    if not sleeves:
        warnings.append("SLEEVE_POLICY_MISSING")
        filtered_fallback = [
            row for row in fallback_rows
            if str(row.get("sleeve_id") or "").strip().upper() in STANDARD_TRADING_SLEEVE_IDS
        ]
        return filtered_fallback, warnings

    active_engines = set(_active_engine_ids_for_day(truth_root, day))
    _ = primary_account
    default_mode = (mode_from_attempt or "UNKNOWN")

    reg_obj, reg_err = _safe_read_json(SLEEVE_REGISTRY)
    registry_rows_by_id: Dict[str, Dict[str, Any]] = {}
    if not isinstance(reg_obj, dict):
        warnings.append(f"SLEEVE_REGISTRY_UNREADABLE:{reg_err}")
    else:
        reg_sleeves = reg_obj.get("sleeves")
        if not isinstance(reg_sleeves, list):
            warnings.append("SLEEVE_REGISTRY_INVALID")
        else:
            for row in reg_sleeves:
                if not isinstance(row, dict):
                    continue
                sleeve_id = str(row.get("sleeve_id") or "").strip()
                if not sleeve_id:
                    continue
                registry_rows_by_id[sleeve_id] = row

    out: List[Dict[str, Any]] = []
    for s in sleeves:
        engine_ids = s.get("engine_ids") or []
        is_active = any(e in active_engines for e in engine_ids)
        sleeve_id = str(s.get("sleeve_id") or "").strip()
        registry_row = registry_rows_by_id.get(sleeve_id, {})
        acct = str(registry_row.get("ib_account") or "").strip() or None
        mode = str(registry_row.get("mode") or default_mode).strip().upper() or default_mode
        enabled = bool(registry_row.get("enabled")) if registry_row else None
        if acct is None:
            warnings.append(f"SLEEVE_ACCOUNT_UNRESOLVED:{sleeve_id}")
        out.append(
            {
                "sleeve_id": sleeve_id,
                "name": s.get("display_name"),
                "mode": mode,
                "ib_account_id": acct,
                "entries_allowed": None,
                "flatten_only": None,
                "engine_ids": engine_ids,
                "active_today": bool(is_active),
                "enabled": enabled,
                "registry_source": "C2_SLEEVE_REGISTRY_V1" if registry_row else "C2_CAPITAL_AUTHORITY_POLICY_V1",
            }
        )
    out.sort(key=lambda x: (x.get("sleeve_id") or "", x.get("name") or ""))

    # Include only canonical standard trading sleeves that are not already engine-mapped.
    # Bond is not a trading sleeve and must never enter the generic sleeve payload path.
    reg_rows = list(registry_rows_by_id.values())
    if not reg_rows:
        return out, warnings

    existing_ids = {str(r.get("sleeve_id") or "").strip() for r in out}
    for rs in reg_rows:
        if not isinstance(rs, dict):
            continue
        sid = str(rs.get("sleeve_id") or "").strip()
        if not sid or sid in existing_ids:
            continue
        if sid.upper() not in STANDARD_TRADING_SLEEVE_IDS:
            continue
        disp = sid
        mode_rs = str(rs.get("mode") or "UNKNOWN").strip().upper()
        acct_rs = str(rs.get("ib_account") or "").strip() or None
        enabled_rs = bool(rs.get("enabled"))
        out.append(
            {
                "sleeve_id": sid,
                "name": disp,
                "mode": mode_rs,
                "ib_account_id": acct_rs,
                "entries_allowed": None,
                "flatten_only": None,
                "engine_ids": [],
                "active_today": False,
                "enabled": enabled_rs,
                "registry_source": "C2_SLEEVE_REGISTRY_V1",
            }
        )
        existing_ids.add(sid)

    out.sort(key=lambda x: (x.get("sleeve_id") or "", x.get("name") or ""))
    return out, warnings


def _build_attempt_summaries(truth_root: Path, day: str, attempts: List[str]) -> List[Dict[str, Any]]:
    from constellation_2.phaseL.ui.server.c2_ops_cockpit_status_v2_collector_diagnostic_v1 import (
        load_attempt_verdict_v1,
    )

    out: List[Dict[str, Any]] = []
    for aid in attempts:
        doc = load_attempt_verdict_v1(truth_root, day, aid)
        if not isinstance(doc, dict):
            out.append(
                {
                    "attempt_id": aid,
                    "attempt_seq": None,
                    "status": "UNKNOWN",
                    "produced_utc": None,
                    "reason_codes": [],
                }
            )
            continue
        out.append(
            {
                "attempt_id": str(doc.get("attempt_id") or aid),
                "attempt_seq": doc.get("attempt_seq") if isinstance(doc.get("attempt_seq"), int) else None,
                "status": _coerce_state(str(doc.get("status") or doc.get("state") or "UNKNOWN")),
                "produced_utc": doc.get("produced_utc") if isinstance(doc.get("produced_utc"), str) else None,
                "reason_codes": _top2_reason_codes(doc.get("reason_codes") or []),
                "producer_git_sha": doc.get("producer", {}).get("git_sha") if isinstance(doc.get("producer"), dict) else None,
            }
        )
    out.sort(key=lambda x: (int(x.get("attempt_seq")) if isinstance(x.get("attempt_seq"), int) else -1, str(x.get("attempt_id") or "")))
    return out


# -------------------------
# Deterministic diff (server memory)
# -------------------------

_LAST_HASH: Optional[str] = None
_LAST_KEY_FIELDS: Optional[Dict[str, Any]] = None


def _compute_key_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    ops = payload.get("ops_health") if isinstance(payload.get("ops_health"), dict) else {}
    flow = payload.get("trade_flow_today") if isinstance(payload.get("trade_flow_today"), dict) else {}
    port = payload.get("portfolio") if isinstance(payload.get("portfolio"), dict) else {}
    platform = payload.get("platform_readiness") if isinstance(payload.get("platform_readiness"), dict) else {}

    tiles = ops.get("tiles") if isinstance(ops.get("tiles"), list) else []
    tile_k: List[Dict[str, Any]] = []
    for t in tiles:
        if not isinstance(t, dict):
            continue
        tid = t.get("tile_id")
        if isinstance(tid, str):
            tile_k.append({"tile_id": tid, "state": t.get("state"), "last_updated_utc": t.get("last_updated_utc")})
    tile_k.sort(key=lambda x: x["tile_id"])

    engines = payload.get("engines") if isinstance(payload.get("engines"), list) else []
    eng_k: List[Dict[str, Any]] = []
    for e in engines:
        if not isinstance(e, dict):
            continue
        eid = e.get("engine_id")
        if isinstance(eid, str):
            eng_k.append({"engine_id": eid, "status": e.get("status"), "mode": e.get("mode"), "ib_account_id": e.get("ib_account_id")})
    eng_k.sort(key=lambda x: x["engine_id"])

    return {
        "selected_day": meta.get("selected_day"),
        "selected_attempt_id": meta.get("selected_attempt_id"),
        "tiles": tile_k,
        "flow": flow.get("counts"),
        "blocked": flow.get("blocked_by_gate"),
        "execution_authority": flow.get("execution_authority"),
        "portfolio": {
            "nav_total": port.get("nav_total"),
            "pnl_today": port.get("pnl_today"),
            "pnl_cumulative": port.get("pnl_cumulative"),
            "asof_utc": port.get("asof_utc"),
            "authority_basis": port.get("authority_basis"),
            "authoritative": port.get("authoritative"),
        },
        "platform_readiness": {
            "state": platform.get("platform_readiness_state"),
            "grade": platform.get("platform_readiness_grade"),
            "score": platform.get("platform_readiness_score"),
            "candidate": platform.get("platform_promotion_candidate"),
        },
        "engines": eng_k,
    }


def _diff_key_fields(prev: Optional[Dict[str, Any]], cur: Dict[str, Any]) -> List[Dict[str, str]]:
    if prev is None:
        return [{"code": "FIRST_LOAD", "summary": "First load"}]

    out: List[Dict[str, str]] = []

    if prev.get("selected_attempt_id") != cur.get("selected_attempt_id"):
        out.append({"code": "ATTEMPT_CHANGED", "summary": "Attempt changed"})

    prev_tiles = {t["tile_id"]: t for t in (prev.get("tiles") or []) if isinstance(t, dict) and "tile_id" in t}
    cur_tiles = {t["tile_id"]: t for t in (cur.get("tiles") or []) if isinstance(t, dict) and "tile_id" in t}
    for tid in sorted(set(prev_tiles.keys()) | set(cur_tiles.keys())):
        a = prev_tiles.get(tid, {})
        b = cur_tiles.get(tid, {})
        if a.get("state") != b.get("state"):
            out.append({"code": "TILE_STATE_CHANGED", "summary": f"{tid}: {a.get('state')} → {b.get('state')}"})

    if prev.get("portfolio") != cur.get("portfolio"):
        out.append({"code": "PORTFOLIO_UPDATED", "summary": "Portfolio metrics updated"})

    prev_eng = {e["engine_id"]: e for e in (prev.get("engines") or []) if isinstance(e, dict) and "engine_id" in e}
    cur_eng = {e["engine_id"]: e for e in (cur.get("engines") or []) if isinstance(e, dict) and "engine_id" in e}
    for eid in sorted(set(prev_eng.keys()) | set(cur_eng.keys())):
        a = prev_eng.get(eid, {})
        b = cur_eng.get(eid, {})
        if a.get("status") != b.get("status"):
            out.append({"code": "ENGINE_STATUS_CHANGED", "summary": f"{eid}: {a.get('status')} → {b.get('status')}"})
        if a.get("mode") != b.get("mode"):
            out.append({"code": "ENGINE_MODE_CHANGED", "summary": f"{eid}: {a.get('mode')} → {b.get('mode')}"})

    return out[:8] if out else [{"code": "NO_CHANGE", "summary": "No change"}]


# -------------------------
# Main builder
# -------------------------

def _build_status_v2_core(
    truth_root: Path,
    instance_config_path: Path,
    day: str,
    attempt_id: Optional[str],
    c3_status: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    from constellation_2.phaseL.ui.server.c2_ops_cockpit_status_v2_collector_control_plane_v1 import (
        load_status_collector_control_plane_bundle_v1,
    )
    from constellation_2.phaseL.ui.server.c2_ops_cockpit_status_v2_collector_diagnostic_v1 import (
        discover_attempts_v1,
        load_broker_connection_observer_tile_v1,
        load_attempt_verdict_v1,
        load_flow_diagnostics_bundle_v1,
        load_fixed_gate_tiles_v1,
        load_portfolio_positions_bundle_v1,
        load_signal_activity_v1,
        load_platform_readiness_history_v1,
        load_platform_readiness_policy_view_v1,
        load_run_scope_diagnostics_bundle_v1,
        select_preferred_attempt_v1,
    )

    global _LAST_HASH, _LAST_KEY_FIELDS

    # Attempts
    attempts, miss_a, sp_a, sm_a, warn_a = discover_attempts_v1(truth_root, day)
    raw_attempt = attempt_id.strip() if isinstance(attempt_id, str) else ""
    if raw_attempt in ("", "attempts", "latest"):
        sel_attempt = select_preferred_attempt_v1(truth_root, day, attempts)
    else:
        sel_attempt = raw_attempt

    # Run verdict
    run_tile, miss_rv, sp_rv, sm_rv, warn_rv = _parse_orchestrator_run_verdict_v2(truth_root, day, sel_attempt)
    run_scope_bundle = load_run_scope_diagnostics_bundle_v1(
        run_tile_artifact_path=None if run_tile is None else run_tile.artifact_path,
        runtime_state_path=RUNTIME_STATE_PATH,
    )
    run_doc = run_scope_bundle["run_doc"]

    control_plane_bundle = load_status_collector_control_plane_bundle_v1(
        truth_root=truth_root,
        global_truth_root=GLOBAL_RUNTIME_TRUTH_ROOT,
        day=day,
    )

    # Gate stack verdict (optional)
    gate_tile = Tile(**control_plane_bundle["gate_tile"]) if isinstance(control_plane_bundle.get("gate_tile"), dict) else None
    miss_gs = list(control_plane_bundle.get("miss_gs") or [])
    sp_gs = list(control_plane_bundle.get("sp_gs") or [])
    sm_gs = dict(control_plane_bundle.get("sm_gs") or {})
    warn_gs = list(control_plane_bundle.get("warn_gs") or [])

    fixed_gate_tiles = load_fixed_gate_tiles_v1(truth_root=truth_root, day=day)
    attest_tile = Tile(**fixed_gate_tiles["attest_tile"])
    warn_att = list(fixed_gate_tiles["warn_att"])
    miss_att = list(fixed_gate_tiles["miss_att"])
    liquidity_tile = Tile(**fixed_gate_tiles["liquidity_tile"])
    warn_liq = list(fixed_gate_tiles["warn_liq"])
    miss_liq = list(fixed_gate_tiles["miss_liq"])
    corr_tile = Tile(**fixed_gate_tiles["corr_tile"])
    warn_cor = list(fixed_gate_tiles["warn_cor"])
    miss_cor = list(fixed_gate_tiles["miss_cor"])
    convex_tile = Tile(**fixed_gate_tiles["convex_tile"])
    warn_cvx = list(fixed_gate_tiles["warn_cvx"])
    miss_cvx = list(fixed_gate_tiles["miss_cvx"])
    capital_tile = Tile(**fixed_gate_tiles["capital_tile"])
    warn_cap = list(fixed_gate_tiles["warn_cap"])
    miss_cap = list(fixed_gate_tiles["miss_cap"])

    # Replay
    replay_tile, miss_rep, sp_rep, sm_rep, warn_rep = _parse_replay_tile(truth_root, day, sel_attempt)

    # Safety breach tile
    safety_state = "UNKNOWN"
    safety_rc: List[str] = []
    safety_last: Optional[str] = None
    safety_path: Optional[str] = None
    safety_sha: Optional[str] = None

    if run_tile is not None:
        safety_last = run_tile.last_updated_utc
        if run_tile.state == "ABORTED":
            safety_state = "ABORTED"
            safety_rc = run_tile.reason_codes
            safety_path = run_tile.artifact_path
            safety_sha = run_tile.artifact_sha256
        else:
            safety_state = "PASS"
    elif gate_tile is not None:
        safety_last = gate_tile.last_updated_utc
        safety_state = "ABORTED" if gate_tile.state == "ABORTED" else "PASS"
        safety_path = gate_tile.artifact_path
        safety_sha = gate_tile.artifact_sha256

    safety_tile = Tile(
        tile_id="safety_breach",
        state=_coerce_state(safety_state),
        last_updated_utc=safety_last,
        reason_codes=safety_rc,
        reason_human=[],
        artifact_path=safety_path,
        artifact_sha256=safety_sha,
    )

    broker_view = load_broker_connection_observer_tile_v1(
        truth_root=truth_root,
        day=day,
        c3_status=c3_status,
        run_doc=run_doc,
    )
    broker_tile = Tile(**broker_view["broker_tile"])
    warn_broker = list(broker_view["warn_broker"])
    miss_broker = list(broker_view["miss_broker"])

    flow_bundle = load_flow_diagnostics_bundle_v1(truth_root=truth_root, day=day)
    rollup_doc = flow_bundle["rollup_doc"]
    miss_roll = list(flow_bundle["miss_roll"])
    activity_flow_doc = flow_bundle["activity_flow_doc"]
    miss_afd = list(flow_bundle["miss_afd"])
    sp_afd = list(flow_bundle["sp_afd"])
    sm_afd = dict(flow_bundle["sm_afd"])
    warn_afd = list(flow_bundle["warn_afd"])
    oms_docs = list(flow_bundle["oms_docs"])
    miss_oms = list(flow_bundle["miss_oms"])
    sp_oms = list(flow_bundle["sp_oms"])
    sm_oms = dict(flow_bundle["sm_oms"])
    warn_oms = list(flow_bundle["warn_oms"])
    oms_summary = dict(flow_bundle["oms_summary"])
    day_start_blocked_doc = control_plane_bundle.get("day_start_blocked_doc")
    miss_dsb = list(control_plane_bundle.get("miss_dsb") or [])
    sp_dsb = list(control_plane_bundle.get("sp_dsb") or [])
    sm_dsb = dict(control_plane_bundle.get("sm_dsb") or {})
    warn_dsb = list(control_plane_bundle.get("warn_dsb") or [])
    trading_day_state_doc = control_plane_bundle.get("trading_day_state_doc")
    miss_tds = list(control_plane_bundle.get("miss_tds") or [])
    sp_tds = list(control_plane_bundle.get("sp_tds") or [])
    sm_tds = dict(control_plane_bundle.get("sm_tds") or {})
    warn_tds = list(control_plane_bundle.get("warn_tds") or [])
    flow = dict(flow_bundle["flow"])
    intents_cnt, miss_int = _count_intents(truth_root, day)
    subs_cnt, miss_sub = _count_submissions_and_fills(truth_root, day)
    rejected_cnt, miss_auth = _count_authorization_rejected(truth_root, day)
    veto_cnt, miss_veto = _count_phasec_veto_records(truth_root, day)

    counts = {
        "intents": int(flow["intents"]) if isinstance(flow.get("intents"), int) else intents_cnt,
        "authorized": int(flow["authorized"]) if isinstance(flow.get("authorized"), int) else None,
        "submitted": int(flow["submitted"]) if isinstance(flow.get("submitted"), int) else subs_cnt["submitted"],
        "filled": int(flow["filled"]) if isinstance(flow.get("filled"), int) else subs_cnt["filled"],
        "reconciled": int(flow["reconciled"]) if isinstance(flow.get("reconciled"), int) else None,
        "rejected": rejected_cnt if rejected_cnt > 0 else None,
        "vetoed": veto_cnt if veto_cnt > 0 else None,
    }
    # Fallback derivations from orchestrator stage truth when day rollup is missing.
    auth_stage = _attempt_stage_status(run_doc, "A6B_AUTHORIZATION_ARTIFACTS_DAY_V1")
    recon_stage = _attempt_stage_status(run_doc, "B2_EXECUTION_RECONCILIATION_V1")
    if counts["authorized"] is None and auth_stage in ("OK", "SKIP", "PASS"):
        counts["authorized"] = counts["submitted"] if isinstance(counts["submitted"], int) else None
    if counts["reconciled"] is None and recon_stage in ("OK", "SKIP", "PASS"):
        counts["reconciled"] = counts["submitted"] if isinstance(counts["submitted"], int) else None
    blocked_by_gate = {
        "liquidity": flow.get("blocked_liquidity"),
        "correlation": flow.get("blocked_correlation"),
        "attestation": flow.get("blocked_attestation"),
        "convex": flow.get("blocked_convex"),
        "capital": flow.get("blocked_capital"),
    }
    execution_authority = read_execution_day_authority_state(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=day,
        mode="PAPER",
    )

    # Portfolio
    portfolio_bundle = load_portfolio_positions_bundle_v1(truth_root=truth_root, day=day)
    nav_doc = portfolio_bundle["nav_doc"]
    miss_nav = list(portfolio_bundle["miss_nav"])
    nav_err = portfolio_bundle["nav_err"]
    nav_path = portfolio_bundle["nav_path"]
    portfolio = dict(portfolio_bundle["portfolio"])
    accounting_authority = read_accounting_authority_state(truth_root=truth_root, day_utc=day)
    portfolio["authority_basis"] = accounting_authority.get("basis_class")
    portfolio["authoritative"] = accounting_authority.get("authoritative")
    portfolio["authority_reason_codes"] = accounting_authority.get("reason_codes")
    portfolio["cash_authority_basis"] = accounting_authority.get("cash_authority_basis")
    if nav_doc is None:
        portfolio["note_if_missing"] = "PnL unavailable (missing accounting/nav)"
        portfolio["missing"] = True
    else:
        portfolio["missing"] = False
    portfolio["nav_path"] = nav_path

    # Positions / Exposure
    positions_doc = portfolio_bundle["positions_doc"]
    miss_pos = list(portfolio_bundle["miss_pos"])
    pos_err = portfolio_bundle["pos_err"]
    pos_path = portfolio_bundle["pos_path"]
    exposure_doc = portfolio_bundle["exposure_doc"]
    miss_exp = list(portfolio_bundle["miss_exp"])
    exp_err = portfolio_bundle["exp_err"]
    exp_path = portfolio_bundle["exp_path"]
    positions_exposure = dict(portfolio_bundle["positions_exposure"])
    order_flow = _collect_submission_order_flow(truth_root, day)
    positions_exposure["order_flow"] = order_flow
    positions_exposure["sources"]["positions_path"] = pos_path
    positions_exposure["sources"]["exposure_path"] = exp_path
    positions_exposure["sources"]["submissions_root"] = order_flow.get("sources", {}).get("submissions_root")

    # Bond sleeve advisory truth + operator-confirmed holdings evidence
    bond_sleeve, warn_bond, miss_bond, sp_bond, sm_bond = _collect_bond_sleeve_view(day)

    # Engine mode/account defaults from attempt manifest
    mode_from_attempt, acct_from_attempt, warn_ma, miss_ma = _attempt_mode_and_account(truth_root, day, sel_attempt)

    # Engines list from truth
    engines_out: List[Dict[str, Any]] = []
    miss_eng: List[str] = []
    warn_eng: List[str] = []

    eids, miss_ae, warn_ae = _engine_ids_from_active_engine_set(truth_root, day)
    miss_eng.extend(miss_ae)
    warn_eng.extend(warn_ae)

    if not eids:
        eids2, miss_el, warn_el = _engine_ids_from_engine_linkage(truth_root, day)
        miss_eng.extend(miss_el)
        warn_eng.extend(warn_el)
        eids = eids2
    if not eids:
        eids3, miss_hb, warn_hb = _engine_ids_from_heartbeat(truth_root, day)
        miss_eng.extend(miss_hb)
        warn_eng.extend(warn_hb)
        eids = eids3

    # Deterministic normalization
    eids = [str(x) for x in eids if isinstance(x, str) and str(x).strip()]
    eids = sorted(set(eids))

    for eid in eids:
        engines_out.append(
            {
                "engine_id": eid,
                "engine_name": eid,
                "mode": (mode_from_attempt or "UNKNOWN"),
                "ib_account_id": acct_from_attempt,
                "entries_allowed": None,
                "flatten_only": None,
                "status": "ACTIVE" if counts["intents"] and safety_tile.state != "ABORTED" else ("FAIL" if safety_tile.state == "ABORTED" else "UNKNOWN"),
                "today": {"intents": None, "authorized": None, "submitted": None, "filled": None},
                "positions": {"open_count": None},
                "exposure": {"net_pct": None, "gross_pct": None, "asof_utc": portfolio.get("asof_utc")},
                "pnl": {
                    "today": None if portfolio.get("missing") else portfolio.get("pnl_today"),
                    "cumulative": None if portfolio.get("missing") else portfolio.get("pnl_cumulative"),
                    "currency": "USD",
                    "asof_utc": portfolio.get("asof_utc"),
                    "note_if_missing": "PnL unavailable (missing accounting/nav)" if portfolio.get("missing") else None,
                },
                "applied_risk": {
                    "base_risk_pct": None,
                    "vol_adjusted_weight": None,
                    "liquidity_scalar": None,
                    "correlation_scalar": None,
                    "convex_scalar": None,
                    "final_authorized_weight": None,
                    "cash_authority_cap": None,
                },
                "details_collapsed": {
                    "top_reason_codes": [],
                    "attempt_ids": [sel_attempt] if isinstance(sel_attempt, str) and sel_attempt else [],
                    "replay_bundle": {
                        "state": replay_tile.state if replay_tile else "MISSING",
                        "summary": None,
                        "artifact_path": replay_tile.artifact_path if replay_tile else None,
                    },
                    "stage_timestamps": {},
                },
            }
        )

    engines_out.sort(key=lambda x: (x.get("engine_id") or "", x.get("engine_name") or ""))

    fallback_sleeves: List[Dict[str, Any]] = []
    for e in engines_out:
        fallback_sleeves.append(
            {
                "sleeve_id": e["engine_id"],
                "name": e["engine_name"],
                "mode": e["mode"],
                "ib_account_id": e.get("ib_account_id"),
                "entries_allowed": e.get("entries_allowed"),
                "flatten_only": e.get("flatten_only"),
            }
        )
    fallback_sleeves.sort(key=lambda x: (x.get("sleeve_id") or "", x.get("name") or ""))
    sleeves_out, warn_sleeves = _build_sleeve_strip_rows(
        truth_root=truth_root,
        day=day,
        mode_from_attempt=mode_from_attempt,
        primary_account=acct_from_attempt,
        fallback_rows=fallback_sleeves,
    )
    attempt_summaries = _build_attempt_summaries(truth_root, day, attempts)

    # Tiles (fixed layout)
    tiles: List[Tile] = []
    tiles.append(run_tile if run_tile else Tile("orchestrator_run_verdict_v2", "MISSING", None, ["RUN_VERDICT_NOT_FOUND"], [], None, None))
    tiles.append(safety_tile)
    tiles.append(broker_tile)
    tiles.append(attest_tile)
    tiles.append(liquidity_tile)
    tiles.append(corr_tile)
    tiles.append(convex_tile)
    tiles.append(replay_tile if replay_tile else Tile("replay_certification", "MISSING", None, ["REPLAY_NOT_FOUND"], [], None, None))

    canonical_pointer_path = (truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()
    kill_switch_state = dict(control_plane_bundle.get("kill_switch_state") or {})
    governed_risk_surfaces = {
        "authority_and_data": [
            _governed_surface_row(
                "Feed Attestation",
                attest_tile.state,
                (attest_tile.reason_codes or ["No attestation detail"])[0],
                attest_tile.artifact_path,
            ),
            _governed_surface_row(
                "Replay Certification",
                replay_tile.state if replay_tile else "MISSING",
                ((replay_tile.reason_codes if replay_tile else []) or ["Replay proof unavailable"])[0],
                replay_tile.artifact_path if replay_tile else None,
            ),
            _governed_surface_row(
                "Canonical Authority Head",
                "PASS" if canonical_pointer_path.exists() else "MISSING",
                "Canonical authority pointer present" if canonical_pointer_path.exists() else "Canonical authority pointer missing",
                str(canonical_pointer_path),
            ),
        ],
        "broker_and_execution": [
            _governed_surface_row(
                "Broker Connection / Observer",
                broker_tile.state,
                (broker_tile.reason_codes or ["Broker observer state available"])[0],
                broker_tile.artifact_path,
            ),
            _governed_surface_row(
                "Gate Stack Verdict",
                gate_tile.state if gate_tile else "MISSING",
                ((gate_tile.reason_codes if gate_tile else []) or ["Gate stack verdict unavailable"])[0],
                gate_tile.artifact_path if gate_tile else None,
            ),
            _governed_surface_row(
                "Kill Switch",
                "PASS" if kill_switch_state.get("state") == "INACTIVE" else str(kill_switch_state.get("state") or "UNKNOWN"),
                ((kill_switch_state.get("reason_codes") or ["Entries allowed" if kill_switch_state.get("allow_entries") is True else "Kill-switch state not confirmed"]))[0],
                kill_switch_state.get("path"),
                authoritative=False,
            ),
        ],
        "risk_envelope": [
            _governed_surface_row(
                "Capital Risk Envelope",
                capital_tile.state,
                (capital_tile.reason_codes or ["Capital risk envelope available"])[0],
                capital_tile.artifact_path,
            ),
            _governed_surface_row(
                "Liquidity Gate",
                liquidity_tile.state,
                (liquidity_tile.reason_codes or ["Liquidity gate available"])[0],
                liquidity_tile.artifact_path,
            ),
            _governed_surface_row(
                "Correlation Gate",
                corr_tile.state,
                (corr_tile.reason_codes or ["Correlation gate available"])[0],
                corr_tile.artifact_path,
            ),
            _governed_surface_row(
                "Convex Risk",
                convex_tile.state,
                (convex_tile.reason_codes or ["Convex risk surface available"])[0],
                convex_tile.artifact_path,
            ),
        ],
    }

    # Provenance aggregation
    missing_paths = sorted(
        set(
            miss_a
            + miss_rv
            + miss_gs
            + miss_rep
            + miss_afd
            + miss_oms
            + miss_dsb
            + miss_tds
            + miss_roll
            + miss_int
            + miss_sub
            + miss_auth
            + miss_veto
            + miss_pos
            + miss_exp
            + [str(x) for x in (order_flow.get("missing_paths") or []) if isinstance(x, str)]
            + miss_nav
            + miss_att
            + miss_liq
            + miss_cor
            + miss_cvx
            + miss_cap
            + miss_eng
            + miss_ma
            + miss_broker
            + miss_bond
        )
    )

    source_paths = sorted(
        set(
            sp_a
            + sp_rv
            + sp_gs
            + sp_rep
            + sp_afd
            + sp_oms
            + sp_dsb
            + sp_tds
            + ([nav_path] if isinstance(nav_path, str) and nav_path else [])
            + sp_bond
            + [str(instance_config_path)]
        )
    )

    source_mtimes: Dict[str, float] = {}
    for dct in (sm_a, sm_rv, sm_gs, sm_rep, sm_afd, sm_oms, sm_dsb, sm_tds, sm_bond):
        source_mtimes.update({k: v for k, v in dct.items() if isinstance(v, (int, float))})

    warnings = sorted(
        set(
            warn_a
            + warn_rv
            + warn_gs
            + warn_rep
            + warn_afd
            + warn_oms
            + warn_dsb
            + warn_tds
            + warn_att
            + warn_liq
            + warn_cor
            + warn_cvx
            + warn_cap
            + warn_broker
            + warn_ma
            + warn_eng
            + warn_sleeves
            + warn_bond
            + (["POSITIONS_UNREADABLE"] if pos_err else [])
            + (["EXPOSURE_UNREADABLE"] if exp_err else [])
            + (["NAV_UNREADABLE"] if nav_err else [])
            + (["SUBMISSION_ORDER_FLOW_MISSING"] if order_flow.get("missing_paths") else [])
        )
    )

    platform_bug_metrics = dict(control_plane_bundle.get("platform_bug_metrics") or {})
    platform_readiness = dict(control_plane_bundle.get("platform_readiness") or {})
    platform_readiness_policy = load_platform_readiness_policy_view_v1(policy_path=PLATFORM_READINESS_POLICY_PATH)
    signal_activity = load_signal_activity_v1(
        truth_root=truth_root,
        day=day,
        run_doc=run_doc,
        engine_ids_from_active_engine_set_fn=_engine_ids_from_active_engine_set,
        engine_ids_from_engine_linkage_fn=_engine_ids_from_engine_linkage,
        load_sleeve_policy_fn=_load_sleeve_policy,
        engine_ids_from_heartbeat_fn=_engine_ids_from_heartbeat,
        load_gate_stack_day_state_fn=_load_gate_stack_day_state,
        load_kill_switch_day_state_fn=_load_kill_switch_day_state,
        compute_signal_frequency_30d_fn=_compute_signal_frequency_30d,
        count_phasec_veto_records_fn=_count_phasec_veto_records,
        discover_phasec_identity_dirs_fn=_discover_phasec_identity_dirs,
        attempt_stage_view_fn=_attempt_stage_view,
        jsonl_same_day_presence_fn=_jsonl_same_day_presence,
        derive_trading_day_outcome_fn=_derive_trading_day_outcome,
    )
    operational_readiness = _derive_operational_readiness(
        day=day,
        platform_readiness=platform_readiness,
        signal_activity=signal_activity,
        trading_day_state=trading_day_state_doc if isinstance(trading_day_state_doc, dict) else {},
        day_start_blocked=day_start_blocked_doc if isinstance(day_start_blocked_doc, dict) else {},
    )
    advisor_mode = str(mode_from_attempt or "PAPER").strip().upper()
    if advisor_mode not in {"PAPER", "LIVE"}:
        advisor_mode = "PAPER"
    advisor_visibility = _build_advisor_visibility(
        truth_root=truth_root,
        day=day,
        mode=advisor_mode,
        platform_readiness=platform_readiness,
        operational_readiness=operational_readiness,
    )

    payload: Dict[str, Any] = {
        "meta": {
            "server_time_utc": _utc_now_iso(),
            "selected_day": day,
            "selected_attempt_id": sel_attempt,
            "attempts": attempts,
            "attempt_summaries": attempt_summaries,
            "canonical_pointer": {
                "exists": (truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve().exists(),
                "points_to_attempt_id": None,
                "last_updated_utc": None,
                "note": "Canonical authority head presence only; attempt linkage not yet derived by this collector.",
            },
        },
        "ops_health": {"tiles": [_tile_dict(t) for t in tiles]},
        "scope_health": dict(run_scope_bundle["scope_health"]),
        "sleeve_live_readiness": dict(control_plane_bundle.get("sleeve_live_readiness") or {}),
        "platform_bug_metrics": platform_bug_metrics,
        "platform_readiness": platform_readiness,
        "operational_readiness": operational_readiness,
        "platform_readiness_policy": platform_readiness_policy,
        "platform_readiness_history": load_platform_readiness_history_v1(global_truth_root=GLOBAL_RUNTIME_TRUTH_ROOT),
        "signal_activity": signal_activity,
        "advisor_visibility": advisor_visibility,
        "governed_risk_surfaces": governed_risk_surfaces,
        "sleeves": sleeves_out,
        "trade_flow_today": {
            "counts": counts,
            "blocked_by_gate": blocked_by_gate,
            "execution_authority": execution_authority,
            "semantics": {
                "intents": "Intents emitted for selected day.",
                "rejected": "Authorization records with status REJECTED.",
                "authorized": "Intents authorized by capital authority.",
                "submitted": "Broker submission records written.",
                "filled": "Execution event records with fill status.",
                "reconciled": "Execution reconciliation completed for day.",
                "vetoed": "PhaseC submit veto records observed for day.",
            },
            "drilldown": _flow_drilldown(truth_root, day, counts),
        },
        "activity_flow_diagnostics": activity_flow_doc if isinstance(activity_flow_doc, dict) else {
            "present": False,
            "terminal_state": "INTERNAL_INCONSISTENCY",
            "suspicion_flags": [],
            "counts": {},
            "rejection_breakdown": [],
        },
        "intent_terminal_dispositions": oms_summary,
        "day_start_blocked": day_start_blocked_doc if isinstance(day_start_blocked_doc, dict) else {
            "present": False,
            "blocked": False,
            "status": "UNKNOWN",
        },
        "trading_day_state": trading_day_state_doc if isinstance(trading_day_state_doc, dict) else {
            "present": False,
            "state": "UNKNOWN_FAILURE",
            "heartbeat_status": "FAIL",
            "dependency_statuses": [],
            "freshness_checks": [],
        },
        "engines": engines_out,
        "portfolio": portfolio,
        "positions_exposure": positions_exposure,
        "bond_sleeve": bond_sleeve,
        "provenance": {
            "warnings": warnings,
            "missing_paths": missing_paths,
            "source_paths": source_paths,
            "source_mtimes": source_mtimes,
        },
        "errors": [],
        "ok": True,
    }
    payload["platform_bug_metrics"] = platform_bug_metrics if isinstance(platform_bug_metrics, dict) else {
        "present": False,
        "path": None,
        "produced_utc": None,
        "bug_velocity_7d_avg": None,
        "bug_velocity_14d_avg": None,
        "recurrence_rate": None,
        "diagnostic_stability_rate": None,
        "new_bug_events_today": None,
        "bug_velocity_trend": None,
        "recurring_bug_events": [],
        "event_counts_by_day": {},
        "metric_views": {},
        "calculation_summary": {},
        "unknown_fields": [],
        "evidence_paths": [],
        "reason_codes": ["ARTIFACT_UNREADABLE"],
    }
    payload["platform_readiness"] = platform_readiness if isinstance(platform_readiness, dict) else {
        "present": False,
        "path": None,
        "requested_day_path": None,
        "requested_day_present": False,
        "resolved_via_latest_pointer": False,
        "latest_pointer_path": None,
        "resolved_day": "",
        "platform_readiness_state": "UNKNOWN",
        "platform_readiness_score": None,
        "platform_readiness_grade": None,
        "score_threshold_ready": None,
        "produced_utc": None,
        "metric_views": {},
        "policy_values": {},
        "score_contribution": [],
        "platform_promotion_candidate": None,
        "readiness_summary": "",
        "promotion_decision_basis": "",
        "root_blockers": [],
        "derived_blockers": [],
        "top_blockers_ordered": [],
        "minimum_conditions_summary": [],
        "current_vs_required": {},
        "promotion_checklist": {},
        "smallest_clearance_set": [],
        "blocker_dependency_order": [],
        "bug_stability_summary": "",
        "aggregate_blocker_summary": {},
        "calibration_support": {},
        "evidence_paths": [],
        "reason_codes": ["ARTIFACT_UNREADABLE"],
    }
    payload["platform_readiness_policy"] = platform_readiness_policy if isinstance(platform_readiness_policy, dict) else {
        "present": False,
        "path": None,
        "score_threshold_ready": None,
        "grade_bands": [],
        "weights": {},
        "hard_blockers": {},
        "reason_codes": ["ARTIFACT_UNREADABLE"],
    }
    payload["signal_activity"] = payload.get("signal_activity") if isinstance(payload.get("signal_activity"), dict) else {
        "day_utc": day,
        "trading_day_outcome": {
            "day_utc": day,
            "classification": "EXECUTION_FAILURE_DAY",
            "label": "EXECUTION BLOCKED",
            "subtitle": "See active blocker below",
            "tone": "negative",
            "execution_path": "BLOCKED",
            "facts": [],
            "gate_stack": {"status": "UNKNOWN", "reason_codes": [], "path": None},
            "kill_switch": {"state": "UNKNOWN", "allow_entries": None, "allow_exits": None, "reason_codes": [], "path": None},
            "orchestrator_status": "UNKNOWN",
            "submit_reason_codes": [],
        },
        "signal_frequency_30d": {
            "window_days": 30,
            "avg_signals_per_day": None,
            "last_signal_date": None,
            "last_submit_date": None,
            "status": "unknown",
            "label": "Signal frequency unavailable",
        },
        "engine_heartbeats": {"expected_count": 0, "present_count": 0, "expected_engine_ids": [], "present_engine_ids": [], "missing_engine_ids": []},
        "intents": {"count": 0, "label": "No real intents produced", "path": None},
        "phasec_outcomes": {"veto_count": 0, "released_identity_dir_count": 0, "label": "no phaseC outputs", "tone": "neutral", "path": None},
        "governed_submit": {"stage_status": None, "label": "governed submit not reached", "tone": "neutral", "reason_codes": []},
        "upstream_data_status": {"label": "upstream data incomplete", "tone": "negative", "symbols": []},
        "gate_stack": {"status": "UNKNOWN", "reason_codes": [], "path": None},
        "kill_switch": {"state": "UNKNOWN", "allow_entries": None, "allow_exits": None, "reason_codes": [], "path": None},
    }
    td = payload.get("trade_flow_today")
    if isinstance(td, dict):
        dd = td.get("drilldown")
        if isinstance(dd, dict):
            sub = dd.get("submitted")
            if isinstance(sub, dict):
                sub["pending_count"] = (
                    order_flow.get("summary", {}).get("pending_orders")
                    if isinstance(order_flow.get("summary"), dict)
                    else None
                )
                sub["pending_records"] = (
                    order_flow.get("pending_orders", [])[:20]
                    if isinstance(order_flow.get("pending_orders"), list)
                    else []
                )

    # Server-side deterministic diff summary
    key_fields = _compute_key_fields(payload)
    cur_hash = _sha256_bytes(_stable_json_bytes(key_fields))
    diffs = _diff_key_fields(_LAST_KEY_FIELDS, key_fields)
    payload["meta"]["what_changed"] = {"diff_from_prev_poll": diffs, "key_fields_sha256": cur_hash}

    _LAST_HASH = cur_hash
    _LAST_KEY_FIELDS = key_fields

    return payload


def build_status_v2(
    truth_root: Path,
    instance_config_path: Path,
    day: str,
    attempt_id: Optional[str],
    c3_status: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    from constellation_2.common.cockpit_status_obligation_pipeline_v1 import (
        run_cockpit_status_obligation_pipeline_v1,
    )

    report = run_cockpit_status_obligation_pipeline_v1(
        truth_root=truth_root,
        instance_config_path=instance_config_path,
        day=day,
        attempt_id=attempt_id,
        c3_status=c3_status,
        pipeline_mode="normal",
        budget_profile="contract_default",
    )
    return dict(report["payload"])
