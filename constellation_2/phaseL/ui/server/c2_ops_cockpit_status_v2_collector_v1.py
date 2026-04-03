#!/usr/bin/env python3
"""
Constellation 2.0 — Ops Cockpit UI V2 — Status Collector (Read-Only, Deterministic)

Contract:
- Reads ONLY canonical truth artifacts under constellation_2/runtime/truth and instance config JSON (if present).
- Produces a single deterministic payload for Operations + Engines.
- Fail-closed: missing/parse errors are explicit, never inferred.
- No nondeterministic ordering: all lists are sorted by stable keys.
- No trading logic changes.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


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
GLOBAL_RUNTIME_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
RUNTIME_STATE_PATH = (GLOBAL_RUNTIME_TRUTH_ROOT / "system_snapshot/constellation_runtime_state.v1.json").resolve()
BOND_OPERATOR_INPUT_ROOT = (REPO_ROOT / "constellation_2/operator_inputs/bond_sleeve").resolve()
BOND_POSITIONS_INPUT_PATH = (BOND_OPERATOR_INPUT_ROOT / "bond_positions_v1.json").resolve()
BOND_POLICY_INPUT_PATH = (BOND_OPERATOR_INPUT_ROOT / "bond_sleeve_policy_v1.json").resolve()
BOND_FAMILY_SPECS: List[Tuple[str, str]] = [
    ("bond_sleeve_policy_snapshot_v1", "bond_sleeve_policy_snapshot.v1.json"),
    ("bond_ladder_recommendation_v1", "bond_ladder_recommendation.v1.json"),
    ("bond_duration_report_v1", "bond_duration_report.v1.json"),
    ("bond_yield_report_v1", "bond_yield_report.v1.json"),
    ("bond_withdrawal_coverage_report_v1", "bond_withdrawal_coverage_report.v1.json"),
    ("bond_sleeve_capital_posture_v1", "bond_sleeve_capital_posture.v1.json"),
    ("bond_purchase_recommendation_v1", "bond_purchase_recommendation.v1.json"),
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
    source_paths.extend([str(BOND_POSITIONS_INPUT_PATH), str(BOND_POLICY_INPUT_PATH)])
    for p in [BOND_POSITIONS_INPUT_PATH, BOND_POLICY_INPUT_PATH]:
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

    policy = merged_artifacts.get("bond_sleeve_policy_snapshot_v1", {})
    ladder = merged_artifacts.get("bond_ladder_recommendation_v1", {})
    duration = merged_artifacts.get("bond_duration_report_v1", {})
    yld = merged_artifacts.get("bond_yield_report_v1", {})
    withdrawal = merged_artifacts.get("bond_withdrawal_coverage_report_v1", {})
    posture = merged_artifacts.get("bond_sleeve_capital_posture_v1", {})
    purchase = merged_artifacts.get("bond_purchase_recommendation_v1", {})

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

def discover_attempts(truth_root: Path, day: str) -> Tuple[List[str], List[str], List[str], Dict[str, float], List[str]]:
    """
    Deterministically list orchestrator v2 attempt directories for day.
    Filters to canonical v2 attempt ids that include "__A" (A0001 etc).
    """
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    attempts: List[str] = []

    v2_day_dir = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day).resolve()
    if not v2_day_dir.exists() or not v2_day_dir.is_dir():
        missing.append(str(v2_day_dir))
        warnings.append("ATTEMPTS_NOT_FOUND")
        return [], missing, [], {}, sorted(set(warnings))

    source_paths.append(str(v2_day_dir))
    mt = _mtime(v2_day_dir)
    if mt is not None:
        source_mtimes[str(v2_day_dir)] = mt

    for p in sorted([x for x in v2_day_dir.iterdir() if x.is_dir()], key=lambda x: x.name):
        name = p.name.strip()
        if name and "__A" in name:
            attempts.append(name)

    attempts = sorted(set(attempts))
    if not attempts:
        warnings.append("ATTEMPTS_NOT_FOUND")

    return attempts, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def select_latest_attempt(attempts: List[str]) -> Optional[str]:
    return attempts[-1] if attempts else None


def _load_attempt_verdict(truth_root: Path, day: str, attempt_id: str) -> Optional[Dict[str, Any]]:
    p = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day / attempt_id / "orchestrator_run_verdict.v2.json").resolve()
    obj, _err = _safe_read_json(p)
    if isinstance(obj, dict):
        return obj
    return None


def select_preferred_attempt(truth_root: Path, day: str, attempts: List[str]) -> Optional[str]:
    """
    Default attempt selection for auto mode:
    - Use latest attempt normally.
    - If latest is ABORTED but there is an earlier PASS on the same day,
      prefer the latest PASS so canonical healthy proof is not masked by a
      later aborted rerun.
    """
    latest = select_latest_attempt(attempts)
    if not isinstance(latest, str) or not latest:
        return None

    latest_doc = _load_attempt_verdict(truth_root, day, latest)
    latest_status = _coerce_state(str(latest_doc.get("status") or latest_doc.get("state") or "UNKNOWN")) if isinstance(latest_doc, dict) else "UNKNOWN"
    if latest_status != "ABORTED":
        return latest

    pass_attempts: List[Tuple[int, str]] = []
    for aid in attempts:
        doc = _load_attempt_verdict(truth_root, day, aid)
        if not isinstance(doc, dict):
            continue
        st = _coerce_state(str(doc.get("status") or doc.get("state") or "UNKNOWN"))
        if st != "PASS":
            continue
        seq = doc.get("attempt_seq")
        seq_i = int(seq) if isinstance(seq, int) else -1
        pass_attempts.append((seq_i, aid))
    if not pass_attempts:
        return latest

    pass_attempts.sort(key=lambda x: (x[0], x[1]))
    return pass_attempts[-1][1]


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


def _load_selected_run_verdict_doc(run_tile: Optional[Tile]) -> Optional[Dict[str, Any]]:
    if run_tile is None or not isinstance(run_tile.artifact_path, str) or not run_tile.artifact_path:
        return None
    obj, _err = _safe_read_json(Path(run_tile.artifact_path))
    if isinstance(obj, dict):
        return obj
    return None


def _load_scope_health_summary() -> Dict[str, Any]:
    obj, _err = _safe_read_json(RUNTIME_STATE_PATH)
    if not isinstance(obj, dict):
        return {
            "sleeve_execution_health": {"status": "UNKNOWN", "reason_codes": ["RUNTIME_STATE_MISSING"]},
            "system_monitoring_health": {"status": "UNKNOWN", "reason_codes": ["RUNTIME_STATE_MISSING"]},
            "overall": {"status": "UNKNOWN", "reason_codes": ["RUNTIME_STATE_MISSING"]},
            "source": {"path": str(RUNTIME_STATE_PATH), "present": False},
        }
    scope = obj.get("scope_health") if isinstance(obj.get("scope_health"), dict) else {}
    return {
        "sleeve_execution_health": scope.get("sleeve_execution_health", {"status": "UNKNOWN"}),
        "system_monitoring_health": scope.get("system_monitoring_health", {"status": "UNKNOWN"}),
        "overall": scope.get("overall", {"status": "UNKNOWN"}),
        "source": {"path": str(RUNTIME_STATE_PATH), "present": True},
    }


def _load_sleeve_live_readiness(truth_root: Path, day: str) -> Dict[str, Any]:
    p = (
        truth_root
        / "readiness_v1"
        / "sleeve_live_readiness_v1"
        / day
        / "sleeve_live_readiness.v1.json"
    ).resolve()
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return {
            "state": "UNKNOWN",
            "reason_codes": ["ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE"],
            "path": str(p),
            "present": False,
        }
    return {
        "state": str(obj.get("readiness_state") or "UNKNOWN"),
        "readiness_summary": str(obj.get("readiness_summary") or ""),
        "promotion_decision_basis": str(obj.get("promotion_decision_basis") or ""),
        "readiness_score": obj.get("readiness_score"),
        "score_threshold": obj.get("score_threshold"),
        "readiness_grade": obj.get("readiness_grade", obj.get("grade_band")),
        "grade_band": obj.get("grade_band", obj.get("readiness_grade")),
        "promotion_candidate": obj.get("promotion_candidate"),
        "promotion_blockers": obj.get("promotion_blockers") if isinstance(obj.get("promotion_blockers"), list) else [],
        "root_blockers": obj.get("root_blockers") if isinstance(obj.get("root_blockers"), list) else [],
        "derived_blockers": obj.get("derived_blockers") if isinstance(obj.get("derived_blockers"), list) else [],
        "aggregate_blocker_summary": obj.get("aggregate_blocker_summary") if isinstance(obj.get("aggregate_blocker_summary"), dict) else {},
        "promotion_blockers_detail": obj.get("promotion_blockers_detail") if isinstance(obj.get("promotion_blockers_detail"), list) else [],
        "minimum_conditions_summary": obj.get("minimum_conditions_summary") if isinstance(obj.get("minimum_conditions_summary"), list) else [],
        "current_vs_required": obj.get("current_vs_required") if isinstance(obj.get("current_vs_required"), dict) else {},
        "smallest_clearance_set": obj.get("smallest_clearance_set") if isinstance(obj.get("smallest_clearance_set"), list) else [],
        "blocker_dependency_order": obj.get("blocker_dependency_order") if isinstance(obj.get("blocker_dependency_order"), list) else [],
        "estimated_promotion_gate_sequence": obj.get("estimated_promotion_gate_sequence") if isinstance(obj.get("estimated_promotion_gate_sequence"), list) else [],
        "top_blockers_ordered": obj.get("top_blockers_ordered") if isinstance(obj.get("top_blockers_ordered"), list) else [],
        "pass_conditions_remaining": obj.get("pass_conditions_remaining") if isinstance(obj.get("pass_conditions_remaining"), list) else [],
        "recommended_next_actions": obj.get("recommended_next_actions") if isinstance(obj.get("recommended_next_actions"), list) else [],
        "calibration_support": obj.get("calibration_support") if isinstance(obj.get("calibration_support"), dict) else {},
        "promotion_checklist": obj.get("promotion_checklist") if isinstance(obj.get("promotion_checklist"), dict) else {},
        "reason_codes": obj.get("reason_codes") if isinstance(obj.get("reason_codes"), list) else [],
        "evidence_paths": obj.get("evidence_paths") if isinstance(obj.get("evidence_paths"), list) else [],
        "path": str(p),
        "present": True,
    }


def _load_platform_bug_metrics(truth_root: Path, day: str) -> Dict[str, Any]:
    _ = truth_root  # Platform readiness artifacts are governed under global runtime truth.
    root = (
        GLOBAL_RUNTIME_TRUTH_ROOT
        / "readiness_v1"
        / "constellation_bug_metrics_v1"
    ).resolve()
    p = (
        root
        / day
        / "constellation_bug_metrics.v1.json"
    ).resolve()
    obj, err = _safe_read_json(p)
    base = {
        "present": False,
        "path": str(p),
        "requested_day_path": str(p),
        "requested_day_present": False,
        "resolved_via_latest_pointer": False,
        "latest_pointer_path": str((root / "latest_pointer.v1.json").resolve()),
        "resolved_day": "",
        "produced_utc": None,
        "new_bug_events_today": None,
        "bug_velocity_7d_avg": None,
        "bug_velocity_14d_avg": None,
        "recurrence_rate": None,
        "diagnostic_stability_rate": None,
        "bug_velocity_trend": None,
        "recurring_bug_events": [],
        "event_counts_by_day": {},
        "metric_views": {},
        "calculation_summary": {},
        "unknown_fields": [],
        "evidence_paths": [],
        "reason_codes": [],
    }
    if not isinstance(obj, dict):
        pointer_path = (root / "latest_pointer.v1.json").resolve()
        pointer_obj, pointer_err = _safe_read_json(pointer_path)
        if isinstance(pointer_obj, dict):
            target_path_raw = str(pointer_obj.get("target_path") or "").strip()
            target_sha = str(pointer_obj.get("target_sha256") or "").strip().lower()
            if target_path_raw:
                target_path = Path(target_path_raw).expanduser().resolve()
                target_obj, target_err = _safe_read_json(target_path)
                if isinstance(target_obj, dict):
                    path_sha = ""
                    sha_ok = False
                    if target_path.exists() and target_path.is_file():
                        path_sha = (_sha256_file(target_path) or "").lower()
                        sha_ok = bool(target_sha and path_sha == target_sha)
                    event_counts = target_obj.get("event_counts_by_day")
                    return {
                        **base,
                        "present": True,
                        "path": str(target_path),
                        "requested_day_path": str(p),
                        "requested_day_present": False,
                        "resolved_via_latest_pointer": True,
                        "latest_pointer_path": str(pointer_path),
                        "latest_pointer_target_sha256": target_sha,
                        "latest_pointer_target_sha256_verified": sha_ok,
                        "resolved_day": str(target_obj.get("day_utc") or ""),
                        "produced_utc": target_obj.get("produced_utc"),
                        "new_bug_events_today": target_obj.get("new_bug_events_today"),
                        "bug_velocity_7d_avg": target_obj.get("bug_velocity_7d_avg"),
                        "bug_velocity_14d_avg": target_obj.get("bug_velocity_14d_avg"),
                        "recurrence_rate": target_obj.get("recurrence_rate"),
                        "diagnostic_stability_rate": target_obj.get("diagnostic_stability_rate"),
                        "bug_velocity_trend": target_obj.get("bug_velocity_trend"),
                        "recurring_bug_events": target_obj.get("recurring_bug_events") if isinstance(target_obj.get("recurring_bug_events"), list) else [],
                        "event_counts_by_day": event_counts if isinstance(event_counts, (dict, list)) else {},
                        "metric_views": target_obj.get("metric_views") if isinstance(target_obj.get("metric_views"), dict) else {},
                        "calculation_summary": target_obj.get("calculation_summary") if isinstance(target_obj.get("calculation_summary"), dict) else {},
                        "unknown_fields": target_obj.get("unknown_fields") if isinstance(target_obj.get("unknown_fields"), list) else [],
                        "evidence_paths": target_obj.get("evidence_paths") if isinstance(target_obj.get("evidence_paths"), list) else [],
                        "reason_codes": ["FALLBACK_TO_LATEST_POINTER"],
                        "fallback_source_reason_codes": ["ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE"],
                    }
                return {
                    **base,
                    "reason_codes": [
                        "ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE",
                        "LATEST_POINTER_TARGET_MISSING" if target_err == "FILE_NOT_FOUND" else "LATEST_POINTER_TARGET_UNREADABLE",
                    ],
                }
        return {
            **base,
            "reason_codes": [
                "ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE",
                "LATEST_POINTER_MISSING" if pointer_err == "FILE_NOT_FOUND" else "LATEST_POINTER_UNREADABLE",
            ],
        }
    event_counts = obj.get("event_counts_by_day")
    return {
        **base,
        "present": True,
        "requested_day_present": True,
        "resolved_day": str(obj.get("day_utc") or day),
        "reason_codes": [],
        "produced_utc": obj.get("produced_utc"),
        "new_bug_events_today": obj.get("new_bug_events_today"),
        "bug_velocity_7d_avg": obj.get("bug_velocity_7d_avg"),
        "bug_velocity_14d_avg": obj.get("bug_velocity_14d_avg"),
        "recurrence_rate": obj.get("recurrence_rate"),
        "diagnostic_stability_rate": obj.get("diagnostic_stability_rate"),
        "bug_velocity_trend": obj.get("bug_velocity_trend"),
        "recurring_bug_events": obj.get("recurring_bug_events") if isinstance(obj.get("recurring_bug_events"), list) else [],
        "event_counts_by_day": event_counts if isinstance(event_counts, (dict, list)) else {},
        "metric_views": obj.get("metric_views") if isinstance(obj.get("metric_views"), dict) else {},
        "calculation_summary": obj.get("calculation_summary") if isinstance(obj.get("calculation_summary"), dict) else {},
        "unknown_fields": obj.get("unknown_fields") if isinstance(obj.get("unknown_fields"), list) else [],
        "evidence_paths": obj.get("evidence_paths") if isinstance(obj.get("evidence_paths"), list) else [],
    }


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


def _load_signal_activity(truth_root: Path, day: str, run_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    expected_engine_ids, _miss_expected, _warn_expected = _engine_ids_from_active_engine_set(truth_root, day)
    if not expected_engine_ids:
        expected_engine_ids, _miss_linkage, _warn_linkage = _engine_ids_from_engine_linkage(truth_root, day)
    if not expected_engine_ids:
        policy_rows = _load_sleeve_policy()
        policy_engine_ids: List[str] = []
        for row in policy_rows:
            policy_engine_ids.extend(row.get("engine_ids") if isinstance(row.get("engine_ids"), list) else [])
        expected_engine_ids = sorted({str(eid).strip() for eid in policy_engine_ids if isinstance(eid, str) and str(eid).strip()})
    present_engine_ids, _miss_hb, _warn_hb = _engine_ids_from_heartbeat(truth_root, day)
    expected_set = sorted(set(expected_engine_ids))
    present_set = sorted(set(present_engine_ids))
    missing_engine_ids = [eid for eid in expected_set if eid not in set(present_set)]

    intents_root = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    intent_paths = sorted(intents_root.glob("*.exposure_intent.v1.json")) if intents_root.exists() and intents_root.is_dir() else []
    intent_count = len(intent_paths)

    veto_count, _ = _count_phasec_veto_records(truth_root, day)
    identity_dirs = _discover_phasec_identity_dirs(truth_root, day)
    released_identity_dir_count = len(identity_dirs)
    if released_identity_dir_count > 0 and veto_count > 0:
        phasec_label = "mixed"
        phasec_tone = "warning"
    elif released_identity_dir_count > 0:
        phasec_label = "released identities present"
        phasec_tone = "positive"
    elif veto_count > 0:
        phasec_label = "veto-only"
        phasec_tone = "warning"
    else:
        phasec_label = "no phaseC outputs"
        phasec_tone = "neutral"

    submit_stage = _attempt_stage_view(run_doc, "A7A_GOVERNED_SUBMIT_V5")
    submit_status = submit_stage.get("status")
    if released_identity_dir_count == 0 and veto_count > 0:
        submit_label = "governed abort / no identity"
        submit_tone = "warning"
    elif released_identity_dir_count == 0 and not submit_stage.get("present"):
        submit_label = "blocked upstream before governed submit"
        submit_tone = "negative"
    elif submit_status in ("OK", "PASS"):
        submit_label = "governed submit passed"
        submit_tone = "positive"
    elif submit_status == "SKIP":
        submit_label = "governed abort / no identity"
        submit_tone = "warning"
    elif submit_status in ("FAIL", "ABORTED"):
        submit_label = "governed submit failed"
        submit_tone = "negative"
    else:
        submit_label = "governed submit not reached"
        submit_tone = "neutral"

    symbols = ["IWM", "SPY", "QQQ"]
    upstream_rows: List[Dict[str, Any]] = []
    for symbol in symbols:
        md_path = (truth_root / "market_data_snapshot_v1" / symbol / f"{day[:4]}.jsonl").resolve()
        same_day_present = _jsonl_same_day_presence(md_path, day) if md_path.exists() and md_path.is_file() else False
        upstream_rows.append(
            {
                "symbol": symbol,
                "same_day_present": same_day_present,
                "path": str(md_path),
                "status": "PRESENT" if same_day_present else "MISSING",
                "tone": "positive" if same_day_present else "negative",
            }
        )

    upstream_summary = "upstream data ready" if all(row["same_day_present"] for row in upstream_rows) else "upstream data incomplete"
    upstream_tone = "positive" if all(row["same_day_present"] for row in upstream_rows) else "negative"
    if released_identity_dir_count == 0 and veto_count == 0 and intent_count == 0 and upstream_tone == "negative":
        submit_label = "blocked upstream before governed submit"
        submit_tone = "negative"

    return {
        "day_utc": day,
        "engine_heartbeats": {
            "expected_count": len(expected_set),
            "present_count": len(present_set),
            "expected_engine_ids": expected_set,
            "present_engine_ids": present_set,
            "missing_engine_ids": missing_engine_ids,
        },
        "intents": {
            "count": intent_count,
            "label": "No real intents produced" if intent_count == 0 else f"{intent_count} real intents produced",
            "path": str(intents_root),
        },
        "phasec_outcomes": {
            "veto_count": veto_count,
            "released_identity_dir_count": released_identity_dir_count,
            "label": phasec_label,
            "tone": phasec_tone,
            "path": str((truth_root / 'phaseC_preflight_v1' / day).resolve()),
        },
        "governed_submit": {
            "stage_status": submit_status,
            "label": submit_label,
            "tone": submit_tone,
            "reason_codes": submit_stage.get("reason_codes") if isinstance(submit_stage.get("reason_codes"), list) else [],
        },
        "upstream_data_status": {
            "label": upstream_summary,
            "tone": upstream_tone,
            "symbols": upstream_rows,
        },
    }


def _load_platform_readiness_policy_view() -> Dict[str, Any]:
    obj, err = _safe_read_json(PLATFORM_READINESS_POLICY_PATH)
    base = {
        "present": False,
        "path": str(PLATFORM_READINESS_POLICY_PATH),
        "score_threshold_ready": None,
        "grade_bands": [],
        "weights": {},
        "hard_blockers": {},
        "reason_codes": ["ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE"],
    }
    if not isinstance(obj, dict):
        return base
    scoring = obj.get("scoring") if isinstance(obj.get("scoring"), dict) else {}
    hard_blockers = obj.get("hard_blockers") if isinstance(obj.get("hard_blockers"), dict) else {}
    weights = obj.get("weights") if isinstance(obj.get("weights"), dict) else {}
    return {
        **base,
        "present": True,
        "reason_codes": [],
        "score_threshold_ready": scoring.get("score_threshold_ready"),
        "grade_bands": scoring.get("grade_bands") if isinstance(scoring.get("grade_bands"), list) else [],
        "weights": weights,
        "hard_blockers": hard_blockers,
    }


def _load_platform_readiness(truth_root: Path, day: str) -> Dict[str, Any]:
    _ = truth_root  # Platform readiness artifacts are governed under global runtime truth.
    root = (
        REPO_ROOT
        / "constellation_2/runtime/truth"
        / "readiness_v1"
        / "constellation_platform_readiness_v1"
    ).resolve()
    p = (
        root
        / day
        / "constellation_platform_readiness.v1.json"
    ).resolve()
    base = {
        "present": False,
        "path": str(p),
        "requested_day_path": str(p),
        "requested_day_present": False,
        "resolved_via_latest_pointer": False,
        "latest_pointer_path": str((root / "latest_pointer.v1.json").resolve()),
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
        "reason_codes": [],
    }
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        pointer_path = (root / "latest_pointer.v1.json").resolve()
        pointer_obj, pointer_err = _safe_read_json(pointer_path)
        if isinstance(pointer_obj, dict):
            target_path_raw = str(pointer_obj.get("target_path") or "").strip()
            target_sha = str(pointer_obj.get("target_sha256") or "").strip().lower()
            if target_path_raw:
                target_path = Path(target_path_raw).expanduser().resolve()
                target_obj, target_err = _safe_read_json(target_path)
                if isinstance(target_obj, dict):
                    path_sha = ""
                    sha_ok = False
                    if target_path.exists() and target_path.is_file():
                        path_sha = _sha256_file(target_path).lower()
                        sha_ok = bool(target_sha and path_sha == target_sha)
                    return {
                        **base,
                        "present": True,
                        "path": str(target_path),
                        "requested_day_path": str(p),
                        "requested_day_present": False,
                        "resolved_via_latest_pointer": True,
                        "latest_pointer_path": str(pointer_path),
                        "latest_pointer_target_sha256": target_sha,
                        "latest_pointer_target_sha256_verified": sha_ok,
                        "resolved_day": str(target_obj.get("day_utc") or ""),
                        "platform_readiness_state": str(target_obj.get("platform_readiness_state") or "UNKNOWN"),
                        "platform_readiness_score": target_obj.get("platform_readiness_score"),
                        "platform_readiness_grade": target_obj.get("platform_readiness_grade"),
                        "score_threshold_ready": target_obj.get("score_threshold_ready"),
                        "produced_utc": target_obj.get("produced_utc"),
                        "metric_views": target_obj.get("metric_views") if isinstance(target_obj.get("metric_views"), dict) else {},
                        "policy_values": target_obj.get("policy_values") if isinstance(target_obj.get("policy_values"), dict) else {},
                        "score_contribution": target_obj.get("score_contribution") if isinstance(target_obj.get("score_contribution"), list) else [],
                        "platform_promotion_candidate": target_obj.get("platform_promotion_candidate"),
                        "readiness_summary": str(target_obj.get("readiness_summary") or ""),
                        "promotion_decision_basis": str(target_obj.get("promotion_decision_basis") or ""),
                        "root_blockers": target_obj.get("root_blockers") if isinstance(target_obj.get("root_blockers"), list) else [],
                        "derived_blockers": target_obj.get("derived_blockers") if isinstance(target_obj.get("derived_blockers"), list) else [],
                        "top_blockers_ordered": target_obj.get("top_blockers_ordered") if isinstance(target_obj.get("top_blockers_ordered"), list) else [],
                        "minimum_conditions_summary": target_obj.get("minimum_conditions_summary") if isinstance(target_obj.get("minimum_conditions_summary"), list) else [],
                        "current_vs_required": target_obj.get("current_vs_required") if isinstance(target_obj.get("current_vs_required"), dict) else {},
                        "promotion_checklist": target_obj.get("promotion_checklist") if isinstance(target_obj.get("promotion_checklist"), dict) else {},
                        "smallest_clearance_set": target_obj.get("smallest_clearance_set") if isinstance(target_obj.get("smallest_clearance_set"), list) else [],
                        "blocker_dependency_order": target_obj.get("blocker_dependency_order") if isinstance(target_obj.get("blocker_dependency_order"), list) else [],
                        "bug_stability_summary": str(target_obj.get("bug_stability_summary") or ""),
                        "aggregate_blocker_summary": target_obj.get("aggregate_blocker_summary") if isinstance(target_obj.get("aggregate_blocker_summary"), dict) else {},
                        "calibration_support": target_obj.get("calibration_support") if isinstance(target_obj.get("calibration_support"), dict) else {},
                        "evidence_paths": target_obj.get("evidence_paths") if isinstance(target_obj.get("evidence_paths"), list) else [],
                        "reason_codes": ["FALLBACK_TO_LATEST_POINTER"],
                        "fallback_source_reason_codes": ["ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE"],
                    }
                return {
                    **base,
                    "reason_codes": [
                        "ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE",
                        "LATEST_POINTER_TARGET_MISSING" if target_err == "FILE_NOT_FOUND" else "LATEST_POINTER_TARGET_UNREADABLE",
                    ],
                }
        return {
            **base,
            "reason_codes": [
                "ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE",
                "LATEST_POINTER_MISSING" if pointer_err == "FILE_NOT_FOUND" else "LATEST_POINTER_UNREADABLE",
            ],
        }
    return {
        **base,
        "present": True,
        "requested_day_present": True,
        "resolved_day": str(obj.get("day_utc") or day),
        "platform_readiness_state": str(obj.get("platform_readiness_state") or "UNKNOWN"),
        "platform_readiness_score": obj.get("platform_readiness_score"),
        "platform_readiness_grade": obj.get("platform_readiness_grade"),
        "score_threshold_ready": obj.get("score_threshold_ready"),
        "produced_utc": obj.get("produced_utc"),
        "metric_views": obj.get("metric_views") if isinstance(obj.get("metric_views"), dict) else {},
        "policy_values": obj.get("policy_values") if isinstance(obj.get("policy_values"), dict) else {},
        "score_contribution": obj.get("score_contribution") if isinstance(obj.get("score_contribution"), list) else [],
        "platform_promotion_candidate": obj.get("platform_promotion_candidate"),
        "readiness_summary": str(obj.get("readiness_summary") or ""),
        "promotion_decision_basis": str(obj.get("promotion_decision_basis") or ""),
        "root_blockers": obj.get("root_blockers") if isinstance(obj.get("root_blockers"), list) else [],
        "derived_blockers": obj.get("derived_blockers") if isinstance(obj.get("derived_blockers"), list) else [],
        "top_blockers_ordered": obj.get("top_blockers_ordered") if isinstance(obj.get("top_blockers_ordered"), list) else [],
        "minimum_conditions_summary": obj.get("minimum_conditions_summary") if isinstance(obj.get("minimum_conditions_summary"), list) else [],
        "current_vs_required": obj.get("current_vs_required") if isinstance(obj.get("current_vs_required"), dict) else {},
        "promotion_checklist": obj.get("promotion_checklist") if isinstance(obj.get("promotion_checklist"), dict) else {},
        "smallest_clearance_set": obj.get("smallest_clearance_set") if isinstance(obj.get("smallest_clearance_set"), list) else [],
        "blocker_dependency_order": obj.get("blocker_dependency_order") if isinstance(obj.get("blocker_dependency_order"), list) else [],
        "bug_stability_summary": str(obj.get("bug_stability_summary") or ""),
        "aggregate_blocker_summary": obj.get("aggregate_blocker_summary") if isinstance(obj.get("aggregate_blocker_summary"), dict) else {},
        "calibration_support": obj.get("calibration_support") if isinstance(obj.get("calibration_support"), dict) else {},
        "evidence_paths": obj.get("evidence_paths") if isinstance(obj.get("evidence_paths"), list) else [],
        "reason_codes": [],
    }


def _load_platform_readiness_history(truth_root: Path) -> Dict[str, Any]:
    _ = truth_root  # Platform readiness artifacts are governed under global runtime truth.
    root = (
        REPO_ROOT
        / "constellation_2/runtime/truth"
        / "readiness_v1"
        / "constellation_platform_readiness_v1"
    ).resolve()
    history: List[Dict[str, Any]] = []
    missing_paths: List[str] = []
    warnings: List[str] = []

    if not root.exists():
        return {
            "present": False,
            "root": str(root),
            "history": [],
            "date_range": None,
            "missing_paths": [str(root)],
            "warnings": ["PLATFORM_READINESS_HISTORY_ROOT_MISSING"],
        }

    for day_dir in sorted((p for p in root.iterdir() if p.is_dir() and _is_day_str(p.name)), key=lambda p: p.name):
        artifact_path = (day_dir / "constellation_platform_readiness.v1.json").resolve()
        obj, err = _safe_read_json(artifact_path)
        if not isinstance(obj, dict):
            missing_paths.append(str(artifact_path))
            warnings.append(
                f"PLATFORM_READINESS_HISTORY_ARTIFACT_{'MISSING' if err == 'FILE_NOT_FOUND' else 'UNREADABLE'}:{day_dir.name}"
            )
            continue
        history.append(
            {
                "day": str(obj.get("day_utc") or day_dir.name),
                "score": obj.get("platform_readiness_score"),
                "grade": str(obj.get("platform_readiness_grade") or ""),
                "state": str(obj.get("platform_readiness_state") or "UNKNOWN"),
                "threshold": obj.get("score_threshold_ready"),
                "path": str(artifact_path),
                "produced_utc": str(obj.get("produced_utc") or ""),
            }
        )

    date_range = None
    if history:
        date_range = {"start": history[0]["day"], "end": history[-1]["day"]}

    comparison: Dict[str, Any]
    if len(history) >= 2:
        previous = history[-2]
        latest = history[-1]
        latest_score = latest.get("score")
        previous_score = previous.get("score")
        score_change = None
        if isinstance(latest_score, (int, float)) and isinstance(previous_score, (int, float)):
            score_change = latest_score - previous_score
        comparison = {
            "present": True,
            "latest_day": latest["day"],
            "previous_day": previous["day"],
            "score_change": score_change,
            "grade_change": {
                "from": previous.get("grade"),
                "to": latest.get("grade"),
            },
            "state_change": {
                "from": previous.get("state"),
                "to": latest.get("state"),
            },
        }
    else:
        comparison = {
            "present": False,
            "reason": "NOT_ENOUGH_HISTORY",
        }

    return {
        "present": bool(history),
        "root": str(root),
        "history": history,
        "date_range": date_range,
        "comparison": comparison,
        "missing_paths": missing_paths,
        "warnings": warnings,
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


def _candidate_activity_rollup_path(truth_root: Path, day: str) -> Path:
    return (truth_root / "monitoring_v1" / "activity_ledger_rollup_v1" / day / "activity_ledger_rollup.v1.json").resolve()


def _load_activity_rollup(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    p = _candidate_activity_rollup_path(truth_root, day)
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)] if err == "FILE_NOT_FOUND" else [str(p)]
    return obj, []


def _extract_flow_from_activity_rollup(doc: Optional[Dict[str, Any]]) -> Dict[str, Optional[int]]:
    out: Dict[str, Optional[int]] = {
        "intents": None,
        "authorized": None,
        "submitted": None,
        "filled": None,
        "reconciled": None,
        "blocked_liquidity": None,
        "blocked_correlation": None,
        "blocked_attestation": None,
        "blocked_convex": None,
        "blocked_capital": None,
    }
    if not isinstance(doc, dict):
        return out

    totals = doc.get("totals") if isinstance(doc.get("totals"), dict) else None
    counts = doc.get("counts") if isinstance(doc.get("counts"), dict) else None
    src = totals or counts or doc

    def _get_int(keys: List[str]) -> Optional[int]:
        for k in keys:
            v = src.get(k) if isinstance(src, dict) else None
            if isinstance(v, int):
                return v
        return None

    out["intents"] = _get_int(["intents_total", "intents_today", "intents"])
    out["submitted"] = _get_int(["submissions_total", "submitted_total", "submitted"])
    out["authorized"] = _get_int(["authorized_total", "authorizations_total", "authorized"])
    out["filled"] = _get_int(["fills_total", "filled_total", "filled"])
    out["reconciled"] = _get_int(["reconciled_total", "reconciled"])

    blocked = doc.get("blocked_by_gate") if isinstance(doc.get("blocked_by_gate"), dict) else None
    if isinstance(blocked, dict):
        mapping = [
            ("liquidity", "blocked_liquidity"),
            ("correlation", "blocked_correlation"),
            ("attestation", "blocked_attestation"),
            ("convex", "blocked_convex"),
            ("capital", "blocked_capital"),
        ]
        for k, field in mapping:
            v = blocked.get(k)
            if isinstance(v, int):
                out[field] = v

    return out


# -------------------------
# Accounting / portfolio
# -------------------------

def _load_nav(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[str], Optional[str]]:
    p = (truth_root / "accounting_v2" / "nav" / day / "nav.v2.json").resolve()
    if not p.exists():
        return None, [str(p)], None, None
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)], str(err), str(p)
    return obj, [], None, str(p)


def _extract_portfolio_metrics(nav_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = {
        "nav_total": None,
        "pnl_today": None,
        "pnl_cumulative": None,
        "drawdown_pct": None,
        "cash_pct": None,
        "net_exposure_pct": None,
        "gross_exposure_pct": None,
        "asof_utc": None,
    }
    if not isinstance(nav_doc, dict):
        return out

    out["asof_utc"] = nav_doc.get("asof_utc") or nav_doc.get("generated_at_utc") or nav_doc.get("generated_utc")

    nav = nav_doc.get("nav") if isinstance(nav_doc.get("nav"), dict) else None
    src = nav or nav_doc

    def _get_num(keys: List[str]) -> Optional[float]:
        for k in keys:
            v = src.get(k) if isinstance(src, dict) else None
            if isinstance(v, (int, float)):
                return float(v)
        return None

    out["nav_total"] = _get_num(["nav_total", "nav_end", "nav"])
    out["pnl_today"] = _get_num(["pnl_today", "pnl_day", "pnl_1d"])
    out["pnl_cumulative"] = _get_num(["pnl_cumulative", "pnl_total", "pnl_cum"])
    out["drawdown_pct"] = _get_num(["drawdown_pct", "dd_pct"])
    out["cash_pct"] = _get_num(["cash_pct"])
    out["net_exposure_pct"] = _get_num(["net_exposure_pct", "net_pct"])
    out["gross_exposure_pct"] = _get_num(["gross_exposure_pct", "gross_pct"])
    return out


def _load_positions_snapshot(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[str], Optional[str]]:
    p = (truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json").resolve()
    if not p.exists():
        return None, [str(p)], None, None
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)], str(err), str(p)
    return obj, [], None, str(p)


def _load_exposure_net(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[str], Optional[str]]:
    p = (truth_root / "risk_v1" / "exposure_net_v1" / day / "exposure_net.v1.json").resolve()
    if not p.exists():
        return None, [str(p)], None, None
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)], str(err), str(p)
    return obj, [], None, str(p)


def _extract_positions_exposure(positions_doc: Optional[Dict[str, Any]], exposure_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "asof_utc": None,
        "summary": {
            "positions_total": 0,
            "open_positions": 0,
            "portfolio_net_notional_usd": None,
            "portfolio_gross_notional_usd": None,
            "capital_at_risk_cents": None,
            "symbol_count": None,
        },
        "positions": [],
        "exposure_by_engine": [],
        "sources": {"positions_path": None, "exposure_path": None},
    }

    if isinstance(positions_doc, dict):
        out["asof_utc"] = positions_doc.get("produced_utc") if isinstance(positions_doc.get("produced_utc"), str) else None
        pos = positions_doc.get("positions") if isinstance(positions_doc.get("positions"), dict) else {}
        items = pos.get("items") if isinstance(pos.get("items"), list) else []
        rows: List[Dict[str, Any]] = []
        open_cnt = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            status = str(it.get("status") or "UNKNOWN")
            qty = it.get("qty")
            if status.upper() == "OPEN":
                open_cnt += 1
            rows.append(
                {
                    "position_id": it.get("position_id"),
                    "engine_id": it.get("engine_id"),
                    "qty": qty if isinstance(qty, (int, float)) else None,
                    "status": status,
                    "market_exposure_type": it.get("market_exposure_type"),
                }
            )
        out["positions"] = rows
        out["summary"]["positions_total"] = len(rows)
        out["summary"]["open_positions"] = open_cnt

    if isinstance(exposure_doc, dict):
        portfolio = exposure_doc.get("portfolio") if isinstance(exposure_doc.get("portfolio"), dict) else {}
        out["summary"]["portfolio_net_notional_usd"] = portfolio.get("net_notional_usd")
        out["summary"]["portfolio_gross_notional_usd"] = portfolio.get("gross_notional_usd")
        out["summary"]["capital_at_risk_cents"] = portfolio.get("capital_at_risk_cents")
        out["summary"]["symbol_count"] = portfolio.get("symbol_count")

        per_engine = exposure_doc.get("per_engine") if isinstance(exposure_doc.get("per_engine"), list) else []
        e_rows: List[Dict[str, Any]] = []
        for it in per_engine:
            if not isinstance(it, dict):
                continue
            e_rows.append(
                {
                    "engine_id": it.get("engine_id"),
                    "net_notional_usd": it.get("net_notional_usd"),
                    "gross_notional_usd": it.get("gross_notional_usd"),
                    "capital_at_risk_cents": it.get("capital_at_risk_cents"),
                }
            )
        e_rows.sort(key=lambda x: str(x.get("engine_id") or ""))
        out["exposure_by_engine"] = e_rows

    return out


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


def _resolve_secondary_paper_account(primary_account: Optional[str]) -> Optional[str]:
    obj, _err = _safe_read_json(IB_ACCOUNT_REGISTRY)
    if not isinstance(obj, dict):
        return None
    accts = obj.get("accounts")
    if not isinstance(accts, list):
        return None
    primary = str(primary_account or "").strip()
    cands: List[str] = []
    for a in accts:
        if not isinstance(a, dict):
            continue
        env = str(a.get("environment") or "").strip().upper()
        enabled = bool(a.get("enabled_for_submission"))
        aid = str(a.get("account_id") or "").strip()
        if env == "PAPER" and enabled and aid:
            cands.append(aid)
    cands = sorted(set(cands))
    for aid in cands:
        if aid != primary:
            return aid
    return None


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
    secondary_account = _resolve_secondary_paper_account(primary_account)
    primary = str(primary_account or "").strip() or None
    mode = (mode_from_attempt or "UNKNOWN")

    out: List[Dict[str, Any]] = []
    for s in sleeves:
        engine_ids = s.get("engine_ids") or []
        is_active = any(e in active_engines for e in engine_ids)
        acct: Optional[str]
        if is_active:
            acct = primary
        else:
            acct = secondary_account or primary
        if acct is None:
            warnings.append(f"SLEEVE_ACCOUNT_UNRESOLVED:{s.get('sleeve_id')}")
        out.append(
            {
                "sleeve_id": s.get("sleeve_id"),
                "name": s.get("display_name"),
                "mode": mode,
                "ib_account_id": acct,
                "entries_allowed": None,
                "flatten_only": None,
                "engine_ids": engine_ids,
                "active_today": bool(is_active),
            }
        )
    out.sort(key=lambda x: (x.get("sleeve_id") or "", x.get("name") or ""))

    # Include only canonical standard trading sleeves that are not already engine-mapped.
    # Bond is not a trading sleeve and must never enter the generic sleeve payload path.
    reg_obj, reg_err = _safe_read_json(SLEEVE_REGISTRY)
    if not isinstance(reg_obj, dict):
        warnings.append(f"SLEEVE_REGISTRY_UNREADABLE:{reg_err}")
        return out, warnings
    reg_sleeves = reg_obj.get("sleeves")
    if not isinstance(reg_sleeves, list):
        warnings.append("SLEEVE_REGISTRY_INVALID")
        return out, warnings

    existing_ids = {str(r.get("sleeve_id") or "").strip() for r in out}
    for rs in reg_sleeves:
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
    out: List[Dict[str, Any]] = []
    for aid in attempts:
        doc = _load_attempt_verdict(truth_root, day, aid)
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
        "portfolio": {
            "nav_total": port.get("nav_total"),
            "pnl_today": port.get("pnl_today"),
            "pnl_cumulative": port.get("pnl_cumulative"),
            "asof_utc": port.get("asof_utc"),
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

def build_status_v2(
    truth_root: Path,
    instance_config_path: Path,
    day: str,
    attempt_id: Optional[str],
    c3_status: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    global _LAST_HASH, _LAST_KEY_FIELDS

    # Attempts
    attempts, miss_a, sp_a, sm_a, warn_a = discover_attempts(truth_root, day)
    raw_attempt = attempt_id.strip() if isinstance(attempt_id, str) else ""
    if raw_attempt in ("", "attempts", "latest"):
        sel_attempt = select_preferred_attempt(truth_root, day, attempts)
    else:
        sel_attempt = raw_attempt

    # Run verdict
    run_tile, miss_rv, sp_rv, sm_rv, warn_rv = _parse_orchestrator_run_verdict_v2(truth_root, day, sel_attempt)
    run_doc = _load_selected_run_verdict_doc(run_tile)

    # Gate stack verdict (optional)
    gate_tile, miss_gs, sp_gs, sm_gs, warn_gs = _parse_gate_stack_verdict_tile(truth_root, day)

    # Gate tiles (authoritative artifacts you proved exist)
    attest_path = (truth_root / "reports" / "feed_attestation_gate_v1" / day / "feed_attestation_gate.v1.json").resolve()
    liquidity_path = (truth_root / "reports" / "liquidity_slippage_gate_v1" / day / "liquidity_slippage_gate.v1.json").resolve()
    correlation_path = (truth_root / "reports" / "correlation_envelope_gate_v1" / day / "correlation_envelope_gate.v1.json").resolve()
    convex_path = (truth_root / "reports" / "convex_risk_assessment_v1" / day / "convex_risk_assessment.v1.json").resolve()

    attest_tile, warn_att, miss_att = _parse_simple_gate_tile(attest_path, "feed_attestation")
    liquidity_tile, warn_liq, miss_liq = _parse_simple_gate_tile(liquidity_path, "liquidity_gate")
    corr_tile, warn_cor, miss_cor = _parse_simple_gate_tile(correlation_path, "correlation_gate")
    convex_tile, warn_cvx, miss_cvx = _parse_simple_gate_tile(convex_path, "convex_gate")

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

    # Broker connection/observer
    broker_path_v2 = (truth_root / "reports" / "broker_reconciliation_v2" / day / "broker_reconciliation.v2.json").resolve()
    broker_path_v1 = (truth_root / "reports" / "broker_reconciliation_v1" / day / "broker_reconciliation.v1.json").resolve()

    if broker_path_v2.exists():
        broker_tile, warn_broker, miss_broker = _parse_simple_gate_tile(broker_path_v2, "broker_connection_observer")
    elif broker_path_v1.exists():
        broker_tile, warn_broker, miss_broker = _parse_simple_gate_tile(broker_path_v1, "broker_connection_observer")
    else:
        broker_state = "UNKNOWN"
        broker_last = None
        if isinstance(c3_status, dict):
            br = c3_status.get("broker_reconciliation") if isinstance(c3_status.get("broker_reconciliation"), dict) else None
            if isinstance(br, dict):
                broker_state = _coerce_state(str(br.get("state") or "UNKNOWN"))
                broker_last = br.get("generated_at_utc") or br.get("generated_utc") or None
        # Fallback to selected orchestrator attempt stage status when broker artifact is absent.
        if broker_state in ("UNKNOWN", "MISSING"):
            s = _attempt_stage_status(run_doc, "A1_BROKER_RECONCILIATION_GATE_V2_CHECK")
            if s in ("OK", "SKIP", "PASS"):
                broker_state = "PASS"
            elif s in ("FAIL", "ABORTED"):
                broker_state = "ABORTED"

        broker_tile = Tile(
            tile_id="broker_connection_observer",
            state=_coerce_state(broker_state),
            last_updated_utc=str(broker_last) if isinstance(broker_last, str) and broker_last else None,
            reason_codes=[],
            reason_human=[],
            artifact_path=None,
            artifact_sha256=None,
        )
        warn_broker = []
        miss_broker = [str(broker_path_v2), str(broker_path_v1)]

    # Flow
    rollup_doc, miss_roll = _load_activity_rollup(truth_root, day)
    flow = _extract_flow_from_activity_rollup(rollup_doc)
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

    # Portfolio
    nav_doc, miss_nav, nav_err, nav_path = _load_nav(truth_root, day)
    portfolio = _extract_portfolio_metrics(nav_doc)
    if nav_doc is None:
        portfolio["note_if_missing"] = "PnL unavailable (missing accounting/nav)"
        portfolio["missing"] = True
    else:
        portfolio["missing"] = False
    portfolio["nav_path"] = nav_path

    # Positions / Exposure
    positions_doc, miss_pos, pos_err, pos_path = _load_positions_snapshot(truth_root, day)
    exposure_doc, miss_exp, exp_err, exp_path = _load_exposure_net(truth_root, day)
    positions_exposure = _extract_positions_exposure(positions_doc, exposure_doc)
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

    # Provenance aggregation
    missing_paths = sorted(
        set(
            miss_a
            + miss_rv
            + miss_gs
            + miss_rep
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
            + ([nav_path] if isinstance(nav_path, str) and nav_path else [])
            + sp_bond
            + [str(instance_config_path)]
        )
    )

    source_mtimes: Dict[str, float] = {}
    for dct in (sm_a, sm_rv, sm_gs, sm_rep, sm_bond):
        source_mtimes.update({k: v for k, v in dct.items() if isinstance(v, (int, float))})

    warnings = sorted(
        set(
            warn_a
            + warn_rv
            + warn_gs
            + warn_rep
            + warn_att
            + warn_liq
            + warn_cor
            + warn_cvx
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

    platform_bug_metrics = _load_platform_bug_metrics(truth_root, day)
    platform_readiness = _load_platform_readiness(truth_root, day)
    platform_readiness_policy = _load_platform_readiness_policy_view()

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
        "scope_health": _load_scope_health_summary(),
        "sleeve_live_readiness": _load_sleeve_live_readiness(truth_root, day),
        "platform_bug_metrics": platform_bug_metrics,
        "platform_readiness": platform_readiness,
        "platform_readiness_policy": platform_readiness_policy,
        "platform_readiness_history": _load_platform_readiness_history(truth_root),
        "signal_activity": _load_signal_activity(truth_root, day, run_doc),
        "sleeves": sleeves_out,
        "trade_flow_today": {
            "counts": counts,
            "blocked_by_gate": blocked_by_gate,
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
        "engine_heartbeats": {"expected_count": 0, "present_count": 0, "expected_engine_ids": [], "present_engine_ids": [], "missing_engine_ids": []},
        "intents": {"count": 0, "label": "No real intents produced", "path": None},
        "phasec_outcomes": {"veto_count": 0, "released_identity_dir_count": 0, "label": "no phaseC outputs", "tone": "neutral", "path": None},
        "governed_submit": {"stage_status": None, "label": "governed submit not reached", "tone": "neutral", "reason_codes": []},
        "upstream_data_status": {"label": "upstream data incomplete", "tone": "negative", "symbols": []},
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
