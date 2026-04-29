#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_second_attempt_clearance_v1 import (
    latest_broker_submission_record_path_v1,
    load_clearance_for_submission_v1,
    load_prior_order_plan_from_clearance_v1,
    plan_hash_v1,
    sha256_file_v1,
    structure_pricing_signature_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1


SCHEMA_ID = "first_ib_accepted_paper_order_pre_submit_checklist"
SCHEMA_VERSION = "v1"
DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
DEFAULT_EXECUTION_ROOT = Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER")
POLICY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json"
PASS = "PASS"
FAIL = "FAIL"
NOT_CHECKED = "NOT_CHECKED"


@dataclass(frozen=True)
class CandidateContext:
    phasec_dir: Path | None
    identity_path: Path | None
    order_plan_path: Path | None
    mapping_path: Path | None
    binding_path: Path | None
    phasec_structure_path: Path | None
    submission_id: str
    identity: dict[str, Any]
    order_plan: dict[str, Any]
    mapping: dict[str, Any]
    binding: dict[str, Any]
    phasec_structure: dict[str, Any]


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def _path_text(path: Path | None) -> str:
    return "" if path is None else str(path.resolve())


def _abs_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return str(Path(text).expanduser().resolve())


def _dec(value: Any) -> Decimal | None:
    try:
        if isinstance(value, bool):
            return None
        out = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    return out if out.is_finite() else None


def _check(check_id: str, status: str, required: str, observed: Any, evidence_path: str, blocker: str) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": status,
        "required_value": required,
        "observed_value": observed,
        "evidence_path": evidence_path,
        "blocker": "" if status == PASS else blocker,
    }


def _report_path(truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "first_ib_accepted_paper_order_checklist_v1"
        / day_utc
        / "checklist.v1.json"
    )


def _market_gate_path(truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "market_open_data_gate_v1" / day_utc / "market_open_data_gate.v1.json"


def _structure_path(truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "structure_decision_supply_v1" / day_utc / "structure_decision_supply.v1.json"


def _submit_boundary_path(truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"


def _source_refs_by_type(identity: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    refs = identity.get("source_refs")
    if not isinstance(refs, list):
        return out
    for row in refs:
        if not isinstance(row, dict):
            continue
        ref_type = str(row.get("ref_type") or row.get("type") or "").strip()
        if ref_type:
            out[ref_type] = row
    return out


def _candidate_dirs(execution_root: Path, day_utc: str) -> list[Path]:
    root = Path(execution_root).resolve() / "phaseC_preflight_v1" / day_utc
    if not root.exists() or not root.is_dir():
        return []
    pointer = root / "latest_active_attempt.v1.json"
    dirs: list[Path] = []
    if pointer.exists() and pointer.is_file():
        payload = _read_json(pointer)
        attempt_dir = _abs_text(payload.get("attempt_dir"))
        if attempt_dir:
            dirs.extend(path.resolve() for path in Path(attempt_dir).glob("*/execution_identity_record.v1.json"))
    dirs.extend(path.resolve() for path in root.glob("attempt_A*/*/execution_identity_record.v1.json"))
    parents = sorted({path.parent.resolve() for path in dirs if path.exists()}, key=lambda path: path.stat().st_mtime)
    return parents


def _resolve_candidate(execution_root: Path, day_utc: str) -> CandidateContext:
    selected: Path | None = None
    for candidate in _candidate_dirs(execution_root, day_utc):
        decision = _read_json(candidate / "submit_preflight_decision.v1.json")
        if str(decision.get("decision") or "").strip().upper() == "ALLOW":
            selected = candidate
    if selected is None:
        dirs = _candidate_dirs(execution_root, day_utc)
        selected = dirs[-1] if dirs else None
    if selected is None:
        return CandidateContext(None, None, None, None, None, None, "", {}, {}, {}, {}, {})
    identity_path = selected / "execution_identity_record.v1.json"
    order_plan_path = selected / "order_plan.v1.json"
    mapping_path = selected / "mapping_ledger_record.v1.json"
    binding_path = selected / "binding_record.v1.json"
    structure_path = selected / "structure_decision_supply.v1.json"
    identity = _read_json(identity_path)
    return CandidateContext(
        phasec_dir=selected,
        identity_path=identity_path,
        order_plan_path=order_plan_path,
        mapping_path=mapping_path,
        binding_path=binding_path,
        phasec_structure_path=structure_path,
        submission_id=str(identity.get("submission_id") or "").strip(),
        identity=identity,
        order_plan=_read_json(order_plan_path),
        mapping=_read_json(mapping_path),
        binding=_read_json(binding_path),
        phasec_structure=_read_json(structure_path),
    )


def _contracts(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = snapshot.get("contracts")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _contract_key(row: dict[str, Any]) -> str:
    return str(row.get("contract_key") or "").strip()


def _con_id(row: dict[str, Any]) -> str:
    ib = row.get("ib") if isinstance(row.get("ib"), dict) else {}
    return str(ib.get("conId") or row.get("ib_conId") or "").strip()


def _quote(row: dict[str, Any], field: str) -> Decimal | None:
    value = _dec(row.get(field))
    if value is not None:
        return value
    quote = row.get("quote") if isinstance(row.get("quote"), dict) else {}
    return _dec(quote.get(field))


def _leg_contract(leg: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any] | None:
    key = str(leg.get("contract_key") or "").strip()
    conid = str(leg.get("ib_conId") or leg.get("conId") or "").strip()
    for row in _contracts(snapshot):
        if key and _contract_key(row) == key:
            return row
        if conid and _con_id(row) == conid:
            return row
    return None


def _selected_legs(order_plan: dict[str, Any], structure: dict[str, Any]) -> list[dict[str, Any]]:
    legs = order_plan.get("legs")
    if isinstance(legs, list) and all(isinstance(row, dict) for row in legs):
        return list(legs)
    decisions = structure.get("structure_decisions")
    if isinstance(decisions, list) and decisions and isinstance(decisions[0], dict):
        option_structure = decisions[0].get("option_structure") if isinstance(decisions[0].get("option_structure"), dict) else {}
        legs = option_structure.get("legs")
        if isinstance(legs, list) and all(isinstance(row, dict) for row in legs):
            return list(legs)
    return []


def _first_structure_decision(structure: dict[str, Any]) -> dict[str, Any]:
    decisions = structure.get("structure_decisions")
    if isinstance(decisions, list) and decisions and isinstance(decisions[0], dict):
        return decisions[0]
    return {}


def _pricing_inputs(structure: dict[str, Any]) -> dict[str, Any]:
    decision = _first_structure_decision(structure)
    pricing = decision.get("pricing_inputs") if isinstance(decision.get("pricing_inputs"), dict) else {}
    return pricing


def _policy_for_engine(engine_id: str) -> dict[str, Any]:
    payload = _read_json(POLICY_PATH)
    for row in payload.get("engine_policies") or []:
        if isinstance(row, dict) and str(row.get("engine_id") or "").strip() == engine_id:
            return row
    return {}


def _policy_engine_id(structure: dict[str, Any]) -> str:
    decision = _first_structure_decision(structure)
    return str(decision.get("sleeve_id") or "").strip()


def _snapshot_hash(path_text: str) -> str:
    path = Path(path_text)
    if not path.exists() or not path.is_file():
        return ""
    try:
        return sha256_file_v1(path)
    except Exception:
        return ""


def _submission_dir(execution_root: Path, day_utc: str, submission_id: str) -> Path | None:
    if not submission_id:
        return None
    return Path(execution_root).resolve() / "execution_evidence_v1" / "submissions" / day_utc / submission_id


def _load_prior_clearance(truth_root: Path, execution_root: Path, day_utc: str) -> tuple[Path | None, dict[str, Any], dict[str, Any]]:
    latest = latest_broker_submission_record_path_v1(execution_root=execution_root, day_utc=day_utc)
    if latest is None:
        return None, {}, {}
    prior_id = latest.parent.name
    try:
        clearance = load_clearance_for_submission_v1(truth_root=truth_root, day_utc=day_utc, prior_submission_id=prior_id)
    except Exception:
        return latest, {}, {}
    try:
        prior_plan = load_prior_order_plan_from_clearance_v1(clearance)
    except Exception:
        prior_plan = {}
    return latest, clearance, prior_plan


def _freshness_window_ok(cert: dict[str, Any], now_utc: str) -> tuple[bool, dict[str, str]]:
    valid_from = str(cert.get("valid_from_utc") or cert.get("snapshot_timestamp_utc") or "").strip()
    valid_until = str(cert.get("valid_until_utc") or cert.get("expiry_timestamp_utc") or "").strip()
    now_dt = _parse_utc(now_utc)
    from_dt = _parse_utc(valid_from)
    until_dt = _parse_utc(valid_until)
    ok = bool(now_dt and from_dt and until_dt and from_dt <= now_dt <= until_dt)
    return ok, {"valid_from_utc": valid_from, "valid_until_utc": valid_until, "validation_time_utc": now_utc}


CHECK_GROUPS = {
    "MARKET_DATA": {"01_MARKET_DATA_SNAPSHOT_FRESH", "02_UNDERLYING_SPOT_PRESENT", "03_OPTION_BID_ASK_PRESENT"},
    "SNAPSHOT_LINEAGE": {"04_MARKET_OPEN_GATE_SNAPSHOT_MATCH", "05_STRUCTURE_SNAPSHOT_MATCH", "06_PHASEC_SNAPSHOT_MATCH", "07_ORDER_PLAN_SNAPSHOT_MATCH"},
    "STRUCTURE": {"08_SELECTED_LEGS_EXIST_IN_SNAPSHOT", "09_STRUCTURE_NOT_NEAR_ITM", "10_WIDTH_VALID", "11_LIQUIDITY_PRESENT"},
    "PRICING": {"12_LIMIT_CREDIT_IN_RANGE", "13_LIMIT_CREDIT_NOT_STALE", "14_IB_PRICE_REALISTIC"},
    "IB_PAYLOAD": {"15_BAG_PARENT_ACTION", "16_BAG_LEG_ACTIONS", "17_BAG_LEG_RATIOS", "18_SMART_ROUTING_NONGUARANTEED"},
    "IB_PREVIEW": {"19_IB_PREVIEW_PASS", "20_NO_IB_ERROR_201", "21_NO_RISKLESS_CLASSIFICATION"},
    "GOVERNANCE": {"22_SECOND_ATTEMPT_CLEARANCE", "23_NEW_PLAN_HASH", "24_NEW_STRUCTURE_PRICING_SIGNATURE", "25_PAPER_ONLY_ACCOUNT"},
    "SUBMIT_AUTHORITY": {"26_SUBMIT_BOUNDARY_AUTHORIZED"},
}


def _check_group(check_id: str) -> str:
    for group, check_ids in CHECK_GROUPS.items():
        if check_id in check_ids:
            return group
    return "UNKNOWN"


def _grouped_check_summary(checks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_id = {str(row.get("check_id") or ""): row for row in checks}
    summary: dict[str, dict[str, Any]] = {}
    for group, check_ids in CHECK_GROUPS.items():
        rows = [by_id[check_id] for check_id in sorted(check_ids) if check_id in by_id]
        failed = [row for row in rows if row.get("status") == FAIL]
        not_checked = [row for row in rows if row.get("status") == NOT_CHECKED]
        first_bad = next((row for row in rows if row.get("status") != PASS), {})
        if failed:
            status = "BLOCKED"
        elif not_checked:
            status = NOT_CHECKED
        else:
            status = PASS
        summary[group] = {
            "status": status,
            "pass_count": sum(1 for row in rows if row.get("status") == PASS),
            "fail_count": len(failed),
            "not_checked_count": len(not_checked),
            "first_failed_check": str(first_bad.get("check_id") or ""),
            "canonical_blocker": str(first_bad.get("blocker") or ""),
        }
    return summary


def _leg_summary(leg: dict[str, Any]) -> dict[str, Any]:
    return {
        "action": str(leg.get("action") or ""),
        "right": str(leg.get("right") or ""),
        "strike": str(leg.get("strike") or ""),
        "expiry": str(leg.get("expiry_utc") or ""),
        "ratio": leg.get("ratio"),
        "contract_key": str(leg.get("contract_key") or ""),
        "ib_conId": leg.get("ib_conId") or leg.get("conId"),
    }


def _trade_summary(
    *,
    order_plan: dict[str, Any],
    structure: dict[str, Any],
    selected_legs: list[dict[str, Any]],
    environment: str,
    ib_account: str,
    preview_status: str,
) -> dict[str, Any]:
    if not order_plan and not structure and not selected_legs:
        return {}
    underlying = order_plan.get("underlying") if isinstance(order_plan.get("underlying"), dict) else {}
    decision = _first_structure_decision(structure)
    option_structure = decision.get("option_structure") if isinstance(decision.get("option_structure"), dict) else {}
    pricing = _pricing_inputs(structure)
    terms = order_plan.get("order_terms") if isinstance(order_plan.get("order_terms"), dict) else {}
    proof = order_plan.get("risk_proof") if isinstance(order_plan.get("risk_proof"), dict) else {}
    short_leg = next((leg for leg in selected_legs if str(leg.get("action") or "").upper() == "SELL"), {})
    long_leg = next((leg for leg in selected_legs if str(leg.get("action") or "").upper() == "BUY"), {})
    expiry = str(short_leg.get("expiry_utc") or long_leg.get("expiry_utc") or "")
    return {
        "symbol": str(underlying.get("symbol") or decision.get("symbol") or ""),
        "expiry": expiry,
        "structure": str(order_plan.get("structure") or option_structure.get("selected_structure") or decision.get("selected_structure") or ""),
        "short_leg": _leg_summary(short_leg) if short_leg else {},
        "long_leg": _leg_summary(long_leg) if long_leg else {},
        "width": str(proof.get("width_points") or option_structure.get("width_points") or ""),
        "credit": str(terms.get("limit_price") or option_structure.get("net_credit") or ""),
        "max_loss": str(proof.get("max_loss_usd") or option_structure.get("max_loss_cents") or ""),
        "account": ib_account,
        "environment": environment,
        "data_mode": str(pricing.get("data_mode") or ""),
        "preview_status": preview_status,
    }


def _plain_english_next_action(first_failed: dict[str, Any] | None) -> str:
    if first_failed is None:
        return "All pre-submit checklist checks passed. Operator may proceed only through the governed PAPER submit command."
    group = _check_group(str(first_failed.get("check_id") or ""))
    blocker = str(first_failed.get("blocker") or "UNKNOWN_BLOCKER")
    if group == "MARKET_DATA":
        return "Regenerate market-open data and rerun the checklist before submit."
    if group == "SNAPSHOT_LINEAGE":
        return "Regenerate structure decision, PhaseC, and order plan from the latest accepted market-open snapshot."
    if group == "STRUCTURE":
        return "Select a clearly OTM, liquid, governed-width structure from the latest accepted snapshot."
    if group == "PRICING":
        return "Regenerate pricing from the latest accepted bid/ask snapshot and keep credit inside the executable range."
    if group == "IB_PAYLOAD":
        return "Regenerate the IB BAG payload from the current order plan and verify routing fields."
    if group == "IB_PREVIEW":
        return "Run IB what-if preview for the exact payload and do not submit until preview passes."
    if group == "GOVERNANCE":
        return "Resolve second-attempt clearance, non-identical retry, or PAPER account governance before submit."
    if group == "SUBMIT_AUTHORITY":
        return "Refresh submit boundary and proceed only after it is AUTHORIZED."
    return f"Resolve {blocker} before broker transmit."


def build_first_ib_accepted_paper_order_checklist_v1(
    *,
    truth_root: Path,
    execution_root: Path,
    day_utc: str,
    environment: str,
    ib_account: str,
    now_utc: str | None = None,
) -> dict[str, Any]:
    day_utc = parse_day_utc_v1(day_utc)
    environment = str(environment or "").strip().upper()
    ib_account = str(ib_account or "").strip()
    now_utc = now_utc or _now_iso()
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve()
    checks: list[dict[str, Any]] = []

    gate_path = _market_gate_path(truth_root, day_utc)
    gate = _read_json(gate_path)
    gate_snapshot_path = _abs_text(gate.get("snapshot_path"))
    gate_cert_path = _abs_text(gate.get("freshness_certificate_path"))
    snapshot = _read_json(Path(gate_snapshot_path) if gate_snapshot_path else None)
    cert = _read_json(Path(gate_cert_path) if gate_cert_path else None)
    candidate = _resolve_candidate(execution_root, day_utc)
    structure = _read_json(_structure_path(truth_root, day_utc))
    selected_legs = _selected_legs(candidate.order_plan, structure)
    submission_dir = _submission_dir(execution_root, day_utc, candidate.submission_id)
    payload_path = None if submission_dir is None else submission_dir / "ib_order_payload.v1.json"
    preview_path = None if submission_dir is None else submission_dir / "ib_combo_preview.v1.json"
    payload = _read_json(payload_path)
    preview = _read_json(preview_path)
    boundary_path = _submit_boundary_path(truth_root, day_utc)
    boundary = _read_json(boundary_path)
    prior_record_path, clearance, prior_plan = _load_prior_clearance(truth_root, execution_root, day_utc)

    freshness_ok, freshness_obs = _freshness_window_ok(cert, now_utc)
    checks.append(_check(
        "01_MARKET_DATA_SNAPSHOT_FRESH",
        PASS if str(gate.get("status") or "").upper() == "PASS" and freshness_ok else FAIL,
        "market_open_data_gate PASS and freshness certificate valid at submit eval_time_utc",
        {"gate_status": gate.get("status"), **freshness_obs},
        gate_cert_path or str(gate_path),
        "MARKET_DATA_STALE",
    ))

    underlying = snapshot.get("underlying") if isinstance(snapshot.get("underlying"), dict) else {}
    spot = _dec(underlying.get("spot_price"))
    checks.append(_check(
        "02_UNDERLYING_SPOT_PRESENT",
        PASS if spot is not None and spot > 0 else FAIL,
        "underlying spot > 0",
        {"symbol": underlying.get("symbol"), "spot_price": underlying.get("spot_price")},
        gate_snapshot_path,
        "UNDERLYING_SPOT_MISSING",
    ))

    leg_quote_rows: list[dict[str, Any]] = []
    quote_ok = bool(selected_legs)
    for leg in selected_legs:
        row = _leg_contract(leg, snapshot) or {}
        bid = _quote(row, "bid")
        ask = _quote(row, "ask")
        leg_ok = bool(row) and bid is not None and ask is not None and bid > 0 and ask >= bid
        quote_ok = quote_ok and leg_ok
        leg_quote_rows.append({"leg": leg, "bid": None if bid is None else str(bid), "ask": None if ask is None else str(ask), "exists": bool(row)})
    checks.append(_check(
        "03_OPTION_BID_ASK_PRESENT",
        PASS if quote_ok else FAIL,
        "every selected leg has bid > 0 and ask >= bid",
        leg_quote_rows,
        gate_snapshot_path,
        "OPTION_BID_ASK_MISSING",
    ))

    checks.append(_check(
        "04_MARKET_OPEN_GATE_SNAPSHOT_MATCH",
        PASS if str(gate.get("status") or "").upper() == "PASS" and bool(gate_snapshot_path) and bool(gate_cert_path) else FAIL,
        "market_open_data_gate snapshot/cert are canonical latest accepted",
        {"gate_status": gate.get("status"), "snapshot_path": gate_snapshot_path, "freshness_certificate_path": gate_cert_path},
        str(gate_path),
        "SNAPSHOT_LINEAGE_MISMATCH",
    ))

    structure_market = structure.get("market_open_data") if isinstance(structure.get("market_open_data"), dict) else {}
    structure_pricing = _pricing_inputs(structure)
    structure_snapshot = _abs_text(structure_market.get("snapshot_path"))
    structure_cert = _abs_text(structure_market.get("freshness_certificate_path"))
    decision_snapshot = _abs_text(structure_pricing.get("snapshot_path"))
    decision_cert = _abs_text(structure_pricing.get("freshness_certificate_path"))
    structure_match = all(
        [
            str(structure.get("status") or "").upper() == "PASS",
            structure_snapshot == gate_snapshot_path,
            structure_cert == gate_cert_path,
            decision_snapshot == gate_snapshot_path,
            decision_cert == gate_cert_path,
        ]
    )
    checks.append(_check(
        "05_STRUCTURE_SNAPSHOT_MATCH",
        PASS if structure_match else FAIL,
        "Structure Decision snapshot/cert exactly match Market Open Data Gate",
        {"structure_snapshot": structure_snapshot, "decision_snapshot": decision_snapshot, "gate_snapshot": gate_snapshot_path, "structure_cert": structure_cert, "decision_cert": decision_cert, "gate_cert": gate_cert_path},
        str(_structure_path(truth_root, day_utc)),
        "SNAPSHOT_LINEAGE_MISMATCH",
    ))

    phasec_market = candidate.phasec_structure.get("market_open_data") if isinstance(candidate.phasec_structure.get("market_open_data"), dict) else {}
    phasec_pricing = _pricing_inputs(candidate.phasec_structure)
    snapshot_sha = _snapshot_hash(gate_snapshot_path)
    cert_sha = _snapshot_hash(gate_cert_path)
    phasec_match = all(
        [
            _abs_text(phasec_market.get("snapshot_path")) == gate_snapshot_path,
            _abs_text(phasec_market.get("freshness_certificate_path")) == gate_cert_path,
            _abs_text(phasec_pricing.get("snapshot_path")) == gate_snapshot_path,
            _abs_text(phasec_pricing.get("freshness_certificate_path")) == gate_cert_path,
            str(candidate.mapping.get("chain_snapshot_hash") or "").strip() == snapshot_sha,
            str(candidate.mapping.get("freshness_cert_hash") or "").strip() == cert_sha,
            str(candidate.binding.get("freshness_cert_hash") or "").strip() == cert_sha,
        ]
    )
    checks.append(_check(
        "06_PHASEC_SNAPSHOT_MATCH",
        PASS if phasec_match else FAIL,
        "PhaseC copied/source refs and mapping/binding hashes match Market Open Data Gate snapshot/cert",
        {"phasec_dir": _path_text(candidate.phasec_dir), "snapshot_hash": candidate.mapping.get("chain_snapshot_hash"), "expected_snapshot_hash": snapshot_sha, "freshness_hash": candidate.mapping.get("freshness_cert_hash"), "expected_freshness_hash": cert_sha},
        _path_text(candidate.phasec_dir),
        "SNAPSHOT_LINEAGE_MISMATCH",
    ))

    refs = _source_refs_by_type(candidate.identity)
    order_ref = refs.get("order_plan_ref") or {}
    order_ref_path = _abs_text(order_ref.get("path"))
    order_sha = _snapshot_hash(_path_text(candidate.order_plan_path))
    order_match = bool(candidate.order_plan) and order_ref_path == _path_text(candidate.order_plan_path) and str(order_ref.get("sha256") or "").strip() == order_sha and phasec_match
    checks.append(_check(
        "07_ORDER_PLAN_SNAPSHOT_MATCH",
        PASS if order_match else FAIL,
        "order_plan source ref is current and associated PhaseC lineage matches Market Open Data Gate snapshot/cert",
        {"order_plan_path": _path_text(candidate.order_plan_path), "order_ref_path": order_ref_path, "order_ref_sha256": order_ref.get("sha256"), "actual_order_sha256": order_sha},
        _path_text(candidate.order_plan_path),
        "SNAPSHOT_LINEAGE_MISMATCH",
    ))

    leg_existence = [{"leg": leg, "exists": _leg_contract(leg, snapshot) is not None} for leg in selected_legs]
    checks.append(_check(
        "08_SELECTED_LEGS_EXIST_IN_SNAPSHOT",
        PASS if bool(selected_legs) and all(row["exists"] for row in leg_existence) else FAIL,
        "all selected conIds/contract keys exist in accepted snapshot",
        leg_existence,
        gate_snapshot_path,
        "SELECTED_LEG_ABSENT",
    ))

    policy = _policy_for_engine(_policy_engine_id(structure))
    selection = ((policy.get("options_template") or {}).get("selection_policy") or {}) if isinstance(policy.get("options_template"), dict) else {}
    allow_near_itm = selection.get("allow_near_itm_same_week") is True
    clearly_otm = False
    width_policy = selection.get("width_policy") if isinstance(selection.get("width_policy"), dict) else {}
    governed_width = _dec(width_policy.get("width_points"))
    strikes = [_dec(leg.get("strike")) for leg in selected_legs]
    width = abs(strikes[0] - strikes[1]) if len(strikes) == 2 and strikes[0] is not None and strikes[1] is not None else None
    if spot is not None:
        for leg in selected_legs:
            if str(leg.get("action") or "").upper() != "SELL":
                continue
            strike = _dec(leg.get("strike"))
            right = str(leg.get("right") or "").upper()
            if strike is not None and width is not None and width > 0 and right == "PUT":
                clearly_otm = strike <= spot - width
            elif strike is not None and width is not None and width > 0 and right == "CALL":
                clearly_otm = strike >= spot + width
            else:
                clearly_otm = False
            break
    checks.append(_check(
        "09_STRUCTURE_NOT_NEAR_ITM",
        PASS if selected_legs and (clearly_otm or allow_near_itm) else FAIL,
        "short leg is clearly OTM by at least one spread width unless explicit policy approval exists",
        {"spot": None if spot is None else str(spot), "computed_width_points": None if width is None else str(width), "allow_near_itm_same_week": allow_near_itm, "selected_legs": selected_legs},
        str(POLICY_PATH),
        "NEAR_ITM_NOT_APPROVED",
    ))

    proof = candidate.order_plan.get("risk_proof") if isinstance(candidate.order_plan.get("risk_proof"), dict) else {}
    proof_width = _dec(proof.get("width_points"))
    width_ok = bool(width is not None and width > 0 and governed_width is not None and width == governed_width and (proof_width is None or proof_width == width))
    checks.append(_check(
        "10_WIDTH_VALID",
        PASS if width_ok else FAIL,
        "spread width is positive, matches governed moderate width policy, and matches risk proof when present",
        {"computed_width_points": None if width is None else str(width), "governed_width_points": None if governed_width is None else str(governed_width), "risk_proof_width_points": proof.get("width_points")},
        _path_text(candidate.order_plan_path),
        "WIDTH_INVALID",
    ))

    max_spread = _dec((((selection.get("liquidity_policy") or {}) if isinstance(selection.get("liquidity_policy"), dict) else {}).get("max_bid_ask_spread"))) or Decimal("0.10")
    liquidity_rows: list[dict[str, Any]] = []
    liquidity_ok = bool(selected_legs)
    for leg in selected_legs:
        row = _leg_contract(leg, snapshot) or {}
        bid = _quote(row, "bid")
        ask = _quote(row, "ask")
        spread = None if bid is None or ask is None else ask - bid
        ok = spread is not None and spread >= 0 and spread <= max_spread
        liquidity_ok = liquidity_ok and ok
        liquidity_rows.append({"leg": leg, "spread": None if spread is None else str(spread), "max_allowed": str(max_spread)})
    checks.append(_check(
        "11_LIQUIDITY_PRESENT",
        PASS if liquidity_ok else FAIL,
        "bid/ask spread within governed liquidity policy",
        liquidity_rows,
        gate_snapshot_path,
        "LIQUIDITY_INVALID",
    ))

    terms = candidate.order_plan.get("order_terms") if isinstance(candidate.order_plan.get("order_terms"), dict) else {}
    limit_credit = _dec(terms.get("limit_price"))
    sell_leg = next((leg for leg in selected_legs if str(leg.get("action") or "").upper() == "SELL"), {})
    buy_leg = next((leg for leg in selected_legs if str(leg.get("action") or "").upper() == "BUY"), {})
    sell_row = _leg_contract(sell_leg, snapshot) or {}
    buy_row = _leg_contract(buy_leg, snapshot) or {}
    conservative_credit = None
    optimistic_credit = None
    if sell_row and buy_row:
        sell_bid, sell_ask = _quote(sell_row, "bid"), _quote(sell_row, "ask")
        buy_bid, buy_ask = _quote(buy_row, "bid"), _quote(buy_row, "ask")
        if sell_bid is not None and buy_ask is not None:
            conservative_credit = sell_bid - buy_ask
        if sell_ask is not None and buy_bid is not None:
            optimistic_credit = sell_ask - buy_bid
    credit_in_range = bool(
        limit_credit is not None
        and conservative_credit is not None
        and optimistic_credit is not None
        and limit_credit > 0
        and conservative_credit <= limit_credit <= optimistic_credit
    )
    checks.append(_check(
        "12_LIMIT_CREDIT_IN_RANGE",
        PASS if credit_in_range else FAIL,
        "limit credit is positive and inside bid/ask-derived credit range",
        {"limit_credit": None if limit_credit is None else str(limit_credit), "conservative_credit": None if conservative_credit is None else str(conservative_credit), "optimistic_credit": None if optimistic_credit is None else str(optimistic_credit)},
        _path_text(candidate.order_plan_path),
        "LIMIT_CREDIT_INVALID",
    ))

    checks.append(_check(
        "13_LIMIT_CREDIT_NOT_STALE",
        PASS if order_match else FAIL,
        "limit credit derived from same accepted snapshot as order_plan",
        {"order_plan_path": _path_text(candidate.order_plan_path), "gate_snapshot": gate_snapshot_path},
        _path_text(candidate.order_plan_path),
        "STALE_PRICING_LINEAGE",
    ))

    realistic = bool(width is not None and limit_credit is not None and limit_credit > 0 and limit_credit < width)
    checks.append(_check(
        "14_IB_PRICE_REALISTIC",
        PASS if realistic else FAIL,
        "credit is positive and less than spread width",
        {"width_points": None if width is None else str(width), "limit_credit": None if limit_credit is None else str(limit_credit)},
        _path_text(candidate.order_plan_path),
        "RISKLESS_PRICING_PROFILE",
    ))

    raw_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    payload_sha256 = str(payload.get("submit_payload_sha256") or payload.get("payload_sha256") or "").strip()
    order = raw_payload.get("order") if isinstance(raw_payload.get("order"), dict) else {}
    bag = raw_payload.get("bag") if isinstance(raw_payload.get("bag"), dict) else {}
    bag_legs = bag.get("legs") if isinstance(bag.get("legs"), list) else []
    routing = raw_payload.get("routing") if isinstance(raw_payload.get("routing"), dict) else {}
    params = routing.get("smart_combo_routing_params") if isinstance(routing.get("smart_combo_routing_params"), list) else []

    checks.append(_check(
        "15_BAG_PARENT_ACTION",
        PASS if order.get("action") == "BUY" else (FAIL if payload else NOT_CHECKED),
        "parent BAG order action == BUY",
        {"order_action": order.get("action")},
        _path_text(payload_path),
        "BAG_PARENT_ACTION_INVALID" if payload else "IB_PAYLOAD_MISSING",
    ))

    payload_actions = sorted(str(row.get("action") or "") for row in bag_legs if isinstance(row, dict))
    selected_actions = sorted(str(row.get("action") or "") for row in selected_legs)
    checks.append(_check(
        "16_BAG_LEG_ACTIONS",
        PASS if payload_actions == selected_actions == ["BUY", "SELL"] else (FAIL if payload else NOT_CHECKED),
        "combo legs encode SELL/BUY as selected structure requires",
        {"payload_actions": payload_actions, "selected_actions": selected_actions},
        _path_text(payload_path),
        "BAG_LEG_ACTION_INVALID" if payload else "IB_PAYLOAD_MISSING",
    ))

    ratios = [row.get("ratio") for row in bag_legs if isinstance(row, dict)]
    checks.append(_check(
        "17_BAG_LEG_RATIOS",
        PASS if ratios and all(ratio == 1 for ratio in ratios) else (FAIL if payload else NOT_CHECKED),
        "every leg ratio == 1 unless governed policy says otherwise",
        {"ratios": ratios},
        _path_text(payload_path),
        "BAG_RATIO_INVALID" if payload else "IB_PAYLOAD_MISSING",
    ))

    has_nonguaranteed = any(isinstance(row, dict) and row.get("tag") == "NonGuaranteed" and str(row.get("value")) == "1" for row in params)
    checks.append(_check(
        "18_SMART_ROUTING_NONGUARANTEED",
        PASS if has_nonguaranteed else (FAIL if payload else NOT_CHECKED),
        "SmartComboRoutingParams include NonGuaranteed=1",
        {"smart_combo_routing_params": params},
        _path_text(payload_path),
        "SMART_ROUTING_PARAM_MISSING" if payload else "IB_PAYLOAD_MISSING",
    ))

    if raw_payload and not payload_sha256:
        try:
            from constellation_2.phaseD.lib.submit_boundary_paper_v4 import _ib_payload_hash  # noqa: PLC0415

            payload_sha256 = _ib_payload_hash(raw_payload)
        except Exception:
            payload_sha256 = ""
    preview_status = str(preview.get("status") or "").upper()
    preview_payload_sha256 = str(preview.get("preview_payload_sha256") or "").strip()
    preview_submit_payload_sha256 = str(preview.get("submit_payload_sha256") or "").strip()
    preview_hashes_match = (
        preview.get("preview_payload_hash_matches_submit_payload_hash") is True
        and bool(payload_sha256)
        and preview_payload_sha256 == payload_sha256
        and preview_submit_payload_sha256 == payload_sha256
    )
    preview_ok = preview_status == "PASS" and preview.get("whatif_ok") is True and preview_hashes_match
    checks.append(_check(
        "19_IB_PREVIEW_PASS",
        PASS if preview_ok else (FAIL if preview else NOT_CHECKED),
        "ib_combo_preview status == PASS, whatif_ok == true, and preview_payload_hash == submit_payload_hash",
        {
            "status": preview.get("status"),
            "whatif_ok": preview.get("whatif_ok"),
            "preview_payload_sha256": preview_payload_sha256,
            "submit_payload_sha256": preview_submit_payload_sha256,
            "ib_order_payload_sha256": payload_sha256,
            "preview_payload_hash_matches_submit_payload_hash": preview.get("preview_payload_hash_matches_submit_payload_hash"),
        },
        _path_text(preview_path),
        "IB_PREVIEW_NOT_PASS" if preview else "IB_PREVIEW_MISSING",
    ))

    preview_text = json.dumps(preview, sort_keys=True).upper() if preview else ""
    has_201 = "ERROR 201" in preview_text or "IB_ERROR_201" in preview_text or "RISKLESS COMBINATION" in preview_text
    checks.append(_check(
        "20_NO_IB_ERROR_201",
        PASS if preview and not has_201 else (FAIL if preview else NOT_CHECKED),
        "preview has no Error 201 / riskless combination code",
        {"contains_error_201_or_riskless": has_201, "canonical_blocker": preview.get("canonical_blocker")},
        _path_text(preview_path),
        "IB_ERROR_201_RISKLESS_COMBINATION" if preview else "IB_PREVIEW_MISSING",
    ))

    riskless = "RISKLESS" in preview_text or "COMBOPAYOUT" in preview_text
    checks.append(_check(
        "21_NO_RISKLESS_CLASSIFICATION",
        PASS if preview and not riskless else (FAIL if preview else NOT_CHECKED),
        "preview/order economics not classified riskless",
        {"contains_riskless": riskless, "detail": preview.get("detail")},
        _path_text(preview_path),
        "RISKLESS_COMBO_REJECTED" if preview else "IB_PREVIEW_MISSING",
    ))

    checks.append(_check(
        "22_SECOND_ATTEMPT_CLEARANCE",
        PASS if str(clearance.get("status") or "").upper() == "CLEARED" and str(clearance.get("environment") or "").upper() == "PAPER" else FAIL,
        "valid PAPER second-attempt clearance exists for prior rejected zero-fill submit",
        {"prior_broker_submission_record": _path_text(prior_record_path), "clearance_status": clearance.get("status"), "clearance_path": clearance.get("_path") or clearance.get("path")},
        str(clearance.get("_path") or clearance.get("path") or ""),
        "SECOND_ATTEMPT_CLEARANCE_MISSING",
    ))

    current_plan_hash = plan_hash_v1(candidate.order_plan) if candidate.order_plan else ""
    prior_plan_hash = plan_hash_v1(prior_plan) if prior_plan else ""
    checks.append(_check(
        "23_NEW_PLAN_HASH",
        PASS if current_plan_hash and prior_plan_hash and current_plan_hash != prior_plan_hash else FAIL,
        "current plan_hash differs from rejected prior submission",
        {"current_plan_hash": current_plan_hash, "prior_plan_hash": prior_plan_hash},
        _path_text(candidate.order_plan_path),
        "IDENTICAL_PLAN_HASH",
    ))

    current_signature = structure_pricing_signature_v1(candidate.order_plan) if candidate.order_plan else ""
    prior_signature = structure_pricing_signature_v1(prior_plan) if prior_plan else ""
    checks.append(_check(
        "24_NEW_STRUCTURE_PRICING_SIGNATURE",
        PASS if current_signature and prior_signature and current_signature != prior_signature else FAIL,
        "current structure/pricing signature differs from rejected prior submission",
        {"current_structure_pricing_signature": current_signature, "prior_structure_pricing_signature": prior_signature},
        _path_text(candidate.order_plan_path),
        "IDENTICAL_STRUCTURE_PRICING",
    ))

    payload_account = str(payload.get("ib_account") or "").strip()
    preview_account = str(preview.get("ib_account") or "").strip()
    paper_scope_ok = environment == "PAPER" and ib_account == "DUO847203" and (not payload or payload_account == ib_account) and (not preview or preview_account == ib_account)
    checks.append(_check(
        "25_PAPER_ONLY_ACCOUNT",
        PASS if paper_scope_ok else FAIL,
        "environment == PAPER and account == DUO847203",
        {"environment": environment, "ib_account": ib_account, "payload_account": payload_account, "preview_account": preview_account},
        _path_text(payload_path) or str(boundary_path),
        "PAPER_ACCOUNT_SCOPE_INVALID",
    ))

    boundary_status = str(boundary.get("boundary_status") or boundary.get("status") or "").upper()
    boundary_ok = boundary_status == "AUTHORIZED" and boundary.get("submission_authorized") is True
    checks.append(_check(
        "26_SUBMIT_BOUNDARY_AUTHORIZED",
        PASS if boundary_ok else FAIL,
        "submit_boundary_status == AUTHORIZED and submission_authorized == true",
        {"boundary_status": boundary_status, "submission_authorized": boundary.get("submission_authorized")},
        str(boundary_path),
        "SUBMIT_BOUNDARY_NOT_AUTHORIZED",
    ))

    first_failed = next((row for row in checks if row["status"] != PASS), None)
    overall_status = PASS if first_failed is None else "BLOCKED"
    grouped_summary = _grouped_check_summary(checks)
    first_failed_check = "" if first_failed is None else str(first_failed.get("check_id") or "")
    first_failed_group = "" if first_failed is None else _check_group(first_failed_check)
    plain_next_action = _plain_english_next_action(first_failed)
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "environment": environment,
        "ib_account": ib_account,
        "status": overall_status,
        "overall_status": overall_status,
        "canonical_blocker": "" if first_failed is None else str(first_failed["blocker"]),
        "canonical_check_id": "" if first_failed is None else str(first_failed["check_id"]),
        "created_at_utc": now_utc,
        "truth_root": str(truth_root),
        "execution_root": str(execution_root),
        "candidate": {
            "phasec_dir": _path_text(candidate.phasec_dir),
            "submission_id": candidate.submission_id,
            "order_plan_path": _path_text(candidate.order_plan_path),
            "submission_dir": _path_text(submission_dir),
        },
        "trade_summary": _trade_summary(
            order_plan=candidate.order_plan,
            structure=structure,
            selected_legs=selected_legs,
            environment=environment,
            ib_account=ib_account,
            preview_status=preview_status,
        ),
        "grouped_check_summary": grouped_summary,
        "first_failed_group": first_failed_group,
        "first_failed_check": first_failed_check,
        "plain_english_next_action": plain_next_action,
        "checks": checks,
        "operator_next_action": plain_next_action,
    }


def run_first_ib_accepted_paper_order_checklist_v1(
    *,
    day_utc: str,
    environment: str,
    ib_account: str,
    truth_root: Path = DEFAULT_TRUTH_ROOT,
    execution_root: Path = DEFAULT_EXECUTION_ROOT,
    now_utc: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    payload = build_first_ib_accepted_paper_order_checklist_v1(
        truth_root=truth_root,
        execution_root=execution_root,
        day_utc=day_utc,
        environment=environment,
        ib_account=ib_account,
        now_utc=now_utc,
    )
    path = _report_path(Path(truth_root).resolve(), payload["day_utc"])
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_first_ib_accepted_paper_order_checklist_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", required=True, choices=["PAPER"])
    parser.add_argument("--ib_account", required=True, choices=["DUO847203"])
    parser.add_argument("--truth_root", default=os.environ.get("C2_TRUTH_ROOT", str(DEFAULT_TRUTH_ROOT)))
    parser.add_argument("--execution_root", default=os.environ.get("C2_EXECUTION_ROOT", str(DEFAULT_EXECUTION_ROOT)))
    args = parser.parse_args(argv)
    path, payload = run_first_ib_accepted_paper_order_checklist_v1(
        day_utc=args.day_utc,
        environment=args.environment,
        ib_account=args.ib_account,
        truth_root=Path(args.truth_root),
        execution_root=Path(args.execution_root),
    )
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": str(path)}, sort_keys=True))
    return 0 if payload["status"] == PASS else 2


if __name__ == "__main__":
    raise SystemExit(main())
