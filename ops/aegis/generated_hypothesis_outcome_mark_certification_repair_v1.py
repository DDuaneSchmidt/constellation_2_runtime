from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.evidence_lineage_integrity_v1 import build_evidence_lineage_integrity_v1, write_evidence_lineage_integrity_v1
from ops.aegis.generated_hypothesis_outcome_eligibility_day_proof_v1 import build_generated_hypothesis_outcome_eligibility_day_proof_v1, write_generated_hypothesis_outcome_eligibility_day_proof_v1
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.paper_outcome_auto_closure_v1 import build_paper_outcome_auto_closure_v1, write_paper_outcome_auto_closure_v1
from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1, write_paper_position_ledger_v1
from ops.aegis.future_target_day_audit_guard_v1 import read_future_target_day_audit_guard_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_generated_hypothesis_outcome_mark_certification_repair_v1"
FILENAME = "generated_hypothesis_outcome_mark_certification_repair.v1.json"
POLICY_VERSION = "AEGIS_GENERATED_HYPOTHESIS_OUTCOME_MARK_CERTIFICATION_REPAIR_V1"
OIL_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_CANDIDATE_ID = "candidate_contract_a3dd44d21952f8098131aa04"
OIL_SLEEVE_ID = "C2_OIL_SHOCK_REVERSAL_V1"

SAFETY = {
    "paper_only": True,
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_outcome_fabrication": True,
    "no_mark_fabrication": True,
    "no_validation_sample_fabrication": True,
    "no_research_quality_fabrication": True,
    "safety_gates_changed": False,
    "outcome_created_by_this_artifact": False,
    "validation_sample_created_by_this_artifact": False,
    "research_quality_result_created_by_this_artifact": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def generated_hypothesis_outcome_mark_certification_repair_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_generated_hypothesis_outcome_mark_certification_repair_v1(
    *, truth_root: Path | str, day_utc: str, repo_root: Path | str | None = None, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root or Path.cwd()).resolve()
    day = str(day_utc)
    before = _before_state(root, day)
    guard = read_future_target_day_audit_guard_v1(truth_root=root, day_utc=day)
    future_guarded = guard.get("guard_status") == "TARGET_DAY_IN_FUTURE_GUARDED" and guard.get("is_future_target_day") is True
    before_position = _find_oil_position(before.get("ledger"))
    required_symbol = _text(before_position.get("symbol") or "USO")
    market_path, market_payload, market_row = _find_market_row(root, day, required_symbol)
    certifiable = _is_certifiable_price(market_row, day) and not future_guarded

    diagnostics_path = _path(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json")
    diagnostics_found_before = diagnostics_path.exists()
    diagnostics_status = _ensure_candidate_generation_diagnostics(root=root, repo=repo, day=day) if not diagnostics_found_before else "ALREADY_PRESENT"
    diagnostics_found_after = diagnostics_path.exists()

    repair_action = "NO_MARK_REPAIR_APPLIED"
    if certifiable:
        ledger_payload = _build_certified_ledger(root=root, day=day)
        write_paper_position_ledger_v1(truth_root=root, day_utc=day, payload=ledger_payload)
        closure_payload = build_paper_outcome_auto_closure_v1(truth_root=root, day_utc=day)
        write_paper_outcome_auto_closure_v1(truth_root=root, day_utc=day, payload=closure_payload)
        write_evidence_lineage_integrity_v1(truth_root=root, day_utc=day)
        write_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=root, day_utc=day)
        repair_action = "CERTIFIED_MARK_ROUTED_TO_LEDGER_AND_OUTCOME_CLOSURE"
    else:
        # Rebuild read-only lineage measurement after diagnostics repair. Do not certify stale or missing marks.
        write_evidence_lineage_integrity_v1(truth_root=root, day_utc=day)
        write_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=root, day_utc=day)

    after = _after_state(root, day)
    after_position = _find_oil_position(after.get("ledger"))
    p20 = _dict(_dict(after.get("package_020")).get("summary"))
    certified_before = _has_certified_mark(before_position)
    certified_after = _has_certified_mark(after_position)
    mark_status = "CERTIFIED" if certified_after else ("SOURCE_STALE" if market_row and not certifiable else "MISSING_MARK")
    routing_status = "ROUTED_TO_OUTCOME_CLOSURE" if _closure_has_certified_mark(after.get("closure")) else ("MARK_ROUTING_MISSING" if certified_after else "NOT_ROUTED")
    blocker_code, blocker_reason, missing_fields, required_inputs, owner = _blocker(
        market_row=market_row,
        certifiable=certifiable,
        certified_after=certified_after,
        routing_status=routing_status,
        evidence_after=after.get("lineage"),
        diagnostics_found_after=diagnostics_found_after,
        p20=p20,
        future_guarded=future_guarded,
    )
    remaining = "NONE" if blocker_code == "NONE" else blocker_code
    position = after_position or before_position
    payload: dict[str, Any] = {
        "schema_id": "aegis_generated_hypothesis_outcome_mark_certification_repair",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": computed_at_utc or _now(),
        "generated_hypothesis_id": _text(_lineage(position).get("hypothesis_id") or position.get("hypothesis_id") or OIL_HYPOTHESIS_ID),
        "sleeve_id": _text(_lineage(position).get("sleeve_id") or position.get("sleeve_id") or OIL_SLEEVE_ID),
        "candidate_contract_id": _text(position.get("candidate_id") or OIL_CANDIDATE_ID),
        "paper_observation_id": _text(position.get("position_id")),
        "paper_position_id": _text(position.get("position_id")),
        "required_mark_symbol": required_symbol,
        "required_mark_date": day,
        "required_mark_type": "CURRENT_CERTIFIED_MARK",
        "authoritative_price_source": _text(market_row.get("provider") or market_row.get("source") or ""),
        "authoritative_price_found": bool(market_row),
        "authoritative_price_path": str(market_path or _text(market_row.get("source_url_or_path"))),
        "authoritative_price_status": "TARGET_DAY_IN_FUTURE" if future_guarded else _price_status(market_row, day),
        "authoritative_price_value": market_row.get("last_price") if market_row.get("last_price") not in {None, ""} else market_row.get("close"),
        "authoritative_price_timestamp": _text(market_row.get("data_timestamp_utc") or market_row.get("source_timestamp_utc") or market_row.get("retrieved_at_utc")),
        "authoritative_price_market_session_date": _text(market_row.get("market_session_date")),
        "certified_mark_found_before_repair": certified_before,
        "certified_mark_found_after_repair": certified_after,
        "certified_mark_path": _text(after_position.get("mark_source_path")),
        "mark_certification_status": "TARGET_DAY_IN_FUTURE" if future_guarded else mark_status,
        "mark_routing_status": "NOT_ROUTED_FUTURE_TARGET_DAY" if future_guarded else routing_status,
        "future_target_day_guarded": future_guarded,
        "future_target_day_guard_status": _text(guard.get("guard_status")),
        "repair_action": repair_action,
        "candidate_generation_diagnostics_repair_status": diagnostics_status,
        "evidence_lineage_status_before_repair": _panel(before.get("lineage")).get("current_integrity_status", "UNKNOWN"),
        "evidence_lineage_status_after_repair": _panel(after.get("lineage")).get("current_integrity_status", "UNKNOWN"),
        "mark_coverage_pct_before_repair": _panel(before.get("lineage")).get("mark_coverage_pct", 0.0),
        "mark_coverage_pct_after_repair": _panel(after.get("lineage")).get("mark_coverage_pct", 0.0),
        "broken_chain_count_before_repair": _panel(before.get("lineage")).get("broken_chain_count", 0),
        "broken_chain_count_after_repair": _panel(after.get("lineage")).get("broken_chain_count", 0),
        "candidate_generation_diagnostics_found_before_repair": diagnostics_found_before,
        "candidate_generation_diagnostics_found_after_repair": diagnostics_found_after,
        "package_020_status_after_repair": _text(p20.get("blocker_code") or p20.get("remaining_blocker") or p20.get("outcome_status")),
        "outcome_readiness_status_after_repair": _text(p20.get("outcome_readiness_status")),
        "outcome_status_after_repair": _text(p20.get("outcome_status")),
        "outcome_row_created_after_repair": p20.get("outcome_row_created") is True,
        "remaining_blocker": remaining,
        "blocker_code": blocker_code,
        "blocker_reason": blocker_reason,
        "missing_fields": missing_fields,
        "required_inputs": required_inputs,
        "owner": owner,
        "david_action_required": owner == "DAVID",
        "source_artifact_paths": {key: str(path) for key, path in _source_paths(root, day).items()},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in _source_paths(root, day).items()},
        "safety_statement": "Package 021 only certifies and routes marks when authoritative target-day price lineage is present. It does not fabricate marks, outcomes, validation samples, research quality results, trades, broker actions, allocations, close rules, holding periods, or safety gates.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["summary"] = {key: payload[key] for key in (
        "generated_hypothesis_id", "sleeve_id", "candidate_contract_id", "paper_observation_id", "paper_position_id",
        "target_day", "required_mark_symbol", "required_mark_date", "required_mark_type", "authoritative_price_source",
        "authoritative_price_found", "authoritative_price_path", "certified_mark_found_before_repair", "certified_mark_found_after_repair",
        "certified_mark_path", "mark_certification_status", "mark_routing_status", "evidence_lineage_status_before_repair",
        "evidence_lineage_status_after_repair", "mark_coverage_pct_before_repair", "mark_coverage_pct_after_repair",
        "broken_chain_count_before_repair", "broken_chain_count_after_repair", "candidate_generation_diagnostics_found_before_repair",
        "candidate_generation_diagnostics_found_after_repair", "package_020_status_after_repair", "outcome_readiness_status_after_repair",
        "outcome_status_after_repair", "outcome_row_created_after_repair", "remaining_blocker", "blocker_code", "blocker_reason",
        "missing_fields", "required_inputs", "owner", "david_action_required"
    )}
    payload["content_hash"] = stable_hash_v1(_without_time(payload))
    return payload


def write_generated_hypothesis_outcome_mark_certification_repair_v1(
    *, truth_root: Path | str, day_utc: str, repo_root: Path | str | None = None, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_generated_hypothesis_outcome_mark_certification_repair_v1(truth_root=truth_root, day_utc=day_utc, repo_root=repo_root)
    return write_json_v1(generated_hypothesis_outcome_mark_certification_repair_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _before_state(root: Path, day: str) -> dict[str, Any]:
    return {
        "ledger": read_json_v1(_path(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json")),
        "closure": read_json_v1(_path(root, "aegis_paper_outcome_auto_closure_v1", day, "paper_outcome_auto_closure.v1.json")),
        "lineage": read_json_v1(_path(root, "aegis_evidence_lineage_integrity_v1", day, "evidence_lineage_integrity.v1.json")),
        "package_020": read_json_v1(_path(root, "aegis_generated_hypothesis_outcome_eligibility_day_proof_v1", day, "generated_hypothesis_outcome_eligibility_day_proof.v1.json")),
    }


def _after_state(root: Path, day: str) -> dict[str, Any]:
    state = _before_state(root, day)
    if not state["lineage"]:
        state["lineage"] = build_evidence_lineage_integrity_v1(truth_root=root, day_utc=day)
    if not state["package_020"]:
        state["package_020"] = build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=root, day_utc=day)
    return state



def _build_certified_ledger(*, root: Path, day: str) -> dict[str, Any]:
    built = build_paper_position_ledger_v1(truth_root=root, day_utc=day)
    existing = read_json_v1(_path(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"))
    if not built.get("open_positions") and existing.get("open_positions"):
        built = dict(existing)
    market_payload = read_json_v1(_path(root, "aegis_market_data_v1", day, "market_data.v1.json"))
    rows = _market_rows(market_payload)
    positions = []
    for row in _list(built.get("positions")):
        positions.append(_certify_position_from_market(dict(row), rows, day)) if isinstance(row, Mapping) else None
    open_positions = []
    for row in _list(built.get("open_positions")):
        open_positions.append(_certify_position_from_market(dict(row), rows, day)) if isinstance(row, Mapping) else None
    built["positions"] = positions
    built["open_positions"] = open_positions
    built["closed_positions"] = [row for row in _list(built.get("closed_positions")) if isinstance(row, Mapping)]
    built["open_position_count"] = len(open_positions)
    built["closed_position_count"] = len(built["closed_positions"])
    return built


def _certify_position_from_market(row: dict[str, Any], rows: Mapping[str, dict[str, Any]], day: str) -> dict[str, Any]:
    if _text(row.get("current_status")).upper() != "OPEN":
        return row
    symbol = _text(row.get("symbol")).upper()
    market_row = rows.get(symbol, {})
    if not _is_certifiable_price(market_row, day):
        return row
    price = market_row.get("last_price") if market_row.get("last_price") not in {None, ""} else market_row.get("close")
    row["mark_price"] = str(price)
    row["current_certified_mark"] = str(price)
    row["mark_timestamp_utc"] = _text(market_row.get("data_timestamp_utc") or market_row.get("source_timestamp_utc") or market_row.get("retrieved_at_utc"))
    row["mark_source_path"] = _text(market_row.get("source_url_or_path"))
    row["mark_source_hash"] = _text(market_row.get("source_hash"))
    row["mark_freshness_status"] = "CURRENT"
    row["mark_market_session_date"] = day
    row["mark_certification_status"] = "CERTIFIED"
    return row

def _find_oil_position(payload: Any) -> dict[str, Any]:
    payload = _dict(payload)
    for key in ("open_positions", "positions", "historical_positions", "closed_positions"):
        for row in _list(payload.get(key)):
            if not isinstance(row, Mapping):
                continue
            lineage = _lineage(row)
            if _text(row.get("candidate_id")) == OIL_CANDIDATE_ID or _text(lineage.get("hypothesis_id") or row.get("hypothesis_id")) == OIL_HYPOTHESIS_ID:
                return dict(row)
    return {}


def _find_market_row(root: Path, day: str, symbol: str) -> tuple[Path | None, dict[str, Any], dict[str, Any]]:
    path = _path(root, "aegis_market_data_v1", day, "market_data.v1.json")
    payload = read_json_v1(path)
    rows = _market_rows(payload)
    symbol = symbol.upper()
    if symbol in rows:
        return path, payload, rows[symbol]
    return path if path.exists() else None, payload, {}


def _market_rows(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    symbols = payload.get("symbols")
    if isinstance(symbols, Mapping):
        return {str(k).upper(): dict(v) for k, v in symbols.items() if isinstance(v, Mapping)}
    if isinstance(symbols, list):
        return {str(row.get("symbol") or row.get("canonical_symbol") or "").upper(): dict(row) for row in symbols if isinstance(row, Mapping) and _text(row.get("symbol") or row.get("canonical_symbol"))}
    return {}


def _is_certifiable_price(row: Mapping[str, Any], day: str) -> bool:
    if not row:
        return False
    price = row.get("last_price") if row.get("last_price") not in {None, ""} else row.get("close")
    if price in {None, ""}:
        return False
    if _text(row.get("freshness_status")).upper() != "CURRENT":
        return False
    session = _text(row.get("market_session_date"))
    if session and session != day:
        return False
    if _text(row.get("data_finality")).upper() in {"PROVISIONAL_INTRADAY", "STALE_PRIOR_DAY"}:
        return False
    if _text(row.get("market_data_mode")).upper() == "STALE_PRIOR_DAY":
        return False
    usable_for = row.get("usable_for") if isinstance(row.get("usable_for"), Mapping) else {}
    if "final_eod_certification" in usable_for and usable_for.get("final_eod_certification") is not True:
        return False
    if _text(row.get("finalization_status")).upper() in {"FINAL_UNAVAILABLE", "NOT_FINAL", "PROVISIONAL"}:
        return False
    return True


def _price_status(row: Mapping[str, Any], day: str) -> str:
    if not row:
        return "MISSING"
    if _is_certifiable_price(row, day):
        return "CERTIFIABLE"
    usable_for = row.get("usable_for") if isinstance(row.get("usable_for"), Mapping) else {}
    if "final_eod_certification" in usable_for and usable_for.get("final_eod_certification") is not True:
        return "NOT_FINAL_EOD_CERTIFIABLE"
    if _text(row.get("finalization_status")).upper() in {"FINAL_UNAVAILABLE", "NOT_FINAL", "PROVISIONAL"}:
        return "FINAL_EOD_UNAVAILABLE"
    if _text(row.get("market_data_mode")).upper() == "STALE_PRIOR_DAY":
        return "STALE_PRIOR_DAY"
    if _text(row.get("market_session_date")) and _text(row.get("market_session_date")) != day:
        return "STALE_SESSION"
    if _text(row.get("freshness_status")).upper() != "CURRENT":
        return "STALE_FRESHNESS"
    return "NOT_CERTIFIABLE"


def _ensure_candidate_generation_diagnostics(*, root: Path, repo: Path, day: str) -> str:
    path = _path(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json")
    if path.exists():
        return "ALREADY_PRESENT"
    try:
        from ops.aegis.candidate_generation_diagnostics_v1 import build_candidate_generation_diagnostics_v1, write_candidate_generation_diagnostics_v1
        payload = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=repo, day_utc=day)
        write_candidate_generation_diagnostics_v1(truth_root=root, day_utc=day, payload=payload)
        return "GENERATED_BY_AUTHORITATIVE_BUILDER"
    except Exception as exc:  # pragma: no cover - exercised by runtime variability, unit tests use happy path or existing artifact.
        fallback = {
            "schema_id": "aegis_candidate_generation_diagnostics",
            "schema_version": "v1",
            "artifact_id": "aegis_candidate_generation_diagnostics_v1",
            "day_utc": day,
            "generated_at": _now(),
            "candidate_generation_status": "DIAGNOSTICS_REPAIR_BLOCKED",
            "blocker_code": "CANDIDATE_DIAGNOSTICS_BUILDER_MISSING",
            "blocker_reason": str(exc),
            "broker_execution_allowed": False,
            "trade_advice_allowed": False,
            "autonomous_execution_allowed": False,
        }
        write_json_v1(path, fallback)
        return "FALLBACK_BLOCKER_ARTIFACT_WRITTEN"


def _has_certified_mark(row: Mapping[str, Any]) -> bool:
    return bool(_text(row.get("current_certified_mark")) and _text(row.get("mark_timestamp_utc")) and _text(row.get("mark_source_path")) and _text(row.get("mark_certification_status")).upper() == "CERTIFIED")


def _closure_has_certified_mark(payload: Any) -> bool:
    payload = _dict(payload)
    for row in _list(payload.get("rows")):
        if not isinstance(row, Mapping):
            continue
        if _text(row.get("candidate_id")) == OIL_CANDIDATE_ID:
            return bool(row.get("exit_mark") not in {None, ""} and _text(row.get("exit_mark_certification_status")).upper() == "CERTIFIED" and _text(row.get("exit_price_source_artifact")))
    return False


def _blocker(*, market_row: Mapping[str, Any], certifiable: bool, certified_after: bool, routing_status: str, evidence_after: Any, diagnostics_found_after: bool, p20: Mapping[str, Any], future_guarded: bool = False) -> tuple[str, str, list[str], list[str], str]:
    panel = _panel(evidence_after)
    if future_guarded:
        return "TARGET_DAY_IN_FUTURE", "Target day is later than actual runtime date; final target-day mark certification, outcome closure, and validation samples are blocked by the future target-day guard.", ["target_day_final_mark"], ["actual runtime date reaches target day", "fresh CURRENT target-day authoritative mark"], "AEGIS_SYSTEM"
    if not market_row:
        return "AUTHORITATIVE_MARK_SOURCE_MISSING", "No authoritative price row exists for the required generated-hypothesis outcome mark symbol on the target day.", ["authoritative_price"], ["target-day market data row for required mark symbol"], "AEGIS_SYSTEM"
    if not certifiable:
        return "AUTHORITATIVE_MARK_SOURCE_STALE", "A price row exists for the required symbol, but it is not certifiable for the target outcome day because freshness/session/finality evidence is stale or provisional.", ["target_day_certified_current_mark"], ["fresh CURRENT target-day authoritative mark with source path, source hash, and timestamp"], "AEGIS_SYSTEM"
    if not certified_after:
        return "MARK_CERTIFICATION_MISSING", "Target-day authoritative price exists, but the paper position ledger did not certify it as the current mark.", ["current_certified_mark"], ["paper position ledger certified mark fields"], "AEGIS_SYSTEM"
    if routing_status != "ROUTED_TO_OUTCOME_CLOSURE":
        return "MARK_ROUTING_MISSING", "Certified mark exists in the paper ledger but is not routed into deterministic outcome closure.", ["exit_mark"], ["paper outcome auto-closure exit mark routing"], "AEGIS_SYSTEM"
    if float(panel.get("mark_coverage_pct") or 0.0) < 100.0 or int(panel.get("broken_chain_count") or 0) != 0:
        return "EVIDENCE_LINEAGE_BROKEN", "Certified mark routing succeeded for Oil Shock, but portfolio evidence lineage still has broken mark chains.", [], ["all open paper positions have certified marks"], "AEGIS_SYSTEM"
    if not diagnostics_found_after:
        return "CANDIDATE_DIAGNOSTICS_SEQUENCING_MISSING", "Candidate generation diagnostics artifact is still missing after repair sequencing.", ["candidate_generation_diagnostics.v1.json"], ["candidate diagnostics builder in audit sequence"], "AEGIS_SYSTEM"
    if _text(p20.get("outcome_status")) == "OUTCOME_CREATED" or _text(p20.get("blocker_code")) in {"CLOSE_CONDITION_NOT_MET", "NONE"}:
        return "NONE", "Certified current mark is present and routed; Package 020 advanced to outcome or the next deterministic close blocker.", [], [], "NONE"
    return "UNKNOWN_DETERMINISTIC_BLOCKER", "Mark repair completed but Package 020 did not advance to outcome or a known next close blocker.", [], ["Package 020 deterministic status"], "AEGIS_SYSTEM"


def _source_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "paper_position_ledger": _path(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"),
        "paper_outcome_auto_closure": _path(root, "aegis_paper_outcome_auto_closure_v1", day, "paper_outcome_auto_closure.v1.json"),
        "market_data": _path(root, "aegis_market_data_v1", day, "market_data.v1.json"),
        "evidence_lineage_integrity": _path(root, "aegis_evidence_lineage_integrity_v1", day, "evidence_lineage_integrity.v1.json"),
        "candidate_generation_diagnostics": _path(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json"),
        "package_020": _path(root, "aegis_generated_hypothesis_outcome_eligibility_day_proof_v1", day, "generated_hypothesis_outcome_eligibility_day_proof.v1.json"),
    }


def _path(root: Path, family: str, day: str, filename: str) -> Path:
    return root / "reports" / family / day / filename


def _panel(payload: Any) -> dict[str, Any]:
    return _dict(_dict(payload).get("evidence_coverage_panel"))


def _lineage(row: Mapping[str, Any]) -> Mapping[str, Any]:
    return row.get("candidate_lineage") if isinstance(row.get("candidate_lineage"), Mapping) else {}


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _without_time(payload: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    out.pop("computed_at_utc", None)
    out.pop("content_hash", None)
    return out
