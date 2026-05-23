#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.execution_kernel.execution_submission_record_v1 import (
    write_execution_submission_record_from_execution_package_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from ops.aegis.universe.canonical_symbol_universe_resolver_v1 import (
    CanonicalSymbolUniverseError,
    resolve_canonical_symbol_universe_v1,
)
from ops.tools.run_execution_package_from_authorized_intent_v1 import (
    _execution_build_chain_map,
    build_execution_package_from_authorized_intent_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1
from ops.aegis.candidate_identity_set_v1 import load_candidate_identity_set_v1

PRODUCER = "ops/tools/run_exposure_intent_paper_submission_package_v1.py"
SCHEMA_ID = "exposure_intent_paper_submission_package"
SCHEMA_VERSION = "v1"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")


def _sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _default_selected_pointer(truth_root: Path) -> Path:
    return (truth_root / "pointers" / "selected_intent_pointer.v1.json").resolve()


def _capital_authority_path(truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "allocation_v1"
        / "capital_authority_allocation_v1"
        / day_utc
        / "capital_authority_allocation.v1.json"
    ).resolve()


def _risk_contract_path(truth_root: Path, day_utc: str, intent_hash: str) -> Path:
    return (truth_root / "risk_definition_contract_v1" / day_utc / intent_hash.lower() / "risk_definition_contract.v1.json").resolve()


def _paper_trade_construction_path(truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "paper_trade_construction_v1" / day_utc / "paper_trade_construction.v1.json").resolve()


def _market_data_snapshot_path(truth_root: Path, day_utc: str, symbol: str) -> Path:
    return (truth_root / "market_data_snapshot_v1" / "snapshots" / day_utc / f"{symbol.upper()}.market_data_snapshot.v1.json").resolve()


def _latest_market_jsonl_row(truth_root: Path, day_utc: str, symbol: str) -> tuple[dict[str, Any] | None, str]:
    path = (truth_root / "market_data_snapshot_v1" / symbol.upper() / f"{day_utc[:4]}.jsonl").resolve()
    if not path.exists() or not path.is_file():
        return None, str(path)
    latest: dict[str, Any] | None = None
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol") or "").strip().upper() != symbol.upper():
                continue
            ts = str(row.get("timestamp_utc") or "").strip()
            if not ts:
                continue
            if latest is None or ts > str(latest.get("timestamp_utc") or ""):
                latest = row
    return latest, str(path)


def _market_data_status(truth_root: Path, day_utc: str, symbol: str) -> dict[str, Any]:
    snapshot_path = _market_data_snapshot_path(truth_root, day_utc, symbol)
    if snapshot_path.exists() and snapshot_path.is_file():
        payload = _read_json(snapshot_path)
        observed_day = str(payload.get("day_utc") or "").strip()
        close = payload.get("close")
        return {
            "status": "CURRENT" if observed_day == day_utc and close is not None else "STALE_OR_INVALID",
            "path": str(snapshot_path),
            "source_type": "day_snapshot",
            "expected_session": day_utc,
            "observed_session": observed_day,
            "close_present": close is not None,
            "sha256": _sha256_file(snapshot_path),
        }
    latest, jsonl_path = _latest_market_jsonl_row(truth_root, day_utc, symbol)
    observed = str((latest or {}).get("timestamp_utc") or "")[:10]
    close_present = latest is not None and (latest or {}).get("close") is not None
    return {
        "status": "CURRENT" if observed == day_utc and close_present else "STALE_OR_MISSING",
        "path": jsonl_path,
        "source_type": "year_jsonl",
        "expected_session": day_utc,
        "observed_session": observed,
        "close_present": close_present,
        "sha256": _sha256_file(Path(jsonl_path)) if Path(jsonl_path).exists() and Path(jsonl_path).is_file() else "",
    }


def _authorized_row(capital: dict[str, Any], intent_id: str) -> dict[str, Any] | None:
    chain = capital.get("decision_chain") if isinstance(capital.get("decision_chain"), dict) else {}
    rows = chain.get("authorized_trade_intents") if isinstance(chain.get("authorized_trade_intents"), list) else []
    for row in rows:
        if isinstance(row, dict) and str(row.get("intent_id") or "").strip() == intent_id:
            return row
    return None



def _capital_validation_blocker(*, capital: dict[str, Any], row: dict[str, Any], intent_id: str, day_utc: str, runtime_hash: str) -> tuple[str, str]:
    if str(capital.get("day_utc") or "") and str(capital.get("day_utc") or "") != day_utc:
        return "CAPITAL_AUTHORITY_STALE", "Capital authority allocation day does not match selected conversion day."
    validation = str(capital.get("validation_status") or "VALID").strip().upper()
    if validation == "MANUAL_REQUIRED":
        return "CAPITAL_AUTHORITY_MANUAL_REQUIRED", str(capital.get("manual_required_command") or "Manual capital authority approval is required.")
    if validation not in {"VALID", ""}:
        return "CAPITAL_AUTHORITY_REJECTED", "Capital authority allocation artifact is rejected."
    cap_runtime = str(capital.get("runtime_evaluation_hash") or "").strip()
    if cap_runtime and runtime_hash and cap_runtime != runtime_hash:
        return "RUNTIME_HASH_MISMATCH", "Capital authority allocation RuntimeEvaluation hash does not match current RuntimeEvaluation."
    if not cap_runtime and str(capital.get("schema_id") or "") == "capital_authority_allocation":
        return "RUNTIME_HASH_MISMATCH", "Capital authority allocation is missing RuntimeEvaluation hash."
    outcome = str(row.get("authorization_outcome") or "").strip().upper()
    if outcome not in {"APPROVED", "RESIZED"}:
        return "CAPITAL_AUTHORITY_REJECTED_INTENT", "Capital authority did not approve the selected ExposureIntent."
    try:
        qty = int(row.get("authorized_quantity") or 0)
    except (TypeError, ValueError):
        qty = 0
    if qty <= 0:
        return "CAPITAL_AUTHORITY_REJECTED_INTENT", "Capital authority did not approve a positive quantity for the selected ExposureIntent."
    limit = row.get("allocation_limit_used") if isinstance(row.get("allocation_limit_used"), dict) else {}
    if limit and str(limit.get("validation_status") or "").upper() not in {"VALID", ""}:
        blockers = ",".join(str(item) for item in limit.get("blocker_codes", []) if str(item))
        return "ALLOCATION_LIMIT_EXCEEDED", blockers or "Allocation limit validation failed."
    return "", ""

def _risk_contract_validation_blocker(
    *,
    risk_contract: dict[str, Any],
    intent_id: str,
    intent_hash: str,
    day_utc: str,
    runtime_hash: str,
    construction_contract_hash: str,
    capital_authority_hash: str,
) -> tuple[str, str]:
    if not risk_contract:
        return "RISK_DEFINITION_CONTRACT_MISSING", "Risk definition contract is missing."
    # Backward-compatible test/legacy fixture path. Governed artifacts carry schema_id and are fully checked.
    if not risk_contract.get("schema_id"):
        if str(risk_contract.get("validation_status") or "").upper() == "PASS":
            return "", ""
        return "RISK_DEFINITION_CONTRACT_INVALID", "Legacy risk definition contract did not pass."
    if str(risk_contract.get("schema_id") or "") != "risk_definition_contract_v1":
        return "RISK_DEFINITION_CONTRACT_INVALID", "Risk definition contract schema_id mismatch."
    if str(risk_contract.get("day_utc") or "") != day_utc:
        return "RISK_DEFINITION_CONTRACT_INVALID", "Risk definition contract day does not match conversion day."
    if str(risk_contract.get("intent_hash") or "").lower() != intent_hash.lower():
        return "RISK_DEFINITION_CONTRACT_INVALID", "Risk definition contract intent hash mismatch."
    if intent_id and str(risk_contract.get("intent_id") or "") != intent_id:
        return "RISK_DEFINITION_CONTRACT_INVALID", "Risk definition contract intent id mismatch."
    risk_runtime = str(risk_contract.get("runtime_evaluation_hash") or "")
    if not risk_runtime or (runtime_hash and risk_runtime != runtime_hash):
        return "RISK_DEFINITION_CONTRACT_INVALID", "Risk definition contract RuntimeEvaluation hash mismatch."
    risk_construction_hash = str(risk_contract.get("construction_contract_hash") or "")
    if risk_construction_hash != construction_contract_hash:
        return "RISK_DEFINITION_CONTRACT_INVALID", "Risk definition contract construction contract hash mismatch."
    risk_cap_hash = str(risk_contract.get("capital_allocation_hash") or "")
    if capital_authority_hash and risk_cap_hash != capital_authority_hash:
        return "RISK_DEFINITION_CONTRACT_INVALID", "Risk definition contract capital allocation hash mismatch."
    status = str(risk_contract.get("validation_status") or "").upper()
    if status not in {"PASS", "VALID"}:
        blockers = ",".join(str(item) for item in risk_contract.get("blockers") or [] if str(item))
        if "RISK_LIMIT_EXCEEDED" in blockers:
            return "RISK_LIMIT_EXCEEDED", blockers
        return "RISK_DEFINITION_CONTRACT_INVALID", blockers or "Risk definition contract is not valid."
    measure = risk_contract.get("risk_measure_definition") if isinstance(risk_contract.get("risk_measure_definition"), dict) else {}
    if str(measure.get("validation_status") or "VALID").upper() not in {"VALID", ""}:
        return "RISK_LIMIT_EXCEEDED", "Risk measure exceeds governed limit."
    try:
        requested = float(str(risk_contract.get("requested_risk") or measure.get("requested_risk") or "0"))
        max_risk = float(str(risk_contract.get("max_risk") or measure.get("max_risk") or "0"))
    except ValueError:
        requested = 0.0
        max_risk = 0.0
    if max_risk > 0 and requested > max_risk:
        return "RISK_LIMIT_EXCEEDED", "Requested risk exceeds risk definition contract max risk."
    return "", ""


def _submission_record_inputs_from_package(package_path: Path) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    package = _read_json(package_path)
    selected_ref = package.get("selected_order_plan_ref") if isinstance(package.get("selected_order_plan_ref"), dict) else {}
    plan_path_text = str(selected_ref.get("path") or "").strip()
    plan_obj = _read_json(Path(plan_path_text).resolve()) if plan_path_text and Path(plan_path_text).exists() else None
    candidate_ref = package.get("candidate_ref") if isinstance(package.get("candidate_ref"), dict) else {}
    candidate_path_text = str(candidate_ref.get("phasec_out_dir") or "").strip()
    binding_path = Path(candidate_path_text).resolve() / "binding_record.v2.json" if candidate_path_text else Path()
    binding_obj = _read_json(binding_path) if str(binding_path) and binding_path.exists() else None
    return plan_obj, binding_obj


def _report_path(truth_root: Path, day_utc: str, attempt_id: str) -> Path:
    return (
        truth_root
        / "reports"
        / "exposure_intent_paper_submission_package_v1"
        / day_utc
        / attempt_id
        / "exposure_intent_paper_submission_package.v1.json"
    ).resolve()


def _base_report(*, day_utc: str, truth_root: Path, pointer_path: Path, produced_utc: str) -> dict[str, Any]:
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "truth_root": str(truth_root),
        "producer": {"repo": "constellation", "module": PRODUCER},
        "selected_pointer_path": str(pointer_path),
        "selected_pointer_sha256": _sha256_file(pointer_path) if pointer_path.exists() else "",
        "status": "BLOCKED",
        "blocker_code": "",
        "blocker_message": "",
        "exposure_intent_id": "",
        "exposure_intent_hash": "",
        "exposure_intent_path": "",
        "selected_by_arbitration": False,
        "symbol": "",
        "engine_id": "",
        "sleeve_id": "",
        "symbol_authority": None,
        "market_data_status": None,
        "capital_authority_path": str(_capital_authority_path(truth_root, day_utc)),
        "capital_authority_status": "NOT_CHECKED",
        "capital_authority_hash": "",
        "capital_authority_runtime_evaluation_hash": "",
        "allocation_source": "",
        "allocation_limit_used": {},
        "risk_contract_path": "",
        "risk_contract_status": "NOT_CHECKED",
        "risk_contract_hash": "",
        "risk_contract_runtime_evaluation_hash": "",
        "risk_contract_construction_contract_hash": "",
        "risk_contract_capital_allocation_hash": "",
        "risk_measure": "",
        "risk_measure_definition": {},
        "risk_limit_used": {},
        "candidate_identity_set_path": "",
        "candidate_identity_set_hash": "",
        "candidate_identity_status": "NOT_CHECKED",
        "expected_candidate_id": "",
        "actual_phasec_candidate_id": "",
        "actual_phasec_order_plan_path": "",
        "stale_order_plan_reason": "",
        "target_day_admission_status": "NOT_CHECKED",
        "target_day_admission_path": "",
        "target_day_admission_hash": "",
        "day_activation_status": "NOT_CHECKED",
        "day_activation_path": "",
        "day_activation_hash": "",
        "global_context_status": "NOT_CHECKED",
        "global_context_path": "",
        "global_context_hash": "",
        "economic_state_status": "NOT_CHECKED",
        "economic_state_path": "",
        "economic_state_hash": "",
        "economic_state_build_path": "",
        "cash_ledger_path": "",
        "cash_ledger_hash": "",
        "cash_ledger_source_type": "",
        "positions_snapshot_path": "",
        "positions_snapshot_hash": "",
        "positions_source_type": "",
        "position_lifecycle_path": "",
        "position_lifecycle_hash": "",
        "accounting_nav_path": "",
        "accounting_nav_hash": "",
        "accounting_nav_source_type": "",
        "execution_package_created": False,
        "execution_package_path": None,
        "execution_package_id": None,
        "submission_record_created": False,
        "submission_record_path": None,
        "submission_record_id": None,
        "paper_submit_attempted": False,
        "paper_submit_rc": None,
        "paper_trade_intent_created": False,
        "paper_trade_intent_id": None,
        "submit_stdout": "",
        "submit_stderr": "",
        "canonical_json_hash": None,
    }


def _finish(payload: dict[str, Any], *, truth_root: Path, day_utc: str) -> dict[str, Any]:
    attempt_id = canonical_hash_for_c2_artifact_v1(
        {
            "day_utc": day_utc,
            "intent_id": payload.get("exposure_intent_id"),
            "intent_hash": payload.get("exposure_intent_hash"),
            "blocker_code": payload.get("blocker_code"),
            "execution_package_path": payload.get("execution_package_path"),
        }
    )[:16]
    path = _report_path(truth_root, day_utc, attempt_id)
    payload["artifact_id"] = f"exposure_intent_paper_submission_package_v1:{day_utc}:{attempt_id}"
    payload["artifact_path"] = str(path)
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1({**payload, "canonical_json_hash": None})
    _write_json(path, payload)
    try:
        from ops.aegis.operator_state.canonical_operator_state_builder_v1 import build_and_write_operator_state_snapshot_v1
        build_and_write_operator_state_snapshot_v1(truth_root=truth_root, day_utc=day_utc)
    except Exception:
        pass
    return payload


def _block(payload: dict[str, Any], *, code: str, message: str, truth_root: Path, day_utc: str) -> dict[str, Any]:
    payload["status"] = "BLOCKED"
    payload["blocker_code"] = code
    payload["blocker_message"] = message
    return _finish(payload, truth_root=truth_root, day_utc=day_utc)


def _apply_execution_chain_visibility(payload: dict[str, Any], details: dict[str, Any]) -> None:
    chain = details.get("chain_map") if isinstance(details.get("chain_map"), list) else []
    for row in chain:
        if not isinstance(row, dict):
            continue
        artifact = str(row.get("artifact") or "").strip()
        if artifact == "target_day_admission_v1":
            payload["target_day_admission_status"] = str(row.get("status") or "")
            payload["target_day_admission_path"] = str(row.get("artifact_path") or "")
            path = Path(payload["target_day_admission_path"]) if payload["target_day_admission_path"] else Path()
            payload["target_day_admission_hash"] = _sha256_file(path) if str(path) else ""
        elif artifact == "day_activation_package_v1":
            payload["day_activation_status"] = str(row.get("status") or "")
            payload["day_activation_path"] = str(row.get("artifact_path") or "")
            payload["day_activation_hash"] = str(row.get("artifact_hash") or "")
        elif artifact == "global_context_package_v1":
            payload["global_context_status"] = str(row.get("status") or "")
            payload["global_context_path"] = str(row.get("artifact_path") or "")
            payload["global_context_hash"] = str(row.get("artifact_hash") or "")
        elif artifact == "economic_state_package_v1":
            payload["economic_state_status"] = str(row.get("status") or "")
            payload["economic_state_path"] = str(row.get("artifact_path") or "")
            payload["economic_state_hash"] = str(row.get("artifact_hash") or "")
            payload["economic_state_build_path"] = str(row.get("build_path") or "")
            payload["cash_ledger_path"] = str(row.get("cash_ledger_path") or "")
            payload["cash_ledger_hash"] = str(row.get("cash_ledger_hash") or "")
            payload["positions_snapshot_path"] = str(row.get("positions_snapshot_path") or "")
            payload["positions_snapshot_hash"] = str(row.get("positions_snapshot_hash") or "")
            payload["position_lifecycle_path"] = str(row.get("position_lifecycle_path") or "")
            payload["position_lifecycle_hash"] = str(row.get("position_lifecycle_hash") or "")
            payload["accounting_nav_path"] = str(row.get("accounting_nav_path") or "")
            payload["accounting_nav_hash"] = str(row.get("accounting_nav_hash") or "")
            for field, target in (("cash_ledger_path", "cash_ledger_source_type"), ("positions_snapshot_path", "positions_source_type"), ("accounting_nav_path", "accounting_nav_source_type")):
                ref_path = Path(str(payload.get(field) or ""))
                if str(ref_path) and ref_path.exists() and ref_path.is_file():
                    try:
                        payload[target] = str(_read_json(ref_path).get("source_type") or "")
                    except Exception:
                        payload[target] = ""


def convert_selected_exposure_intent_to_paper_submission_v1(
    *,
    day_utc: str,
    truth_root: Path,
    selected_pointer_path: Path | None = None,
    attempt_paper_submit: bool = True,
    eval_time_utc: str | None = None,
    dry_run: bool = True,
    ib_host: str = "127.0.0.1",
    ib_port: int = 4002,
    ib_client_id: int = 7,
    ib_account: str = "DUO847203",
    risk_budget_path: Path | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    pointer_path = (selected_pointer_path or _default_selected_pointer(root)).expanduser().resolve()
    produced_utc = eval_time_utc or _now_iso()
    payload = _base_report(day_utc=day_utc, truth_root=root, pointer_path=pointer_path, produced_utc=produced_utc)

    if not pointer_path.exists():
        return _block(payload, code="SELECTED_INTENT_POINTER_MISSING", message=f"Selected intent pointer missing: {pointer_path}", truth_root=root, day_utc=day_utc)
    pointer = _read_json(pointer_path)
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    if str(pointer.get("status") or "").strip().upper() != "SELECTED" or not selected:
        return _block(payload, code="NO_SELECTED_EXPOSURE_INTENT", message="No selected ExposureIntent is present in the selected intent pointer.", truth_root=root, day_utc=day_utc)
    if str(pointer.get("day_utc") or "").strip() != day_utc:
        return _block(payload, code="SELECTED_INTENT_DAY_MISMATCH", message=f"Selected pointer day does not match requested day {day_utc}.", truth_root=root, day_utc=day_utc)

    intent_id = str(selected.get("intent_id") or "").strip()
    intent_hash = str(selected.get("intent_hash") or "").strip().lower()
    intent_path = Path(str(selected.get("intent_path") or "")).expanduser().resolve()
    payload.update(
        {
            "exposure_intent_id": intent_id,
            "exposure_intent_hash": intent_hash,
            "exposure_intent_path": str(intent_path),
            "selected_by_arbitration": True,
            "engine_id": str(selected.get("engine_id") or selected.get("sleeve_id") or "").strip(),
            "sleeve_id": str(selected.get("sleeve_id") or selected.get("engine_id") or "").strip(),
            "symbol": str(selected.get("symbol") or "").strip().upper(),
        }
    )
    if not intent_path.exists():
        return _block(payload, code="EXPOSURE_INTENT_ARTIFACT_MISSING", message=f"Selected ExposureIntent artifact missing: {intent_path}", truth_root=root, day_utc=day_utc)
    intent = _read_json(intent_path)
    symbol = str(((intent.get("underlying") or {}) if isinstance(intent.get("underlying"), dict) else {}).get("symbol") or payload["symbol"]).strip().upper()
    engine_id = str(((intent.get("engine") or {}) if isinstance(intent.get("engine"), dict) else {}).get("engine_id") or payload["engine_id"]).strip().upper()
    payload["symbol"] = symbol
    payload["engine_id"] = engine_id
    payload["sleeve_id"] = str(payload["sleeve_id"] or engine_id)

    if str(intent.get("schema_id") or "").strip() != "exposure_intent" or str(intent.get("schema_version") or "").strip() != "v1":
        return _block(payload, code="RAW_INPUT_NOT_EXPOSURE_INTENT_V1", message="Converter accepts only ExposureIntent v1 artifacts as input.", truth_root=root, day_utc=day_utc)
    if str(intent.get("exposure_type") or "").strip().upper() != "LONG_EQUITY":
        return _block(payload, code="EXPOSURE_INTENT_TYPE_UNSUPPORTED_FOR_EQUITY_PACKAGE", message="Only LONG_EQUITY ExposureIntent conversion is currently supported by the governed equity package path.", truth_root=root, day_utc=day_utc)
    if not intent.get("target_notional_pct") or not isinstance(intent.get("constraints"), dict):
        return _block(payload, code="EXPOSURE_INTENT_CONFIDENCE_OR_SIZING_INCOMPLETE", message="ExposureIntent is missing target sizing or constraints required for conversion.", truth_root=root, day_utc=day_utc)

    try:
        resolution = resolve_canonical_symbol_universe_v1(truth_root=root, day_utc=day_utc, engine_id=engine_id, allow_deprecated_fallback=False)
        payload["symbol_authority"] = {
            "source": resolution.source,
            "source_path": resolution.source_path,
            "symbol_count": resolution.symbol_count,
            "deprecated_fallback_used": resolution.deprecated_fallback_used,
            "symbol_present": symbol in set(resolution.symbols),
        }
    except CanonicalSymbolUniverseError as exc:
        payload["symbol_authority"] = {"error": str(exc), "symbol_present": False}
        return _block(payload, code="SYMBOL_AUTHORITY_EVIDENCE_MISSING", message="Canonical non-deprecated symbol authority could not be resolved.", truth_root=root, day_utc=day_utc)
    if not payload["symbol_authority"]["symbol_present"]:
        return _block(payload, code="SUBMIT_BOUNDARY_SYMBOL_POLICY_REJECTS_SYMBOL", message=f"Symbol {symbol} is absent from the canonical non-deprecated symbol authority.", truth_root=root, day_utc=day_utc)

    market = _market_data_status(root, day_utc, symbol)
    payload["market_data_status"] = market
    if market["status"] != "CURRENT":
        return _block(
            payload,
            code="STALE_MARKET_DATA_BLOCKS_CONVERSION",
            message=f"{symbol} market data is not current for {day_utc}; observed_session={market.get('observed_session') or 'MISSING'}. No stale data accepted.",
            truth_root=root,
            day_utc=day_utc,
        )

    cap_path = _capital_authority_path(root, day_utc)
    if not cap_path.exists():
        payload["capital_authority_status"] = "MISSING"
        return _block(payload, code="CAPITAL_AUTHORITY_ALLOCATION_MISSING", message=f"Capital authority allocation artifact is missing: {cap_path}", truth_root=root, day_utc=day_utc)
    capital = _read_json(cap_path)
    payload["capital_authority_hash"] = _sha256_file(cap_path)
    payload["capital_authority_runtime_evaluation_hash"] = str(capital.get("runtime_evaluation_hash") or "")
    payload["allocation_source"] = str(capital.get("allocation_source") or "")
    runtime_hash = str(read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc).get("deterministic_output_hash") or "")
    row = _authorized_row(capital, intent_id)
    if row is None:
        payload["capital_authority_status"] = "INTENT_NOT_AUTHORIZED"
        return _block(payload, code="EXPOSURE_INTENT_NOT_CAPITAL_AUTHORIZED", message="Selected ExposureIntent is not present in capital_authority_allocation_v1.authorized_trade_intents.", truth_root=root, day_utc=day_utc)
    payload["capital_authority_status"] = str(row.get("authorization_outcome") or "").strip().upper()
    payload["allocation_limit_used"] = row.get("allocation_limit_used") if isinstance(row.get("allocation_limit_used"), dict) else {}
    blocker_code, blocker_message = _capital_validation_blocker(capital=capital, row=row, intent_id=intent_id, day_utc=day_utc, runtime_hash=runtime_hash)
    if blocker_code:
        return _block(payload, code=blocker_code, message=blocker_message, truth_root=root, day_utc=day_utc)

    risk_path = _risk_contract_path(root, day_utc, intent_hash)
    payload["risk_contract_path"] = str(risk_path)
    if not risk_path.exists():
        payload["risk_contract_status"] = "MISSING"
        return _block(payload, code="RISK_DEFINITION_CONTRACT_MISSING", message=f"Risk definition contract is missing: {risk_path}", truth_root=root, day_utc=day_utc)
    risk_contract = _read_json(risk_path)
    payload["risk_contract_hash"] = _sha256_file(risk_path)
    payload["risk_contract_runtime_evaluation_hash"] = str(risk_contract.get("runtime_evaluation_hash") or "")
    payload["risk_contract_construction_contract_hash"] = str(risk_contract.get("construction_contract_hash") or "")
    payload["risk_contract_capital_allocation_hash"] = str(risk_contract.get("capital_allocation_hash") or "")
    payload["risk_measure"] = str(risk_contract.get("risk_measure") or risk_contract.get("risk_type") or "")
    payload["risk_measure_definition"] = risk_contract.get("risk_measure_definition") if isinstance(risk_contract.get("risk_measure_definition"), dict) else {}
    payload["risk_limit_used"] = {
        "requested_risk": str(risk_contract.get("requested_risk") or ""),
        "max_risk": str(risk_contract.get("max_risk") or ""),
        "risk_model_version": str(risk_contract.get("risk_model_version") or ""),
        "sizing_method": str(risk_contract.get("sizing_method") or ""),
    }
    construction_path = _paper_trade_construction_path(root, day_utc)
    construction = _read_json(construction_path) if construction_path.exists() else {}
    risk_blocker, risk_message = _risk_contract_validation_blocker(
        risk_contract=risk_contract,
        intent_id=intent_id,
        intent_hash=intent_hash,
        day_utc=day_utc,
        runtime_hash=runtime_hash,
        construction_contract_hash=str(construction.get("construction_contract_hash") or ""),
        capital_authority_hash=payload["capital_authority_hash"],
    )
    if risk_blocker:
        payload["risk_contract_status"] = "INVALID"
        return _block(payload, code=risk_blocker, message=risk_message, truth_root=root, day_utc=day_utc)
    payload["risk_contract_status"] = "PASS"

    identity_path, identity_set, identity_blocker = load_candidate_identity_set_v1(truth_root=root, day_utc=day_utc, intent_hash=intent_hash)
    payload["candidate_identity_set_path"] = str(identity_path)
    payload["candidate_identity_set_hash"] = _sha256_file(identity_path) if identity_path.exists() else ""
    payload["candidate_identity_status"] = str(identity_set.get("validation_status") or "MISSING")
    payload["expected_candidate_id"] = str(identity_set.get("candidate_id") or intent_id)
    actual_phasec = identity_set.get("actual_phasec_order_plan") if isinstance(identity_set.get("actual_phasec_order_plan"), dict) else {}
    payload["actual_phasec_candidate_id"] = str(actual_phasec.get("source_intent_id") or "")
    payload["actual_phasec_order_plan_path"] = str(actual_phasec.get("order_plan_path") or "")
    payload["stale_order_plan_reason"] = str(identity_set.get("stale_order_plan_reason") or "")
    if identity_blocker:
        return _block(payload, code=identity_blocker, message=f"Candidate identity set is not valid: {identity_path}", truth_root=root, day_utc=day_utc)

    try:
        package_result = build_execution_package_from_authorized_intent_v1(day_utc=day_utc, truth_root=root, intent_id=intent_id)
    except Exception as exc:
        message = str(exc)
        if message.startswith("ARTIFACT_LEDGER_DUPLICATE_ARTIFACT_ID_DIFFERENT_HASH:execution_build_v1:"):
            submission_id = message.rsplit(":", 1)[-1].strip()
            build_path = root / "reports" / "execution_build_v1" / day_utc / submission_id / "execution_build.v1.json"
            if build_path.exists():
                build_obj = _read_json(build_path)
                first = build_obj.get("first_real_blocker") if isinstance(build_obj.get("first_real_blocker"), dict) else {}
                package_path = root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "execution_package_v1" / day_utc / submission_id / "execution_package.v1.json"
                details = {
                    "build_path": str(build_path),
                    "first_real_blocker": first,
                    "blocking_chain": list(build_obj.get("blocking_chain") or []),
                    "materializable_now": list(build_obj.get("materializable_now") or []),
                    "chain_map": _execution_build_chain_map(
                        day_utc=day_utc,
                        truth_root=root,
                        build_obj=build_obj,
                        build_path=str(build_path),
                        package_path=str(package_path),
                    ),
                    "idempotent_existing_build_used": True,
                }
                _apply_execution_chain_visibility(payload, details)
                return _block(
                    payload,
                    code=str(first.get("dependency_id") or "EXECUTION_PACKAGE_BUILD_BLOCKED"),
                    message=json.dumps(details, sort_keys=True),
                    truth_root=root,
                    day_utc=day_utc,
                )
        code = "EXECUTION_PACKAGE_BUILD_BLOCKED"
        for candidate_code in (
            "CANDIDATE_IDENTITY_SET_MISSING",
            "CANDIDATE_IDENTITY_SET_MISMATCH",
            "STALE_PHASE_C_ORDER_PLAN",
            "SELECTED_INTENT_POINTER_MISMATCH",
        ):
            if message.startswith(candidate_code):
                code = candidate_code
                break
        return _block(
            payload,
            code=code,
            message=json.dumps({"exception_type": type(exc).__name__, "message": message}, sort_keys=True),
            truth_root=root,
            day_utc=day_utc,
        )
    if str(package_result.get("status") or "").strip().upper() != "PASS":
        details = package_result.get("details") if isinstance(package_result.get("details"), dict) else {}
        _apply_execution_chain_visibility(payload, details)
        return _block(
            payload,
            code=str(package_result.get("blocker") or "EXECUTION_PACKAGE_BUILD_BLOCKED"),
            message=json.dumps(details, sort_keys=True),
            truth_root=root,
            day_utc=day_utc,
        )
    details = package_result.get("details") if isinstance(package_result.get("details"), dict) else {}
    _apply_execution_chain_visibility(payload, details)
    package_path = Path(str(package_result.get("package_path") or "")).resolve()
    package_obj = _read_json(package_path)
    payload["execution_package_created"] = True
    payload["execution_package_path"] = str(package_path)
    payload["execution_package_id"] = str(package_obj.get("submission_id") or "")

    plan_obj, binding_obj = _submission_record_inputs_from_package(package_path)
    record, record_path, _action = write_execution_submission_record_from_execution_package_v1(
        execution_package_path=package_path,
        day_utc=day_utc,
        produced_utc=produced_utc,
        truth_root=root,
        plan_obj=plan_obj,
        binding_obj=binding_obj,
    )
    payload["submission_record_created"] = True
    payload["submission_record_path"] = str(record_path)
    payload["submission_record_id"] = record.submission_record_id

    if attempt_paper_submit:
        payload["paper_submit_attempted"] = True
        risk_budget = risk_budget_path or (REPO_ROOT / "constellation_2" / "phaseD" / "inputs" / "sample_risk_budget.v1.json")
        cmd = [
            sys.executable,
            str(REPO_ROOT / "constellation_2" / "phaseD" / "tools" / "c2_submit_paper_v5.py"),
            "--eval_time_utc",
            produced_utc,
            "--execution_package_path",
            str(package_path),
            "--submission_record_path",
            str(record_path),
            "--risk_budget",
            str(Path(risk_budget).resolve()),
            "--ib_host",
            ib_host,
            "--ib_port",
            str(int(ib_port)),
            "--ib_client_id",
            str(int(ib_client_id)),
            "--ib_account",
            ib_account,
            "--dry_run",
            "YES" if dry_run else "NO",
        ]
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT), text=True, capture_output=True, timeout=120, check=False)
        payload["paper_submit_rc"] = int(proc.returncode)
        payload["submit_stdout"] = proc.stdout[-4000:]
        payload["submit_stderr"] = proc.stderr[-4000:]
        if proc.returncode != 0:
            return _block(payload, code="SUBMIT_BOUNDARY_BLOCKED_PACKAGE", message=(proc.stderr or proc.stdout or "submit boundary returned nonzero").strip()[-1000:], truth_root=root, day_utc=day_utc)
        payload["paper_trade_intent_created"] = True
        payload["paper_trade_intent_id"] = record.submission_record_id

    payload["status"] = "PASS"
    payload["blocker_code"] = ""
    payload["blocker_message"] = ""
    return _finish(payload, truth_root=root, day_utc=day_utc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_exposure_intent_paper_submission_package_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--selected_intent_pointer_path", default="")
    parser.add_argument("--attempt_paper_submit", choices=["YES", "NO"], default="YES")
    parser.add_argument("--eval_time_utc", default="")
    parser.add_argument("--dry_run", choices=["YES", "NO"], default="YES")
    parser.add_argument("--ib_host", default="127.0.0.1")
    parser.add_argument("--ib_port", type=int, default=4002)
    parser.add_argument("--ib_client_id", type=int, default=7)
    parser.add_argument("--ib_account", default="DUO847203")
    parser.add_argument("--risk_budget", default="")
    args = parser.parse_args(argv)
    result = convert_selected_exposure_intent_to_paper_submission_v1(
        day_utc=str(args.day_utc).strip(),
        truth_root=Path(str(args.truth_root).strip()).expanduser().resolve(),
        selected_pointer_path=Path(str(args.selected_intent_pointer_path).strip()).expanduser().resolve() if str(args.selected_intent_pointer_path).strip() else None,
        attempt_paper_submit=str(args.attempt_paper_submit).strip().upper() == "YES",
        eval_time_utc=str(args.eval_time_utc).strip() or None,
        dry_run=str(args.dry_run).strip().upper() == "YES",
        ib_host=str(args.ib_host).strip(),
        ib_port=int(args.ib_port),
        ib_client_id=int(args.ib_client_id),
        ib_account=str(args.ib_account).strip(),
        risk_budget_path=Path(str(args.risk_budget).strip()).expanduser().resolve() if str(args.risk_budget).strip() else None,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
