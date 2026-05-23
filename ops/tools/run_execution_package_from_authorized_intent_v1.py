#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisory.execution_package_builder_from_execution_intent_v1 import (
    stage_candidate_from_execution_intent_v1,
)
from constellation_2.common.execution_build_authority_v1 import run_execution_build_authority_v1
from constellation_2.common.day_activation_authority_v1 import (
    day_activation_build_path_v1,
    day_activation_package_path_v1,
    resolve_day_activation_context_v1,
)
from constellation_2.common.global_context_authority_v1 import (
    global_context_build_path_v1,
    global_context_package_path_v1,
    resolve_global_context_v1,
)
from constellation_2.common.economic_state_authority_v1 import (
    economic_state_build_path_v1,
    economic_state_package_path_v1,
    resolve_economic_state_context_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.tools.run_risk_definition_contract_v1 import load_valid_risk_definition_contract_v1
from ops.aegis.candidate_identity_set_v1 import load_candidate_identity_set_v1


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"])
        return out.decode("utf-8").strip()
    except Exception:
        return "0" * 40


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _read_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        return _read_json(path)
    except Exception:
        return {}


def _sha256_file(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    import hashlib
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _capital_authority_path(truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "allocation_v1"
        / "capital_authority_allocation_v1"
        / day_utc
        / "capital_authority_allocation.v1.json"
    ).resolve()


def _intent_path(truth_root: Path, day_utc: str, intent_hash: str) -> Path:
    direct = (truth_root / "intents_v1" / "snapshots" / day_utc / f"{intent_hash}.exposure_intent.v1.json").resolve()
    if direct.exists():
        return direct
    pointer_path = truth_root / "pointers" / "selected_intent_pointer.v1.json"
    pointer = _read_json_if_exists(pointer_path)
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    if str(selected.get("intent_hash") or "").lower() == intent_hash.lower():
        selected_path = Path(str(selected.get("intent_path") or "")).expanduser().resolve()
        if selected_path.exists():
            return selected_path
    sleeve = (truth_root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "intents_v1" / "snapshots" / day_utc / f"{intent_hash}.exposure_intent.v1.json").resolve()
    return sleeve if sleeve.exists() else direct


def _authorized_row(*, capital: dict[str, Any], intent_id: str) -> dict[str, Any]:
    rows = ((capital.get("decision_chain") or {}) if isinstance(capital.get("decision_chain"), dict) else {}).get("authorized_trade_intents")
    if not isinstance(rows, list):
        raise ValueError("CAPITAL_AUTHORITY_AUTHORIZED_TRADE_INTENTS_MISSING")
    for row in rows:
        if isinstance(row, dict) and str(row.get("intent_id") or "").strip() == intent_id:
            return row
    raise ValueError(f"AUTHORIZED_INTENT_NOT_FOUND:intent_id={intent_id}")


def _fail_payload(*, day_utc: str, intent_id: str, truth_root: Path, blocker: str, details: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "BLOCKED",
        "day_utc": day_utc,
        "intent_id": intent_id,
        "truth_root": str(truth_root.resolve()),
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_execution_package_from_authorized_intent_v1.py",
            "git_sha": _git_sha(),
        },
        "blocker": blocker,
        "details": details,
        "package_path": None,
    }


def _defined_risk_diagnostic(*, day_utc: str, exposure_type: str) -> dict[str, Any]:
    return {
        "exposure_type": exposure_type,
        "defined_risk_evidence_required": True,
        "missing_fields": ["structure type", "strikes", "expiry", "max loss", "quantity basis", "options-chain ref"],
        "expected_options_chain_artifact_pattern": (
            f"options_chain_snapshot_v1/{day_utc}/<capture_id>/options_chain_snapshot.v1.json"
        ),
        "options_chain_producer": "ops/tools/run_options_chain_snapshot_required_day_v1.py",
        "recovery_command": (
            f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_options_chain_snapshot_required_day_v1.py "
            f"--day_utc {day_utc} --truth_root <TRUTH_ROOT> --symbol <OPTION_UNDERLYING>"
        ),
    }


def _node_status(payload: dict[str, Any], *fields: str) -> str:
    for field in fields:
        value = str(payload.get(field) or "").strip().upper()
        if value:
            return value
    return "MISSING"


def _node_blocker(payload: dict[str, Any]) -> str:
    first = payload.get("first_real_blocker") if isinstance(payload.get("first_real_blocker"), dict) else {}
    if first:
        return str(first.get("dependency_id") or first.get("first_real_blocker_dependency_id") or "").strip()
    codes = payload.get("blocking_reason_codes") or payload.get("blocking_codes")
    if isinstance(codes, list) and codes:
        return ",".join(str(code) for code in codes if str(code))
    chain = payload.get("blocking_chain") or payload.get("blocker_chain")
    if isinstance(chain, list) and chain:
        first_chain = chain[0] if isinstance(chain[0], dict) else {}
        return str(first_chain.get("blocker_code") or first_chain.get("dependency_id") or "").strip()
    return ""


def _classify_root_blocker(payload: dict[str, Any]) -> str:
    codes = {str(code).strip().upper() for code in payload.get("blocking_reason_codes", []) if str(code).strip()} if isinstance(payload.get("blocking_reason_codes"), list) else set()
    hidden = payload.get("hidden_dependency_check_result") if isinstance(payload.get("hidden_dependency_check_result"), dict) else {}
    undeclared = hidden.get("undeclared_dependency_artifacts") if isinstance(hidden.get("undeclared_dependency_artifacts"), list) else []
    if "HIDDEN_DEPENDENCY_DETECTED" in codes or undeclared:
        return "DEFECTIVE_OR_MISSING_EVIDENCE"
    if codes:
        return "REAL_POLICY_OR_REQUIRED_GATE_FAILURE"
    return "UNKNOWN"


def _execution_build_chain_map(
    *,
    day_utc: str,
    truth_root: Path,
    build_obj: dict[str, Any],
    build_path: str,
    package_path: str,
) -> list[dict[str, Any]]:
    candidate_ref = build_obj.get("candidate_ref") if isinstance(build_obj.get("candidate_ref"), dict) else {}
    canonical_root = Path(str(candidate_ref.get("canonical_truth_root") or "/home/node/constellation_runtime_data/truth")).resolve()
    execution_root = Path(str(candidate_ref.get("execution_truth_root") or truth_root)).resolve()
    sleeve_id = str(candidate_ref.get("sleeve_id") or "PRIMARY").strip() or "PRIMARY"
    environment = str(candidate_ref.get("environment") or "PAPER").strip() or "PAPER"
    target_admission_path = (canonical_root / "target_day_admission_v1" / f"{day_utc}.json").resolve()
    target_admission = _read_json_if_exists(target_admission_path)
    execution_build_path = Path(str(build_path or "")).resolve() if str(build_path or "").strip() else Path()
    execution_build = _read_json_if_exists(execution_build_path) if str(build_path or "").strip() else build_obj
    operation_type = str(candidate_ref.get("operation_type") or "fresh_paper_entry_v1").strip() or "fresh_paper_entry_v1"
    ib_account = str(candidate_ref.get("ib_account") or candidate_ref.get("account_id") or "DUO847203").strip() or "DUO847203"
    day_ctx = resolve_day_activation_context_v1(
        repo_root=REPO_ROOT,
        operation_type=operation_type,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
    )
    global_ctx = resolve_global_context_v1(
        repo_root=REPO_ROOT,
        operation_type=operation_type,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
    )
    day_build_path_obj = day_activation_build_path_v1(ctx=day_ctx)
    day_package_path_obj = day_activation_package_path_v1(ctx=day_ctx)
    global_build_path_obj = global_context_build_path_v1(ctx=global_ctx)
    global_package_path_obj = global_context_package_path_v1(ctx=global_ctx)
    economic_ctx = resolve_economic_state_context_v1(
        repo_root=REPO_ROOT,
        operation_type=operation_type,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
    )
    economic_build_path_obj = economic_state_build_path_v1(ctx=economic_ctx)
    economic_package_path_obj = economic_state_package_path_v1(ctx=economic_ctx)
    day_build = _read_json_if_exists(day_build_path_obj)
    day_package = _read_json_if_exists(day_package_path_obj)
    global_build = _read_json_if_exists(global_build_path_obj)
    global_package = _read_json_if_exists(global_package_path_obj)
    economic_build = _read_json_if_exists(economic_build_path_obj)
    economic_package = _read_json_if_exists(economic_package_path_obj)
    economic_refs = {
        str(row.get("dependency_id") or ""): row
        for row in (economic_build.get("dependency_results") or [])
        if isinstance(row, dict)
    }
    first = execution_build.get("first_real_blocker") if isinstance(execution_build.get("first_real_blocker"), dict) else {}
    submission_id = str(execution_build.get("submission_id") or "").strip()
    expected_package_path = (
        str(package_path)
        if str(package_path or "").strip()
        else str((execution_root / "execution_package_v1" / day_utc / submission_id / "execution_package.v1.json").resolve())
        if submission_id
        else ""
    )
    return [
        {
            "artifact": "target_day_admission_v1",
            "status": _node_status(target_admission, "admission_status", "status"),
            "blocker": _node_blocker(target_admission),
            "artifact_path": str(target_admission_path),
            "producer": "ops/tools/run_session_authority_v1.py",
            "recovery_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_session_authority_v1.py --target_day {day_utc} --truth_root {canonical_root} --environment {environment} --ib_account {ib_account} --phase admit",
            "blocker_classification": _classify_root_blocker(target_admission),
        },
        {
            "artifact": "day_activation_package_v1",
            "status": str(day_package.get("validation_status") or _node_status(day_build, "closure_status", "status")),
            "blocker": _node_blocker(day_build) if not day_package else "",
            "artifact_path": str(day_package_path_obj),
            "artifact_hash": _sha256_file(day_package_path_obj) if day_package_path_obj.exists() else "",
            "build_path": str(day_build_path_obj),
            "producer": "ops/tools/run_day_activation_authority_v1.py",
            "recovery_command": f'PYTHONPATH="$PWD" python3 ops/tools/run_day_activation_authority_v1.py --operation_type {operation_type} --day_utc {day_utc} --sleeve_id {sleeve_id} --environment {environment} --ib_account {ib_account} --materialize YES --emit_package YES',
            "blocker_classification": "BLOCKED_BY_UPSTREAM" if not day_package else "CLEARED",
        },
        {
            "artifact": "global_context_package_v1",
            "status": str(global_package.get("validation_status") or _node_status(global_build, "closure_status", "status")),
            "blocker": _node_blocker(global_build) if not global_package else "",
            "artifact_path": str(global_package_path_obj),
            "artifact_hash": _sha256_file(global_package_path_obj) if global_package_path_obj.exists() else "",
            "build_path": str(global_build_path_obj),
            "producer": "ops/tools/run_global_context_authority_v1.py",
            "recovery_command": f'PYTHONPATH="$PWD" python3 ops/tools/run_global_context_authority_v1.py --operation_type {operation_type} --day_utc {day_utc} --sleeve_id {sleeve_id} --environment {environment} --ib_account {ib_account} --materialize YES --emit_package YES',
            "blocker_classification": "BLOCKED_BY_UPSTREAM" if not global_package else "CLEARED",
        },
        {
            "artifact": "economic_state_package_v1",
            "status": str(economic_package.get("validation_status") or _node_status(economic_build, "closure_status", "status")),
            "blocker": _node_blocker(economic_build) if not economic_package else "",
            "artifact_path": str(economic_package_path_obj),
            "artifact_hash": _sha256_file(economic_package_path_obj) if economic_package_path_obj.exists() else "",
            "build_path": str(economic_build_path_obj),
            "cash_ledger_path": str((economic_refs.get("cash_ledger_snapshot_v1") or {}).get("path") or ""),
            "cash_ledger_hash": str((economic_refs.get("cash_ledger_snapshot_v1") or {}).get("sha256") or ""),
            "positions_snapshot_path": str((economic_refs.get("positions_snapshot_v5") or {}).get("path") or ""),
            "positions_snapshot_hash": str((economic_refs.get("positions_snapshot_v5") or {}).get("sha256") or ""),
            "position_lifecycle_path": str((economic_refs.get("position_lifecycle_snapshot_v2") or {}).get("path") or ""),
            "position_lifecycle_hash": str((economic_refs.get("position_lifecycle_snapshot_v2") or {}).get("sha256") or ""),
            "accounting_nav_path": str((economic_refs.get("accounting_nav_v2") or {}).get("path") or ""),
            "accounting_nav_hash": str((economic_refs.get("accounting_nav_v2") or {}).get("sha256") or ""),
            "producer": "ops/tools/build_account_economic_state_authority_v1.py",
            "recovery_command": f'PYTHONPATH="$PWD" python3 ops/tools/build_account_economic_state_authority_v1.py --truth-root {canonical_root} --day-utc {day_utc} --operation-type {operation_type} --sleeve-id {sleeve_id} --environment {environment} --ib-account {ib_account}',
            "blocker_classification": "BLOCKED_BY_UPSTREAM" if not economic_package else "CLEARED",
        },
        {
            "artifact": "execution_build_v1",
            "status": _node_status(execution_build, "closure_status", "status"),
            "blocker": _node_blocker(execution_build),
            "artifact_path": str(execution_build_path) if str(build_path or "").strip() else "",
            "producer": "ops/tools/run_execution_package_from_authorized_intent_v1.py",
            "recovery_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_execution_package_from_authorized_intent_v1.py --day_utc {day_utc} --truth_root {truth_root} --intent_id <AUTHORIZED_INTENT_ID>",
            "blocker_classification": "BLOCKED_BY_UPSTREAM",
        },
        {
            "artifact": "execution_package_v1",
            "status": "PRESENT" if expected_package_path and Path(expected_package_path).exists() else "MISSING",
            "blocker": "" if expected_package_path and Path(expected_package_path).exists() else "EXECUTION_BUILD_NOT_COMPLETE",
            "artifact_path": expected_package_path,
            "producer": "ops/tools/run_execution_package_from_authorized_intent_v1.py",
            "recovery_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_execution_package_from_authorized_intent_v1.py --day_utc {day_utc} --truth_root {truth_root} --intent_id <AUTHORIZED_INTENT_ID>",
            "blocker_classification": "BLOCKED_BY_UPSTREAM",
        },
    ]


def _build_execution_intent(
    *,
    day_utc: str,
    truth_root: Path,
    row: dict[str, Any],
    intent_obj: dict[str, Any],
    intent_path: Path,
    risk_contract_path: Path,
    candidate_identity_set_path: Path,
) -> ExecutionIntentV1:
    intent_hash = str(row.get("intent_hash") or "").strip()
    symbol = str(((intent_obj.get("underlying") or {}) if isinstance(intent_obj.get("underlying"), dict) else {}).get("symbol") or row.get("symbol") or "").strip().upper()
    currency = str(((intent_obj.get("underlying") or {}) if isinstance(intent_obj.get("underlying"), dict) else {}).get("currency") or "USD").strip().upper()
    quantity = int(row.get("authorized_quantity") or 0)
    payload = {
        "schema_id": "execution_intent",
        "schema_version": "v1",
        "record_id": str(row.get("intent_id") or ""),
        "execution_intent_id": str(row.get("intent_id") or ""),
        "promotion_record_id": f"capital_authority_allocation_v1:{str(row.get('authorized_trade_intent_id') or intent_hash)}",
        "household_id": "AegisPaper",
        "created_at_utc": f"{day_utc}T00:00:00Z",
        "effective_at_utc": f"{day_utc}T00:00:00Z",
        "actor_source": "run_execution_package_from_authorized_intent_v1",
        "contract_version": "execution_intent_contract_v1",
        "builder_version": "execution_intent_builder_from_authorized_intent_v1",
        "idempotency_key": intent_hash,
        "operation_type": "fresh_paper_entry_v1",
        "day_utc": day_utc,
        "environment": "PAPER",
        "sleeve_id": str(row.get("execution_sleeve_id") or "PRIMARY").strip().upper(),
        "account_id": str(row.get("account_id") or "").strip(),
        "engine_id": str(row.get("engine_id") or "").strip(),
        "instrument": {"kind": "EQUITY", "symbol": symbol, "currency": currency, "ib_conId": None, "ib_localSymbol": symbol},
        "side": "BUY" if str(row.get("action_type") or "").strip().upper() == "OPEN" else "SELL",
        "quantity_shares": quantity,
        "order_terms": {"order_type": "MARKET", "limit_price": None, "time_in_force": "DAY"},
        "parent_lineage_refs": [f"capital_authority_allocation_path:{_capital_authority_path(truth_root, day_utc)}"],
        "source_artifact_refs": [f"exposure_intent_path:{intent_path}", f"risk_contract_path:{risk_contract_path}", f"candidate_identity_set_path:{candidate_identity_set_path}", f"intent_hash:{intent_hash}"],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1({**payload, "canonical_json_hash": None})
    validate_against_repo_schema_v1(payload, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/ADVISORY/execution_intent.v1.schema.json")
    return ExecutionIntentV1.from_dict(payload)


def build_execution_package_from_authorized_intent_v1(*, day_utc: str, truth_root: Path, intent_id: str) -> dict[str, Any]:
    truth_root = truth_root.resolve()
    cap_path = _capital_authority_path(truth_root, day_utc)
    capital = _read_json(cap_path)
    row = _authorized_row(capital=capital, intent_id=intent_id)
    outcome = str(row.get("authorization_outcome") or "").strip().upper()
    qty = int(row.get("authorized_quantity") or 0)
    if outcome not in {"APPROVED", "RESIZED"} or qty <= 0:
        intent_hash = str(row.get("intent_hash") or "").strip()
        diagnostic: dict[str, Any] = {}
        if intent_hash:
            path = _intent_path(truth_root, day_utc, intent_hash)
            if path.exists():
                intent_obj = _read_json(path)
                exposure_type = str(intent_obj.get("exposure_type") or "").strip().upper()
                if exposure_type and exposure_type != "LONG_EQUITY":
                    diagnostic = _defined_risk_diagnostic(day_utc=day_utc, exposure_type=exposure_type)
        return _fail_payload(
            day_utc=day_utc,
            intent_id=intent_id,
            truth_root=truth_root,
            blocker="AUTHORIZED_INTENT_NOT_APPROVED",
            details={
                "authorization_outcome": outcome,
                "authorized_quantity": qty,
                "reason_codes": list(row.get("reason_codes") or []),
                **diagnostic,
            },
        )
    intent_hash = str(row.get("intent_hash") or "").strip()
    path = _intent_path(truth_root, day_utc, intent_hash)
    if not path.exists():
        return _fail_payload(day_utc=day_utc, intent_id=intent_id, truth_root=truth_root, blocker="EXPOSURE_INTENT_MISSING", details={"expected_path": str(path)})
    intent_obj = _read_json(path)
    risk_contract_path, risk_contract, risk_blocker = load_valid_risk_definition_contract_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        intent_hash=intent_hash,
        intent_id=intent_id,
    )
    if risk_blocker:
        return _fail_payload(
            day_utc=day_utc,
            intent_id=intent_id,
            truth_root=truth_root,
            blocker=risk_blocker.split(":", 1)[0],
            details={"risk_contract_path": str(risk_contract_path), "risk_contract_blocker": risk_blocker},
        )
    identity_path, identity_set, identity_blocker = load_candidate_identity_set_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        intent_hash=intent_hash,
    )
    if identity_blocker:
        return _fail_payload(
            day_utc=day_utc,
            intent_id=intent_id,
            truth_root=truth_root,
            blocker=identity_blocker,
            details={"candidate_identity_set_path": str(identity_path), "candidate_identity_set_blocker": identity_blocker},
        )
    if str(identity_set.get("candidate_id") or "") != intent_id or str(identity_set.get("intent_hash") or "").lower() != intent_hash.lower():
        return _fail_payload(
            day_utc=day_utc,
            intent_id=intent_id,
            truth_root=truth_root,
            blocker="CANDIDATE_IDENTITY_SET_MISMATCH",
            details={"candidate_identity_set_path": str(identity_path), "expected_intent_id": intent_id, "actual_candidate_id": identity_set.get("candidate_id"), "expected_intent_hash": intent_hash, "actual_intent_hash": identity_set.get("intent_hash")},
        )

    exposure_type = str(intent_obj.get("exposure_type") or "").strip().upper()
    if exposure_type != "LONG_EQUITY":
        if str(risk_contract.get("risk_type") or "").strip().upper() != "DEFINED_RISK":
            return _fail_payload(
                day_utc=day_utc,
                intent_id=intent_id,
                truth_root=truth_root,
                blocker="DEFINED_RISK_CONTRACT_REQUIRED",
                details={"risk_contract_path": str(risk_contract_path), **_defined_risk_diagnostic(day_utc=day_utc, exposure_type=exposure_type)},
            )
        return _fail_payload(
            day_utc=day_utc,
            intent_id=intent_id,
            truth_root=truth_root,
            blocker="DEFINED_RISK_EXECUTION_PACKAGE_REQUIRES_GOVERNED_OPTIONS_EVIDENCE",
            details=_defined_risk_diagnostic(day_utc=day_utc, exposure_type=exposure_type),
        )
    if str(risk_contract.get("risk_type") or "").strip().upper() != "STOP_BASED":
        return _fail_payload(
            day_utc=day_utc,
            intent_id=intent_id,
            truth_root=truth_root,
            blocker="STOP_BASED_RISK_CONTRACT_REQUIRED",
            details={"risk_contract_path": str(risk_contract_path), "risk_type": str(risk_contract.get("risk_type") or "")},
        )
    execution_intent = _build_execution_intent(day_utc=day_utc, truth_root=truth_root, row=row, intent_obj=intent_obj, intent_path=path, risk_contract_path=risk_contract_path, candidate_identity_set_path=identity_path)
    staged = stage_candidate_from_execution_intent_v1(repo_root=REPO_ROOT, execution_intent=execution_intent)
    result = run_execution_build_authority_v1(
        repo_root=REPO_ROOT,
        operation_type=execution_intent.operation_type,
        candidate_path=Path(staged["candidate_path"]).resolve(),
        materialize=True,
        emit_package=True,
    )
    build_obj = result.get("build_obj") if isinstance(result.get("build_obj"), dict) else {}
    if str(build_obj.get("closure_status") or "").strip().upper() != "COMPLETE":
        first = build_obj.get("first_real_blocker") if isinstance(build_obj.get("first_real_blocker"), dict) else {}
        build_path = str(result.get("build_path") or "")
        return _fail_payload(
            day_utc=day_utc,
            intent_id=intent_id,
            truth_root=truth_root,
            blocker=str(first.get("dependency_id") or "EXECUTION_PACKAGE_BUILD_NOT_COMPLETE"),
            details={
                "build_path": build_path,
                "first_real_blocker": first,
                "blocking_chain": list(build_obj.get("blocking_chain") or []),
                "materializable_now": list(build_obj.get("materializable_now") or []),
                "chain_map": _execution_build_chain_map(
                    day_utc=day_utc,
                    truth_root=truth_root,
                    build_obj=build_obj,
                    build_path=build_path,
                    package_path=str(result.get("package_path") or ""),
                ),
            },
        )
    return {
        "status": "PASS",
        "day_utc": day_utc,
        "intent_id": intent_id,
        "truth_root": str(truth_root),
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_execution_package_from_authorized_intent_v1.py",
            "git_sha": _git_sha(),
        },
        "blocker": "",
        "details": {
            "authorization_outcome": outcome,
            "authorized_quantity": qty,
            "intent_hash": intent_hash,
            "risk_contract_path": str(risk_contract_path),
            "risk_contract_id": str(risk_contract.get("contract_id") or ""),
            "chain_map": _execution_build_chain_map(
                day_utc=day_utc,
                truth_root=truth_root,
                build_obj=build_obj,
                build_path=str(result.get("build_path") or ""),
                package_path=str(result.get("package_path") or ""),
            ),
        },
        "package_path": str(result["package_path"]),
        "build_path": str(result["build_path"]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_execution_package_from_authorized_intent_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--intent_id", required=True)
    args = parser.parse_args(argv)
    result = build_execution_package_from_authorized_intent_v1(
        day_utc=str(args.day_utc).strip(),
        truth_root=Path(str(args.truth_root).strip()).expanduser().resolve(),
        intent_id=str(args.intent_id).strip(),
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
