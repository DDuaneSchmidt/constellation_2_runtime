#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod

SCHEMA_VERSION = "authorization_supply.v1"
ALLOWED_BLOCKERS = {
    "ACTIVE_INTENT_MISSING",
    "MARKET_DATA_SUPPLY_BLOCKED",
    "RISK_BUDGET_SUPPLY_BLOCKED",
    "STRATEGY_DECISION_MISSING",
    "STRUCTURE_DECISION_MISSING",
    "MARKET_OPEN_DATA_MISSING",
    "OPTIONS_SNAPSHOT_MISSING",
    "NO_ELIGIBLE_OPTION_STRUCTURE",
    "STRUCTURE_POLICY_MISSING",
    "STRUCTURE_DECISION_VALIDATION_FAILED",
    "PHASEC_INPUT_MARKET_DATA_MISSING",
    "PHASEC_INPUT_RISK_BUDGET_MISSING",
    "PHASEC_EXECUTION_IDENTITY_MISSING",
    "PHASEC_DEFINED_RISK_NOT_PROVEN",
    "AUTHORIZATION_EVIDENCE_MISSING",
    "AUTHORIZATION_REJECTED",
    "AUTHORIZED_INTENTS_EMPTY",
}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def authorization_supply_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "authorization_supply_v1" / day_utc / "authorization_supply.v1.json").resolve()


def _market_data_supply_path(ctx: bod.BodContext) -> Path:
    return (ctx.truth_root / "reports" / "market_data_supply_v1" / ctx.day_utc / "market_data_supply.v1.json").resolve()


def _risk_budget_supply_path(ctx: bod.BodContext) -> Path:
    return (ctx.truth_root / "reports" / "risk_budget_supply_v1" / ctx.day_utc / "risk_budget_supply.v1.json").resolve()


def _strategy_decision_path(ctx: bod.BodContext) -> Path:
    return (ctx.truth_root / "reports" / "strategy_decision_authority_v1" / ctx.day_utc / "strategy_decision_authority.v1.json").resolve()


def _structure_decision_supply_path(ctx: bod.BodContext) -> Path:
    return (ctx.truth_root / "reports" / "structure_decision_supply_v1" / ctx.day_utc / "structure_decision_supply.v1.json").resolve()


def _json_files(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted(path.resolve() for path in root.glob("*.json") if path.is_file())


def _intent_hash(path: Path, payload: dict[str, Any]) -> str:
    return str(payload.get("intent_hash") or payload.get("canonical_json_hash") or path.name.split(".", 1)[0]).strip()


def _intent_id(path: Path, payload: dict[str, Any]) -> str:
    return str(payload.get("intent_id") or _intent_hash(path, payload)).strip()


def _instrument(payload: dict[str, Any]) -> str:
    underlying = payload.get("underlying") if isinstance(payload.get("underlying"), dict) else {}
    return str(payload.get("instrument") or payload.get("symbol") or underlying.get("symbol") or "").strip()


def _intent_type(payload: dict[str, Any]) -> str:
    engine = payload.get("engine") if isinstance(payload.get("engine"), dict) else {}
    return str(payload.get("intent_type") or payload.get("exposure_type") or payload.get("risk_class") or engine.get("engine_id") or "").strip()


def _requires_defined_risk(payload: dict[str, Any]) -> bool:
    if payload.get("requires_defined_risk") is True:
        return True
    text = json.dumps(payload, sort_keys=True).upper()
    return any(token in text for token in ("DEFINED_RISK", "VERTICAL_SPREAD", "OPTION", "OPTIONS", "SHORT_VOL"))


def _has_structure_decision(payload: dict[str, Any]) -> bool:
    if isinstance(payload.get("structure_decision"), dict):
        return True
    if isinstance(payload.get("structure"), dict) or str(payload.get("structure") or "").strip():
        return True
    if isinstance(payload.get("option_structure"), dict) or str(payload.get("option_structure") or "").strip():
        return True
    if isinstance(payload.get("selected_structure"), dict) or str(payload.get("selected_structure") or "").strip():
        return True
    legs = payload.get("legs")
    return isinstance(legs, list) and len(legs) > 0


def _active_intents(ctx: bod.BodContext) -> list[tuple[Path, dict[str, Any]]]:
    roots = [ctx.execution_root, ctx.truth_root] if ctx.execution_root != ctx.truth_root else [ctx.truth_root]
    seen: set[str] = set()
    rows: list[tuple[Path, dict[str, Any]]] = []
    for root in roots:
        for path in _json_files(root / "intents_v1" / "snapshots" / ctx.day_utc):
            payload = _read_json(path)
            if not payload or str(payload.get("day_utc") or ctx.day_utc).strip() != ctx.day_utc:
                continue
            key = _intent_hash(path, payload)
            if key in seen:
                continue
            seen.add(key)
            rows.append((path, payload))
    return rows


def _active_intent_rows(ctx: bod.BodContext) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path, payload in _active_intents(ctx):
        rows.append(
            {
                "intent_id": _intent_id(path, payload),
                "instrument": _instrument(payload),
                "intent_type": _intent_type(payload),
                "requires_defined_risk": _requires_defined_risk(payload),
                "intent_path": str(path),
                "intent_hash": _intent_hash(path, payload),
                "status": "PRESENT",
                "structure_present": _has_structure_decision(payload),
            }
        )
    return rows


def _supply_input(path: Path, day_utc: str, missing_blocker: str) -> tuple[dict[str, Any], str]:
    payload = _read_json(path)
    if not payload:
        return {"status": "MISSING", "path": str(path), "canonical_blocker": missing_blocker}, missing_blocker
    if str(payload.get("day_utc") or "").strip() != day_utc:
        return {"status": "WRONG_DAY", "path": str(path), "canonical_blocker": missing_blocker}, missing_blocker
    status = str(payload.get("status") or "").strip().upper()
    row = {
        "status": status,
        "path": str(path),
        "canonical_blocker": str(payload.get("canonical_blocker") or "").strip(),
        "operator_next_action": str(payload.get("operator_next_action") or "").strip(),
    }
    if status == "PASS":
        return row, ""
    return row, row["canonical_blocker"] or missing_blocker


def _run_command(cmd: list[str], *, timeout_seconds: int = 120) -> dict[str, Any]:
    started = _now_iso()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
        return {
            "command": " ".join(cmd),
            "started_at_utc": started,
            "completed_at_utc": _now_iso(),
            "exit_code": proc.returncode,
            "stdout_summary": (proc.stdout or "").strip().splitlines()[-1:] or [],
            "stderr_summary": (proc.stderr or "").strip().splitlines()[-3:] or [],
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "command": " ".join(cmd),
            "started_at_utc": started,
            "completed_at_utc": _now_iso(),
            "exit_code": 124,
            "stdout_summary": [],
            "stderr_summary": [f"TIMEOUT:{exc}"],
        }


def _ensure_strategy_decision(ctx: bod.BodContext) -> tuple[dict[str, Any], str]:
    path = _strategy_decision_path(ctx)
    if not path.exists():
        _run_command(
            [
                sys.executable,
                "ops/tools/run_strategy_decision_authority_v1.py",
                "--day_utc",
                ctx.day_utc,
                "--truth_root",
                str(ctx.truth_root),
                "--execution_root",
                str(ctx.execution_root),
            ]
        )
    payload = _read_json(path)
    state = str(payload.get("strategy_decision_state") or payload.get("status") or "").strip().upper()
    intent_count = payload.get("intent_count")
    passed = state in {"INTENT_CREATED", "READY", "VALID", "PASS"} or (isinstance(intent_count, int) and intent_count > 0)
    return (
        {
            "status": "PASS" if passed else "BLOCKED",
            "path": str(path),
            "strategy_decision_state": state or "MISSING",
            "intent_count": intent_count if isinstance(intent_count, int) else None,
        },
        "" if passed else "STRATEGY_DECISION_MISSING",
    )


def _run_structure_decision_supply(ctx: bod.BodContext) -> dict[str, Any]:
    return _run_command(
        [
            sys.executable,
            "ops/tools/run_structure_decision_supply_v1.py",
            "--day_utc",
            ctx.day_utc,
            "--environment",
            ctx.environment,
        ]
    )


def _structure_decision(ctx: bod.BodContext, active_intents: list[dict[str, Any]]) -> tuple[dict[str, Any], str]:
    required_ids = [row["intent_id"] for row in active_intents if row.get("requires_defined_risk") is True]
    supply_path = _structure_decision_supply_path(ctx)
    supply = _read_json(supply_path)
    if str(supply.get("day_utc") or "").strip() != ctx.day_utc:
        supply = {}
    export = supply.get("structure_export") if isinstance(supply.get("structure_export"), dict) else {}
    supplied = {
        str(row.get("intent_id") or "").strip()
        for row in export.get("decisions") or []
        if isinstance(row, dict)
    } if export.get("usable_for_authorization_supply") is True else set()
    missing = [
        row["intent_id"]
        for row in active_intents
        if row.get("requires_defined_risk") is True
        and row.get("structure_present") is not True
        and row["intent_id"] not in supplied
    ]
    if missing and not supply:
        _run_structure_decision_supply(ctx)
        supply = _read_json(supply_path)
        export = supply.get("structure_export") if isinstance(supply.get("structure_export"), dict) else {}
        supplied = {
            str(row.get("intent_id") or "").strip()
            for row in export.get("decisions") or []
            if isinstance(row, dict)
        } if export.get("usable_for_authorization_supply") is True else set()
        missing = [
            row["intent_id"]
            for row in active_intents
            if row.get("requires_defined_risk") is True
            and row.get("structure_present") is not True
            and row["intent_id"] not in supplied
        ]
    status = str(supply.get("status") or "").strip().upper()
    supply_blocker = str(supply.get("canonical_blocker") or "").strip()
    if status == "BLOCKED" and supply_blocker:
        return (
            {
                "status": "BLOCKED",
                "required": bool(required_ids),
                "missing_intent_ids": missing or required_ids,
                "structure_decision_supply_path": str(supply_path),
                "structure_decision_supply_status": status,
                "structure_decision_supply_blocker": supply_blocker,
                "producer_command": f"python3 ops/tools/run_structure_decision_supply_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
            },
            supply_blocker,
        )
    return (
        {
            "status": "BLOCKED" if missing else "PASS",
            "required": bool(required_ids),
            "missing_intent_ids": missing,
            "structure_decision_supply_path": str(supply_path),
            "structure_decision_supply_status": status or ("MISSING" if required_ids else "NOT_REQUIRED"),
            "structure_decision_count": int(export.get("decision_count") or 0) if isinstance(export, dict) else 0,
            "producer_command": f"python3 ops/tools/run_structure_decision_supply_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
        },
        "STRUCTURE_DECISION_MISSING" if missing else "",
    )


def _run_phasec_identity_materializer(ctx: bod.BodContext) -> dict[str, Any]:
    return _run_command(
        [
            sys.executable,
            "ops/tools/run_phasec_identity_materializer_day_v1.py",
            "--day_utc",
            ctx.day_utc,
            "--eval_time_utc",
            f"{ctx.day_utc}T00:00:00Z",
            "--truth_root",
            str(ctx.truth_root),
            "--execution_truth_root",
            str(ctx.execution_root),
        ]
    )


def _identity_paths(ctx: bod.BodContext) -> list[Path]:
    root = ctx.execution_root / "phaseC_preflight_v1" / ctx.day_utc
    if not root.exists() or not root.is_dir():
        return []
    return sorted(path.resolve() for path in root.glob("attempt_*/*/execution_identity_record.v1.json") if path.is_file())


def _identity_for_intent(ctx: bod.BodContext, intent: dict[str, Any]) -> tuple[Path | None, dict[str, Any]]:
    intent_id = str(intent.get("intent_id") or "").strip()
    intent_hash = str(intent.get("intent_hash") or "").strip()
    for path in reversed(_identity_paths(ctx)):
        payload = _read_json(path)
        if str(payload.get("day_utc") or "").strip() != ctx.day_utc:
            continue
        if intent_hash and intent_hash in path.parts:
            return path, payload
        if intent_id and str(payload.get("intent_id") or "").strip() == intent_id:
            return path, payload
    return None, {}


def _defined_risk_proven(payload: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    risk_proof = payload.get("risk_proof") if isinstance(payload.get("risk_proof"), dict) else {}
    nested = payload.get("defined_risk_proof") if isinstance(payload.get("defined_risk_proof"), dict) else {}
    if risk_proof.get("defined_risk_proven") is True:
        return True, risk_proof
    if nested.get("defined_risk_proven") is True:
        return True, nested
    if payload.get("defined_risk_proven") is True:
        return True, {"defined_risk_proven": True}
    return False, risk_proof or nested


def _phasec_defined_risk(ctx: bod.BodContext, active_intents: list[dict[str, Any]]) -> tuple[dict[str, Any], str]:
    if not _identity_paths(ctx):
        _run_phasec_identity_materializer(ctx)
    identities: list[dict[str, Any]] = []
    for intent in active_intents:
        if intent.get("requires_defined_risk") is not True:
            continue
        path, payload = _identity_for_intent(ctx, intent)
        if not path or not payload:
            return (
                {
                    "status": "BLOCKED",
                    "execution_identity_record_path": "",
                    "defined_risk_proven": False,
                    "risk_proof": {},
                    "blocker": "PHASEC_EXECUTION_IDENTITY_MISSING",
                    "producer_command": "python3 ops/tools/run_phasec_identity_materializer_day_v1.py",
                },
                "PHASEC_EXECUTION_IDENTITY_MISSING",
            )
        proven, proof = _defined_risk_proven(payload)
        identities.append({"intent_id": intent.get("intent_id"), "path": str(path), "defined_risk_proven": proven})
        if not proven:
            return (
                {
                    "status": "BLOCKED",
                    "execution_identity_record_path": str(path),
                    "defined_risk_proven": False,
                    "risk_proof": proof,
                    "blocker": "PHASEC_DEFINED_RISK_NOT_PROVEN",
                    "producer_command": "python3 ops/tools/run_phasec_identity_materializer_day_v1.py",
                },
                "PHASEC_DEFINED_RISK_NOT_PROVEN",
            )
    return (
        {
            "status": "PASS",
            "execution_identity_record_path": str(identities[0]["path"]) if identities else "",
            "defined_risk_proven": True,
            "risk_proof": {"identity_count": len(identities), "identities": identities},
            "blocker": "",
            "producer_command": "python3 ops/tools/run_phasec_identity_materializer_day_v1.py",
        },
        "",
    )


def _authorization_root(ctx: bod.BodContext) -> Path:
    return (ctx.execution_root / "engine_activity_v1" / "authorization_v1" / ctx.day_utc).resolve()


def _run_authorization_artifacts(ctx: bod.BodContext) -> dict[str, Any]:
    return _run_command([sys.executable, "ops/tools/run_authorization_artifacts_day_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.execution_root)])


def _authorization_evidence(ctx: bod.BodContext, active_intents: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any], str]:
    root = _authorization_root(ctx)
    if not root.exists() or not any(root.glob("*.authorization.v1.json")):
        _run_authorization_artifacts(ctx)
    files = sorted(root.glob("*.authorization.v1.json")) if root.exists() else []
    by_intent: dict[str, tuple[Path, dict[str, Any]]] = {}
    rejected: list[dict[str, Any]] = []
    for path in files:
        payload = _read_json(path)
        if str(payload.get("day_utc") or "").strip() != ctx.day_utc:
            continue
        intent_id = str(payload.get("intent_id") or "").strip()
        if not intent_id:
            continue
        status = str(payload.get("status") or "").strip().upper()
        decision = str((payload.get("authorization") if isinstance(payload.get("authorization"), dict) else {}).get("decision") or "").strip().upper()
        by_intent[intent_id] = (path, payload)
        if status == "REJECTED" or decision == "REJECTED":
            rejected.append({"intent_id": intent_id, "path": str(path), "reason_codes": payload.get("reason_codes") or []})
    if rejected:
        return (
            {"status": "BLOCKED", "path": str(root), "rejected": rejected, "authorized_count": 0},
            {"usable_for_submit_readiness": False, "authorized_intents": []},
            "AUTHORIZATION_REJECTED",
        )
    authorized: list[dict[str, Any]] = []
    for intent in active_intents:
        intent_id = str(intent.get("intent_id") or "").strip()
        item = by_intent.get(intent_id)
        if not item:
            continue
        path, payload = item
        status = str(payload.get("status") or "").strip().upper()
        decision = str((payload.get("authorization") if isinstance(payload.get("authorization"), dict) else {}).get("decision") or "").strip().upper()
        if status == "AUTHORIZED" or decision == "AUTHORIZED":
            authorized.append(
                {
                    "intent_id": intent_id,
                    "instrument": str(intent.get("instrument") or ""),
                    "authorization_path": str(path),
                    "defined_risk_proven": True,
                    "risk_budget_available": True,
                    "market_data_available": True,
                }
            )
    if not by_intent:
        return (
            {"status": "BLOCKED", "path": str(root), "rejected": [], "authorized_count": 0},
            {"usable_for_submit_readiness": False, "authorized_intents": []},
            "AUTHORIZATION_EVIDENCE_MISSING",
        )
    if not authorized:
        return (
            {"status": "BLOCKED", "path": str(root), "rejected": [], "authorized_count": 0},
            {"usable_for_submit_readiness": False, "authorized_intents": []},
            "AUTHORIZED_INTENTS_EMPTY",
        )
    return (
        {"status": "PASS", "path": str(root), "rejected": [], "authorized_count": len(authorized)},
        {"usable_for_submit_readiness": True, "authorized_intents": authorized},
        "",
    )


def _blocked_payload(ctx: bod.BodContext, *, active_intents: list[dict[str, Any]], blocker: str, action: str, **sections: Any) -> dict[str, Any]:
    if blocker not in ALLOWED_BLOCKERS:
        blocker = "AUTHORIZATION_EVIDENCE_MISSING"
    return {
        "schema_id": "authorization_supply",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "status": "BLOCKED",
        "canonical_blocker": blocker,
        "active_intents": active_intents,
        "market_data_input": sections.get("market_data_input", {}),
        "risk_budget_input": sections.get("risk_budget_input", {}),
        "strategy_decision": sections.get("strategy_decision", {}),
        "structure_decision": sections.get("structure_decision", {}),
        "phasec_defined_risk": sections.get("phasec_defined_risk", {"status": "SKIPPED", "blocker": blocker}),
        "authorization": sections.get("authorization", {"status": "SKIPPED"}),
        "authorization_export": sections.get("authorization_export", {"usable_for_submit_readiness": False, "authorized_intents": []}),
        "operator_next_action": action,
        "generated_at_utc": _now_iso(),
    }


def build_authorization_supply_v1(ctx: bod.BodContext) -> dict[str, Any]:
    active_intents = _active_intent_rows(ctx)
    if not active_intents:
        return _blocked_payload(
            ctx,
            active_intents=[],
            blocker="ACTIVE_INTENT_MISSING",
            action=f"Generate or provide current-day active intents under {ctx.execution_root / 'intents_v1' / 'snapshots' / ctx.day_utc}.",
        )

    market_data_input, market_blocker = _supply_input(_market_data_supply_path(ctx), ctx.day_utc, "PHASEC_INPUT_MARKET_DATA_MISSING")
    if market_blocker:
        return _blocked_payload(
            ctx,
            active_intents=active_intents,
            blocker="MARKET_DATA_SUPPLY_BLOCKED",
            action=market_data_input.get("operator_next_action") or "Resolve Market Data Supply before running PhaseC authorization.",
            market_data_input=market_data_input,
        )

    risk_budget_input, risk_blocker = _supply_input(_risk_budget_supply_path(ctx), ctx.day_utc, "PHASEC_INPUT_RISK_BUDGET_MISSING")
    if risk_blocker:
        return _blocked_payload(
            ctx,
            active_intents=active_intents,
            blocker="RISK_BUDGET_SUPPLY_BLOCKED",
            action=risk_budget_input.get("operator_next_action") or "Resolve Risk Budget Supply before running PhaseC authorization.",
            market_data_input=market_data_input,
            risk_budget_input=risk_budget_input,
        )

    strategy_decision, strategy_blocker = _ensure_strategy_decision(ctx)
    if strategy_blocker:
        return _blocked_payload(
            ctx,
            active_intents=active_intents,
            blocker=strategy_blocker,
            action=f"Run python3 ops/tools/run_strategy_decision_authority_v1.py --day_utc {ctx.day_utc} --truth_root {ctx.truth_root} --execution_root {ctx.execution_root}.",
            market_data_input=market_data_input,
            risk_budget_input=risk_budget_input,
            strategy_decision=strategy_decision,
        )

    structure_decision, structure_blocker = _structure_decision(ctx, active_intents)
    if structure_blocker:
        return _blocked_payload(
            ctx,
            active_intents=active_intents,
            blocker=structure_blocker,
            action=(
                "Materialize structure decision/legs for each active defined-risk intent before PhaseC."
                if structure_blocker == "STRUCTURE_DECISION_MISSING"
                else f"Resolve {structure_blocker} from Structure Decision Supply before PhaseC."
            ),
            market_data_input=market_data_input,
            risk_budget_input=risk_budget_input,
            strategy_decision=strategy_decision,
            structure_decision=structure_decision,
        )

    phasec_defined_risk, phasec_blocker = _phasec_defined_risk(ctx, active_intents)
    if phasec_blocker:
        return _blocked_payload(
            ctx,
            active_intents=active_intents,
            blocker=phasec_blocker,
            action=f"Run python3 ops/tools/run_phasec_identity_materializer_day_v1.py --day_utc {ctx.day_utc} --eval_time_utc {ctx.day_utc}T00:00:00Z --truth_root {ctx.truth_root} --execution_truth_root {ctx.execution_root}.",
            market_data_input=market_data_input,
            risk_budget_input=risk_budget_input,
            strategy_decision=strategy_decision,
            structure_decision=structure_decision,
            phasec_defined_risk=phasec_defined_risk,
        )

    authorization, export, auth_blocker = _authorization_evidence(ctx, active_intents)
    if auth_blocker:
        return _blocked_payload(
            ctx,
            active_intents=active_intents,
            blocker=auth_blocker,
            action=f"Run python3 ops/tools/run_authorization_artifacts_day_v1.py --day_utc {ctx.day_utc} --truth_root {ctx.execution_root}.",
            market_data_input=market_data_input,
            risk_budget_input=risk_budget_input,
            strategy_decision=strategy_decision,
            structure_decision=structure_decision,
            phasec_defined_risk=phasec_defined_risk,
            authorization=authorization,
            authorization_export=export,
        )

    return {
        "schema_id": "authorization_supply",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "status": "PASS",
        "canonical_blocker": "",
        "active_intents": active_intents,
        "market_data_input": market_data_input,
        "risk_budget_input": risk_budget_input,
        "strategy_decision": strategy_decision,
        "structure_decision": structure_decision,
        "phasec_defined_risk": phasec_defined_risk,
        "authorization": authorization,
        "authorization_export": export,
        "operator_next_action": "Authorization Supply passed; continue to risk sizing and submit readiness.",
        "generated_at_utc": _now_iso(),
    }


def run_authorization_supply_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_authorization_supply_v1(ctx)
    path = authorization_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_authorization_supply_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment).strip().upper()
    path, payload = run_authorization_supply_v1(day_utc, environment, str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": str(path)}, sort_keys=True))
    return 0 if payload.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
