#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_day_lifecycle_v1 import write_lifecycle_transition_v1
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_path,
    resolve_paper_capital_seed_path,
)
from ops.tools import run_aegis_bod_prepare_v1 as bod


def _read_json(path: Path) -> dict[str, Any]:
    return bod._read_json(path)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    bod._write_json(path, payload)


def _manifest_path(ctx: bod.BodContext) -> Path:
    return (
        ctx.truth_root
        / "reports"
        / "aegis_pre_open_verify_v1"
        / ctx.day_utc
        / "aegis_pre_open_verify.v1.json"
    ).resolve()


def _blocker_from_codes(payload: dict[str, Any], fallback: str) -> str:
    for key in ("canonical_blocker", "first_blocker", "first_blocker_code", "reason_code"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    codes = payload.get("blocking_reason_codes") or payload.get("reason_codes") or payload.get("blocker_chain")
    if isinstance(codes, list) and codes:
        return str(codes[0]).strip() or fallback
    return fallback


def evaluate_pre_open_verify_v1(ctx: bod.BodContext) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def add_check(name: str, path: Path, ok: bool, blocker: str) -> None:
        checks.append(
            {
                "name": name,
                "status": "PASS" if ok else "BLOCKED",
                "path": str(path),
                "blocker": "" if ok else blocker,
            }
        )

    session_path = ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json"
    session = _read_json(session_path)
    session_status = str(session.get("authority_status") or session.get("status") or "").strip().upper()
    session_ok = session_status == "GRANTED" and session.get("submission_authorized") is True
    add_check("session_authority", session_path, session_ok, _blocker_from_codes(session, "SESSION_AUTHORITY_DENIED" if session else "SESSION_AUTHORITY_MISSING"))

    pre_open_path = ctx.truth_root / "reports" / "pre_open_bundle_v1" / ctx.day_utc / "pre_open_bundle.v1.json"
    pre_open = _read_json(pre_open_path)
    pre_open_state = str(pre_open.get("materialization_state") or pre_open.get("bundle_state") or pre_open.get("status") or "").strip().upper()
    pre_open_ok = pre_open_state in {"COMPLETE", "READY", "PASS"}
    pre_open_blocker = _blocker_from_codes(pre_open, "PRE_OPEN_BUNDLE_INCOMPLETE" if pre_open else "PRE_OPEN_BUNDLE_MISSING")
    add_check("pre_open_bundle", pre_open_path, pre_open_ok, pre_open_blocker)
    ib_ok = pre_open_ok and "IB_API_HANDSHAKE_NOT_OK" not in json.dumps(pre_open, sort_keys=True)
    add_check("ib_connection", pre_open_path, ib_ok, "IB_API_HANDSHAKE_NOT_OK")

    seed_path = resolve_paper_capital_seed_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    add_check("paper_capital_seed", seed_path, seed_path.exists(), "PAPER_CAPITAL_SEED_MISSING")

    statement_path = resolve_operator_statement_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    add_check("operator_statement", statement_path, statement_path.exists(), "OPERATOR_STATEMENT_MISSING")

    execution_path = ctx.truth_root / "reports" / "execution_mode_authority_v1" / ctx.day_utc / "execution_mode_authority.v1.json"
    execution = _read_json(execution_path)
    execution_state = str(execution.get("mode_state") or "").strip().upper()
    execution_ok = execution_state in {"DRY_RUN_LOCKED", "PAPER_READY_NOT_TRANSMITTED", "PAPER_TRANSMIT_ENABLED", "SUBMIT_BLOCKED"}
    add_check("execution_mode", execution_path, execution_ok, "EXECUTION_MODE_UNKNOWN")

    runtime_path = ctx.truth_root / "reports" / "runtime_service_authority_v1" / ctx.day_utc / "runtime_service_authority.v1.json"
    runtime = _read_json(runtime_path)
    runtime_state = str(runtime.get("service_state") or runtime.get("status") or "").strip().upper()
    runtime_ok = runtime_state not in {"", "UNKNOWN", "MISSING_REQUIRED_SERVICE"}
    add_check("runtime_services", runtime_path, runtime_ok, "MISSING_REQUIRED_SERVICE")

    first_blocked = next((row for row in checks if row["status"] == "BLOCKED"), {})
    status = "PRE_OPEN_READY" if not first_blocked else "PRE_OPEN_BLOCKED"
    blocker = str(first_blocked.get("blocker") or "")
    return {
        "schema_id": "aegis_pre_open_verify",
        "schema_version": "v1",
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "status": status,
        "canonical_blocker": blocker,
        "checks": checks,
        "truth_root": str(ctx.truth_root),
        "execution_root": str(ctx.execution_root),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_pre_open_verify_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    ctx = bod._resolve_context(day_utc, str(args.environment).strip().upper(), args.truth_root)
    payload = evaluate_pre_open_verify_v1(ctx)
    path = _manifest_path(ctx)
    payload["path"] = str(path)
    lifecycle_path = write_lifecycle_transition_v1(
        truth_root=ctx.truth_root,
        day_utc=ctx.day_utc,
        state=str(payload["status"]),
        producer="ops/tools/run_aegis_pre_open_verify_v1.py",
        blocker=str(payload.get("canonical_blocker") or ""),
        reason="Pre-open verification complete",
        evidence_paths=[str(path)],
    )
    payload["lifecycle_path"] = str(lifecycle_path)
    _write_json(path, payload)
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": str(path)}, sort_keys=True))
    return 0 if payload["status"] == "PRE_OPEN_READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
