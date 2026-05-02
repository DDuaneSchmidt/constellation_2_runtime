#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, timedelta
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.gate_authority_foundation_v1 import (  # noqa: E402
    write_gate_authority_plane,
)
from constellation_2.common.runtime_authority_bridge_v1 import resolve_truth_root_bridge_v1  # noqa: E402
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root  # noqa: E402
from constellation_2.common.diagnostic_foundation_v1 import (  # noqa: E402
    _require_day_utc,
    _require_produced_utc,
)
from constellation_2.common.truth_lifecycle_orchestrator_v1 import (  # noqa: E402
    LifecycleRunContextV1,
    PhaseRunResultV1,
    run_truth_lifecycle_orchestrator_v1,
)
from ops.tools.c2_account_resolution_v1 import (  # noqa: E402
    resolve_single_paper_ib_account_from_sleeve_registry,
)

GATE_STACK_VERDICT_TOOL = (REPO_ROOT / "ops/tools/run_gate_stack_verdict_v1.py").resolve()
CORRELATION_ENVELOPE_GATE_TOOL = (REPO_ROOT / "ops/tools/run_correlation_envelope_gate_v1.py").resolve()
CAPITAL_RISK_ENVELOPE_GATE_TOOL = (REPO_ROOT / "ops/tools/run_c2_capital_risk_envelope_gate_v2.py").resolve()
ENGINE_CORRELATION_MATRIX_TOOL = (
    REPO_ROOT / "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py"
).resolve()
FEED_ATTESTATION_GATE_TOOL = (REPO_ROOT / "ops/tools/run_feed_attestation_gate_v1.py").resolve()
HEARTBEAT_GATE_TOOL = (REPO_ROOT / "ops/tools/run_heartbeat_gate_v1.py").resolve()
REPLAY_CERTIFICATION_GATE_TOOL = (REPO_ROOT / "ops/tools/run_replay_certification_gate_v1.py").resolve()
LIQUIDITY_SLIPPAGE_GATE_TOOL = (REPO_ROOT / "ops/tools/run_liquidity_slippage_gate_v1.py").resolve()
OPERATOR_DAILY_GATE_TOOL = (REPO_ROOT / "ops/tools/run_operator_daily_gate_v3.py").resolve()
RECONCILIATION_REPORT_TOOL = (REPO_ROOT / "ops/tools/run_reconciliation_report_v3.py").resolve()
EXIT_RECONCILIATION_TOOL = (
    REPO_ROOT / "constellation_2/phaseI/exit_reconciliation/run/run_exit_reconciliation_day_v1.py"
).resolve()
EXECUTION_RECONCILIATION_TOOL = (REPO_ROOT / "ops/tools/run_execution_reconciliation_day_v1.py").resolve()
FILL_LEDGER_TOOL = (REPO_ROOT / "ops/tools/run_fill_ledger_day_v1.py").resolve()
PAPER_SESSION_LEDGER_TOOL = (REPO_ROOT / "ops/tools/run_paper_session_ledger_v1.py").resolve()
SLEEVE_INTENT_TRADE_ATTRIBUTION_TOOL = (
    REPO_ROOT / "ops/tools/run_sleeve_intent_trade_attribution_v1.py"
).resolve()
DAY_ACTIVATION_AUTHORITY_TOOL = (REPO_ROOT / "ops/tools/run_day_activation_authority_v1.py").resolve()
GLOBAL_CONTEXT_AUTHORITY_TOOL = (REPO_ROOT / "ops/tools/run_global_context_authority_v1.py").resolve()
ECONOMIC_STATE_AUTHORITY_TOOL = (REPO_ROOT / "ops/tools/run_economic_state_authority_v1.py").resolve()
ENGINE_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()


def _truth_report_path(*, truth_root: Path, family: str, day_utc: str, filename: str) -> str:
    return str((truth_root / "reports" / family / day_utc / filename).resolve())


def _invocation(
    *,
    phase_id: str,
    producer_id: str,
    entrypoint: Path | str,
    status: str,
    artifact_paths: list[str] | None = None,
    detail: str = "",
) -> dict[str, Any]:
    return {
        "phase_id": phase_id,
        "producer_id": producer_id,
        "entrypoint": str(entrypoint),
        "status": status,
        "artifact_paths": list(artifact_paths or []),
        "detail": str(detail or ""),
    }


def _fail_reason_code(exc: BaseException) -> str:
    text = str(exc).strip()
    if text.startswith("FAIL:"):
        detail = text[5:].strip()
        code = detail.split(":", 1)[0].strip()
        if code:
            return code
    return "PHASE_PRODUCER_FAILED"


def _prev_day_utc(day_utc: str) -> str:
    return (date.fromisoformat(day_utc) - timedelta(days=1)).isoformat()


def _find_previous_day_complete_economic_state_build(*, truth_root: Path, day_utc: str) -> tuple[bool, str, str]:
    prev_day = _prev_day_utc(day_utc)
    # economic_state_build_v1 is a canonical-truth artifact. The gate authority
    # may be running against an execution truth root, so searching that root
    # would make governed economic builds look missing.
    _ = truth_root
    build_root = (resolve_canonical_truth_root().resolve() / "reports" / "economic_state_build_v1" / prev_day).resolve()
    if not build_root.exists() or not build_root.is_dir():
        return False, "", "PREVIOUS_DAY_ECONOMIC_STATE_BUILD_MISSING"
    candidates = sorted(build_root.glob("*/economic_state_build.v1.json"))
    if not candidates:
        return False, "", "PREVIOUS_DAY_ECONOMIC_STATE_BUILD_MISSING"
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("closure_status") or "").strip().upper() == "COMPLETE":
            return True, str(path.resolve()), ""
    return False, str(candidates[0].resolve()), "PREVIOUS_DAY_ECONOMIC_STATE_BUILD_NOT_COMPLETE"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _active_engine_rows() -> list[dict[str, str]]:
    payload = json.loads(ENGINE_REGISTRY_PATH.read_text(encoding="utf-8"))
    engines = payload.get("engines") if isinstance(payload, dict) else None
    if not isinstance(engines, list):
        raise SystemExit(f"FAIL: ENGINE_REGISTRY_INVALID: engines_not_list path={ENGINE_REGISTRY_PATH}")
    rows: list[dict[str, str]] = []
    for row in engines:
        if not isinstance(row, dict):
            continue
        if str(row.get("activation_status") or "").strip().upper() != "ACTIVE":
            continue
        engine_id = str(row.get("engine_id") or "").strip()
        if not engine_id:
            raise SystemExit("FAIL: ENGINE_REGISTRY_ACTIVE_ENGINE_ID_MISSING")
        rows.append(
            {
                "engine_id": engine_id,
                "engine_runner_path": str(row.get("engine_runner_path") or "").strip(),
                "engine_runner_sha256": str(row.get("engine_runner_sha256") or "").strip(),
            }
        )
    rows.sort(key=lambda item: item["engine_id"])
    return rows


def _resolve_truth_root(raw: str) -> Path:
    text = str(raw or "").strip()
    truth_root = (
        Path(text).expanduser().resolve()
        if text
        else resolve_truth_root_bridge_v1(
            repo_root=REPO_ROOT,
            caller="ops/tools/run_gate_authority_plane_v1.py",
        )
    )
    if not truth_root.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {truth_root}")
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not directory: {truth_root}")
    return truth_root


def _refresh_gate_stack_verdict(*, day_utc: str, truth_root: Path, produced_utc: str, mode: str) -> None:
    cmd = [
        sys.executable,
        str(GATE_STACK_VERDICT_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
        "--produced_utc",
        produced_utc,
        "--mode",
        mode,
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    successful_write = "OK: gate_stack_verdict_v1:" in stdout
    if proc.returncode not in (0, 1) or not successful_write:
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: GATE_STACK_VERDICT_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _refresh_correlation_envelope_gate(*, day_utc: str, truth_root: Path, produced_utc: str, mode: str) -> None:
    cmd = [
        sys.executable,
        str(CORRELATION_ENVELOPE_GATE_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
        "--produced_utc",
        produced_utc,
        "--mode",
        mode,
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        stderr = str(proc.stderr or "").strip()
        stdout = str(proc.stdout or "").strip()
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: CORRELATION_ENVELOPE_GATE_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _refresh_capital_risk_envelope(*, day_utc: str, truth_root: Path, produced_utc: str) -> None:
    cmd = [
        sys.executable,
        str(CAPITAL_RISK_ENVELOPE_GATE_TOOL),
        "--out_day_utc",
        day_utc,
        "--input_day_utc",
        day_utc,
        "--produced_utc",
        produced_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    successful_write = "CAPITAL_RISK_ENVELOPE_V2_WRITTEN" in stdout
    if proc.returncode not in (0, 2) or not successful_write:
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: CAPITAL_RISK_ENVELOPE_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _refresh_engine_correlation_matrix(*, day_utc: str, truth_root: Path) -> None:
    cmd = [
        sys.executable,
        str(ENGINE_CORRELATION_MATRIX_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    successful_write = "OK: ENGINE_CORRELATION_MATRIX_V1_WRITTEN" in stdout
    if proc.returncode not in (0, 2) or not successful_write:
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: ENGINE_CORRELATION_MATRIX_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _refresh_feed_attestation_gate(*, day_utc: str, truth_root: Path) -> None:
    cmd = [
        sys.executable,
        str(FEED_ATTESTATION_GATE_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    successful_write = "OK: FEED_ATTESTATION_GATE_V1_WRITTEN" in stdout
    if proc.returncode not in (0, 2) or not successful_write:
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: FEED_ATTESTATION_GATE_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _refresh_heartbeat_gate(*, day_utc: str, truth_root: Path) -> None:
    cmd = [
        sys.executable,
        str(HEARTBEAT_GATE_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    successful_write = "HEARTBEAT_GATE_V1_WRITTEN" in (stdout or stderr)
    if proc.returncode not in (0, 1) or not successful_write:
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: HEARTBEAT_GATE_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _refresh_replay_certification_gate(*, day_utc: str, truth_root: Path, produced_utc: str, mode: str) -> None:
    cmd = [
        sys.executable,
        str(REPLAY_CERTIFICATION_GATE_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
        "--produced_utc",
        produced_utc,
        "--mode",
        mode,
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    gate_path = (
        truth_root
        / "reports"
        / "replay_certification_gate_v1"
        / day_utc
        / "replay_certification_gate.v1.json"
    ).resolve()
    successful_write = gate_path.exists() and gate_path.is_file()
    if proc.returncode != 0 or not successful_write:
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: REPLAY_CERTIFICATION_GATE_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _refresh_liquidity_slippage_gate(*, day_utc: str, truth_root: Path) -> None:
    cmd = [
        sys.executable,
        str(LIQUIDITY_SLIPPAGE_GATE_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    successful_write = "OK: liquidity_slippage_gate_v1" in stdout
    if proc.returncode not in (0, 1) or not successful_write:
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: LIQUIDITY_SLIPPAGE_GATE_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _refresh_operator_daily_gate(*, day_utc: str, truth_root: Path, produced_utc: str, mode: str) -> None:
    cmd = [
        sys.executable,
        str(OPERATOR_DAILY_GATE_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
        "--produced_utc",
        produced_utc,
        "--mode",
        mode,
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    successful_write = "OK: OPERATOR_DAILY_GATE_V3_WRITTEN" in stdout
    if proc.returncode not in (0, 1) or not successful_write:
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: OPERATOR_DAILY_GATE_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _refresh_reconciliation_report(*, day_utc: str, truth_root: Path) -> None:
    cmd = [
        sys.executable,
        str(RECONCILIATION_REPORT_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    successful_write = "OK: RECON_REPORT_V3_WRITTEN" in stdout
    if proc.returncode not in (0, 1) or not successful_write:
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: RECONCILIATION_REPORT_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _refresh_exit_reconciliation(*, day_utc: str, truth_root: Path) -> None:
    cmd = [
        sys.executable,
        str(EXIT_RECONCILIATION_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    path_text = stdout.splitlines()[-1].strip() if stdout else ""
    wrote_path = Path(path_text) if path_text else None
    successful_write = bool(wrote_path and wrote_path.is_absolute() and wrote_path.exists() and wrote_path.is_file())
    if proc.returncode != 0 or not successful_write:
        detail = stderr or stdout or "no_output"
        raise SystemExit(
            "FAIL: EXIT_RECONCILIATION_REFRESH_FAILED "
            f"returncode={proc.returncode} detail={detail}"
        )


def _bootstrap_previous_day_economic_state(*, day_utc: str, mode: str) -> str:
    if str(mode).strip().upper() != "PAPER":
        return ""
    prev_day_utc = (date.fromisoformat(day_utc) - timedelta(days=1)).isoformat()
    paper_ib_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    common_args = [
        "--operation_type",
        "fresh_paper_entry_v1",
        "--day_utc",
        prev_day_utc,
        "--sleeve_id",
        "PRIMARY",
        "--environment",
        "PAPER",
        "--ib_account",
        paper_ib_account,
        "--materialize",
        "YES",
        "--emit_package",
        "YES",
    ]

    def _run_bootstrap_authority(*, tool: Path, stage_name: str) -> dict[str, Any]:
        cmd = [sys.executable, str(tool), *common_args]
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
        stdout = str(proc.stdout or "").strip()
        stderr = str(proc.stderr or "").strip()
        try:
            payload = json.loads(stdout) if stdout else {}
        except Exception:
            payload = {}
        build_path = str(payload.get("build_path") or "").strip()
        closure_status = str(payload.get("closure_status") or "").strip().upper()
        if proc.returncode not in (0, 2) or not build_path:
            detail = stderr or stdout or "no_output"
            raise SystemExit(
                f"FAIL: PREV_DAY_{stage_name}_BOOTSTRAP_FAILED "
                f"day_utc={prev_day_utc} returncode={proc.returncode} detail={detail}"
            )
        if closure_status != "COMPLETE":
            blocker = payload.get("first_real_blocker")
            raise SystemExit(
                f"FAIL: PREV_DAY_{stage_name}_BOOTSTRAP_NOT_COMPLETE "
                f"day_utc={prev_day_utc} build_path={build_path} blocker={blocker}"
            )
        return payload

    _run_bootstrap_authority(tool=DAY_ACTIVATION_AUTHORITY_TOOL, stage_name="DAY_ACTIVATION")
    _run_bootstrap_authority(tool=GLOBAL_CONTEXT_AUTHORITY_TOOL, stage_name="GLOBAL_CONTEXT")
    economic_payload = _run_bootstrap_authority(tool=ECONOMIC_STATE_AUTHORITY_TOOL, stage_name="ECONOMIC_STATE")
    return str(economic_payload.get("build_path") or "").strip()


def _emit_engine_heartbeats(*, day_utc: str, truth_root: Path, produced_utc: str) -> None:
    registry_sha = _sha256_file(ENGINE_REGISTRY_PATH)
    for engine in _active_engine_rows():
        engine_id = engine["engine_id"]
        hb_path = (
            truth_root
            / "monitoring_v1"
            / "engine_heartbeat_v1"
            / day_utc
            / engine_id
            / "engine_heartbeat.v1.json"
        ).resolve()
        if hb_path.exists() and hb_path.is_file():
            continue
        fingerprints = [
            f"engine_registry_v1|governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json|{registry_sha}|true",
        ]
        runner_path = str(engine.get("engine_runner_path") or "").strip()
        runner_sha = str(engine.get("engine_runner_sha256") or "").strip() or ("0" * 64)
        if runner_path:
            fingerprints.append(f"engine_runner_path|{runner_path}|{runner_sha}|true")
        cmd = [
            sys.executable,
            str(REPO_ROOT / "ops/tools/run_engine_heartbeat_emit_v1.py"),
            "--day_utc",
            day_utc,
            "--engine_id",
            engine_id,
            "--status",
            "WARN",
            "--reason_code",
            "ENGINE_DIRECT_RUN_NOT_PRESENT_V2_STRUCTURAL_PREPASS",
            "--last_run_utc",
            produced_utc,
            "--expected_period_seconds",
            "86400",
            "--stale_after_seconds",
            "172800",
            "--truth_root",
            str(truth_root),
            "--producer_repo",
            REPO_ROOT.name,
            "--producer_module",
            "ops/tools/run_gate_authority_plane_v1.py",
        ]
        for fp in fingerprints:
            cmd.extend(["--fingerprint", fp])
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
        out = str(proc.stdout or "").strip()
        err = str(proc.stderr or "").strip()
        if proc.returncode != 0:
            detail = err or out or "no_output"
            raise SystemExit(
                "FAIL: ENGINE_HEARTBEAT_EMIT_FAILED "
                f"engine_id={engine_id} returncode={proc.returncode} detail={detail}"
            )
        if "ENGINE_HEARTBEAT_V1_WRITTEN" not in out and "ENGINE_HEARTBEAT_V1_EXISTS" not in out:
            detail = err or out or "no_output"
            raise SystemExit(
                "FAIL: ENGINE_HEARTBEAT_EMIT_INVALID_OUTPUT "
                f"engine_id={engine_id} detail={detail}"
            )


def _phase_genesis_bootstrap(
    context: LifecycleRunContextV1,
    phase: dict[str, Any],
) -> PhaseRunResultV1:
    phase_id = str(phase.get("phase_id") or "GENESIS_BOOTSTRAP")
    if str(context.mode).strip().upper() != "PAPER":
        return PhaseRunResultV1(
            status="SKIPPED",
            reason_codes=("GENESIS_BOOTSTRAP_ONLY_APPLIES_TO_PAPER",),
        )
    complete, build_path, reason = _find_previous_day_complete_economic_state_build(
        truth_root=context.truth_root,
        day_utc=context.day_utc,
    )
    if complete:
        return PhaseRunResultV1(
            status="PASS",
            reason_codes=("PREVIOUS_DAY_ECONOMIC_STATE_CHAIN_PRESENT",),
            produced_artifacts=(build_path,),
        )
    invocations = [
        _invocation(
            phase_id=phase_id,
            producer_id="economic_state_authority_v1",
            entrypoint=ECONOMIC_STATE_AUTHORITY_TOOL,
            status="INVOKED",
            artifact_paths=[build_path] if build_path else [],
        )
    ]
    try:
        bootstrap_build_path = _bootstrap_previous_day_economic_state(day_utc=context.day_utc, mode=context.mode)
    except SystemExit as exc:
        fail_code = _fail_reason_code(exc)
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code=fail_code,
            first_blocker_artifact_path=build_path,
            upstream_dependency_id="economic_state_build_v1",
            upstream_dependency_artifact_path=build_path,
            reason_codes=(fail_code, str(exc)),
            producer_invocations=tuple(
                invocations
                + [
                    _invocation(
                        phase_id=phase_id,
                        producer_id="economic_state_authority_v1",
                        entrypoint=ECONOMIC_STATE_AUTHORITY_TOOL,
                        status="FAIL",
                        artifact_paths=[build_path] if build_path else [],
                        detail=str(exc),
                    )
                ]
            ),
        )
    complete_after, complete_build_path, complete_reason = _find_previous_day_complete_economic_state_build(
        truth_root=context.truth_root,
        day_utc=context.day_utc,
    )
    if not complete_after:
        code = complete_reason or "GENESIS_BOOTSTRAP_INCOMPLETE"
        artifact_path = complete_build_path or bootstrap_build_path or build_path
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code=code,
            first_blocker_artifact_path=artifact_path,
            upstream_dependency_id="economic_state_build_v1",
            upstream_dependency_artifact_path=artifact_path,
            reason_codes=(code,),
            producer_invocations=tuple(
                invocations
                + [
                    _invocation(
                        phase_id=phase_id,
                        producer_id="economic_state_authority_v1",
                        entrypoint=ECONOMIC_STATE_AUTHORITY_TOOL,
                        status="PASS",
                        artifact_paths=[artifact_path] if artifact_path else [],
                    )
                ]
            ),
        )
    return PhaseRunResultV1(
        status="BOOTSTRAP",
        reason_codes=("GENESIS_BOOTSTRAP_APPLIED",),
        produced_artifacts=(complete_build_path,),
        producer_invocations=tuple(
            invocations
            + [
                _invocation(
                    phase_id=phase_id,
                    producer_id="economic_state_authority_v1",
                    entrypoint=ECONOMIC_STATE_AUTHORITY_TOOL,
                    status="PASS",
                    artifact_paths=[complete_build_path],
                )
            ]
        ),
        bootstrap_applied=True,
        bootstrap_reason="NO_PRIOR_DAY_COMPLETE_ECONOMIC_STATE_CHAIN",
    )


def _phase_prior_day_close(
    context: LifecycleRunContextV1,
    phase: dict[str, Any],
) -> PhaseRunResultV1:
    phase_id = str(phase.get("phase_id") or "PRIOR_DAY_CLOSE")
    prev_day = _prev_day_utc(context.day_utc)
    recon_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="reconciliation_report_v3",
        day_utc=prev_day,
        filename="reconciliation_report.v3.json",
    )
    exit_artifact = str((context.truth_root / "exit_reconciliation_v1" / prev_day / "exit_reconciliation.v1.json").resolve())
    invocations: list[dict[str, Any]] = []
    try:
        _refresh_reconciliation_report(day_utc=prev_day, truth_root=context.truth_root)
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="reconciliation_report_v3",
                entrypoint=RECONCILIATION_REPORT_TOOL,
                status="PASS",
                artifact_paths=[recon_artifact],
            )
        )
        _refresh_exit_reconciliation(day_utc=prev_day, truth_root=context.truth_root)
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="exit_reconciliation_v1",
                entrypoint=EXIT_RECONCILIATION_TOOL,
                status="PASS",
                artifact_paths=[exit_artifact],
            )
        )
    except SystemExit as exc:
        code = _fail_reason_code(exc)
        artifact = recon_artifact if "RECONCILIATION_REPORT" in str(exc) else exit_artifact
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="prior_day_close",
                entrypoint="PRIOR_DAY_CLOSE_CHAIN",
                status="FAIL",
                artifact_paths=[artifact],
                detail=str(exc),
            )
        )
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code=code,
            first_blocker_artifact_path=artifact,
            upstream_dependency_id="prior_day_close",
            upstream_dependency_artifact_path=artifact,
            reason_codes=(code, str(exc)),
            producer_invocations=tuple(invocations),
        )
    return PhaseRunResultV1(
        status="PASS",
        reason_codes=("PRIOR_DAY_CLOSE_MATERIALIZED",),
        produced_artifacts=(recon_artifact, exit_artifact),
        producer_invocations=tuple(invocations),
    )


def _phase_day_admission(context: LifecycleRunContextV1, phase: dict[str, Any]) -> PhaseRunResultV1:
    phase_id = str(phase.get("phase_id") or "DAY_ADMISSION")
    admission_path = (context.truth_root / "target_day_admission_v1" / f"{context.day_utc}.json").resolve()
    if not admission_path.exists() or not admission_path.is_file():
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="TARGET_DAY_ADMISSION_MISSING",
            first_blocker_artifact_path=str(admission_path),
            upstream_dependency_id="target_day_admission_v1",
            upstream_dependency_artifact_path=str(admission_path),
            reason_codes=("TARGET_DAY_ADMISSION_MISSING",),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="target_day_admission_v1",
                    entrypoint="target_day_admission_v1",
                    status="FAIL",
                    artifact_paths=[str(admission_path)],
                    detail="missing_artifact",
                ),
            ),
        )
    try:
        admission_obj = _read_json_object(admission_path)
    except Exception as exc:  # noqa: BLE001
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="TARGET_DAY_ADMISSION_INVALID",
            first_blocker_artifact_path=str(admission_path),
            upstream_dependency_id="target_day_admission_v1",
            upstream_dependency_artifact_path=str(admission_path),
            reason_codes=("TARGET_DAY_ADMISSION_INVALID", repr(exc)),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="target_day_admission_v1",
                    entrypoint="target_day_admission_v1",
                    status="FAIL",
                    artifact_paths=[str(admission_path)],
                    detail=repr(exc),
                ),
            ),
        )
    admission_status = str(admission_obj.get("admission_status") or "").strip().upper()
    if admission_status != "ADMIT":
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="TARGET_DAY_ADMISSION_NOT_ADMIT",
            first_blocker_artifact_path=str(admission_path),
            upstream_dependency_id="target_day_admission_v1",
            upstream_dependency_artifact_path=str(admission_path),
            reason_codes=(f"TARGET_DAY_ADMISSION_STATUS:{admission_status or 'UNKNOWN'}",),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="target_day_admission_v1",
                    entrypoint="target_day_admission_v1",
                    status="FAIL",
                    artifact_paths=[str(admission_path)],
                    detail=f"admission_status={admission_status or 'UNKNOWN'}",
                ),
            ),
        )
    return PhaseRunResultV1(
        status="PASS",
        reason_codes=("TARGET_DAY_ADMISSION_ADMIT",),
        produced_artifacts=(str(admission_path),),
        producer_invocations=(
            _invocation(
                phase_id=phase_id,
                producer_id="target_day_admission_v1",
                entrypoint="target_day_admission_v1",
                status="PASS",
                artifact_paths=[str(admission_path)],
            ),
        ),
    )


def _phase_context_authority(context: LifecycleRunContextV1, phase: dict[str, Any]) -> PhaseRunResultV1:
    phase_id = str(phase.get("phase_id") or "CONTEXT_AUTHORITY")
    build_path = (context.truth_root / "target_day_build_v1" / f"{context.day_utc}.json").resolve()
    if not build_path.exists() or not build_path.is_file():
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="TARGET_DAY_BUILD_MISSING",
            first_blocker_artifact_path=str(build_path),
            upstream_dependency_id="target_day_build_v1",
            upstream_dependency_artifact_path=str(build_path),
            reason_codes=("TARGET_DAY_BUILD_MISSING",),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="target_day_build_v1",
                    entrypoint="target_day_build_v1",
                    status="FAIL",
                    artifact_paths=[str(build_path)],
                    detail="missing_artifact",
                ),
            ),
        )
    try:
        build_obj = _read_json_object(build_path)
    except Exception as exc:  # noqa: BLE001
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="TARGET_DAY_BUILD_INVALID",
            first_blocker_artifact_path=str(build_path),
            upstream_dependency_id="target_day_build_v1",
            upstream_dependency_artifact_path=str(build_path),
            reason_codes=("TARGET_DAY_BUILD_INVALID", repr(exc)),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="target_day_build_v1",
                    entrypoint="target_day_build_v1",
                    status="FAIL",
                    artifact_paths=[str(build_path)],
                    detail=repr(exc),
                ),
            ),
        )
    build_status = str(build_obj.get("build_status") or "").strip().upper()
    closure_status = str(build_obj.get("closure_status") or "").strip().upper()
    if build_status != "COMPLETE" or closure_status != "CLOSED":
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="TARGET_DAY_BUILD_NOT_CLOSED",
            first_blocker_artifact_path=str(build_path),
            upstream_dependency_id="target_day_build_v1",
            upstream_dependency_artifact_path=str(build_path),
            reason_codes=(
                f"TARGET_DAY_BUILD_STATUS:{build_status or 'UNKNOWN'}",
                f"TARGET_DAY_BUILD_CLOSURE_STATUS:{closure_status or 'UNKNOWN'}",
            ),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="target_day_build_v1",
                    entrypoint="target_day_build_v1",
                    status="FAIL",
                    artifact_paths=[str(build_path)],
                    detail=f"build_status={build_status or 'UNKNOWN'} closure_status={closure_status or 'UNKNOWN'}",
                ),
            ),
        )
    return PhaseRunResultV1(
        status="PASS",
        reason_codes=("TARGET_DAY_BUILD_COMPLETE_CLOSED",),
        produced_artifacts=(str(build_path),),
        producer_invocations=(
            _invocation(
                phase_id=phase_id,
                producer_id="target_day_build_v1",
                entrypoint="target_day_build_v1",
                status="PASS",
                artifact_paths=[str(build_path)],
            ),
        ),
    )


def _phase_gate_inputs(context: LifecycleRunContextV1, phase: dict[str, Any]) -> PhaseRunResultV1:
    phase_id = str(phase.get("phase_id") or "GATE_INPUTS")
    cap_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="capital_risk_envelope_v2",
        day_utc=context.day_utc,
        filename="capital_risk_envelope.v2.json",
    )
    corr_matrix_artifact = str(
        (context.truth_root / "monitoring_v1" / "engine_correlation_matrix_v1" / context.day_utc / "engine_correlation_matrix.v1.json").resolve()
    )
    invocations: list[dict[str, Any]] = []
    try:
        _refresh_capital_risk_envelope(day_utc=context.day_utc, truth_root=context.truth_root, produced_utc=context.produced_utc)
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="capital_risk_envelope_v2",
                entrypoint=CAPITAL_RISK_ENVELOPE_GATE_TOOL,
                status="PASS",
                artifact_paths=[cap_artifact],
            )
        )
        _refresh_engine_correlation_matrix(day_utc=context.day_utc, truth_root=context.truth_root)
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="engine_correlation_matrix_v1",
                entrypoint=ENGINE_CORRELATION_MATRIX_TOOL,
                status="PASS",
                artifact_paths=[corr_matrix_artifact],
            )
        )
        _emit_engine_heartbeats(day_utc=context.day_utc, truth_root=context.truth_root, produced_utc=context.produced_utc)
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="engine_heartbeat_v1",
                entrypoint=(REPO_ROOT / "ops/tools/run_engine_heartbeat_emit_v1.py").resolve(),
                status="PASS",
            )
        )
    except SystemExit as exc:
        code = _fail_reason_code(exc)
        artifact = cap_artifact
        if "ENGINE_CORRELATION_MATRIX" in str(exc):
            artifact = corr_matrix_artifact
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="gate_inputs",
                entrypoint="GATE_INPUT_CHAIN",
                status="FAIL",
                artifact_paths=[artifact],
                detail=str(exc),
            )
        )
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code=code,
            first_blocker_artifact_path=artifact,
            upstream_dependency_id="gate_inputs",
            upstream_dependency_artifact_path=artifact,
            reason_codes=(code, str(exc)),
            producer_invocations=tuple(invocations),
        )
    return PhaseRunResultV1(
        status="PASS",
        reason_codes=("GATE_INPUTS_REFRESHED",),
        produced_artifacts=(cap_artifact, corr_matrix_artifact),
        producer_invocations=tuple(invocations),
    )


def _phase_gate_production(context: LifecycleRunContextV1, phase: dict[str, Any]) -> PhaseRunResultV1:
    phase_id = str(phase.get("phase_id") or "GATE_PRODUCTION")
    correlation_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="correlation_envelope_gate_v1",
        day_utc=context.day_utc,
        filename="correlation_envelope_gate.v1.json",
    )
    feed_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="feed_attestation_gate_v1",
        day_utc=context.day_utc,
        filename="feed_attestation_gate.v1.json",
    )
    heartbeat_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="heartbeat_gate_v1",
        day_utc=context.day_utc,
        filename="heartbeat_gate.v1.json",
    )
    replay_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="replay_certification_gate_v1",
        day_utc=context.day_utc,
        filename="replay_certification_gate.v1.json",
    )
    liquidity_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="liquidity_slippage_gate_v1",
        day_utc=context.day_utc,
        filename="liquidity_slippage_gate.v1.json",
    )
    operator_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="operator_daily_gate_v3",
        day_utc=context.day_utc,
        filename="operator_daily_gate.v3.json",
    )
    invocations: list[dict[str, Any]] = []
    try:
        _refresh_correlation_envelope_gate(
            day_utc=context.day_utc,
            truth_root=context.truth_root,
            produced_utc=context.produced_utc,
            mode=context.mode,
        )
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="correlation_envelope_gate_v1",
                entrypoint=CORRELATION_ENVELOPE_GATE_TOOL,
                status="PASS",
                artifact_paths=[correlation_artifact],
            )
        )
        _refresh_feed_attestation_gate(day_utc=context.day_utc, truth_root=context.truth_root)
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="feed_attestation_gate_v1",
                entrypoint=FEED_ATTESTATION_GATE_TOOL,
                status="PASS",
                artifact_paths=[feed_artifact],
            )
        )
        _refresh_heartbeat_gate(day_utc=context.day_utc, truth_root=context.truth_root)
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="heartbeat_gate_v1",
                entrypoint=HEARTBEAT_GATE_TOOL,
                status="PASS",
                artifact_paths=[heartbeat_artifact],
            )
        )
        _refresh_replay_certification_gate(
            day_utc=context.day_utc,
            truth_root=context.truth_root,
            produced_utc=context.produced_utc,
            mode=context.mode,
        )
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="replay_certification_gate_v1",
                entrypoint=REPLAY_CERTIFICATION_GATE_TOOL,
                status="PASS",
                artifact_paths=[replay_artifact],
            )
        )
        _refresh_liquidity_slippage_gate(day_utc=context.day_utc, truth_root=context.truth_root)
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="liquidity_slippage_gate_v1",
                entrypoint=LIQUIDITY_SLIPPAGE_GATE_TOOL,
                status="PASS",
                artifact_paths=[liquidity_artifact],
            )
        )
        _refresh_operator_daily_gate(
            day_utc=context.day_utc,
            truth_root=context.truth_root,
            produced_utc=context.produced_utc,
            mode=context.mode,
        )
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="operator_daily_gate_v3",
                entrypoint=OPERATOR_DAILY_GATE_TOOL,
                status="PASS",
                artifact_paths=[operator_artifact],
            )
        )
    except SystemExit as exc:
        code = _fail_reason_code(exc)
        artifact = operator_artifact
        text = str(exc)
        if "CORRELATION_ENVELOPE_GATE" in text:
            artifact = correlation_artifact
        elif "FEED_ATTESTATION_GATE" in text:
            artifact = feed_artifact
        elif "HEARTBEAT_GATE" in text:
            artifact = heartbeat_artifact
        elif "REPLAY_CERTIFICATION_GATE" in text:
            artifact = replay_artifact
        elif "LIQUIDITY_SLIPPAGE_GATE" in text:
            artifact = liquidity_artifact
        invocations.append(
            _invocation(
                phase_id=phase_id,
                producer_id="gate_production",
                entrypoint="GATE_PRODUCTION_CHAIN",
                status="FAIL",
                artifact_paths=[artifact],
                detail=text,
            )
        )
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code=code,
            first_blocker_artifact_path=artifact,
            upstream_dependency_id="gate_production",
            upstream_dependency_artifact_path=artifact,
            reason_codes=(code, text),
            producer_invocations=tuple(invocations),
        )
    return PhaseRunResultV1(
        status="PASS",
        reason_codes=("REQUIRED_GATES_MATERIALIZED",),
        produced_artifacts=(
            correlation_artifact,
            feed_artifact,
            heartbeat_artifact,
            replay_artifact,
            liquidity_artifact,
            operator_artifact,
        ),
        producer_invocations=tuple(invocations),
    )


def _phase_gate_aggregation(context: LifecycleRunContextV1, phase: dict[str, Any]) -> PhaseRunResultV1:
    phase_id = str(phase.get("phase_id") or "GATE_AGGREGATION")
    artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="gate_stack_verdict_v1",
        day_utc=context.day_utc,
        filename="gate_stack_verdict.v1.json",
    )
    try:
        _refresh_gate_stack_verdict(
            day_utc=context.day_utc,
            truth_root=context.truth_root,
            produced_utc=context.produced_utc,
            mode=context.mode,
        )
    except SystemExit as exc:
        code = _fail_reason_code(exc)
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code=code,
            first_blocker_artifact_path=artifact,
            upstream_dependency_id="gate_stack_verdict_v1",
            upstream_dependency_artifact_path=artifact,
            reason_codes=(code, str(exc)),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="gate_stack_verdict_v1",
                    entrypoint=GATE_STACK_VERDICT_TOOL,
                    status="FAIL",
                    artifact_paths=[artifact],
                    detail=str(exc),
                ),
            ),
        )
    return PhaseRunResultV1(
        status="PASS",
        reason_codes=("GATE_STACK_VERDICT_MATERIALIZED",),
        produced_artifacts=(artifact,),
        producer_invocations=(
            _invocation(
                phase_id=phase_id,
                producer_id="gate_stack_verdict_v1",
                entrypoint=GATE_STACK_VERDICT_TOOL,
                status="PASS",
                artifact_paths=[artifact],
            ),
        ),
    )


def _phase_authorization(
    context: LifecycleRunContextV1,
    phase: dict[str, Any],
    run_state: Dict[str, Any],
) -> PhaseRunResultV1:
    phase_id = str(phase.get("phase_id") or "AUTHORIZATION")
    auth_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="authorization_gate_verdict_v1",
        day_utc=context.day_utc,
        filename="authorization_gate_verdict.v1.json",
    )
    try:
        writes = write_gate_authority_plane(
            repo_root=REPO_ROOT,
            truth_root=context.truth_root,
            day_utc=context.day_utc,
            produced_utc=context.produced_utc,
            mode=context.mode,
        )
    except Exception as exc:  # noqa: BLE001
        code = "AUTHORIZATION_GATE_WRITE_FAILED"
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code=code,
            first_blocker_artifact_path=auth_artifact,
            upstream_dependency_id="authorization_gate_verdict_v1",
            upstream_dependency_artifact_path=auth_artifact,
            reason_codes=(code, repr(exc)),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="gate_authority_foundation_v1",
                    entrypoint="constellation_2/common/gate_authority_foundation_v1.py",
                    status="FAIL",
                    artifact_paths=[auth_artifact],
                    detail=repr(exc),
                ),
            ),
        )
    run_state["gate_writes"] = {
        "lifecycle_state_action": writes["lifecycle_state"].action,
        "authorization_verdict_action": writes["authorization_verdict"].action,
        "economic_verdict_action": writes["economic_verdict"].action,
        "ledger_action": writes["ledger"].action,
    }
    return PhaseRunResultV1(
        status="PASS",
        reason_codes=("AUTHORIZATION_ARTIFACTS_MATERIALIZED",),
        produced_artifacts=(auth_artifact,),
        producer_invocations=(
            _invocation(
                phase_id=phase_id,
                producer_id="gate_authority_foundation_v1",
                entrypoint="constellation_2/common/gate_authority_foundation_v1.py",
                status="PASS",
                artifact_paths=[auth_artifact],
            ),
        ),
        phase_payload={"gate_writes": dict(run_state["gate_writes"])},
    )


def _phase_execution_readiness(context: LifecycleRunContextV1, phase: dict[str, Any]) -> PhaseRunResultV1:
    _ = phase
    auth_path = Path(
        _truth_report_path(
            truth_root=context.truth_root,
            family="authorization_gate_verdict_v1",
            day_utc=context.day_utc,
            filename="authorization_gate_verdict.v1.json",
        )
    )
    if not auth_path.exists() or not auth_path.is_file():
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="AUTHORIZATION_VERDICT_MISSING",
            first_blocker_artifact_path=str(auth_path),
            upstream_dependency_id="authorization_gate_verdict_v1",
            upstream_dependency_artifact_path=str(auth_path),
            reason_codes=("AUTHORIZATION_VERDICT_MISSING",),
        )
    try:
        payload = json.loads(auth_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="AUTHORIZATION_VERDICT_INVALID",
            first_blocker_artifact_path=str(auth_path),
            upstream_dependency_id="authorization_gate_verdict_v1",
            upstream_dependency_artifact_path=str(auth_path),
            reason_codes=("AUTHORIZATION_VERDICT_INVALID", repr(exc)),
        )
    status = str(payload.get("status") or "").strip().upper()
    if status != "PASS":
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="AUTHORIZATION_VERDICT_NOT_PASS",
            first_blocker_artifact_path=str(auth_path),
            upstream_dependency_id="authorization_gate_verdict_v1",
            upstream_dependency_artifact_path=str(auth_path),
            reason_codes=(f"AUTHORIZATION_VERDICT_STATUS:{status or 'UNKNOWN'}",),
        )
    return PhaseRunResultV1(
        status="PASS",
        reason_codes=("AUTHORIZATION_VERDICT_PASS",),
        produced_artifacts=(str(auth_path),),
    )


def _phase_execution(context: LifecycleRunContextV1, phase: dict[str, Any]) -> PhaseRunResultV1:
    _ = phase
    submissions_root = (context.truth_root / "execution_evidence_v1" / "submissions" / context.day_utc).resolve()
    if submissions_root.exists() and submissions_root.is_dir():
        try:
            next(submissions_root.iterdir())
            return PhaseRunResultV1(
                status="PASS",
                reason_codes=("EXECUTION_EVIDENCE_PRESENT",),
                produced_artifacts=(str(submissions_root),),
            )
        except StopIteration:
            pass
    return PhaseRunResultV1(
        status="SKIPPED",
        reason_codes=("EXECUTION_NOT_REACHED_FOR_DAY",),
    )


def _phase_post_execution_reconciliation(context: LifecycleRunContextV1, phase: dict[str, Any]) -> PhaseRunResultV1:
    phase_id = str(phase.get("phase_id") or "POST_EXECUTION_RECONCILIATION")
    submissions_root = (context.truth_root / "execution_evidence_v1" / "submissions" / context.day_utc).resolve()
    if not submissions_root.exists() or not submissions_root.is_dir():
        return PhaseRunResultV1(
            status="SKIPPED",
            reason_codes=("POST_EXECUTION_RECONCILIATION_NO_SUBMISSION_DIR",),
        )
    has_submissions = any(p.is_dir() for p in submissions_root.iterdir())
    if not has_submissions:
        return PhaseRunResultV1(
            status="SKIPPED",
            reason_codes=("POST_EXECUTION_RECONCILIATION_NO_SUBMISSIONS",),
        )
    artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="execution_reconciliation_v1",
        day_utc=context.day_utc,
        filename="execution_reconciliation.v1.json",
    )
    fill_ledger_root = (context.truth_root / "fill_ledger_v1" / context.day_utc).resolve()
    fill_cmd = [
        sys.executable,
        str(FILL_LEDGER_TOOL),
        "--day_utc",
        context.day_utc,
        "--truth_root",
        str(context.truth_root),
    ]
    fill_proc = subprocess.run(fill_cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    if fill_proc.returncode != 0:
        detail = str(fill_proc.stderr or "").strip() or str(fill_proc.stdout or "").strip() or "no_output"
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="POST_EXECUTION_FILL_LEDGER_REFRESH_FAILED",
            first_blocker_artifact_path=str(fill_ledger_root),
            upstream_dependency_id="fill_ledger_v1",
            upstream_dependency_artifact_path=str(fill_ledger_root),
            reason_codes=("POST_EXECUTION_FILL_LEDGER_REFRESH_FAILED", detail),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="fill_ledger_v1",
                    entrypoint=FILL_LEDGER_TOOL,
                    status="FAIL",
                    artifact_paths=[str(fill_ledger_root)],
                    detail=detail,
                ),
            ),
        )
    if not fill_ledger_root.exists() or not fill_ledger_root.is_dir():
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="POST_EXECUTION_FILL_LEDGER_ARTIFACT_MISSING",
            first_blocker_artifact_path=str(fill_ledger_root),
            upstream_dependency_id="fill_ledger_v1",
            upstream_dependency_artifact_path=str(fill_ledger_root),
            reason_codes=("POST_EXECUTION_FILL_LEDGER_ARTIFACT_MISSING",),
        )
    fill_records = list(fill_ledger_root.glob("*.fill_ledger.v1.json"))
    if not fill_records:
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="POST_EXECUTION_FILL_LEDGER_EMPTY",
            first_blocker_artifact_path=str(fill_ledger_root),
            upstream_dependency_id="fill_ledger_v1",
            upstream_dependency_artifact_path=str(fill_ledger_root),
            reason_codes=("POST_EXECUTION_FILL_LEDGER_EMPTY",),
        )

    cmd = [
        sys.executable,
        str(EXECUTION_RECONCILIATION_TOOL),
        "--day_utc",
        context.day_utc,
        "--truth_root",
        str(context.truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        detail = str(proc.stderr or "").strip() or str(proc.stdout or "").strip() or "no_output"
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="POST_EXECUTION_RECONCILIATION_REFRESH_FAILED",
            first_blocker_artifact_path=artifact,
            upstream_dependency_id="execution_reconciliation_v1",
            upstream_dependency_artifact_path=artifact,
            reason_codes=("POST_EXECUTION_RECONCILIATION_REFRESH_FAILED", detail),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="fill_ledger_v1",
                    entrypoint=FILL_LEDGER_TOOL,
                    status="PASS",
                    artifact_paths=[str(fill_ledger_root)],
                ),
                _invocation(
                    phase_id=phase_id,
                    producer_id="execution_reconciliation_v1",
                    entrypoint=EXECUTION_RECONCILIATION_TOOL,
                    status="FAIL",
                    artifact_paths=[artifact],
                    detail=detail,
                ),
            ),
        )
    if not Path(artifact).exists():
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="POST_EXECUTION_RECONCILIATION_ARTIFACT_MISSING",
            first_blocker_artifact_path=artifact,
            upstream_dependency_id="execution_reconciliation_v1",
            upstream_dependency_artifact_path=artifact,
            reason_codes=("POST_EXECUTION_RECONCILIATION_ARTIFACT_MISSING",),
        )
    return PhaseRunResultV1(
        status="PASS",
        reason_codes=("POST_EXECUTION_RECONCILIATION_REFRESHED",),
        produced_artifacts=(str(fill_ledger_root), artifact),
        producer_invocations=(
            _invocation(
                phase_id=phase_id,
                producer_id="fill_ledger_v1",
                entrypoint=FILL_LEDGER_TOOL,
                status="PASS",
                artifact_paths=[str(fill_ledger_root)],
            ),
            _invocation(
                phase_id=phase_id,
                producer_id="execution_reconciliation_v1",
                entrypoint=EXECUTION_RECONCILIATION_TOOL,
                status="PASS",
                artifact_paths=[artifact],
            ),
        ),
    )


def _phase_day_close(context: LifecycleRunContextV1, phase: dict[str, Any]) -> PhaseRunResultV1:
    phase_id = str(phase.get("phase_id") or "DAY_CLOSE")
    ledger_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="paper_session_ledger_v1",
        day_utc=context.day_utc,
        filename="paper_session_ledger.v1.json",
    )
    attribution_artifact = _truth_report_path(
        truth_root=context.truth_root,
        family="sleeve_intent_trade_attribution_v1",
        day_utc=context.day_utc,
        filename="sleeve_intent_trade_attribution.v1.json",
    )
    cmd = [
        sys.executable,
        str(PAPER_SESSION_LEDGER_TOOL),
        "--day_utc",
        context.day_utc,
        "--truth_root",
        str(context.truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        detail = str(proc.stderr or "").strip() or str(proc.stdout or "").strip() or "no_output"
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="DAY_CLOSE_LEDGER_REFRESH_FAILED",
            first_blocker_artifact_path=ledger_artifact,
            upstream_dependency_id="paper_session_ledger_v1",
            upstream_dependency_artifact_path=ledger_artifact,
            reason_codes=("DAY_CLOSE_LEDGER_REFRESH_FAILED", detail),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="paper_session_ledger_v1",
                    entrypoint=PAPER_SESSION_LEDGER_TOOL,
                    status="FAIL",
                    artifact_paths=[ledger_artifact],
                    detail=detail,
                ),
            ),
        )
    if not Path(ledger_artifact).exists():
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="DAY_CLOSE_LEDGER_ARTIFACT_MISSING",
            first_blocker_artifact_path=ledger_artifact,
            upstream_dependency_id="paper_session_ledger_v1",
            upstream_dependency_artifact_path=ledger_artifact,
            reason_codes=("DAY_CLOSE_LEDGER_ARTIFACT_MISSING",),
        )

    attribution_cmd = [
        sys.executable,
        str(SLEEVE_INTENT_TRADE_ATTRIBUTION_TOOL),
        "--day_utc",
        context.day_utc,
        "--truth_root",
        str(context.truth_root),
        "--environment",
        context.mode,
    ]
    attribution_proc = subprocess.run(
        attribution_cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if attribution_proc.returncode != 0:
        detail = (
            str(attribution_proc.stderr or "").strip()
            or str(attribution_proc.stdout or "").strip()
            or "no_output"
        )
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="DAY_CLOSE_ATTRIBUTION_REFRESH_FAILED",
            first_blocker_artifact_path=attribution_artifact,
            upstream_dependency_id="sleeve_intent_trade_attribution_v1",
            upstream_dependency_artifact_path=attribution_artifact,
            reason_codes=("DAY_CLOSE_ATTRIBUTION_REFRESH_FAILED", detail),
            producer_invocations=(
                _invocation(
                    phase_id=phase_id,
                    producer_id="paper_session_ledger_v1",
                    entrypoint=PAPER_SESSION_LEDGER_TOOL,
                    status="PASS",
                    artifact_paths=[ledger_artifact],
                ),
                _invocation(
                    phase_id=phase_id,
                    producer_id="sleeve_intent_trade_attribution_v1",
                    entrypoint=SLEEVE_INTENT_TRADE_ATTRIBUTION_TOOL,
                    status="FAIL",
                    artifact_paths=[attribution_artifact],
                    detail=detail,
                ),
            ),
        )
    if not Path(attribution_artifact).exists():
        return PhaseRunResultV1(
            status="BLOCKED",
            first_blocker_code="DAY_CLOSE_ATTRIBUTION_ARTIFACT_MISSING",
            first_blocker_artifact_path=attribution_artifact,
            upstream_dependency_id="sleeve_intent_trade_attribution_v1",
            upstream_dependency_artifact_path=attribution_artifact,
            reason_codes=("DAY_CLOSE_ATTRIBUTION_ARTIFACT_MISSING",),
        )

    return PhaseRunResultV1(
        status="PASS",
        reason_codes=(
            "DAY_CLOSE_LEDGER_REFRESHED",
            "DAY_CLOSE_ATTRIBUTION_REFRESHED",
        ),
        produced_artifacts=(ledger_artifact, attribution_artifact),
        producer_invocations=(
            _invocation(
                phase_id=phase_id,
                producer_id="paper_session_ledger_v1",
                entrypoint=PAPER_SESSION_LEDGER_TOOL,
                status="PASS",
                artifact_paths=[ledger_artifact],
            ),
            _invocation(
                phase_id=phase_id,
                producer_id="sleeve_intent_trade_attribution_v1",
                entrypoint=SLEEVE_INTENT_TRADE_ATTRIBUTION_TOOL,
                status="PASS",
                artifact_paths=[attribution_artifact],
            ),
        ),
    )


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_gate_authority_plane_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--produced_utc", required=True)
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    args = ap.parse_args()

    day_utc = _require_day_utc(args.day_utc)
    produced_utc = _require_produced_utc(day_utc, args.produced_utc)
    truth_root = _resolve_truth_root(args.truth_root)
    context = LifecycleRunContextV1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
        mode=args.mode,
    )
    run_state: Dict[str, Any] = {}
    phase_runners = {
        "GENESIS_BOOTSTRAP": _phase_genesis_bootstrap,
        "PRIOR_DAY_CLOSE": _phase_prior_day_close,
        "DAY_ADMISSION": _phase_day_admission,
        "CONTEXT_AUTHORITY": _phase_context_authority,
        "GATE_INPUTS": _phase_gate_inputs,
        "GATE_PRODUCTION": _phase_gate_production,
        "GATE_AGGREGATION": _phase_gate_aggregation,
        "AUTHORIZATION": lambda ctx, spec: _phase_authorization(ctx, spec, run_state),
        "EXECUTION_READINESS": _phase_execution_readiness,
        "EXECUTION": _phase_execution,
        "POST_EXECUTION_RECONCILIATION": _phase_post_execution_reconciliation,
        "DAY_CLOSE": _phase_day_close,
    }
    orchestration = run_truth_lifecycle_orchestrator_v1(
        context=context,
        phase_runners=phase_runners,
    )
    writes = dict(run_state.get("gate_writes") or {})
    print(
        "OK: GATE_AUTHORITY_PLANE_V1 "
        f"day_utc={day_utc} truth_root={truth_root} "
        f"lifecycle_state_action={writes.get('lifecycle_state_action', '')} "
        f"authorization_verdict_action={writes.get('authorization_verdict_action', '')} "
        f"economic_verdict_action={writes.get('economic_verdict_action', '')} "
        f"ledger_action={writes.get('ledger_action', '')} "
        f"phase_run_ledger={orchestration['run_ledger_path']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
