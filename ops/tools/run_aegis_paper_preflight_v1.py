#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.gate_authority_foundation_v1 import write_gate_authority_plane
from constellation_2.common.paper_day_manifest_v1 import (
    diagnostic_artifacts_v1,
    load_paper_day_manifest_v1,
    required_artifacts_v1,
    topological_manifest_names_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1


def _run_step(name: str, cmd: List[str]) -> Dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "name": name,
        "cmd": cmd,
        "return_code": int(proc.returncode),
        "stdout": str(proc.stdout or "").strip(),
        "stderr": str(proc.stderr or "").strip(),
    }


def _refresh_authorization_gate_verdict(
    *,
    day_utc: str,
    produced_utc: str,
    execution_truth_root: Path,
) -> List[Dict[str, Any]]:
    steps: List[Dict[str, Any]] = []
    steps.append(
        _run_step(
            "gate_stack_verdict_v1",
            [
                sys.executable,
                "ops/tools/run_gate_stack_verdict_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(execution_truth_root),
                "--produced_utc",
                produced_utc,
                "--mode",
                "PAPER",
            ],
        )
    )
    gate_stack_result = steps[-1]
    if int(gate_stack_result.get("return_code") or 0) not in (0, 1):
        return steps

    try:
        writes = write_gate_authority_plane(
            repo_root=REPO_ROOT,
            truth_root=execution_truth_root,
            day_utc=day_utc,
            produced_utc=produced_utc,
            mode="PAPER",
        )
        auth_write = writes.get("authorization_verdict")
        auth_action = str(getattr(auth_write, "action", "") or "")
        auth_path = str(getattr(auth_write, "path", "") or "")
        auth_sha = str(getattr(auth_write, "sha256", "") or "")
        steps.append(
            {
                "name": "authorization_gate_verdict_v1",
                "cmd": [
                    "python",
                    "-m",
                    "constellation_2.common.gate_authority_foundation_v1",
                    "write_gate_authority_plane",
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(execution_truth_root),
                    "--produced_utc",
                    produced_utc,
                    "--mode",
                    "PAPER",
                ],
                "return_code": 0,
                "stdout": json.dumps(
                    {
                        "authorization_verdict_action": auth_action,
                        "authorization_verdict_path": auth_path,
                        "authorization_verdict_sha256": auth_sha,
                    },
                    sort_keys=True,
                ),
                "stderr": "",
            }
        )
    except Exception as exc:  # noqa: BLE001
        steps.append(
            {
                "name": "authorization_gate_verdict_v1",
                "cmd": [
                    "python",
                    "-m",
                    "constellation_2.common.gate_authority_foundation_v1",
                    "write_gate_authority_plane",
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(execution_truth_root),
                    "--produced_utc",
                    produced_utc,
                    "--mode",
                    "PAPER",
                ],
                "return_code": 2,
                "stdout": "",
                "stderr": f"{type(exc).__name__}: {exc}",
            }
        )
    return steps


def _load_json(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _load_json_if_present(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _submission_dir_is_dry_run_only(path: Path) -> bool:
    attempt = _load_json_if_present(path / "broker_submit_attempt_v1.json")
    record = _load_json_if_present(path / "broker_submission_record.v2.json")
    broker = record.get("broker") if isinstance(record.get("broker"), dict) else {}
    error = record.get("error") if isinstance(record.get("error"), dict) else {}
    broker_ids = record.get("broker_ids") if isinstance(record.get("broker_ids"), dict) else {}
    environment = str(attempt.get("environment") or broker.get("environment") or "PAPER").strip().upper()
    dry_run = bool(attempt.get("dry_run") is True or str(error.get("code") or "").strip().upper() == "DRY_RUN_NO_BROKER_ID")
    broker_transmitted = bool(record.get("broker_transmitted") is True)
    order_id = _coerce_positive_int(broker_ids.get("order_id"))
    perm_id = _coerce_positive_int(broker_ids.get("perm_id"))
    return bool(environment == "PAPER" and dry_run and not broker_transmitted and order_id is None and perm_id is None)


def _move_superseded_path(src: Path, dst: Path) -> Dict[str, str] | None:
    src = Path(src).resolve()
    dst = Path(dst).resolve()
    if not src.exists():
        return None
    if dst.exists():
        suffix = 1
        while True:
            candidate = dst.with_name(f"{dst.name}.{suffix}")
            if not candidate.exists():
                dst = candidate
                break
            suffix += 1
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
    return {"from": str(src), "to": str(dst)}


def _supersede_dry_run_submission_sidecars_v1(
    *,
    execution_truth_root: Path,
    day_utc: str,
    reset_id: str,
    submission_id: str,
) -> List[Dict[str, str]]:
    moved: List[Dict[str, str]] = []
    sidecar_specs = [
        (
            execution_truth_root / "execution_kernel_v1" / "submission_records" / day_utc / submission_id,
            execution_truth_root
            / "execution_kernel_v1"
            / "superseded_dry_run_submission_records_v1"
            / day_utc
            / reset_id
            / submission_id,
        ),
        (
            execution_truth_root / "fill_ledger_v1" / day_utc / f"{submission_id}.fill_ledger.v1.json",
            execution_truth_root
            / "fill_ledger_v1"
            / "superseded_dry_run_fill_ledger_v1"
            / day_utc
            / reset_id
            / f"{submission_id}.fill_ledger.v1.json",
        ),
    ]
    for src, dst in sidecar_specs:
        row = _move_superseded_path(src, dst)
        if row is not None:
            row["kind"] = "dry_run_submission_sidecar"
            moved.append(row)

    manifests_root = execution_truth_root / "execution_evidence_v1" / "manifests" / day_utc
    if manifests_root.exists() and manifests_root.is_dir():
        manifest_destination_root = (
            execution_truth_root
            / "execution_evidence_v1"
            / "superseded_dry_run_submission_manifests_v1"
            / day_utc
            / reset_id
        )
        for manifest_path in sorted(manifests_root.glob(f"{submission_id}*")):
            if not manifest_path.is_file():
                continue
            row = _move_superseded_path(manifest_path, manifest_destination_root / manifest_path.name)
            if row is not None:
                row["kind"] = "dry_run_submission_manifest"
                moved.append(row)
    return moved


def _dry_run_reset_marker_path_v1(*, execution_truth_root: Path, day_utc: str) -> Path:
    return (
        execution_truth_root
        / "execution_evidence_v1"
        / "dry_run_submission_reset_v1"
        / day_utc
        / "latest_reset.v1.json"
    ).resolve()


def _write_dry_run_reset_marker_v1(*, execution_truth_root: Path, day_utc: str, reset_id: str) -> Path:
    marker_path = _dry_run_reset_marker_path_v1(
        execution_truth_root=execution_truth_root,
        day_utc=day_utc,
    )
    payload = {
        "schema_id": "dry_run_submission_reset",
        "schema_version": "v1",
        "day_utc": day_utc,
        "mode": "PAPER",
        "reset_id": reset_id,
        "status": "ACTIVE",
        "scope": "NEXT_AUTO_PREFLIGHT_ONLY",
        "produced_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "reason_code": "OPERATOR_RESET_DRY_RUN_SUBMISSION_EVIDENCE",
    }
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return marker_path


def reset_dry_run_submission_evidence_v1(*, execution_truth_root: Path, day_utc: str) -> Dict[str, Any]:
    source_root = (execution_truth_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    reset_id = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z").replace(":", "").replace("-", "")
    destination_root = (
        execution_truth_root
        / "execution_evidence_v1"
        / "superseded_dry_run_submissions_v1"
        / day_utc
        / reset_id
    ).resolve()
    marker_path = _write_dry_run_reset_marker_v1(
        execution_truth_root=execution_truth_root,
        day_utc=day_utc,
        reset_id=reset_id,
    )
    moved: List[Dict[str, str]] = []
    sidecars_moved: List[Dict[str, str]] = []
    skipped: List[Dict[str, str]] = []
    if not source_root.exists() or not source_root.is_dir():
        return {
            "status": "PASS",
            "day_utc": day_utc,
            "mode": "PAPER",
            "source_root": str(source_root),
            "destination_root": str(destination_root),
            "reset_marker_path": str(marker_path),
            "moved_count": 0,
            "sidecar_moved_count": 0,
            "skipped_count": 0,
            "moved": moved,
            "sidecars_moved": sidecars_moved,
            "skipped": skipped,
        }
    for submission_dir in sorted(path for path in source_root.iterdir() if path.is_dir()):
        if not _submission_dir_is_dry_run_only(submission_dir):
            skipped.append({"submission_id": submission_dir.name, "reason": "NOT_DRY_RUN_ONLY_OR_TRANSMITTED"})
            continue
        destination = destination_root / submission_dir.name
        row = _move_superseded_path(submission_dir, destination)
        if row is None:
            continue
        row["submission_id"] = submission_dir.name
        moved.append(row)
        sidecars_moved.extend(
            _supersede_dry_run_submission_sidecars_v1(
                execution_truth_root=execution_truth_root,
                day_utc=day_utc,
                reset_id=reset_id,
                submission_id=submission_dir.name,
            )
        )
    return {
        "status": "PASS",
        "day_utc": day_utc,
        "mode": "PAPER",
        "source_root": str(source_root),
        "destination_root": str(destination_root),
        "reset_marker_path": str(marker_path),
        "moved_count": len(moved),
        "sidecar_moved_count": len(sidecars_moved),
        "skipped_count": len(skipped),
        "moved": moved,
        "sidecars_moved": sidecars_moved,
        "skipped": skipped,
    }


def _append_reset_dry_run_submission_evidence_step(
    *,
    steps: List[Dict[str, Any]],
    execution_truth_root: Path,
    day_utc: str,
    name: str,
) -> bool:
    try:
        reset_result = reset_dry_run_submission_evidence_v1(
            execution_truth_root=execution_truth_root,
            day_utc=day_utc,
        )
        steps.append(
            {
                "name": name,
                "cmd": [
                    "internal",
                    "reset_dry_run_submission_evidence_v1",
                    "--day_utc",
                    day_utc,
                    "--mode",
                    "PAPER",
                ],
                "return_code": 0,
                "stdout": json.dumps(reset_result, sort_keys=True),
                "stderr": "",
            }
        )
        return True
    except Exception as exc:  # noqa: BLE001
        steps.append(
            {
                "name": name,
                "cmd": [
                    "internal",
                    "reset_dry_run_submission_evidence_v1",
                    "--day_utc",
                    day_utc,
                    "--mode",
                    "PAPER",
                ],
                "return_code": 2,
                "stdout": "",
                "stderr": f"{type(exc).__name__}: {exc}",
            }
        )
        return False


def _authority_path(truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "paper_trading_day_authority_v1"
        / day_utc
        / "paper_trading_day_authority.v1.json"
    ).resolve()


def _evaluate_authority_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    state = str(payload.get("state") or "").strip().upper()
    canonical_blocker = str(payload.get("canonical_blocker") or "").strip()
    missing = payload.get("missing_or_stale_inputs")
    blockers = payload.get("blocker_tree")
    first_missing = missing[0] if isinstance(missing, list) and missing else {}
    first_blocker = blockers[0] if isinstance(blockers, list) and blockers else {}
    return {
        "state": state,
        "canonical_blocker": canonical_blocker,
        "first_missing_path": str(first_missing.get("path") or ""),
        "first_missing_gate": str(first_missing.get("owning_gate") or ""),
        "first_missing_cmd": str(first_missing.get("required_producer_command") or ""),
        "first_missing_operator_actionable": bool(first_missing.get("operator_actionable") is True),
        "first_blocker_code": str(first_blocker.get("code") or canonical_blocker),
        "first_blocker_path": str(first_blocker.get("source_path") or ""),
        "first_blocker_gate": str(first_blocker.get("owning_gate") or ""),
        "first_blocker_cmd": str(first_blocker.get("required_producer_command") or ""),
        "first_blocker_operator_actionable": bool(first_blocker.get("operator_actionable") is True),
    }


def _authority_required_issues(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    input_status = payload.get("input_status")
    if not isinstance(input_status, dict):
        return []
    out: List[Dict[str, Any]] = []
    for name, row_any in input_status.items():
        if not isinstance(row_any, dict):
            continue
        if str(row_any.get("required_or_diagnostic") or "").strip().lower() != "required":
            continue
        if str(row_any.get("readiness_role") or "").strip().lower() not in {"authority_input", "authority_output"}:
            continue
        status = str(row_any.get("status") or "").strip().upper()
        if status in {"PASS"}:
            continue
        out.append(
            {
                "artifact_name": str(name),
                "status": status,
                "path": str(row_any.get("path") or ""),
                "owner": str(row_any.get("owning_subsystem") or name),
                "producer_command": str(row_any.get("producer_command") or ""),
                "reason_codes": list(row_any.get("reason_codes") or []),
                "operator_actionable": True,
            }
        )
    return out


def _authority_diagnostic_issues(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    input_status = payload.get("input_status")
    if not isinstance(input_status, dict):
        return []
    out: List[Dict[str, Any]] = []
    for name, row_any in input_status.items():
        if not isinstance(row_any, dict):
            continue
        if str(row_any.get("required_or_diagnostic") or "").strip().lower() != "diagnostic":
            continue
        status = str(row_any.get("status") or "").strip().upper()
        if status in {"PASS"}:
            continue
        out.append(
            {
                "artifact_name": str(name),
                "status": status,
                "path": str(row_any.get("path") or ""),
                "owner": str(row_any.get("owning_subsystem") or name),
                "producer_command": str(row_any.get("producer_command") or ""),
                "reason_codes": list(row_any.get("reason_codes") or []),
            }
        )
    return out


def _session_authority_already_current(payload: Dict[str, Any]) -> bool:
    input_status = payload.get("input_status")
    if not isinstance(input_status, dict):
        return False
    row = input_status.get("paper_session_authority_v1")
    if not isinstance(row, dict):
        return False
    return (
        str(row.get("status") or "").strip().upper() == "PASS"
        and str(row.get("freshness_status") or "").strip().upper() in {"", "CURRENT"}
        and str(row.get("required_or_diagnostic") or "").strip().lower() == "required"
    )


def main(argv: List[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    ap = argparse.ArgumentParser(prog="run_aegis_paper_preflight_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument(
        "--reset-dry-run-submission-evidence",
        "--allow-new-paper-dry-run-attempt",
        action="store_true",
        dest="reset_dry_run_submission_evidence",
    )
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root or "",
        repo_root=REPO_ROOT,
        caller="ops/tools/run_aegis_paper_preflight_v1.py",
    )
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_truth_root = resolve_sleeve_execution_root_v1(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=paper_account,
        sleeve_id="PRIMARY",
    ).execution_root_path.resolve()
    produced_utc = f"{day_utc}T00:00:00Z"
    manifest = load_paper_day_manifest_v1(REPO_ROOT)
    required_artifact_names = [
        str(item.get("artifact_name") or "").strip()
        for item in required_artifacts_v1(manifest)
        if str(item.get("artifact_name") or "").strip()
    ]
    diagnostic_artifact_names = [
        str(item.get("artifact_name") or "").strip()
        for item in diagnostic_artifacts_v1(manifest)
        if str(item.get("artifact_name") or "").strip()
    ]
    manifest_topological_order = topological_manifest_names_v1(manifest)

    # Step 1: preflight evidence producers (before session authority and boundary).
    steps: List[Dict[str, Any]] = []
    if bool(args.reset_dry_run_submission_evidence):
        if not _append_reset_dry_run_submission_evidence_step(
            steps=steps,
            execution_truth_root=execution_truth_root,
            day_utc=day_utc,
            name="reset_dry_run_submission_evidence_v1",
        ):
            print(
                json.dumps(
                    {
                        "status": "PREFLIGHT_BLOCKED",
                        "day_utc": day_utc,
                        "canonical_blocker": "DRY_RUN_SUBMISSION_EVIDENCE_RESET_FAILED",
                        "steps": steps,
                    },
                    sort_keys=True,
                )
            )
            return 2
    steps.append(
        _run_step(
            "trading_day_intent_generation_v1",
            [
                sys.executable,
                "ops/tools/run_trading_day_intent_generation_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
            ],
        )
    )
    steps.append(
        _run_step(
            "strategy_decision_authority_v1",
            [
                sys.executable,
                "ops/tools/run_strategy_decision_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--execution_root",
                str(execution_truth_root),
            ],
        )
    )
    steps.append(
        _run_step(
            "portfolio_account_authority_v1",
            [
                sys.executable,
                "ops/tools/run_portfolio_account_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--execution_root",
                str(execution_truth_root),
            ],
        )
    )
    steps.append(
        _run_step(
            "risk_sizing_authority_v1",
            [
                sys.executable,
                "ops/tools/run_risk_sizing_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--execution_root",
                str(execution_truth_root),
            ],
        )
    )
    steps.append(
        _run_step(
            "correlation_envelope_gate_v1",
            [
                sys.executable,
                "ops/tools/run_correlation_envelope_gate_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(execution_truth_root),
                "--produced_utc",
                produced_utc,
                "--mode",
                "PAPER",
            ],
        )
    )
    steps.append(
        _run_step(
            "replay_certification_gate_v1",
            [
                sys.executable,
                "ops/tools/run_replay_certification_gate_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(execution_truth_root),
                "--produced_utc",
                produced_utc,
                "--mode",
                "PAPER",
            ],
        )
    )
    steps.extend(
        _refresh_authorization_gate_verdict(
            day_utc=day_utc,
            produced_utc=produced_utc,
            execution_truth_root=execution_truth_root,
        )
    )
    steps.append(
        _run_step(
            "global_kill_switch_v1",
            [
                sys.executable,
                "ops/tools/run_global_kill_switch_v1.py",
                "--day_utc",
                day_utc,
            ],
        )
    )
    steps.append(
        _run_step(
            "options_chain_snapshot_v1",
            [
                sys.executable,
                "ops/tools/run_options_chain_snapshot_required_day_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(execution_truth_root),
                "--symbols_from_intents",
                "YES",
            ],
        )
    )
    steps.append(
        _run_step(
            "market_data_authority_v1",
            [
                sys.executable,
                "ops/tools/run_market_data_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--execution_root",
                str(execution_truth_root),
            ],
        )
    )
    steps.append(
        _run_step(
            "strategy_decision_authority_v1.after_market_data",
            [
                sys.executable,
                "ops/tools/run_strategy_decision_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--execution_root",
                str(execution_truth_root),
            ],
        )
    )
    steps.append(
        _run_step(
            "session_readiness_refresh_v1",
            [
                sys.executable,
                "ops/tools/run_session_readiness_refresh_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--build_fast_path",
                "NO",
            ],
        )
    )
    steps.append(
        _run_step(
            "session_authority_v1.refresh_target_day",
            [
                sys.executable,
                "ops/tools/run_session_authority_v1.py",
                "--target_day",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--environment",
                "PAPER",
                "--phase",
                "all",
            ],
        )
    )
    if bool(args.reset_dry_run_submission_evidence):
        if not _append_reset_dry_run_submission_evidence_step(
            steps=steps,
            execution_truth_root=execution_truth_root,
            day_utc=day_utc,
            name="reset_dry_run_submission_evidence_v1.after_session_refresh",
        ):
            print(
                json.dumps(
                    {
                        "status": "PREFLIGHT_BLOCKED",
                        "day_utc": day_utc,
                        "canonical_blocker": "DRY_RUN_SUBMISSION_EVIDENCE_RESET_FAILED",
                        "steps": steps,
                    },
                    sort_keys=True,
                )
            )
            return 2
    steps.append(
        _run_step(
            "execution_mode_authority_v1",
            [
                sys.executable,
                "ops/tools/run_execution_mode_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--execution_root",
                str(execution_truth_root),
                "--environment",
                "PAPER",
            ],
        )
    )
    steps.append(
        _run_step(
            "runtime_service_authority_v1",
            [
                sys.executable,
                "ops/tools/run_runtime_service_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--expected_run_mode",
                "MANUAL",
            ],
        )
    )
    steps.append(
        _run_step(
            "paper_trading_day_authority_v1.precheck",
            [
                sys.executable,
                "ops/tools/run_paper_trading_day_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
            ],
        )
    )

    authority_path = _authority_path(truth_root=truth_root, day_utc=day_utc)
    authority_payload = _load_json(authority_path) if authority_path.exists() else {}
    authority_block = _evaluate_authority_block(authority_payload) if authority_payload else {}
    authority_state = str(authority_block.get("state") or "")
    authority_missing_inputs = (
        list(authority_payload.get("missing_or_stale_inputs") or [])
        if isinstance(authority_payload, dict)
        else []
    )

    early_missing_inputs = [
        item
        for item in authority_missing_inputs
        if str(item.get("logical_name") or "").strip() != "paper_session_authority_v1"
    ]
    session_authority_only_missing = bool(authority_missing_inputs) and not bool(early_missing_inputs)
    authority_not_ready_before_session = authority_state not in {"OPEN_READY", "AUTHORIZED_NOT_OPEN"}
    first_blocker_gate = str(authority_block.get("first_blocker_gate") or authority_block.get("first_missing_gate") or "").strip()
    if authority_not_ready_before_session and first_blocker_gate == "paper_session_authority_v1":
        steps.append(
            _run_step(
                "paper_session_bootstrap_v1.refresh_blocking_authority",
                [
                    sys.executable,
                    "ops/tools/run_paper_session_bootstrap_v1.py",
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(truth_root),
                    "--materialize",
                    "YES",
                    "--emit_report",
                    "YES",
                ],
            )
        )
        steps.append(
            _run_step(
                "paper_trading_day_authority_v1.refresh_after_session",
                [
                    sys.executable,
                    "ops/tools/run_paper_trading_day_authority_v1.py",
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(truth_root),
                ],
            )
        )
        authority_payload = _load_json(authority_path) if authority_path.exists() else {}
        authority_block = _evaluate_authority_block(authority_payload) if authority_payload else {}
        authority_state = str(authority_block.get("state") or "")
        authority_missing_inputs = (
            list(authority_payload.get("missing_or_stale_inputs") or [])
            if isinstance(authority_payload, dict)
            else []
        )
        early_missing_inputs = [
            item
            for item in authority_missing_inputs
            if str(item.get("logical_name") or "").strip() != "paper_session_authority_v1"
        ]
        session_authority_only_missing = bool(authority_missing_inputs) and not bool(early_missing_inputs)
        authority_not_ready_before_session = authority_state not in {"OPEN_READY", "AUTHORIZED_NOT_OPEN"}
    if (
        authority_state == "UNKNOWN"
        or bool(early_missing_inputs)
        or (authority_not_ready_before_session and not session_authority_only_missing)
    ):
        steps.append(
            _run_step(
                "submit_boundary_status_v1",
                [
                    sys.executable,
                    "ops/tools/run_submit_boundary_status_v1.py",
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(truth_root),
                ],
            )
        )
        steps.append(
            _run_step(
                "trading_day_control_plane_v1",
                [
                    sys.executable,
                    "ops/tools/run_trading_day_control_plane_v1.py",
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(truth_root),
                ],
            )
        )
        steps.append(
            _run_step(
                "c2_daily_operator_gate_v1",
                [
                    sys.executable,
                    "ops/tools/run_c2_daily_operator_gate_v1.py",
                    "--day_utc",
                    day_utc,
                ],
            )
        )
        out = {
            "status": "PREFLIGHT_BLOCKED",
            "day_utc": day_utc,
            "authority_state": authority_state or "UNKNOWN",
            "canonical_blocker": str(authority_block.get("canonical_blocker") or ""),
            "manifest_path": str((REPO_ROOT / "governance/04_DATA/MANIFESTS/C2/paper_day_manifest.v1.json").resolve()),
            "manifest_topological_order": manifest_topological_order,
            "required_artifacts": required_artifact_names,
            "diagnostic_artifacts": diagnostic_artifact_names,
            "required_missing_stale_failing_artifacts": _authority_required_issues(authority_payload),
            "diagnostic_only_mismatches": _authority_diagnostic_issues(authority_payload),
            "missing_artifact_path": str(authority_block.get("first_missing_path") or ""),
            "owning_gate": str(
                authority_block.get("first_missing_gate")
                or authority_block.get("first_blocker_gate")
                or ""
            ),
            "required_producer_command": str(
                authority_block.get("first_missing_cmd")
                or authority_block.get("first_blocker_cmd")
                or ""
            ),
            "operator_actionable": bool(
                authority_block.get("first_missing_operator_actionable") is True
                or authority_block.get("first_blocker_operator_actionable") is True
            ),
            "authority_path": str(authority_path),
            "steps": steps,
        }
        print(json.dumps(out, sort_keys=True))
        return 2

    # Step 2: evaluate downstream surfaces after preflight evidence/authority refresh.
    if _session_authority_already_current(authority_payload):
        steps.append(
            {
                "name": "paper_session_bootstrap_v1",
                "cmd": [
                    sys.executable,
                    "ops/tools/run_paper_session_bootstrap_v1.py",
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(truth_root),
                    "--materialize",
                    "YES",
                    "--emit_report",
                    "YES",
                ],
                "return_code": 0,
                "stdout": "SKIPPED_ALREADY_CURRENT",
                "stderr": "",
            }
        )
    else:
        steps.append(
            _run_step(
                "paper_session_bootstrap_v1",
                [
                    sys.executable,
                    "ops/tools/run_paper_session_bootstrap_v1.py",
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(truth_root),
                    "--materialize",
                    "YES",
                    "--emit_report",
                    "YES",
                ],
        )
    )
    if bool(args.reset_dry_run_submission_evidence):
        if not _append_reset_dry_run_submission_evidence_step(
            steps=steps,
            execution_truth_root=execution_truth_root,
            day_utc=day_utc,
            name="reset_dry_run_submission_evidence_v1.before_submit_boundary",
        ):
            print(
                json.dumps(
                    {
                        "status": "PREFLIGHT_BLOCKED",
                        "day_utc": day_utc,
                        "canonical_blocker": "DRY_RUN_SUBMISSION_EVIDENCE_RESET_FAILED",
                        "steps": steps,
                    },
                    sort_keys=True,
                )
            )
            return 2
    steps.append(
        _run_step(
            "submit_boundary_status_v1",
            [
                sys.executable,
                "ops/tools/run_submit_boundary_status_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
            ],
        )
    )
    steps.append(
        _run_step(
            "trading_day_control_plane_v1",
            [
                sys.executable,
                "ops/tools/run_trading_day_control_plane_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
            ],
        )
    )
    steps.append(
        _run_step(
            "c2_daily_operator_gate_v1",
            [
                sys.executable,
                "ops/tools/run_c2_daily_operator_gate_v1.py",
                "--day_utc",
                day_utc,
            ],
        )
    )
    steps.append(
        _run_step(
            "execution_mode_authority_v1.final",
            [
                sys.executable,
                "ops/tools/run_execution_mode_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--execution_root",
                str(execution_truth_root),
                "--environment",
                "PAPER",
            ],
        )
    )
    steps.append(
        _run_step(
            "runtime_service_authority_v1.final",
            [
                sys.executable,
                "ops/tools/run_runtime_service_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--expected_run_mode",
                "MANUAL",
            ],
        )
    )
    steps.append(
        _run_step(
            "paper_trading_day_authority_v1.final",
            [
                sys.executable,
                "ops/tools/run_paper_trading_day_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
            ],
        )
    )

    authority_payload = _load_json(authority_path) if authority_path.exists() else {}
    authority_block = _evaluate_authority_block(authority_payload) if authority_payload else {}
    out = {
        "status": "OK"
        if str(authority_block.get("state") or "").upper() == "OPEN_READY"
        else "PREFLIGHT_BLOCKED",
        "day_utc": day_utc,
        "authority_state": str(authority_block.get("state") or ""),
        "canonical_blocker": str(authority_block.get("canonical_blocker") or ""),
        "manifest_path": str((REPO_ROOT / "governance/04_DATA/MANIFESTS/C2/paper_day_manifest.v1.json").resolve()),
        "manifest_topological_order": manifest_topological_order,
        "required_artifacts": required_artifact_names,
        "diagnostic_artifacts": diagnostic_artifact_names,
        "required_missing_stale_failing_artifacts": _authority_required_issues(authority_payload),
        "diagnostic_only_mismatches": _authority_diagnostic_issues(authority_payload),
        "missing_artifact_path": str(authority_block.get("first_missing_path") or ""),
        "owning_gate": str(
            authority_block.get("first_missing_gate")
            or authority_block.get("first_blocker_gate")
            or ""
        ),
        "required_producer_command": str(
            authority_block.get("first_missing_cmd")
            or authority_block.get("first_blocker_cmd")
            or ""
        ),
        "operator_actionable": bool(
            authority_block.get("first_missing_operator_actionable") is True
            or authority_block.get("first_blocker_operator_actionable") is True
        ),
        "authority_path": str(authority_path),
        "steps": steps,
    }
    print(json.dumps(out, sort_keys=True))
    return 0 if out["status"] == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
