#!/usr/bin/env python3
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")

# ---- imports AFTER bootstrap ----

import argparse
import hashlib
import json
import os
import sys
from typing import Any, Dict, List, Tuple

from constellation_2.common.day_authority_decision_v1 import read_day_authority_decision_v1
from constellation_2.common.capability_state_v1 import (
    resolve_capability_state_path,
    resolve_paper_policy_verdict_path,
    resolve_policy_diff_path,
    resolve_production_policy_verdict_path,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    resolve_governed_account_binding,
    resolve_governed_sleeve_truth_bindings,
    resolve_pointer_bound_handshake_state,
    validate_trade_submit_readiness_status_obj,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    validate_against_repo_schema_v1,
)

REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
OUT_ROOT = (TRUTH_ROOT / "trade_submit_readiness_c2_v1").resolve()
OUT_DIR = OUT_ROOT

SCHEMA_STATUS = "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json"
SCHEMA_LATEST = "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.latest_pointer.v1.schema.json"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonical_json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _read_json(p: Path) -> Any:
    if not p.exists() or not p.is_file():
        raise SystemExit(f"FAIL: missing_required_file: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(payload)
    os.replace(str(tmp), str(path))


def _git_sha() -> str:
    try:
        import subprocess
        return subprocess.check_output(
            ["/usr/bin/git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
        ).decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _day_anchor_ts(day_utc: str) -> Tuple[str, str]:
    as_of = f"{day_utc}T00:00:00Z"
    expires = f"{day_utc}T00:02:00Z"
    return as_of, expires


def _today_utc_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _out_dir_for(environment: str, ib_account: str) -> Path:
    return (OUT_ROOT / str(environment).strip().upper() / str(ib_account).strip()).resolve()


def _history_out_dir_for(environment: str, ib_account: str, day_utc: str) -> Path:
    return (OUT_ROOT / "_history" / str(environment).strip().upper() / str(ib_account).strip() / str(day_utc).strip()).resolve()


def _load_day_authority(day_utc: str) -> tuple[Dict[str, Any] | None, Path, str | None]:
    path = (TRUTH_ROOT / "reports" / "day_authority_decision_v1" / day_utc / "day_authority_decision.v1.json").resolve()
    if not path.exists() or not path.is_file():
        return None, path, None
    payload = read_day_authority_decision_v1(truth_root=TRUTH_ROOT, trading_day=day_utc)
    return payload.payload, payload.path, payload.sha256


def _refresh_handshake_spine_for_day(*, day_utc: str) -> int:
    import ops.tools.run_ib_api_handshake_spine_v1 as handshake_module

    original_repo_root = handshake_module.REPO_ROOT
    original_truth_root = handshake_module.TRUTH_ROOT
    original_events_root = handshake_module.AUTH_BROKER_EVENTS_ROOT
    original_argv = list(sys.argv)
    try:
        handshake_module.REPO_ROOT = original_repo_root
        handshake_module.TRUTH_ROOT = TRUTH_ROOT
        handshake_module.AUTH_BROKER_EVENTS_ROOT = (TRUTH_ROOT / "execution_evidence_v1" / "broker_events").resolve()
        sys.argv = ["run_ib_api_handshake_spine_v1.py", "--day_utc", day_utc]
        return int(handshake_module.main())
    finally:
        sys.argv = original_argv
        handshake_module.REPO_ROOT = original_repo_root
        handshake_module.TRUTH_ROOT = original_truth_root
        handshake_module.AUTH_BROKER_EVENTS_ROOT = original_events_root


def _refresh_scoped_gate_for_day(*, truth_root: Path, day_utc: str, mode: str = "PAPER") -> int:
    import ops.tools.run_gate_stack_verdict_v1 as gate_module

    original_argv = list(sys.argv)
    try:
        sys.argv = [
            "run_gate_stack_verdict_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(Path(truth_root).resolve()),
            "--produced_utc",
            f"{day_utc}T00:00:00Z",
            "--mode",
            str(mode or "").strip().upper(),
        ]
        return int(gate_module.main())
    finally:
        sys.argv = original_argv


def _load_primary_scoped_gate_snapshot(*, repo_root: Path, environment: str, ib_account: str, day_utc: str) -> Dict[str, Any]:
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=repo_root,
        environment=environment,
        requested_ib_account=ib_account,
    )
    primary = None
    for binding in bindings:
        if str(binding.sleeve_id).strip().upper() == "PRIMARY":
            primary = binding
            break
    if primary is None:
        primary = bindings[0]

    head_path = (primary.truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()
    gate_path = (primary.truth_root / "reports" / "gate_stack_verdict_v1" / day_utc / "gate_stack_verdict.v1.json").resolve()
    input_manifest: List[Dict[str, Any]] = []
    if not gate_path.exists() or not gate_path.is_file():
        _refresh_scoped_gate_for_day(truth_root=primary.truth_root, day_utc=day_utc, mode=environment)
    if head_path.exists() and head_path.is_file():
        head = _read_json(head_path)
        input_manifest.append(
            {
                "type": f"canonical_authority_head_v1_scoped:{primary.sleeve_id}",
                "path": str(head_path),
                "sha256": _sha256_file(head_path),
            }
        )
        candidate_gate_path = Path(str(head.get("points_to") or "").strip()).resolve()
        head_day = str(head.get("day_utc") or "").strip()
        if (
            head_day == day_utc
            and candidate_gate_path.exists()
            and candidate_gate_path.is_file()
        ):
            gate_path = candidate_gate_path
    if not gate_path.exists() or not gate_path.is_file():
        raise ValueError(f"FINAL_GATE_STACK_NOT_PASS:sleeve_id={primary.sleeve_id}:reason=MISSING_GATE_STACK_AUTHORITY")
    gate = _read_json(gate_path)
    gate_status = str(gate.get("status") or "").strip().upper()
    gate_day = str(gate.get("day_utc") or day_utc).strip()
    input_manifest.append(
        {
            "type": f"gate_stack_verdict_v1_scoped:{primary.sleeve_id}",
            "path": str(gate_path),
            "sha256": _sha256_file(gate_path),
        }
    )
    if gate_day != day_utc:
        raise ValueError(
            f"FINAL_GATE_STACK_NOT_PASS:sleeve_id={primary.sleeve_id}:reason=GATE_STACK_DAY_MISMATCH:expected_day_utc={day_utc}:actual_day_utc={gate_day}"
        )
    return {
        "binding": primary,
        "gate_path": gate_path,
        "gate_sha256": _sha256_file(gate_path),
        "gate_payload": gate,
        "gate_status": gate_status or "MISSING",
        "input_manifest": input_manifest,
    }


def _resolve_primary_scoped_gate_state(*, repo_root: Path, environment: str, ib_account: str, day_utc: str) -> Dict[str, Any]:
    state = _load_primary_scoped_gate_snapshot(
        repo_root=repo_root,
        environment=environment,
        ib_account=ib_account,
        day_utc=day_utc,
    )
    gate_status = str(state.get("gate_status") or "").strip().upper()
    if gate_status != "PASS":
        raise ValueError(
            f"FINAL_GATE_STACK_NOT_PASS:sleeve_id={state['binding'].sleeve_id}:reason=GATE_STACK_STATUS_NOT_PASS:status={gate_status or 'MISSING'}"
        )
    return state


def _refresh_policy_stack_for_day(*, day_utc: str, ib_account: str, environment: str) -> None:
    import ops.tools.run_capability_state_v1 as capability_module
    import ops.tools.run_paper_policy_verdict_v1 as paper_policy_module
    import ops.tools.run_production_policy_verdict_v1 as production_policy_module
    import ops.tools.run_policy_diff_v1 as policy_diff_module

    capability_rc = int(
        capability_module.main(
            [
                "--day_utc",
                day_utc,
                "--truth_root",
                str(TRUTH_ROOT),
                "--environment",
                str(environment or "").strip().upper(),
                "--ib_account",
                ib_account,
            ]
        )
    )
    if capability_rc != 0:
        raise ValueError(f"CAPABILITY_STATE_REFRESH_FAILED:returncode={capability_rc}")

    paper_rc = int(
        paper_policy_module.main(
            [
                "--day_utc",
                day_utc,
                "--truth_root",
                str(TRUTH_ROOT),
            ]
        )
    )
    if paper_rc not in (0, 2):
        raise ValueError(f"PAPER_POLICY_REFRESH_FAILED:returncode={paper_rc}")

    production_rc = int(
        production_policy_module.main(
            [
                "--day_utc",
                day_utc,
                "--truth_root",
                str(TRUTH_ROOT),
                "--environment",
                str(environment or "").strip().upper(),
                "--ib_account",
                ib_account,
            ]
        )
    )
    if production_rc not in (0, 2):
        raise ValueError(f"PRODUCTION_POLICY_REFRESH_FAILED:returncode={production_rc}")

    diff_rc = int(
        policy_diff_module.main(
            [
                "--day_utc",
                day_utc,
                "--truth_root",
                str(TRUTH_ROOT),
            ]
        )
    )
    if diff_rc != 0:
        raise ValueError(f"POLICY_DIFF_REFRESH_FAILED:returncode={diff_rc}")


def _read_policy_artifact(*, path: Path, expected_schema_id: str, day_utc: str) -> Dict[str, Any]:
    payload = _read_json(path)
    if str(payload.get("schema_id") or "").strip() != expected_schema_id:
        raise ValueError(f"READINESS_POLICY_SCHEMA_ID_INVALID:path={path}")
    if str(payload.get("schema_version") or "").strip() != "v1":
        raise ValueError(f"READINESS_POLICY_SCHEMA_VERSION_INVALID:path={path}")
    if str(payload.get("day_utc") or "").strip() != day_utc:
        raise ValueError(f"READINESS_POLICY_DAY_MISMATCH:path={path}:requested_day_utc={day_utc}")
    return {
        "path": path,
        "sha256": _sha256_file(path),
        "payload": payload,
    }


def _build_session_authority_attestation(
    *,
    day_utc: str,
    reasons: List[str],
    day_authority_payload: Dict[str, Any] | None,
    day_authority_path: Path,
    day_authority_sha256: str | None,
    state: str,
) -> Dict[str, Any]:
    decision_state = "UNKNOWN"
    policy_action = "SKIP"
    stage_id = "PRE_ORCHESTRATION_PREFLIGHT"
    if day_authority_payload is not None:
        decision_state = "OK" if str(day_authority_payload.get("decision_state") or "").strip().upper() == "OPEN" else "BLOCKED"
        stage_id = str(day_authority_payload.get("stage") or stage_id).strip()
    return {
        "decision_artifact_path": str(day_authority_path),
        "decision_artifact_sha256": day_authority_sha256 or ("0" * 64),
        "policy_version": "validation_result_only",
        "evaluator_version": "validation_result_only",
        "venue": "C2",
        "session_date": day_utc,
        "decision_status": decision_state,
        "session_class": None,
        "stage_id": stage_id,
        "policy_action": policy_action,
        "stage_execution_status": state,
        "reason_codes": sorted(set(reasons)),
    }


def _build_run_state_authority_attestation(
    *,
    day_utc: str,
    reasons: List[str],
    state: str,
    day_authority_payload: Dict[str, Any] | None,
    day_authority_path: Path,
    day_authority_sha256: str | None,
    cycle_snapshot_family: str,
    cycle_snapshot_artifact_path: str,
    cycle_snapshot_artifact_sha256: str,
    cycle_coherence_status: str,
    upstream_refs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    decision_state = "UNKNOWN"
    classification_value = "UNKNOWN"
    if day_authority_payload is not None and str(day_authority_payload.get("decision_state") or "").strip().upper() == "OPEN":
        decision_state = "OK"
        classification_value = "OPEN"
    elif day_authority_payload is not None:
        decision_state = "BLOCKED"
        classification_value = "BLOCKED"
    if day_authority_payload is not None:
        upstream_refs.append(
            {
                "artifact_family": "day_authority_decision_v1",
                "artifact_path": str(day_authority_path),
                "artifact_sha256": day_authority_sha256 or ("0" * 64),
                "decision_status": decision_state,
                "generated_at": str(day_authority_payload.get("emitted_at") or ""),
                "reason_codes": list(day_authority_payload.get("blocking_evidence") or []),
                "classification_field": "decision_state",
                "classification_value": str(day_authority_payload.get("decision_state") or ""),
                "stage_id": str(day_authority_payload.get("stage") or ""),
            }
        )
    normalized_upstream = []
    for row in upstream_refs:
        if isinstance(row, dict) and "artifact_family" in row:
            normalized_upstream.append(row)
        elif isinstance(row, dict):
            normalized_upstream.append(
                {
                    "artifact_family": str(row.get("type") or "").strip(),
                    "artifact_path": str(row.get("path") or "").strip(),
                    "artifact_sha256": str(row.get("sha256") or "").strip(),
                    "decision_status": "OK",
                    "generated_at": f"{day_utc}T00:00:00Z",
                    "reason_codes": sorted(set(reasons)),
                }
            )
    return {
        "authority_family": "day_authority_decision_v1",
        "authority_artifact_path": str(day_authority_path),
        "authority_artifact_sha256": day_authority_sha256 or ("0" * 64),
        "policy_version": "validation_result_only",
        "evaluator_version": "validation_result_only",
        "decision_status": decision_state,
        "classification_field": "decision_state",
        "classification_value": classification_value,
        "cycle_snapshot_family": cycle_snapshot_family,
        "cycle_snapshot_artifact_path": cycle_snapshot_artifact_path,
        "cycle_snapshot_artifact_sha256": cycle_snapshot_artifact_sha256,
        "cycle_id": f"{day_utc}:{state}",
        "cycle_coherence_status": cycle_coherence_status,
        "stage_id": "TRADE_SUBMIT_READINESS",
        "stage_execution_status": state,
        "reason_codes": sorted(set(reasons)),
        "upstream_authority_refs": normalized_upstream,
    }


def _append_fail_reason(reasons: List[str], message: str) -> None:
    detail = f"FAIL:{message}"
    reasons.append(detail)
    base = detail.split(":", 2)
    if len(base) >= 2:
        short = ":".join(base[:2])
        if short != detail:
            reasons.append(short)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_trade_submit_readiness_c2_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--ib_account", required=True)
    ap.add_argument("--environment", required=True, choices=["PAPER", "LIVE"])
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    ib_account = str(args.ib_account).strip()
    env = str(args.environment).strip().upper()

    if not TRUTH_ROOT.exists():
        raise SystemExit(f"FAIL: truth_root_missing: {TRUTH_ROOT}")

    reasons: List[str] = []
    input_manifest: List[Dict[str, Any]] = []

    ok_registry = False
    ok_handshake = False
    ok_gate = False
    gate_state: Dict[str, Any] | None = None
    cycle_snapshot_family = "gate_stack_verdict_v1"
    cycle_snapshot_artifact_path = "UNAVAILABLE"
    cycle_snapshot_artifact_sha256 = "0" * 64
    cycle_coherence_status = "BLOCKED"
    cycle_upstream_refs: List[Dict[str, Any]] = []
    day_authority_payload, day_authority_path, day_authority_sha256 = _load_day_authority(day)

    try:
        account_binding = resolve_governed_account_binding(
            repo_root=REPO_ROOT,
            environment=env,
            requested_ib_account=ib_account,
        )
        ok_registry = True
    except ValueError as exc:
        account_binding = None
        _append_fail_reason(reasons, str(exc))

    registry_sha256 = account_binding.account_registry_sha256 if account_binding is not None else "UNAVAILABLE"
    sleeve_registry_sha256 = account_binding.sleeve_registry_sha256 if account_binding is not None else "UNAVAILABLE"
    if account_binding is not None:
        input_manifest.append(
            {
                "type": "ib_account_registry_v1",
                "path": str(account_binding.account_registry_path),
                "sha256": registry_sha256,
            }
        )
        input_manifest.append(
            {
                "type": "sleeve_registry_v1",
                "path": str(account_binding.sleeve_registry_path),
                "sha256": sleeve_registry_sha256,
            }
        )

    try:
        handshake = resolve_pointer_bound_handshake_state(
            truth_root=TRUTH_ROOT,
            day_utc=day,
            environment=env,
            ib_account=ib_account,
        )
        ok_handshake = True
        reasons.append("IB_API_HANDSHAKE_POINTER_OK")
        input_manifest.append(
            {
                "type": "ib_api_handshake_latest_pointer_v1",
                "path": str(handshake.pointer_path),
                "sha256": handshake.pointer_sha256,
            }
        )
        input_manifest.append(
            {
                "type": "ib_api_handshake_v1",
                "path": str(handshake.handshake_path),
                "sha256": handshake.handshake_sha256,
            }
        )
    except ValueError as exc:
        exc_text = str(exc)
        if exc_text.startswith("IB_API_HANDSHAKE_POINTER_MISSING:") or exc_text.startswith("IB_API_HANDSHAKE_STALE_POINTER:"):
            _refresh_handshake_spine_for_day(day_utc=day)
            try:
                handshake = resolve_pointer_bound_handshake_state(
                    truth_root=TRUTH_ROOT,
                    day_utc=day,
                    environment=env,
                    ib_account=ib_account,
                )
                ok_handshake = True
                reasons.append("IB_API_HANDSHAKE_POINTER_OK")
                input_manifest.append(
                    {
                        "type": "ib_api_handshake_latest_pointer_v1",
                        "path": str(handshake.pointer_path),
                        "sha256": handshake.pointer_sha256,
                    }
                )
                input_manifest.append(
                    {
                        "type": "ib_api_handshake_v1",
                        "path": str(handshake.handshake_path),
                        "sha256": handshake.handshake_sha256,
                    }
                )
            except ValueError as refreshed_exc:
                _append_fail_reason(reasons, str(refreshed_exc))
        else:
            _append_fail_reason(reasons, exc_text)

    try:
        gate_state = _load_primary_scoped_gate_snapshot(
            repo_root=REPO_ROOT,
            environment=env,
            ib_account=ib_account,
            day_utc=day,
        )
        input_manifest.extend(gate_state["input_manifest"])
        cycle_snapshot_artifact_path = str(gate_state["gate_path"])
        cycle_snapshot_artifact_sha256 = str(gate_state["gate_sha256"])
        cycle_coherence_status = "COHERENT"
        cycle_upstream_refs.extend(gate_state["input_manifest"])
    except ValueError as exc:
        _append_fail_reason(reasons, str(exc))

    if gate_state is not None:
        try:
            _refresh_policy_stack_for_day(day_utc=day, ib_account=ib_account, environment=env)
            capability_path = resolve_capability_state_path(truth_root=TRUTH_ROOT, day_utc=day)
            paper_policy_path = resolve_paper_policy_verdict_path(truth_root=TRUTH_ROOT, day_utc=day)
            production_policy_path = resolve_production_policy_verdict_path(truth_root=TRUTH_ROOT, day_utc=day)
            diff_path = resolve_policy_diff_path(truth_root=TRUTH_ROOT, day_utc=day)

            capability_state = _read_policy_artifact(path=capability_path, expected_schema_id="capability_state", day_utc=day)
            paper_policy_state = _read_policy_artifact(path=paper_policy_path, expected_schema_id="paper_policy_verdict", day_utc=day)
            production_policy_state = _read_policy_artifact(
                path=production_policy_path,
                expected_schema_id="production_policy_verdict",
                day_utc=day,
            )
            policy_diff_state = _read_policy_artifact(path=diff_path, expected_schema_id="policy_diff", day_utc=day)

            input_manifest.extend(
                [
                    {"type": "capability_state_v1", "path": str(capability_state["path"]), "sha256": capability_state["sha256"]},
                    {"type": "paper_policy_verdict_v1", "path": str(paper_policy_state["path"]), "sha256": paper_policy_state["sha256"]},
                    {
                        "type": "production_policy_verdict_v1",
                        "path": str(production_policy_state["path"]),
                        "sha256": production_policy_state["sha256"],
                    },
                    {"type": "policy_diff_v1", "path": str(policy_diff_state["path"]), "sha256": policy_diff_state["sha256"]},
                ]
            )
            cycle_snapshot_family = "paper_policy_verdict_v1"
            cycle_snapshot_artifact_path = str(paper_policy_state["path"])
            cycle_snapshot_artifact_sha256 = str(paper_policy_state["sha256"])
            cycle_upstream_refs.extend(
                [
                    {"type": "capability_state_v1", "path": str(capability_state["path"]), "sha256": capability_state["sha256"]},
                    {"type": "paper_policy_verdict_v1", "path": str(paper_policy_state["path"]), "sha256": paper_policy_state["sha256"]},
                    {
                        "type": "production_policy_verdict_v1",
                        "path": str(production_policy_state["path"]),
                        "sha256": production_policy_state["sha256"],
                    },
                    {"type": "policy_diff_v1", "path": str(policy_diff_state["path"]), "sha256": policy_diff_state["sha256"]},
                ]
            )

            paper_status = str(paper_policy_state["payload"].get("overall_status") or "").strip().upper()
            ok_gate = paper_status == "PASS"
            if ok_gate:
                reasons.append("PAPER_POLICY_VERDICT_OK")
            else:
                for item in paper_policy_state["payload"].get("blocking_items") or []:
                    capability_id = str(item.get("capability_id") or "UNKNOWN").strip()
                    item_status = str(item.get("status") or "UNKNOWN").strip().upper()
                    _append_fail_reason(reasons, f"PAPER_POLICY_NOT_PASS:{capability_id}:status={item_status}")

            production_status = str(production_policy_state["payload"].get("overall_status") or "").strip().upper()
            if production_status != "PASS":
                reasons.append("INFO:PRODUCTION_POLICY_NOT_PASS")
            for item in policy_diff_state["payload"].get("production_only_open_items") or []:
                item_id = str(item.get("capability_id") or item.get("item_id") or "").strip()
                if item_id:
                    reasons.append(f"INFO:PRODUCTION_ONLY_OPEN:{item_id}")
        except Exception as exc:
            reasons.append(f"INFO:PAPER_POLICY_VERDICT_UNAVAILABLE:{type(exc).__name__}")
            gate_status = str(gate_state.get("gate_status") or "").strip().upper()
            if gate_status == "PASS":
                ok_gate = True
            else:
                _append_fail_reason(
                    reasons,
                    f"FINAL_GATE_STACK_NOT_PASS:sleeve_id={gate_state['binding'].sleeve_id}:reason=GATE_STACK_STATUS_NOT_PASS:status={gate_status or 'MISSING'}",
                )

    if day_authority_payload is None:
        reasons.append("INFO:DAY_AUTHORITY_VALIDATION_MISSING")
    else:
        input_manifest.append(
            {
                "type": "day_authority_decision_v1",
                "path": str(day_authority_path),
                "sha256": day_authority_sha256,
            }
        )
        if str(day_authority_payload.get("decision_state") or "").strip().upper() != "OPEN":
            reasons.append(
                f"INFO:DAY_AUTHORITY_VALIDATION_BLOCKED:blocking_class={str(day_authority_payload.get('blocking_class') or 'UNKNOWN').strip()}"
            )

    ok = bool(ok_registry and ok_handshake and ok_gate)
    state = "OK" if ok else "FAIL"

    as_of_utc, expires_utc = _day_anchor_ts(day)
    session_authority_attestation = _build_session_authority_attestation(
        day_utc=day,
        reasons=reasons,
        day_authority_payload=day_authority_payload,
        day_authority_path=day_authority_path,
        day_authority_sha256=day_authority_sha256,
        state=state,
    )
    run_state_authority_attestation = _build_run_state_authority_attestation(
        day_utc=day,
        reasons=reasons,
        state=state,
        day_authority_payload=day_authority_payload,
        day_authority_path=day_authority_path,
        day_authority_sha256=day_authority_sha256,
        cycle_snapshot_family=cycle_snapshot_family,
        cycle_snapshot_artifact_path=cycle_snapshot_artifact_path,
        cycle_snapshot_artifact_sha256=cycle_snapshot_artifact_sha256,
        cycle_coherence_status=cycle_coherence_status,
        upstream_refs=cycle_upstream_refs,
    )

    status_obj: Dict[str, Any] = {
        "schema_id": "trade_submit_readiness_c2",
        "schema_version": "v1",
        "day_utc": day,
        "as_of_utc": as_of_utc,
        "expires_utc": expires_utc,
        "ok": ok,
        "state": state,
        "environment": env,
        "ib_account": ib_account,
        "reasons": sorted(set(reasons)),
        "input_manifest": input_manifest,
        "producer": {
            "repo": "constellation",
            "module": "ops/tools/run_trade_submit_readiness_c2_v1.py",
            "git_sha": _git_sha(),
        },
        "provenance": {
            "truth_root": str(TRUTH_ROOT),
            "registry_sha256": registry_sha256,
            "sleeve_registry_sha256": sleeve_registry_sha256,
        },
        "session_authority_attestation": session_authority_attestation,
        "run_state_authority_attestation": run_state_authority_attestation,
    }

    validate_trade_submit_readiness_status_obj(status_obj)
    validate_against_repo_schema_v1(status_obj, REPO_ROOT, SCHEMA_STATUS)

    status_bytes = _canonical_json_bytes(status_obj)
    status_sha = _sha256_bytes(status_bytes)

    latest_obj: Dict[str, Any] = {
        "schema_id": "trade_submit_readiness_c2_latest_pointer",
        "schema_version": "v1",
        "day_utc": day,
        "as_of_utc": as_of_utc,
        "expires_utc": expires_utc,
        "ok": ok,
        "state": state,
        "environment": env,
        "ib_account": ib_account,
        "target_path": "status.json",
        "target_sha256": status_sha,
        "producer": {
            "repo": "constellation",
            "module": "ops/tools/run_trade_submit_readiness_c2_v1.py",
            "git_sha": _git_sha(),
        },
        "provenance": {
            "truth_root": str(TRUTH_ROOT),
        },
    }

    validate_against_repo_schema_v1(latest_obj, REPO_ROOT, SCHEMA_LATEST)

    out_dir = _out_dir_for(env, ib_account)
    history_out_dir = _history_out_dir_for(env, ib_account, day)
    out_dir.mkdir(parents=True, exist_ok=True)
    history_out_dir.mkdir(parents=True, exist_ok=True)
    _atomic_write(history_out_dir / "status.json", status_bytes)
    _atomic_write(history_out_dir / "latest_pointer.v1.json", _canonical_json_bytes(latest_obj))
    _atomic_write(out_dir / "status.json", status_bytes)
    _atomic_write(out_dir / "latest_pointer.v1.json", _canonical_json_bytes(latest_obj))
    if env == "PAPER" and day == _today_utc_iso():
        OUT_ROOT.mkdir(parents=True, exist_ok=True)
        _atomic_write(OUT_ROOT / "status.json", status_bytes)
        _atomic_write(OUT_ROOT / "latest_pointer.v1.json", _canonical_json_bytes(latest_obj))

    print(
        "OK: TRADE_SUBMIT_READINESS_C2_V1 "
        f"state={state} ok={ok} "
        f"current_path={out_dir / 'status.json'} "
        f"history_path={history_out_dir / 'status.json'}"
    )
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
