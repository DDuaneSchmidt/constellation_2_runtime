#!/usr/bin/env python3
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
_CONSTITUTIONAL_REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()
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
from constellation_2.common.paper_session_fact_plane_v1 import resolve_market_calendar_record_v1
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
)
from constellation_2.common.capability_state_v1 import (
    resolve_capability_state_path,
    resolve_paper_policy_verdict_path,
    resolve_policy_diff_path,
    resolve_production_policy_verdict_path,
)
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.common.artifact_authority_v1 import assert_artifact_consumer_allowed_v1
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    resolve_governed_account_binding,
    resolve_governed_sleeve_truth_bindings,
    resolve_pointer_bound_handshake_state,
    validate_trade_submit_readiness_status_obj,
)
from constellation_2.common.economic_state_authority_v1 import run_economic_state_authority_v1
from constellation_2.common.runtime_authority_bridge_v1 import resolve_canonical_truth_root_bridge_v1
from constellation_2.common.safety_state_authority_v1 import safety_state_authority_output_path
from constellation_2.common.trading_day_readiness_authority_v1 import (
    PREOPEN_MODES,
    read_or_evaluate_trading_day_readiness_authority_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    validate_against_repo_schema_v1,
)

REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()
TRUTH_ROOT = resolve_canonical_truth_root_bridge_v1(caller="ops/tools/run_trade_submit_readiness_c2_v1.py").resolve()
OUT_ROOT = (TRUTH_ROOT / "trade_submit_readiness_c2_v1").resolve()
OUT_DIR = OUT_ROOT

SCHEMA_STATUS = "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json"
SCHEMA_LATEST = "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.latest_pointer.v1.schema.json"
ECONOMIC_PACKAGE_FAMILY = "economic_state_package_v1"
ECONOMIC_DRAWDOWN_BLOCK_LIMIT = Decimal("-0.100000")
READINESS_FRESHNESS_WINDOW_MINUTES_DEFAULT = 15


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


def _try_read_json(p: Path) -> Dict[str, Any] | None:
    if not p.exists() or not p.is_file():
        return None
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


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


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc_second_iso(ts: datetime) -> str:
    normalized = ts.astimezone(timezone.utc).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def _runtime_freshness_ts(*, freshness_window_minutes: int, now_utc: datetime | None = None) -> Tuple[str, str]:
    if freshness_window_minutes <= 0:
        raise ValueError("invalid_freshness_window_minutes")
    base = now_utc if now_utc is not None else _now_utc()
    base_utc = base.astimezone(timezone.utc).replace(microsecond=0)
    expires_utc = base_utc + timedelta(minutes=freshness_window_minutes)
    return _to_utc_second_iso(base_utc), _to_utc_second_iso(expires_utc)


def _prior_day_utc(day_utc: str) -> str:
    return (date.fromisoformat(day_utc) - timedelta(days=1)).isoformat()


def _prior_trading_day_utc(day_utc: str) -> str:
    current = date.fromisoformat(day_utc)
    for offset in range(1, 15):
        candidate = (current - timedelta(days=offset)).isoformat()
        try:
            calendar_state = resolve_market_calendar_record_v1(truth_root=TRUTH_ROOT, day_utc=candidate)
        except Exception:
            calendar_state = {"status": "ERROR", "record": None}
        record = calendar_state.get("record") if isinstance(calendar_state, dict) else None
        if (
            isinstance(calendar_state, dict)
            and str(calendar_state.get("status") or "").strip().upper() == "OK"
            and isinstance(record, dict)
            and record.get("is_trading_session") is True
        ):
            return candidate

    # Fail conservative if the governed calendar is unavailable: preserve the
    # prior behavior so missing calendar coverage cannot silently skip evidence.
    return _prior_day_utc(day_utc)


def _dedupe_reason_codes(reason_codes: List[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for code in reason_codes:
        text = str(code or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


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


def _build_day_authority_constitutional_ref(
    *,
    day_utc: str,
    day_authority_path: Path,
    day_authority_sha256: str | None,
) -> Dict[str, str]:
    path_text = str(day_authority_path or "").strip()
    sha_text = str(day_authority_sha256 or "").strip()
    if not path_text or not sha_text:
        raise ValueError(
            "DAY_AUTHORITY_DECISION_REF_MISSING:"
            f"day_utc={str(day_utc).strip()}:"
            f"path={path_text or '<missing>'}:"
            f"sha256_present={'YES' if bool(sha_text) else 'NO'}"
        )
    return {
        "artifact_id": "day_authority_decision_v1",
        "path": path_text,
        "sha256": sha_text,
        "artifact_class": "admission_result",
        "finality_state": "provisional",
    }


def _refresh_handshake_spine_for_day(*, day_utc: str, execution_truth_root: Path) -> int:
    import ops.tools.run_ib_api_handshake_spine_v1 as handshake_module

    original_repo_root = handshake_module.REPO_ROOT
    original_truth_root = handshake_module.TRUTH_ROOT
    original_events_root = handshake_module.AUTH_BROKER_EVENTS_ROOT
    original_argv = list(sys.argv)
    try:
        handshake_module.REPO_ROOT = original_repo_root
        handshake_module.TRUTH_ROOT = execution_truth_root
        handshake_module.AUTH_BROKER_EVENTS_ROOT = (execution_truth_root / "execution_evidence_v1" / "broker_events").resolve()
        sys.argv = ["run_ib_api_handshake_spine_v1.py", "--day_utc", day_utc]
        return int(handshake_module.main())
    finally:
        sys.argv = original_argv
        handshake_module.REPO_ROOT = original_repo_root
        handshake_module.TRUTH_ROOT = original_truth_root
        handshake_module.AUTH_BROKER_EVENTS_ROOT = original_events_root


def _refresh_authorization_convergence_for_day(
    *,
    day_utc: str,
    ib_account: str,
    environment: str,
    execution_truth_root: Path,
) -> int:
    import ops.tools.run_paper_startup_authorization_convergence_v1 as convergence_module

    original_argv = list(sys.argv)
    try:
        sys.argv = [
            "run_paper_startup_authorization_convergence_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(execution_truth_root),
            "--environment",
            str(environment or "").strip().upper(),
            "--ib_account",
            ib_account,
        ]
        return int(convergence_module.main())
    finally:
        sys.argv = original_argv


def _load_primary_scoped_authorization_snapshot(*, repo_root: Path, environment: str, ib_account: str, day_utc: str) -> Dict[str, Any]:
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
    authorization_path = (primary.truth_root / "reports" / "authorization_gate_verdict_v1" / day_utc / "authorization_gate_verdict.v1.json").resolve()
    input_manifest: List[Dict[str, Any]] = []
    if head_path.exists() and head_path.is_file():
        head = _read_json(head_path)
        input_manifest.append(
            {
                "type": f"canonical_authority_head_v1_scoped:{primary.sleeve_id}",
                "path": str(head_path),
                "sha256": _sha256_file(head_path),
            }
        )
        candidate_auth_path = Path(str(head.get("points_to") or "").strip()).resolve()
        head_day = str(head.get("day_utc") or "").strip()
        if (
            head_day == day_utc
            and "authorization_gate_verdict_v1" in str(candidate_auth_path)
            and candidate_auth_path.exists()
            and candidate_auth_path.is_file()
        ):
            authorization_path = candidate_auth_path
    if not authorization_path.exists() or not authorization_path.is_file():
        raise ValueError(f"AUTHORIZATION_VERDICT_NOT_PASS:sleeve_id={primary.sleeve_id}:reason=MISSING_AUTHORIZATION_VERDICT")
    authorization = _read_json(authorization_path)
    authorization_status = str(authorization.get("status") or "").strip().upper()
    authorization_day = str(authorization.get("day_utc") or day_utc).strip()
    input_manifest.append(
        {
            "type": f"authorization_gate_verdict_v1_scoped:{primary.sleeve_id}",
            "path": str(authorization_path),
            "sha256": _sha256_file(authorization_path),
        }
    )
    if authorization_day != day_utc:
        raise ValueError(
            "AUTHORIZATION_VERDICT_NOT_PASS:"
            f"sleeve_id={primary.sleeve_id}:reason=AUTHORIZATION_DAY_MISMATCH:expected_day_utc={day_utc}:actual_day_utc={authorization_day}"
        )
    return {
        "binding": primary,
        "authorization_path": authorization_path,
        "authorization_sha256": _sha256_file(authorization_path),
        "authorization_payload": authorization,
        "authorization_status": authorization_status or "MISSING",
        "input_manifest": input_manifest,
    }


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


def _decimal_or_none(raw: Any) -> Decimal | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _materialize_previous_day_economic_package(
    *,
    repo_root: Path,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
) -> Dict[str, Any]:
    prev_day_utc = _prior_trading_day_utc(day_utc)
    try:
        result = run_economic_state_authority_v1(
            repo_root=repo_root,
            operation_type="fresh_paper_entry_v1",
            day_utc=prev_day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            materialize=True,
            emit_package=True,
        )
    except Exception:
        return {
            "attempted": True,
            "reason_codes": [
                "BUNDLE_C_PREVIOUS_DAY_ECONOMIC_MATERIALIZATION_FAILED",
                "BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BOOTSTRAP_REQUIRED",
            ],
        }

    build_obj = result.get("build_obj") if isinstance(result.get("build_obj"), dict) else {}
    package_path = result.get("package_path")
    if package_path is not None and Path(package_path).exists():
        return {"attempted": True, "reason_codes": []}

    reason_codes = ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_MATERIALIZATION_BLOCKED"]
    if str(build_obj.get("closure_status") or "").strip().upper() != "COMPLETE":
        reason_codes.append("BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BOOTSTRAP_REQUIRED")
        first_blocker = build_obj.get("first_real_blocker") if isinstance(build_obj.get("first_real_blocker"), dict) else {}
        blocker_id = str(first_blocker.get("dependency_id") or "").strip()
        if blocker_id:
            reason_codes.append(f"BUNDLE_C_PREVIOUS_DAY_ECONOMIC_FIRST_BLOCKER:{blocker_id}")
    else:
        reason_codes.append("BUNDLE_C_PREVIOUS_DAY_ECONOMIC_PACKAGE_WRITE_MISSING")
    return {"attempted": True, "reason_codes": reason_codes}


def _load_previous_day_economic_package_state(
    *,
    execution_truth_root: Path,
    day_utc: str,
    repo_root: Path | None = None,
    sleeve_id: str = "",
    environment: str = "",
    ib_account: str = "",
) -> Dict[str, Any]:
    assert_artifact_consumer_allowed_v1(_CONSTITUTIONAL_REPO_ROOT, "economic_state_package_v1", "trade_submit_readiness_c2_v1")
    assert_artifact_consumer_allowed_v1(_CONSTITUTIONAL_REPO_ROOT, "economic_state_build_v1", "trade_submit_readiness_c2_v1")
    prev_day_utc = _prior_trading_day_utc(day_utc)
    package_root = (execution_truth_root / ECONOMIC_PACKAGE_FAMILY / prev_day_utc).resolve()
    unknown = {
        "status": "UNKNOWN",
        "source_day_utc": prev_day_utc,
        "package_path": "",
        "package_sha256": "",
        "build_path": "",
        "build_sha256": "",
        "drawdown_pct": None,
        "drawdown_guard_status": "UNKNOWN",
        "policy_baseline_comparison_vs_portfolio_return": None,
        "external_benchmark_underperformer_count": 0,
        "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_PACKAGE_MISSING"],
    }
    materialization_reason_codes: List[str] = []
    if not package_root.exists() or not package_root.is_dir():
        if repo_root is not None and sleeve_id and environment and ib_account:
            materialization = _materialize_previous_day_economic_package(
                repo_root=repo_root,
                day_utc=day_utc,
                sleeve_id=sleeve_id,
                environment=environment,
                ib_account=ib_account,
            )
            materialization_reason_codes = list(materialization.get("reason_codes") or [])
            package_root = (execution_truth_root / ECONOMIC_PACKAGE_FAMILY / prev_day_utc).resolve()
        if not package_root.exists() or not package_root.is_dir():
            return {**unknown, "reason_codes": _dedupe_reason_codes(unknown["reason_codes"] + materialization_reason_codes)}

    candidates = sorted(package_root.glob("*/economic_state_package.v1.json"))
    if not candidates:
        if repo_root is not None and sleeve_id and environment and ib_account and not materialization_reason_codes:
            materialization = _materialize_previous_day_economic_package(
                repo_root=repo_root,
                day_utc=day_utc,
                sleeve_id=sleeve_id,
                environment=environment,
                ib_account=ib_account,
            )
            materialization_reason_codes = list(materialization.get("reason_codes") or [])
            candidates = sorted(package_root.glob("*/economic_state_package.v1.json"))
        if not candidates:
            return {**unknown, "reason_codes": _dedupe_reason_codes(unknown["reason_codes"] + materialization_reason_codes)}
    if len(candidates) != 1:
        raise ValueError(
            f"BUNDLE_C_PREVIOUS_DAY_ECONOMIC_PACKAGE_AMBIGUOUS:day_utc={prev_day_utc}:count={len(candidates)}"
        )

    package_path = candidates[0].resolve()
    package_sha256 = _sha256_file(package_path)
    package_obj = _read_json(package_path)
    if str(package_obj.get("schema_id") or "").strip() != "economic_state_package":
        return {
            **unknown,
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_PACKAGE_SCHEMA_INVALID"],
        }
    if str(package_obj.get("day_utc") or "").strip() != prev_day_utc:
        return {
            **unknown,
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_PACKAGE_DAY_MISMATCH"],
        }
    if package_obj.get("sealed") is not True:
        return {
            **unknown,
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_PACKAGE_NOT_SEALED"],
        }

    build_ref = package_obj.get("build_ref") if isinstance(package_obj.get("build_ref"), dict) else {}
    build_path_text = str(build_ref.get("path") or "").strip()
    if not build_path_text:
        return {
            **unknown,
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BUILD_REF_MISSING"],
        }
    build_path = Path(build_path_text).resolve()
    if not build_path.exists() or not build_path.is_file():
        return {
            **unknown,
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "build_path": str(build_path),
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BUILD_MISSING"],
        }

    build_sha256 = _sha256_file(build_path)
    build_obj = _read_json(build_path)
    if str(build_obj.get("schema_id") or "").strip() != "economic_state_build":
        return {
            **unknown,
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "build_path": str(build_path),
            "build_sha256": build_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BUILD_SCHEMA_INVALID"],
        }
    if str(build_obj.get("day_utc") or "").strip() != prev_day_utc:
        return {
            **unknown,
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "build_path": str(build_path),
            "build_sha256": build_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BUILD_DAY_MISMATCH"],
        }
    if str(build_obj.get("closure_status") or "").strip().upper() != "COMPLETE":
        return {
            **unknown,
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "build_path": str(build_path),
            "build_sha256": build_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BUILD_NOT_COMPLETE"],
        }

    evaluation = build_obj.get("economic_evaluation")
    if not isinstance(evaluation, dict):
        return {
            **unknown,
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "build_path": str(build_path),
            "build_sha256": build_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_EVALUATION_MISSING"],
        }

    risk_state = evaluation.get("risk_state") if isinstance(evaluation.get("risk_state"), dict) else {}
    benchmark_state = evaluation.get("benchmark_state") if isinstance(evaluation.get("benchmark_state"), dict) else {}
    policy_baseline = benchmark_state.get("policy_baseline") if isinstance(benchmark_state.get("policy_baseline"), dict) else {}
    drawdown_pct = risk_state.get("drawdown_pct")
    drawdown_decimal = _decimal_or_none(drawdown_pct)
    drawdown_guard_status = "UNKNOWN"
    if drawdown_decimal is not None:
        drawdown_guard_status = "BLOCKED" if drawdown_decimal <= ECONOMIC_DRAWDOWN_BLOCK_LIMIT else "PASS"

    external_underperformer_count = 0
    for row in benchmark_state.get("external_benchmarks") or []:
        if not isinstance(row, dict):
            continue
        comparison = _decimal_or_none(row.get("comparison_vs_portfolio_return"))
        if comparison is not None and comparison < 0:
            external_underperformer_count += 1

    return {
        "status": "OK",
        "source_day_utc": prev_day_utc,
        "package_path": str(package_path),
        "package_sha256": package_sha256,
        "build_path": str(build_path),
        "build_sha256": build_sha256,
        "drawdown_pct": drawdown_pct,
        "drawdown_guard_status": drawdown_guard_status,
        "policy_baseline_comparison_vs_portfolio_return": policy_baseline.get("comparison_vs_portfolio_return"),
        "external_benchmark_underperformer_count": external_underperformer_count,
        "reason_codes": [],
    }


def _economic_state_from_safety_state(*, truth_root: Path, day_utc: str) -> tuple[Dict[str, Any] | None, Path]:
    path = safety_state_authority_output_path(truth_root=truth_root, day_utc=day_utc).resolve()
    payload = _try_read_json(path)
    if not isinstance(payload, dict) or str(payload.get("day_utc") or payload.get("target_day") or "").strip() != day_utc:
        return None, path
    drawdown_status = str(payload.get("drawdown_status") or "").strip().upper()
    drawdown_pct = payload.get("drawdown_pct")
    status = str(payload.get("status") or "").strip().upper()
    if drawdown_status not in {"PASS", "BLOCKED", "NAV_INVALID"}:
        return None, path
    reason_codes: List[str] = []
    if drawdown_status == "NAV_INVALID" or payload.get("nav_valid") is False:
        reason_codes.append("SAFETY_STATE_NAV_INVALID")
    if drawdown_status == "BLOCKED":
        reason_codes.append("SAFETY_STATE_DRAWDOWN_BLOCKED")
    canonical_blocker = str(payload.get("canonical_blocker") or "").strip().upper()
    if canonical_blocker in {"NAV_INVALID", "DRAWDOWN_LIMIT_EXCEEDED", "KILL_SWITCH_ACTIVE", "CAPITAL_RISK_ENVELOPE_NOT_PASS"} and not reason_codes:
        reason_codes.append(f"SAFETY_STATE_{canonical_blocker}")
    if status == "DEGRADED" and not reason_codes:
        blocker = str(payload.get("canonical_blocker") or status).strip().upper()
        if blocker:
            reason_codes.append(f"SAFETY_STATE_{blocker}")
    return {
        "status": "OK" if not reason_codes else "UNKNOWN",
        "source_day_utc": day_utc,
        "package_path": str(path),
        "package_sha256": _sha256_file(path),
        "build_path": str(path),
        "build_sha256": _sha256_file(path),
        "drawdown_pct": str(drawdown_pct) if drawdown_pct is not None else None,
        "drawdown_guard_status": "BLOCKED" if drawdown_status in {"BLOCKED", "NAV_INVALID"} else "PASS",
        "policy_baseline_comparison_vs_portfolio_return": None,
        "external_benchmark_underperformer_count": 0,
        "reason_codes": reason_codes,
    }, path


def main() -> int:
    global OUT_ROOT, OUT_DIR
    ap = argparse.ArgumentParser(prog="run_trade_submit_readiness_c2_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--ib_account", required=True)
    ap.add_argument("--environment", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--freshness_window_minutes", type=int, default=READINESS_FRESHNESS_WINDOW_MINUTES_DEFAULT)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    ib_account = str(args.ib_account).strip()
    env = str(args.environment).strip().upper()
    freshness_window_minutes = int(args.freshness_window_minutes)
    if freshness_window_minutes <= 0:
        raise SystemExit("FAIL: freshness_window_minutes must be > 0")

    if not TRUTH_ROOT.exists():
        raise SystemExit(f"FAIL: truth_root_missing: {TRUTH_ROOT}")

    reasons: List[str] = []
    input_manifest: List[Dict[str, Any]] = []

    ok_registry = False
    ok_handshake = False
    ok_gate = False
    ok_economic = True
    ok_mode = True
    readiness_payload: Dict[str, Any] = {}
    readiness_path = Path("")
    readiness_mode = ""
    safety_state_payload: Dict[str, Any] | None = None
    safety_state_path = Path("")
    authorization_state: Dict[str, Any] | None = None
    cycle_snapshot_family = "authorization_gate_verdict_v1"
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
        execution_root = resolve_sleeve_execution_root_v1(
            repo_root=REPO_ROOT,
            environment=env,
            ib_account=ib_account,
            sleeve_id="PRIMARY",
        )
        execution_truth_root = execution_root.execution_root_path.resolve()
        OUT_ROOT = (execution_truth_root / "trade_submit_readiness_c2_v1").resolve()
        OUT_DIR = OUT_ROOT
        input_manifest.append(
            {
                "type": "sleeve_execution_root_v1",
                "path": str(execution_truth_root),
                "sha256": "",
            }
        )
    except ValueError as exc:
        raise SystemExit(f"FAIL: {exc}")

    readiness_path, readiness_payload = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=day,
        truth_root=TRUTH_ROOT,
        execution_root=execution_truth_root,
        environment=env,
    )
    readiness_mode = str(readiness_payload.get("readiness_mode") or "").strip().upper()
    ok_mode = bool(readiness_payload.get("submit_allowed_by_mode") is True)
    input_manifest.append(
        {
            "type": "trading_day_readiness_authority_v1",
            "path": str(readiness_path),
            "sha256": _sha256_file(readiness_path) if readiness_path.exists() else "",
        }
    )
    if not ok_mode:
        _append_fail_reason(
            reasons,
            f"SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE:readiness_mode={readiness_mode or 'UNKNOWN'}",
        )

    if readiness_mode in PREOPEN_MODES or bool(readiness_payload.get("requires_live_account_truth") is not True):
        ok_handshake = True
        reasons.append(f"INFO:IB_API_HANDSHAKE_NOT_REQUIRED_BY_READINESS_MODE:{readiness_mode or 'UNKNOWN'}")
    else:
        try:
            handshake = resolve_pointer_bound_handshake_state(
                truth_root=execution_truth_root,
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
                _refresh_handshake_spine_for_day(day_utc=day, execution_truth_root=execution_truth_root)
                try:
                    handshake = resolve_pointer_bound_handshake_state(
                        truth_root=execution_truth_root,
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

    if not ok_mode:
        ok_gate = True
        reasons.append(f"INFO:AUTHORIZATION_CONVERGENCE_NOT_REQUIRED_BY_READINESS_MODE:{readiness_mode or 'UNKNOWN'}")
    else:
        try:
            convergence_rc = _refresh_authorization_convergence_for_day(
                day_utc=day,
                ib_account=ib_account,
                environment=env,
                execution_truth_root=execution_truth_root,
            )
            if convergence_rc not in (0, 2):
                raise ValueError(f"AUTHORIZATION_CONVERGENCE_REFRESH_FAILED:returncode={convergence_rc}")
            authorization_state = _load_primary_scoped_authorization_snapshot(
                repo_root=REPO_ROOT,
                environment=env,
                ib_account=ib_account,
                day_utc=day,
            )
            input_manifest.extend(authorization_state["input_manifest"])
            cycle_snapshot_artifact_path = str(authorization_state["authorization_path"])
            cycle_snapshot_artifact_sha256 = str(authorization_state["authorization_sha256"])
            cycle_coherence_status = "COHERENT"
            cycle_upstream_refs.extend(authorization_state["input_manifest"])
        except ValueError as exc:
            _append_fail_reason(reasons, str(exc))

    economic_state, safety_state_path = _economic_state_from_safety_state(truth_root=TRUTH_ROOT, day_utc=day)
    safety_state_payload = _try_read_json(safety_state_path)
    if economic_state is not None:
        input_manifest.append(
            {
                "type": "safety_state_authority_v1",
                "path": str(safety_state_path),
                "sha256": _sha256_file(safety_state_path),
            }
        )
        cycle_upstream_refs.append(
            {
                "type": "safety_state_authority_v1",
                "path": str(safety_state_path),
                "sha256": _sha256_file(safety_state_path),
            }
        )
        reasons.append("INFO:ECONOMIC_STATE_DERIVED_FROM_SAFETY_STATE_AUTHORITY")
    else:
        economic_state = _load_previous_day_economic_package_state(
            execution_truth_root=execution_truth_root,
            day_utc=day,
            repo_root=REPO_ROOT,
            sleeve_id=str(authorization_state["binding"].sleeve_id) if authorization_state is not None else "",
            environment=env,
            ib_account=ib_account,
        )
    if str(economic_state.get("package_path") or "").strip():
        input_manifest.append(
            {
                "type": "economic_state_package_v1",
                "path": str(economic_state["package_path"]),
                "sha256": str(economic_state.get("package_sha256") or ""),
            }
        )
        cycle_upstream_refs.append(
            {
                "type": "economic_state_package_v1",
                "path": str(economic_state["package_path"]),
                "sha256": str(economic_state.get("package_sha256") or ""),
            }
        )
    if str(economic_state.get("build_path") or "").strip() and str(economic_state.get("build_sha256") or "").strip():
        input_manifest.append(
            {
                "type": "economic_state_build_v1",
                "path": str(economic_state["build_path"]),
                "sha256": str(economic_state.get("build_sha256") or ""),
            }
        )
        cycle_upstream_refs.append(
            {
                "type": "economic_state_build_v1",
                "path": str(economic_state["build_path"]),
                "sha256": str(economic_state.get("build_sha256") or ""),
            }
        )
    if str(economic_state.get("status") or "").strip().upper() == "OK":
        if str(economic_state.get("drawdown_guard_status") or "").strip().upper() == "BLOCKED":
            ok_economic = False
            _append_fail_reason(
                reasons,
                "BUNDLE_C_DRAWDOWN_LIMIT_EXCEEDED:"
                f"source_day_utc={economic_state['source_day_utc']}:"
                f"drawdown_pct={economic_state.get('drawdown_pct')}:"
                f"limit_pct={str(ECONOMIC_DRAWDOWN_BLOCK_LIMIT)}",
            )
        policy_comparison = _decimal_or_none(
            economic_state.get("policy_baseline_comparison_vs_portfolio_return")
        )
        if policy_comparison is not None:
            if policy_comparison < 0:
                reasons.append("INFO:BUNDLE_C_POLICY_BASELINE_UNDERPERFORMANCE")
            elif policy_comparison > 0:
                reasons.append("INFO:BUNDLE_C_POLICY_BASELINE_OUTPERFORMANCE")
            else:
                reasons.append("INFO:BUNDLE_C_POLICY_BASELINE_INLINE")
        external_underperformer_count = int(economic_state.get("external_benchmark_underperformer_count") or 0)
        if external_underperformer_count > 0:
            reasons.append(
                f"INFO:BUNDLE_C_EXTERNAL_BENCHMARK_UNDERPERFORMANCE:count={external_underperformer_count}"
            )
    else:
        ok_economic = False
        _append_fail_reason(
            reasons,
            "BUNDLE_C_PREVIOUS_DAY_ECONOMIC_STATE_REQUIRED:"
            f"source_day_utc={economic_state['source_day_utc']}:"
            f"reason_codes={','.join(str(x) for x in (economic_state.get('reason_codes') or [])) or 'UNKNOWN'}",
        )

    if authorization_state is not None:
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
                ]
            )
            scoped_authorization_type = f"authorization_gate_verdict_v1_scoped:{authorization_state['binding'].sleeve_id}"
            cycle_snapshot_family = scoped_authorization_type
            cycle_snapshot_artifact_path = str(authorization_state["authorization_path"])
            cycle_snapshot_artifact_sha256 = str(authorization_state["authorization_sha256"])
            cycle_upstream_refs.extend(
                [
                    {
                        "type": scoped_authorization_type,
                        "path": str(authorization_state["authorization_path"]),
                        "sha256": authorization_state["authorization_sha256"],
                    },
                    {"type": "capability_state_v1", "path": str(capability_state["path"]), "sha256": capability_state["sha256"]},
                    {"type": "paper_policy_verdict_v1", "path": str(paper_policy_state["path"]), "sha256": paper_policy_state["sha256"]},
                ]
            )

            authorization_status = str(authorization_state.get("authorization_status") or "").strip().upper()
            paper_status = str(paper_policy_state["payload"].get("overall_status") or "").strip().upper()
            authorization_ok = authorization_status in {"PASS", "BOOTSTRAP_PASS"}
            ok_gate = authorization_ok and paper_status == "PASS"
            if not authorization_ok:
                _append_fail_reason(
                    reasons,
                    f"AUTHORIZATION_VERDICT_NOT_PASS:sleeve_id={authorization_state['binding'].sleeve_id}:status={authorization_status or 'MISSING'}",
                )
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
            authorization_status = str(authorization_state.get("authorization_status") or "").strip().upper()
            if authorization_status in {"PASS", "BOOTSTRAP_PASS"}:
                ok_gate = True
            else:
                _append_fail_reason(
                    reasons,
                    f"AUTHORIZATION_VERDICT_NOT_PASS:sleeve_id={authorization_state['binding'].sleeve_id}:status={authorization_status or 'MISSING'}",
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

    ok = bool(ok_registry and ok_handshake and ok_gate and ok_economic and ok_mode)
    state = "OK" if ok else "FAIL"

    as_of_utc, expires_utc = _runtime_freshness_ts(
        freshness_window_minutes=freshness_window_minutes,
        now_utc=_now_utc(),
    )
    reasons.append(f"INFO:READINESS_FRESHNESS_WINDOW_MINUTES:{freshness_window_minutes}")
    readiness_contract = assert_constitutional_writer_allowed_v1(
        _CONSTITUTIONAL_REPO_ROOT,
        "trade_submit_readiness_c2_v1",
        "ops/tools/run_trade_submit_readiness_c2_v1.py",
    )
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
    # paper_trading_day_authority_v1 now owns paper day readiness. The legacy
    # day_authority_decision_v1 surface is diagnostic/compatibility-only and
    # must not veto current-day trade-submit readiness when absent.
    constitutional_dependency_refs: List[Dict[str, str]] = []
    constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type="trade_submit_readiness_c2_v1",
        artifact_class=str(readiness_contract.get("artifact_class") or "").strip(),
        authority_id="trade_submit_readiness_c2_v1",
        declared_dependency_artifacts=[
            str(item).strip()
            for item in (readiness_contract.get("required_upstream_dependencies") or [])
            if str(item).strip()
        ],
        dependency_refs=constitutional_dependency_refs,
    )
    constitutional_lineage = build_governed_artifact_lineage_v1(
        artifact_type="trade_submit_readiness_c2_v1",
        artifact_version="v1",
        artifact_class=str(readiness_contract.get("artifact_class") or "").strip(),
        authority_id="trade_submit_readiness_c2_v1",
        producer_id="ops/tools/run_trade_submit_readiness_c2_v1.py",
        generated_at_utc=as_of_utc,
        effective_at_utc=as_of_utc,
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=constitutional_dependency_refs,
        policy_snapshot_refs=[],
        code_version=_git_sha(),
        run_id=f"{day}:{env}:{ib_account}",
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
        "constitutional_dependency_declaration": constitutional_dependency_declaration,
        "constitutional_lineage": constitutional_lineage,
        "provenance": {
            "truth_root": str(execution_truth_root),
            "registry_sha256": registry_sha256,
            "sleeve_registry_sha256": sleeve_registry_sha256,
        },
        "economic_state": {
            "status": str(economic_state.get("status") or "UNKNOWN"),
            "source_day_utc": str(economic_state.get("source_day_utc") or ""),
            "package_path": str(economic_state.get("package_path") or ""),
            "package_sha256": str(economic_state.get("package_sha256") or ""),
            "build_path": str(economic_state.get("build_path") or ""),
            "build_sha256": str(economic_state.get("build_sha256") or ""),
            "drawdown_pct": economic_state.get("drawdown_pct"),
            "drawdown_guard_status": str(economic_state.get("drawdown_guard_status") or "UNKNOWN"),
            "policy_baseline_comparison_vs_portfolio_return": economic_state.get(
                "policy_baseline_comparison_vs_portfolio_return"
            ),
            "external_benchmark_underperformer_count": int(
                economic_state.get("external_benchmark_underperformer_count") or 0
            ),
            "reason_codes": list(economic_state.get("reason_codes") or []),
        },
        "trading_day_readiness": {
            "path": str(readiness_path),
            "readiness_mode": readiness_mode,
            "submit_allowed_by_mode": bool(readiness_payload.get("submit_allowed_by_mode") is True),
            "requires_live_account_truth": bool(readiness_payload.get("requires_live_account_truth") is True),
            "requires_same_day_broker_event_log": bool(readiness_payload.get("requires_same_day_broker_event_log") is True),
            "requires_same_day_options_snapshot": bool(readiness_payload.get("requires_same_day_options_snapshot") is True),
            "canonical_blocker": str(readiness_payload.get("canonical_blocker") or ""),
            "evidence_policy_used": readiness_payload.get("evidence_policy") if isinstance(readiness_payload.get("evidence_policy"), dict) else {},
        },
        "safety_state_authority": {
            "path": str(safety_state_path),
            "status": str(safety_state_payload.get("status") or "") if isinstance(safety_state_payload, dict) else "",
            "canonical_blocker": str(safety_state_payload.get("canonical_blocker") or "") if isinstance(safety_state_payload, dict) else "",
            "drawdown_pct": safety_state_payload.get("drawdown_pct") if isinstance(safety_state_payload, dict) else None,
            "drawdown_status": str(safety_state_payload.get("drawdown_status") or "") if isinstance(safety_state_payload, dict) else "",
            "nav_valid": bool(safety_state_payload.get("nav_valid") is True) if isinstance(safety_state_payload, dict) else False,
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
            "truth_root": str(execution_truth_root),
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

    print(
        "OK: TRADE_SUBMIT_READINESS_C2_V1 "
        f"state={state} ok={ok} "
        f"current_path={out_dir / 'status.json'} "
        f"history_path={history_out_dir / 'status.json'}"
    )
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
