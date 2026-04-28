from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_ID = "C2_EXECUTION_MODE_AUTHORITY_V1"
SCHEMA_VERSION = 1
STATES = {"DRY_RUN_LOCKED", "PAPER_READY_NOT_TRANSMITTED", "PAPER_TRANSMIT_ENABLED", "SUBMIT_BLOCKED"}


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _sha256_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _coerce_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    try:
        parsed = int(str(value or "").strip())
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _broker_ids(payload: dict[str, Any] | None) -> tuple[int | None, int | None]:
    ids = payload.get("broker_ids") if isinstance(payload, dict) and isinstance(payload.get("broker_ids"), dict) else {}
    return _coerce_positive_int(ids.get("order_id")), _coerce_positive_int(ids.get("perm_id"))


def _mode_from_dry_run(value: Any) -> str:
    if value is True:
        return "DRY_RUN_LOCKED"
    if value is False:
        return "PAPER_TRANSMIT_ENABLED"
    text = str(value or "").strip().upper()
    if text in {"YES", "TRUE", "1", "DRY_RUN", "DRY_RUN_LOCKED"}:
        return "DRY_RUN_LOCKED"
    if text in {"NO", "FALSE", "0", "PAPER_TRANSMIT", "TRANSMIT", "PAPER_TRANSMIT_ENABLED"}:
        return "PAPER_TRANSMIT_ENABLED"
    if text == "PAPER_READY_NOT_TRANSMITTED":
        return "PAPER_READY_NOT_TRANSMITTED"
    if text == "SUBMIT_BLOCKED":
        return "SUBMIT_BLOCKED"
    if text == "LIVE":
        return "SUBMIT_BLOCKED"
    return ""


def _latest_submission_dirs(execution_root: Path, truth_root: Path, day_utc: str) -> list[Path]:
    out: list[Path] = []
    for root in (execution_root, truth_root):
        base = root / "execution_evidence_v1" / "submissions" / day_utc
        if base.exists() and base.is_dir():
            out.extend(path.resolve() for path in base.iterdir() if path.is_dir() and not path.name.startswith("_"))
    seen: set[str] = set()
    uniq: list[Path] = []
    for path in sorted(out):
        key = path.name
        if key in seen:
            continue
        seen.add(key)
        uniq.append(path)
    return uniq


def execution_mode_authority_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "execution_mode_authority_v1"
        / day_utc
        / "execution_mode_authority.v1.json"
    ).resolve()


def evaluate_execution_mode_authority_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path | None = None,
    environment: str = "PAPER",
    env: dict[str, str] | None = None,
    produced_utc: str | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root is not None else truth_root
    env = dict(os.environ if env is None else env)
    produced = produced_utc or _utc_now_iso()
    sources: list[dict[str, Any]] = []
    modes: set[str] = set()

    env_mode = str(env.get("C2_EXECUTION_MODE") or "").strip().upper()
    if env_mode:
        mapped_env_mode = _mode_from_dry_run(env_mode)
        if mapped_env_mode:
            sources.append({"source": "env:C2_EXECUTION_MODE", "mode": mapped_env_mode, "value": env_mode})
            modes.add(mapped_env_mode)
    dry_run_env = _mode_from_dry_run(env.get("C2_GOVERNED_SUBMIT_DRY_RUN"))
    if dry_run_env:
        sources.append({"source": "env:C2_GOVERNED_SUBMIT_DRY_RUN", "mode": dry_run_env, "value": env.get("C2_GOVERNED_SUBMIT_DRY_RUN")})
        modes.add(dry_run_env)

    boundary_path = truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"
    boundary = _read_json(boundary_path) or {}
    boundary_mode = ""
    if str(boundary.get("submit_mode_status") or "").strip().upper() == "DRY_RUN_COMPLETE":
        boundary_mode = "DRY_RUN_LOCKED"
    elif isinstance(boundary.get("broker_transmit_enabled"), bool):
        boundary_mode = "PAPER_TRANSMIT_ENABLED" if boundary.get("broker_transmit_enabled") else "PAPER_READY_NOT_TRANSMITTED"
    if boundary_mode:
        sources.append({"source": "submit_boundary_status_v1", "mode": boundary_mode, "path": str(boundary_path)})
        modes.add(boundary_mode)

    submission_index_path = execution_root / "submission_index_v1" / day_utc / "submission_index.v1.json"
    submission_index = _read_json(submission_index_path) or {}
    attempts = submission_index.get("attempts") if isinstance(submission_index.get("attempts"), list) else []
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        attempt_mode = ""
        if str(attempt.get("submit_mode_status") or "").strip().upper() == "DRY_RUN_COMPLETE":
            attempt_mode = "DRY_RUN_LOCKED"
        elif isinstance(attempt.get("broker_transmit_enabled"), bool):
            attempt_mode = "PAPER_TRANSMIT_ENABLED" if attempt.get("broker_transmit_enabled") else "PAPER_READY_NOT_TRANSMITTED"
        if attempt_mode:
            sources.append({"source": "submission_index_v1", "mode": attempt_mode, "path": str(submission_index_path), "submission_id": str(attempt.get("attempt_id") or "")})
            modes.add(attempt_mode)

    submissions: list[dict[str, Any]] = []
    for subdir in _latest_submission_dirs(execution_root, truth_root, day_utc):
        attempt_path = subdir / "broker_submit_attempt_v1.json"
        broker_path = subdir / "broker_submission_record.v2.json"
        attempt = _read_json(attempt_path) or {}
        broker = _read_json(broker_path) or {}
        submission_id = str(broker.get("submission_id") or attempt.get("submission_id") or subdir.name).strip()
        attempt_mode = _mode_from_dry_run(attempt.get("dry_run"))
        if attempt_mode:
            sources.append({"source": "broker_submit_attempt_v1", "mode": attempt_mode, "path": str(attempt_path), "submission_id": submission_id})
            modes.add(attempt_mode)
        order_id, perm_id = _broker_ids(broker)
        error = broker.get("error") if isinstance(broker.get("error"), dict) else {}
        if str(error.get("code") or "").strip().upper() == "DRY_RUN_NO_BROKER_ID":
            sources.append({"source": "broker_submission_record.v2", "mode": "DRY_RUN_LOCKED", "path": str(broker_path), "submission_id": submission_id})
            modes.add("DRY_RUN_LOCKED")
        submissions.append(
            {
                "submission_id": submission_id,
                "broker_submission_record_path": str(broker_path),
                "broker_submit_attempt_path": str(attempt_path),
                "broker_order_id": order_id,
                "broker_perm_id": perm_id,
                "broker_ids_present": order_id is not None and perm_id is not None,
            }
        )

    conflict = False
    unresolved_reason = ""
    if len(modes) > 1:
        conflict = True
        state = "SUBMIT_BLOCKED"
    elif modes:
        state = next(iter(modes))
    elif str(environment).strip().upper() == "LIVE":
        state = "SUBMIT_BLOCKED"
        unresolved_reason = "LIVE_ENVIRONMENT_NOT_PAPER_AUTHORIZED"
        sources.append({"source": "environment_argument", "mode": "SUBMIT_BLOCKED", "value": "LIVE"})
    else:
        state = "DRY_RUN_LOCKED"
        sources.append({"source": "default_paper_safety_policy", "mode": "DRY_RUN_LOCKED", "value": "no explicit transmit evidence"})

    broker_transmit_enabled = state == "PAPER_TRANSMIT_ENABLED"
    broker_ids_expected = broker_transmit_enabled
    fills_expected = broker_transmit_enabled
    missing_ids = [
        row
        for row in submissions
        if broker_ids_expected and not bool(row.get("broker_ids_present"))
    ]
    status = "PASS"
    first_blocker = ""
    if conflict:
        status = "FAIL"
        first_blocker = "EXECUTION_MODE_CONFLICT"
    elif state == "SUBMIT_BLOCKED":
        status = "FAIL"
        first_blocker = unresolved_reason or "EXECUTION_MODE_SUBMIT_BLOCKED"
    elif missing_ids:
        status = "FAIL"
        first_blocker = "BROKER_IDS_MISSING_AFTER_TRANSMIT"

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "environment": str(environment).strip().upper() or "PAPER",
        "produced_utc": produced,
        "authority_scope": "EXECUTION_MODE_EXPECTATIONS",
        "status": status,
        "mode_state": state,
        "mode": state,
        "dry_run_policy": "NO" if state == "PAPER_TRANSMIT_ENABLED" else ("UNKNOWN" if state == "SUBMIT_BLOCKED" else "YES"),
        "broker_transmit_enabled": broker_transmit_enabled,
        "broker_ids_expected": broker_ids_expected,
        "fills_expected": fills_expected,
        "post_submit_lineage_gap_policy": "BLOCKER" if broker_transmit_enabled else "DIAGNOSTIC",
        "missing_broker_ids_diagnostic": state == "DRY_RUN_LOCKED",
        "missing_broker_ids_blocker": bool(missing_ids),
        "first_blocker": first_blocker,
        "mode_sources": sources,
        "submissions": submissions,
        "input_evidence": [
            {"artifact_type": "submit_boundary_status_v1", "path": str(boundary_path), "exists": boundary_path.exists()},
            {"artifact_type": "submission_index_v1", "path": str(submission_index_path), "exists": submission_index_path.exists()},
            {"artifact_type": "execution_evidence_v1/submissions", "path": str((execution_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()), "exists": (execution_root / "execution_evidence_v1" / "submissions" / day_utc).exists()},
            {"artifact_type": "execution_lifecycle_authority_v1", "path": str((execution_root / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json").resolve()), "exists": (execution_root / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json").exists()},
        ],
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_execution_mode_authority_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = execution_mode_authority_output_path(truth_root=truth_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(_canonical_bytes(payload) + b"\n")
    return output_path
