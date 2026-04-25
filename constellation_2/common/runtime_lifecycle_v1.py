from __future__ import annotations

import errno
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict

from constellation_2.common.runtime_identity_v1 import (
    load_active_runtime_identity_snapshot_v1,
)
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    validate_against_repo_schema_v1,
)


RUNTIME_LIFECYCLE_ACTIVE_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_lifecycle_active.v1.schema.json"
)
RUNTIME_LIFECYCLE_RECEIPT_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_lifecycle_receipt.v1.schema.json"
)
RUNTIME_LIFECYCLE_EXIT_RECEIPT_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_lifecycle_exit_receipt.v1.schema.json"
)

ADMISSION_REASON_GRANTED = "RUNTIME_LIFECYCLE_ADMITTED"
ADMISSION_REASON_ACTIVE_EXISTS = "RUNTIME_LIFECYCLE_ACTIVE_RUN_EXISTS"
ADMISSION_REASON_STALE_ACTIVE = "RUNTIME_LIFECYCLE_STALE_ACTIVE_RUN_PRESENT"
ADMISSION_REASON_ACTIVE_INVALID = "RUNTIME_LIFECYCLE_ACTIVE_STATE_INVALID"
ADMISSION_REASON_ACTIVE_MISSING = "RUNTIME_LIFECYCLE_ACTIVE_STATE_MISSING"
ADMISSION_REASON_ACTIVE_MISMATCH = "RUNTIME_LIFECYCLE_ACTIVE_RUN_MISMATCH"

UNIQUENESS_OUTCOME_GRANTED = "NEW_ACTIVE_RUN_REGISTERED"
UNIQUENESS_OUTCOME_DENIED_ACTIVE_EXISTS = "DENIED_ACTIVE_RUN_EXISTS"
UNIQUENESS_OUTCOME_DENIED_STALE_ACTIVE = "DENIED_STALE_ACTIVE_RUN_PRESENT"
UNIQUENESS_OUTCOME_DENIED_ACTIVE_INVALID = "DENIED_ACTIVE_STATE_INVALID"

EXIT_REASON_SUCCESS = "RUNTIME_LIFECYCLE_EXITED_SUCCESSFULLY"
EXIT_REASON_NONZERO = "RUNTIME_LIFECYCLE_EXITED_NONZERO"
EXIT_REASON_SIGNAL = "RUNTIME_LIFECYCLE_TERMINATED_BY_SIGNAL"
EXIT_REASON_PRELAUNCH = "RUNTIME_LIFECYCLE_PRELAUNCH_FAILURE"


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_now_compact() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _require_text(value: Any, *, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SystemExit(f"FAIL: runtime_lifecycle_{label}_missing")
    return text


def _require_positive_int(value: Any, *, label: str) -> int:
    try:
        parsed = int(value)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"FAIL: runtime_lifecycle_{label}_invalid") from exc
    if parsed < 1:
        raise SystemExit(f"FAIL: runtime_lifecycle_{label}_invalid")
    return parsed


def _sanitize_token(value: str) -> str:
    raw = _require_text(value, label="token").lower()
    sanitized = "".join(ch if ch.isalnum() else "_" for ch in raw).strip("_")
    sanitized = "_".join(part for part in sanitized.split("_") if part)
    return sanitized or "runtime_lifecycle"


def _sha256_hex(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _lock_acquire(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.write(fd, f"pid={os.getpid()}\n".encode("utf-8"))
    os.fsync(fd)
    return fd


def _lock_release(fd: int, lock_path: Path) -> None:
    try:
        os.close(fd)
    finally:
        try:
            os.unlink(str(lock_path))
        except FileNotFoundError:
            pass


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        raise


def _runtime_identity_ref(runtime_identity: Dict[str, Any]) -> Dict[str, Any]:
    primary_execution_ref = runtime_identity.get("primary_execution_identity_ref")
    if not isinstance(primary_execution_ref, dict):
        raise SystemExit("FAIL: runtime_lifecycle_primary_execution_identity_ref_invalid")
    return {
        "contract_path": _require_text(runtime_identity.get("contract_path"), label="runtime_identity.contract_path"),
        "contract_sha256": _require_text(
            runtime_identity.get("contract_sha256"),
            label="runtime_identity.contract_sha256",
        ),
        "authoritative_repo_root": _require_text(
            runtime_identity.get("authoritative_repo_root"),
            label="runtime_identity.authoritative_repo_root",
        ),
        "runtime_environment": _require_text(
            runtime_identity.get("runtime_environment"),
            label="runtime_identity.runtime_environment",
        ),
        "primary_execution_identity_ref": {
            "authority_owner": _require_text(
                primary_execution_ref.get("authority_owner"),
                label="runtime_identity.primary_execution_identity_ref.authority_owner",
            ),
            "sleeve_id": _require_text(
                primary_execution_ref.get("sleeve_id"),
                label="runtime_identity.primary_execution_identity_ref.sleeve_id",
            ),
        },
    }


def _derive_seam_id(*, entrypoint_name: str, service_name: str) -> str:
    seed = str(service_name or "").strip() or str(entrypoint_name or "").strip()
    return _sanitize_token(seed)


def _derive_run_id(*, seam_id: str, launcher_pid: int) -> str:
    return f"{_utc_now_compact()}__{_sanitize_token(seam_id)}__pid{int(launcher_pid)}"


def resolve_runtime_lifecycle_active_path(*, runtime_data_root: Path, seam_id: str) -> Path:
    return (
        Path(runtime_data_root).resolve()
        / "runtime_lifecycle_v1"
        / "active"
        / _require_text(seam_id, label="seam_id")
        / "current.json"
    ).resolve()


def resolve_runtime_lifecycle_receipt_path(*, runtime_data_root: Path, run_id: str) -> Path:
    return (
        Path(runtime_data_root).resolve()
        / "runtime_lifecycle_v1"
        / "receipts"
        / _require_text(run_id, label="run_id")
        / "runtime_lifecycle_receipt.v1.json"
    ).resolve()


def resolve_runtime_lifecycle_exit_receipt_path(*, runtime_data_root: Path, run_id: str) -> Path:
    return (
        Path(runtime_data_root).resolve()
        / "runtime_lifecycle_v1"
        / "exit_receipts"
        / _require_text(run_id, label="run_id")
        / "runtime_lifecycle_exit_receipt.v1.json"
    ).resolve()


def _resolve_runtime_lifecycle_lock_path(*, runtime_data_root: Path, seam_id: str) -> Path:
    return (
        Path(runtime_data_root).resolve()
        / "runtime_lifecycle_v1"
        / "active"
        / _require_text(seam_id, label="seam_id")
        / ".admission.lock"
    ).resolve()


def _load_runtime_lifecycle_active_payload(path: Path, *, repo_root: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"FAIL: runtime_lifecycle_active_state_read_failed: path={path} err={exc!r}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"FAIL: runtime_lifecycle_active_state_invalid: path={path}")
    validate_against_repo_schema_v1(payload, Path(repo_root).resolve(), RUNTIME_LIFECYCLE_ACTIVE_SCHEMA_RELPATH)
    return payload


def _write_runtime_lifecycle_active_payload(*, repo_root: Path, payload: Dict[str, Any], path: Path) -> Path:
    validate_against_repo_schema_v1(payload, Path(repo_root).resolve(), RUNTIME_LIFECYCLE_ACTIVE_SCHEMA_RELPATH)
    _atomic_write(path, canonical_json_bytes_v1(payload) + b"\n")
    return path


def _build_active_payload(
    *,
    runtime_identity: Dict[str, Any],
    seam_id: str,
    run_id: str,
    admitted_at_utc: str,
    entrypoint_name: str,
    entrypoint_path: Path,
    service_name: str,
    launcher_pid: int,
    startup_id: str = "",
    startup_receipt_path: str = "",
    startup_receipt_sha256: str = "",
    lifecycle_receipt_path: str = "",
    lifecycle_receipt_sha256: str = "",
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "schema_id": "runtime_lifecycle_active.v1",
        "schema_version": "v1",
        "seam_id": _require_text(seam_id, label="seam_id"),
        "status": "ACTIVE",
        "run_id": _require_text(run_id, label="run_id"),
        "admission_decision": "ADMITTED",
        "admitted_at_utc": _require_text(admitted_at_utc, label="admitted_at_utc"),
        "entrypoint_name": _require_text(entrypoint_name, label="entrypoint_name"),
        "entrypoint_path": str(Path(entrypoint_path).resolve()),
        "service_name": str(service_name or "").strip(),
        "launcher_pid": int(launcher_pid),
        "runtime_identity_ref": _runtime_identity_ref(runtime_identity),
    }
    if str(startup_id or "").strip():
        payload["startup_id"] = str(startup_id).strip()
        payload["startup_receipt_path"] = _require_text(
            startup_receipt_path,
            label="startup_receipt_path",
        )
        payload["startup_receipt_sha256"] = _require_text(
            startup_receipt_sha256,
            label="startup_receipt_sha256",
        )
    if str(lifecycle_receipt_path or "").strip():
        payload["lifecycle_receipt_path"] = _require_text(
            lifecycle_receipt_path,
            label="lifecycle_receipt_path",
        )
        payload["lifecycle_receipt_sha256"] = _require_text(
            lifecycle_receipt_sha256,
            label="lifecycle_receipt_sha256",
        )
    return payload


def _build_lifecycle_receipt_payload(
    *,
    runtime_identity: Dict[str, Any],
    seam_id: str,
    run_id: str,
    entrypoint_name: str,
    entrypoint_path: Path,
    service_name: str,
    launcher_pid: int,
    status: str,
    reason_code: str,
    reason_summary: str,
    active_uniqueness_outcome: str,
    active_state_path: str,
    startup_id: str = "",
    startup_receipt_path: str = "",
    startup_receipt_sha256: str = "",
    conflicting_active_run: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "schema_id": "runtime_lifecycle_receipt.v1",
        "schema_version": "v1",
        "run_id": _require_text(run_id, label="run_id"),
        "seam_id": _require_text(seam_id, label="seam_id"),
        "status": _require_text(status, label="status"),
        "generated_at_utc": _utc_now_iso(),
        "entrypoint_name": _require_text(entrypoint_name, label="entrypoint_name"),
        "entrypoint_path": str(Path(entrypoint_path).resolve()),
        "service_name": str(service_name or "").strip(),
        "launcher_pid": int(launcher_pid),
        "reason_codes": [_require_text(reason_code, label="reason_code")],
        "admission_reason_code": _require_text(reason_code, label="reason_code"),
        "admission_reason_summary": _require_text(reason_summary, label="reason_summary"),
        "active_uniqueness_outcome": _require_text(
            active_uniqueness_outcome,
            label="active_uniqueness_outcome",
        ),
        "active_state_path": str(active_state_path or "").strip(),
        "runtime_identity_ref": _runtime_identity_ref(runtime_identity),
    }
    if str(startup_id or "").strip():
        payload["startup_identity_ref"] = {
            "startup_id": _require_text(startup_id, label="startup_id"),
            "startup_receipt_path": _require_text(
                startup_receipt_path,
                label="startup_receipt_path",
            ),
            "startup_receipt_sha256": _require_text(
                startup_receipt_sha256,
                label="startup_receipt_sha256",
            ),
        }
    if conflicting_active_run:
        payload["conflicting_active_run"] = conflicting_active_run
    return payload


def write_runtime_lifecycle_receipt_v1(
    *,
    repo_root: Path,
    payload: Dict[str, Any],
    runtime_data_root: Path,
) -> Path:
    path = resolve_runtime_lifecycle_receipt_path(
        runtime_data_root=Path(runtime_data_root).resolve(),
        run_id=_require_text(payload.get("run_id"), label="run_id"),
    )
    validate_against_repo_schema_v1(payload, Path(repo_root).resolve(), RUNTIME_LIFECYCLE_RECEIPT_SCHEMA_RELPATH)
    path.parent.mkdir(parents=True, exist_ok=False)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def _build_runtime_lifecycle_exit_receipt_payload(
    *,
    runtime_identity: Dict[str, Any],
    seam_id: str,
    run_id: str,
    entrypoint_name: str,
    entrypoint_path: Path,
    service_name: str,
    launcher_pid: int,
    launch_phase: str,
    wrapper_exit_code: int,
    termination_signal: str,
    active_state_path: str,
    released_at_utc: str,
    startup_identity_ref: Dict[str, Any] | None = None,
    lifecycle_start_ref: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_launch_phase = _require_text(launch_phase, label="launch_phase").upper()
    normalized_signal = str(termination_signal or "").strip().upper()
    exit_code = int(wrapper_exit_code)
    if normalized_launch_phase == "PRELAUNCH":
        exit_disposition = "PRELAUNCH_FAILURE"
        reason_code = EXIT_REASON_PRELAUNCH
        reason_summary = "runtime lifecycle admitted but wrapper exited before child launch"
    elif normalized_signal:
        exit_disposition = "SIGNAL_EXIT"
        reason_code = EXIT_REASON_SIGNAL
        reason_summary = f"runtime lifecycle wrapper terminated after signal {normalized_signal}"
    elif exit_code == 0:
        exit_disposition = "NORMAL_EXIT"
        reason_code = EXIT_REASON_SUCCESS
        reason_summary = "runtime lifecycle wrapper exited successfully"
    else:
        exit_disposition = "NONZERO_EXIT"
        reason_code = EXIT_REASON_NONZERO
        reason_summary = f"runtime lifecycle wrapper exited nonzero rc={exit_code}"

    payload: Dict[str, Any] = {
        "schema_id": "runtime_lifecycle_exit_receipt.v1",
        "schema_version": "v1",
        "run_id": _require_text(run_id, label="run_id"),
        "seam_id": _require_text(seam_id, label="seam_id"),
        "generated_at_utc": _utc_now_iso(),
        "entrypoint_name": _require_text(entrypoint_name, label="entrypoint_name"),
        "entrypoint_path": str(Path(entrypoint_path).resolve()),
        "service_name": str(service_name or "").strip(),
        "launcher_pid": int(launcher_pid),
        "launch_phase": normalized_launch_phase,
        "exit_disposition": exit_disposition,
        "wrapper_exit_code": exit_code,
        "reason_codes": [reason_code],
        "exit_reason_code": reason_code,
        "exit_reason_summary": reason_summary,
        "active_state_path": str(active_state_path or "").strip(),
        "active_state_release_status": "RELEASED",
        "released_at_utc": _require_text(released_at_utc, label="released_at_utc"),
        "runtime_identity_ref": _runtime_identity_ref(runtime_identity),
    }
    if normalized_signal:
        payload["termination_signal"] = normalized_signal
    if startup_identity_ref:
        payload["startup_identity_ref"] = startup_identity_ref
    if lifecycle_start_ref:
        payload["lifecycle_start_ref"] = lifecycle_start_ref
    return payload


def write_runtime_lifecycle_exit_receipt_v1(
    *,
    repo_root: Path,
    payload: Dict[str, Any],
    runtime_data_root: Path,
) -> Path:
    path = resolve_runtime_lifecycle_exit_receipt_path(
        runtime_data_root=Path(runtime_data_root).resolve(),
        run_id=_require_text(payload.get("run_id"), label="run_id"),
    )
    validate_against_repo_schema_v1(
        payload,
        Path(repo_root).resolve(),
        RUNTIME_LIFECYCLE_EXIT_RECEIPT_SCHEMA_RELPATH,
    )
    path.parent.mkdir(parents=True, exist_ok=False)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def request_runtime_lifecycle_admission_v1(
    *,
    repo_root: Path,
    entrypoint_name: str,
    entrypoint_path: Path,
    service_name: str,
    launcher_pid: int,
) -> Dict[str, Any]:
    runtime_identity = load_active_runtime_identity_snapshot_v1(repo_root=Path(repo_root).resolve())
    runtime_data_root = Path(
        _require_text(runtime_identity.get("runtime_data_root"), label="runtime_identity.runtime_data_root")
    ).resolve()
    launcher_pid_int = _require_positive_int(launcher_pid, label="launcher_pid")
    seam_id = _derive_seam_id(entrypoint_name=entrypoint_name, service_name=service_name)
    run_id = _derive_run_id(seam_id=seam_id, launcher_pid=launcher_pid_int)
    admitted_at_utc = _utc_now_iso()
    active_state_path = resolve_runtime_lifecycle_active_path(
        runtime_data_root=runtime_data_root,
        seam_id=seam_id,
    )
    lock_path = _resolve_runtime_lifecycle_lock_path(runtime_data_root=runtime_data_root, seam_id=seam_id)

    lock_fd = _lock_acquire(lock_path)
    try:
        if active_state_path.exists():
            conflicting_active_run: Dict[str, Any] | None = None
            if active_state_path.is_file():
                try:
                    existing = _load_runtime_lifecycle_active_payload(
                        active_state_path,
                        repo_root=Path(repo_root).resolve(),
                    )
                    existing_pid = _require_positive_int(
                        existing.get("launcher_pid"),
                        label="existing.launcher_pid",
                    )
                    existing_run_id = _require_text(existing.get("run_id"), label="existing.run_id")
                    conflicting_active_run = {
                        "run_id": existing_run_id,
                        "launcher_pid": existing_pid,
                        "active_state_path": str(active_state_path),
                    }
                    if _pid_is_running(existing_pid):
                        reason_code = ADMISSION_REASON_ACTIVE_EXISTS
                        reason_summary = (
                            f"runtime lifecycle active run already exists for seam {seam_id}"
                        )
                        uniqueness_outcome = UNIQUENESS_OUTCOME_DENIED_ACTIVE_EXISTS
                    else:
                        reason_code = ADMISSION_REASON_STALE_ACTIVE
                        reason_summary = (
                            f"stale runtime lifecycle active state present for seam {seam_id}"
                        )
                        uniqueness_outcome = UNIQUENESS_OUTCOME_DENIED_STALE_ACTIVE
                except SystemExit:
                    reason_code = ADMISSION_REASON_ACTIVE_INVALID
                    reason_summary = (
                        f"invalid runtime lifecycle active state present for seam {seam_id}"
                    )
                    uniqueness_outcome = UNIQUENESS_OUTCOME_DENIED_ACTIVE_INVALID
            else:
                reason_code = ADMISSION_REASON_ACTIVE_INVALID
                reason_summary = f"runtime lifecycle active path is not a file for seam {seam_id}"
                uniqueness_outcome = UNIQUENESS_OUTCOME_DENIED_ACTIVE_INVALID

            denied_payload = _build_lifecycle_receipt_payload(
                runtime_identity=runtime_identity,
                seam_id=seam_id,
                run_id=run_id,
                entrypoint_name=entrypoint_name,
                entrypoint_path=entrypoint_path,
                service_name=service_name,
                launcher_pid=launcher_pid_int,
                status="DENIED",
                reason_code=reason_code,
                reason_summary=reason_summary,
                active_uniqueness_outcome=uniqueness_outcome,
                active_state_path=str(active_state_path),
                conflicting_active_run=conflicting_active_run,
            )
            receipt_path = write_runtime_lifecycle_receipt_v1(
                repo_root=Path(repo_root).resolve(),
                payload=denied_payload,
                runtime_data_root=runtime_data_root,
            )
            return {
                "admission_decision": "DENIED",
                "reason_code": reason_code,
                "run_id": run_id,
                "seam_id": seam_id,
                "active_state_path": str(active_state_path),
                "receipt_path": str(receipt_path),
            }

        active_payload = _build_active_payload(
            runtime_identity=runtime_identity,
            seam_id=seam_id,
            run_id=run_id,
            admitted_at_utc=admitted_at_utc,
            entrypoint_name=entrypoint_name,
            entrypoint_path=entrypoint_path,
            service_name=service_name,
            launcher_pid=launcher_pid_int,
        )
        _write_runtime_lifecycle_active_payload(
            repo_root=Path(repo_root).resolve(),
            payload=active_payload,
            path=active_state_path,
        )
        return {
            "admission_decision": "ADMITTED",
            "reason_code": ADMISSION_REASON_GRANTED,
            "run_id": run_id,
            "seam_id": seam_id,
            "active_state_path": str(active_state_path),
        }
    finally:
        _lock_release(lock_fd, lock_path)


def record_runtime_lifecycle_start_v1(
    *,
    repo_root: Path,
    entrypoint_name: str,
    entrypoint_path: Path,
    service_name: str,
    launcher_pid: int,
    run_id: str,
    startup_id: str,
    startup_receipt_path: Path,
) -> Path:
    runtime_identity = load_active_runtime_identity_snapshot_v1(repo_root=Path(repo_root).resolve())
    runtime_data_root = Path(
        _require_text(runtime_identity.get("runtime_data_root"), label="runtime_identity.runtime_data_root")
    ).resolve()
    launcher_pid_int = _require_positive_int(launcher_pid, label="launcher_pid")
    seam_id = _derive_seam_id(entrypoint_name=entrypoint_name, service_name=service_name)
    active_state_path = resolve_runtime_lifecycle_active_path(
        runtime_data_root=runtime_data_root,
        seam_id=seam_id,
    )
    lock_path = _resolve_runtime_lifecycle_lock_path(runtime_data_root=runtime_data_root, seam_id=seam_id)
    startup_receipt = Path(startup_receipt_path).resolve()
    if not startup_receipt.exists():
        raise SystemExit(f"FAIL: runtime_lifecycle_startup_receipt_missing: {startup_receipt}")
    startup_receipt_sha256 = _sha256_hex(startup_receipt)

    lock_fd = _lock_acquire(lock_path)
    try:
        if not active_state_path.exists():
            raise SystemExit("FAIL: runtime_lifecycle_active_state_missing_for_record_start")
        active_payload = _load_runtime_lifecycle_active_payload(
            active_state_path,
            repo_root=Path(repo_root).resolve(),
        )
        if _require_text(active_payload.get("run_id"), label="active.run_id") != _require_text(
            run_id, label="run_id"
        ):
            raise SystemExit("FAIL: runtime_lifecycle_active_run_id_mismatch")
        if _require_positive_int(active_payload.get("launcher_pid"), label="active.launcher_pid") != launcher_pid_int:
            raise SystemExit("FAIL: runtime_lifecycle_active_launcher_pid_mismatch")

        receipt_payload = _build_lifecycle_receipt_payload(
            runtime_identity=runtime_identity,
            seam_id=seam_id,
            run_id=run_id,
            entrypoint_name=entrypoint_name,
            entrypoint_path=entrypoint_path,
            service_name=service_name,
            launcher_pid=launcher_pid_int,
            status="ADMITTED",
            reason_code=ADMISSION_REASON_GRANTED,
            reason_summary=f"runtime lifecycle admission granted for seam {seam_id}",
            active_uniqueness_outcome=UNIQUENESS_OUTCOME_GRANTED,
            active_state_path=str(active_state_path),
            startup_id=startup_id,
            startup_receipt_path=str(startup_receipt),
            startup_receipt_sha256=startup_receipt_sha256,
        )
        receipt_path = write_runtime_lifecycle_receipt_v1(
            repo_root=Path(repo_root).resolve(),
            payload=receipt_payload,
            runtime_data_root=runtime_data_root,
        )
        updated_active_payload = _build_active_payload(
            runtime_identity=runtime_identity,
            seam_id=seam_id,
            run_id=run_id,
            admitted_at_utc=_require_text(active_payload.get("admitted_at_utc"), label="active.admitted_at_utc"),
            entrypoint_name=entrypoint_name,
            entrypoint_path=entrypoint_path,
            service_name=service_name,
            launcher_pid=launcher_pid_int,
            startup_id=startup_id,
            startup_receipt_path=str(startup_receipt),
            startup_receipt_sha256=startup_receipt_sha256,
            lifecycle_receipt_path=str(receipt_path),
            lifecycle_receipt_sha256=_sha256_hex(receipt_path),
        )
        _write_runtime_lifecycle_active_payload(
            repo_root=Path(repo_root).resolve(),
            payload=updated_active_payload,
            path=active_state_path,
        )
        return receipt_path
    finally:
        _lock_release(lock_fd, lock_path)


def record_runtime_lifecycle_stop_v1(
    *,
    repo_root: Path,
    entrypoint_name: str,
    entrypoint_path: Path,
    service_name: str,
    launcher_pid: int,
    run_id: str,
    launch_phase: str,
    wrapper_exit_code: int,
    termination_signal: str = "",
) -> Path:
    runtime_identity = load_active_runtime_identity_snapshot_v1(repo_root=Path(repo_root).resolve())
    runtime_data_root = Path(
        _require_text(runtime_identity.get("runtime_data_root"), label="runtime_identity.runtime_data_root")
    ).resolve()
    launcher_pid_int = _require_positive_int(launcher_pid, label="launcher_pid")
    seam_id = _derive_seam_id(entrypoint_name=entrypoint_name, service_name=service_name)
    active_state_path = resolve_runtime_lifecycle_active_path(
        runtime_data_root=runtime_data_root,
        seam_id=seam_id,
    )
    exit_receipt_path = resolve_runtime_lifecycle_exit_receipt_path(
        runtime_data_root=runtime_data_root,
        run_id=_require_text(run_id, label="run_id"),
    )
    lock_path = _resolve_runtime_lifecycle_lock_path(runtime_data_root=runtime_data_root, seam_id=seam_id)

    lock_fd = _lock_acquire(lock_path)
    try:
        if exit_receipt_path.exists():
            return exit_receipt_path
        if not active_state_path.exists():
            raise SystemExit("FAIL: runtime_lifecycle_active_state_missing_for_record_stop")
        active_payload = _load_runtime_lifecycle_active_payload(
            active_state_path,
            repo_root=Path(repo_root).resolve(),
        )
        if _require_text(active_payload.get("run_id"), label="active.run_id") != _require_text(
            run_id, label="run_id"
        ):
            raise SystemExit("FAIL: runtime_lifecycle_stop_run_id_mismatch")
        if _require_positive_int(active_payload.get("launcher_pid"), label="active.launcher_pid") != launcher_pid_int:
            raise SystemExit("FAIL: runtime_lifecycle_stop_launcher_pid_mismatch")

        startup_identity_ref: Dict[str, Any] | None = None
        if str(active_payload.get("startup_id") or "").strip():
            startup_identity_ref = {
                "startup_id": _require_text(active_payload.get("startup_id"), label="active.startup_id"),
                "startup_receipt_path": _require_text(
                    active_payload.get("startup_receipt_path"),
                    label="active.startup_receipt_path",
                ),
                "startup_receipt_sha256": _require_text(
                    active_payload.get("startup_receipt_sha256"),
                    label="active.startup_receipt_sha256",
                ),
            }

        lifecycle_start_ref: Dict[str, Any] | None = None
        if str(active_payload.get("lifecycle_receipt_path") or "").strip():
            lifecycle_start_ref = {
                "receipt_path": _require_text(
                    active_payload.get("lifecycle_receipt_path"),
                    label="active.lifecycle_receipt_path",
                ),
                "receipt_sha256": _require_text(
                    active_payload.get("lifecycle_receipt_sha256"),
                    label="active.lifecycle_receipt_sha256",
                ),
            }

        released_at_utc = _utc_now_iso()
        active_state_path.unlink()
        receipt_payload = _build_runtime_lifecycle_exit_receipt_payload(
            runtime_identity=runtime_identity,
            seam_id=seam_id,
            run_id=_require_text(run_id, label="run_id"),
            entrypoint_name=entrypoint_name,
            entrypoint_path=entrypoint_path,
            service_name=service_name,
            launcher_pid=launcher_pid_int,
            launch_phase=launch_phase,
            wrapper_exit_code=int(wrapper_exit_code),
            termination_signal=termination_signal,
            active_state_path=str(active_state_path),
            released_at_utc=released_at_utc,
            startup_identity_ref=startup_identity_ref,
            lifecycle_start_ref=lifecycle_start_ref,
        )
        return write_runtime_lifecycle_exit_receipt_v1(
            repo_root=Path(repo_root).resolve(),
            payload=receipt_payload,
            runtime_data_root=runtime_data_root,
        )
    finally:
        _lock_release(lock_fd, lock_path)


def release_runtime_lifecycle_active_state_v1(
    *,
    repo_root: Path,
    entrypoint_name: str,
    service_name: str,
    run_id: str,
) -> Dict[str, Any]:
    runtime_identity = load_active_runtime_identity_snapshot_v1(repo_root=Path(repo_root).resolve())
    runtime_data_root = Path(
        _require_text(runtime_identity.get("runtime_data_root"), label="runtime_identity.runtime_data_root")
    ).resolve()
    seam_id = _derive_seam_id(entrypoint_name=entrypoint_name, service_name=service_name)
    active_state_path = resolve_runtime_lifecycle_active_path(
        runtime_data_root=runtime_data_root,
        seam_id=seam_id,
    )
    lock_path = _resolve_runtime_lifecycle_lock_path(runtime_data_root=runtime_data_root, seam_id=seam_id)

    lock_fd = _lock_acquire(lock_path)
    try:
        if not active_state_path.exists():
            return {"release_status": "ABSENT", "active_state_path": str(active_state_path)}
        active_payload = _load_runtime_lifecycle_active_payload(
            active_state_path,
            repo_root=Path(repo_root).resolve(),
        )
        if _require_text(active_payload.get("run_id"), label="active.run_id") != _require_text(
            run_id, label="run_id"
        ):
            raise SystemExit("FAIL: runtime_lifecycle_release_run_id_mismatch")
        active_state_path.unlink()
        return {"release_status": "RELEASED", "active_state_path": str(active_state_path)}
    finally:
        _lock_release(lock_fd, lock_path)
