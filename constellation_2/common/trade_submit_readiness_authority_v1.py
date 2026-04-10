from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Tuple

from constellation_2.common.runtime_contract_v1 import resolve_truth_sleeves_root


@dataclass(frozen=True)
class GovernedAccountBinding:
    environment: str
    ib_account: str
    sleeve_ids: Tuple[str, ...]
    account_registry_path: Path
    account_registry_sha256: str
    sleeve_registry_path: Path
    sleeve_registry_sha256: str


@dataclass(frozen=True)
class GovernedSleeveTruthBinding:
    environment: str
    ib_account: str
    sleeve_id: str
    truth_partition: str
    truth_root: Path
    sleeve_registry_path: Path
    sleeve_registry_sha256: str


@dataclass(frozen=True)
class HandshakeAuthorityState:
    pointer_path: Path
    handshake_path: Path
    pointer_day_utc: str
    handshake_day_utc: str
    environment: str
    ib_account: str
    handshake_sha256: str
    pointer_sha256: str


@dataclass(frozen=True)
class TradeSubmitReadinessAuthorityState:
    status_path: Path
    payload: Dict[str, Any]
    environment: str
    ib_account: str
    day_utc: str


def validate_trade_submit_readiness_status_obj(obj: Dict[str, Any]) -> None:
    missing_fields: list[str] = []
    if not isinstance(obj, dict):
        raise ValueError("TRADE_SUBMIT_READINESS_CONTRACT_INVALID:top_level_not_object")

    required_top_level = (
        "schema_id",
        "schema_version",
        "day_utc",
        "as_of_utc",
        "expires_utc",
        "ok",
        "state",
        "environment",
        "ib_account",
        "reasons",
        "input_manifest",
        "producer",
        "provenance",
        "session_authority_attestation",
        "run_state_authority_attestation",
    )
    for field in required_top_level:
        if field not in obj:
            missing_fields.append(field)

    prov = obj.get("provenance")
    if not isinstance(prov, dict):
        missing_fields.append("provenance")
        prov = {}
    for field in ("truth_root", "registry_sha256", "sleeve_registry_sha256"):
        if not str(prov.get(field) or "").strip():
            missing_fields.append(f"provenance.{field}")
    session_attestation = obj.get("session_authority_attestation")
    if not isinstance(session_attestation, dict):
        missing_fields.append("session_authority_attestation")
    run_state_attestation = obj.get("run_state_authority_attestation")
    if not isinstance(run_state_attestation, dict):
        missing_fields.append("run_state_authority_attestation")
        run_state_attestation = {}
    for field in (
        "cycle_snapshot_family",
        "cycle_snapshot_artifact_path",
        "cycle_snapshot_artifact_sha256",
        "cycle_id",
        "cycle_coherence_status",
    ):
        if not str(run_state_attestation.get(field) or "").strip():
            missing_fields.append(f"run_state_authority_attestation.{field}")

    if str(obj.get("schema_id") or "").strip() != "trade_submit_readiness_c2":
        missing_fields.append("schema_id")
    if str(obj.get("schema_version") or "").strip() != "v1":
        missing_fields.append("schema_version")
    if not str(obj.get("day_utc") or "").strip():
        missing_fields.append("day_utc")
    if not str(obj.get("as_of_utc") or "").strip():
        missing_fields.append("as_of_utc")
    if not str(obj.get("expires_utc") or "").strip():
        missing_fields.append("expires_utc")
    if not isinstance(obj.get("reasons"), list):
        missing_fields.append("reasons")
    if not isinstance(obj.get("input_manifest"), list):
        missing_fields.append("input_manifest")
    if not isinstance(obj.get("producer"), dict):
        missing_fields.append("producer")

    if missing_fields:
        uniq = ",".join(sorted(set(missing_fields)))
        raise ValueError(f"TRADE_SUBMIT_READINESS_CONTRACT_INVALID:missing_fields={uniq}")


def read_trade_submit_readiness_authority_state(
    *,
    repo_root: Path,
    environment: str,
    ib_account: str,
    day_utc: str,
) -> TradeSubmitReadinessAuthorityState:
    env = str(environment or "").strip().upper()
    account = str(ib_account or "").strip()
    day = str(day_utc or "").strip()
    global_truth_root = (Path(repo_root).resolve() / "constellation_2" / "runtime" / "truth").resolve()
    current_path = (global_truth_root / "trade_submit_readiness_c2_v1" / env / account / "status.json").resolve()
    history_path = (global_truth_root / "trade_submit_readiness_c2_v1" / "_history" / env / account / day / "status.json").resolve()
    try:
        history_obj = _read_and_validate_trade_submit_readiness_obj(
            status_path=history_path,
            environment=env,
            ib_account=account,
            day_utc=day,
            expected_truth_root=global_truth_root,
        )
    except ValueError as exc:
        if current_path.exists() and current_path.is_file():
            try:
                _read_and_validate_trade_submit_readiness_obj(
                    status_path=current_path,
                    environment=env,
                    ib_account=account,
                    day_utc=str(_read_json_obj(current_path).get("day_utc") or "").strip(),
                    expected_truth_root=global_truth_root,
                    allow_day_mismatch=True,
                )
            except ValueError as current_exc:
                raise current_exc from exc
        raise

    if current_path.exists() and current_path.is_file():
        current_obj = _read_and_validate_trade_submit_readiness_obj(
            status_path=current_path,
            environment=env,
            ib_account=account,
            day_utc=str(_read_json_obj(current_path).get("day_utc") or "").strip(),
            expected_truth_root=global_truth_root,
            allow_day_mismatch=True,
        )
        current_day = str(current_obj.get("day_utc") or "").strip()
        if current_day == day:
            if _canonical_obj_bytes(current_obj) != _canonical_obj_bytes(history_obj):
                raise ValueError(
                    f"TRADE_SUBMIT_READINESS_ALIAS_DRIFT:requested_day_utc={day}:current_path={current_path}:history_path={history_path}"
                )

    return TradeSubmitReadinessAuthorityState(
        status_path=history_path,
        payload=history_obj,
        environment=env,
        ib_account=account,
        day_utc=day,
    )


def _canonical_obj_bytes(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _read_and_validate_trade_submit_readiness_obj(
    *,
    status_path: Path,
    environment: str,
    ib_account: str,
    day_utc: str,
    expected_truth_root: Path,
    allow_day_mismatch: bool = False,
) -> Dict[str, Any]:
    obj = _read_json_obj(status_path)
    validate_trade_submit_readiness_status_obj(obj)
    if str(obj.get("schema_id") or "").strip() != "trade_submit_readiness_c2":
        raise ValueError(f"TRADE_SUBMIT_READINESS_SCHEMA_ID_INVALID:path={status_path}")
    if str(obj.get("schema_version") or "").strip() != "v1":
        raise ValueError(f"TRADE_SUBMIT_READINESS_SCHEMA_VERSION_INVALID:path={status_path}")
    if str(obj.get("environment") or "").strip().upper() != environment:
        raise ValueError(f"TRADE_SUBMIT_READINESS_ENVIRONMENT_MISMATCH:path={status_path}")
    if str(obj.get("ib_account") or "").strip() != ib_account:
        raise ValueError(f"TRADE_SUBMIT_READINESS_ACCOUNT_MISMATCH:path={status_path}")
    payload_day = str(obj.get("day_utc") or "").strip()
    if not allow_day_mismatch and payload_day != day_utc:
        raise ValueError(f"TRADE_SUBMIT_READINESS_DAY_MISMATCH:path={status_path}:requested_day_utc={day_utc}:payload_day_utc={payload_day}")
    provenance = obj.get("provenance")
    truth_root_value = ""
    if isinstance(provenance, dict):
        truth_root_value = str(provenance.get("truth_root") or "").strip()
    if truth_root_value != str(expected_truth_root):
        raise ValueError(
            f"TRADE_SUBMIT_READINESS_NONAUTHORITATIVE:provenance_truth_root={truth_root_value!r}:expected={str(expected_truth_root)!r}:path={status_path}"
        )
    return obj


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ValueError(f"MISSING_FILE:path={path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"JSON_PARSE_ERROR:path={path}:err={type(exc).__name__}:{exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def _load_registry(path: Path, *, schema_id: str, schema_version: str) -> Dict[str, Any]:
    obj = _read_json_obj(path)
    if str(obj.get("schema_id") or "").strip() != schema_id or str(obj.get("schema_version") or "").strip() != schema_version:
        raise ValueError(
            f"REGISTRY_SCHEMA_MISMATCH:path={path}:schema_id={obj.get('schema_id')!r}:schema_version={obj.get('schema_version')!r}"
        )
    return obj


def resolve_governed_account_binding(
    *,
    repo_root: Path,
    environment: str,
    requested_ib_account: str = "",
    sleeve_id: str = "",
) -> GovernedAccountBinding:
    env = str(environment or "").strip().upper()
    requested = str(requested_ib_account or "").strip()
    requested_sleeve = str(sleeve_id or "").strip().upper()
    if env not in {"PAPER", "LIVE"}:
        raise ValueError(f"INVALID_ENVIRONMENT:environment={environment!r}")

    sleeve_registry_path = (repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
    account_registry_path = (repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
    sleeve_registry = _load_registry(sleeve_registry_path, schema_id="c2_sleeve_registry", schema_version="v1")
    account_registry = _load_registry(account_registry_path, schema_id="c2_ib_account_registry", schema_version="v1")

    sleeves = sleeve_registry.get("sleeves")
    if not isinstance(sleeves, list):
        raise ValueError("SLEEVE_REGISTRY_SLEEVES_NOT_LIST")
    accounts = account_registry.get("accounts")
    if not isinstance(accounts, list):
        raise ValueError("IB_ACCOUNT_REGISTRY_ACCOUNTS_NOT_LIST")

    active_sleeves = []
    for row in sleeves:
        if not isinstance(row, dict):
            continue
        if row.get("enabled") is not True:
            continue
        if str(row.get("mode") or "").strip().upper() != env:
            continue
        active_sleeves.append(row)

    if requested_sleeve:
        active_sleeves = [
            row
            for row in active_sleeves
            if str(row.get("sleeve_id") or "").strip().upper() == requested_sleeve
        ]
        if not active_sleeves:
            raise ValueError(f"SLEEVE_NOT_ACTIVE_FOR_ENV:sleeve_id={requested_sleeve}:environment={env}")

    account_to_sleeves: Dict[str, list[str]] = {}
    for row in active_sleeves:
        account_id = str(row.get("ib_account") or "").strip()
        sleeve_name = str(row.get("sleeve_id") or "").strip().upper()
        if not account_id:
            raise ValueError(f"ACTIVE_SLEEVE_MISSING_IB_ACCOUNT:sleeve_id={sleeve_name or 'UNKNOWN'}:environment={env}")
        account_to_sleeves.setdefault(account_id, []).append(sleeve_name)

    if not account_to_sleeves:
        raise ValueError(f"NO_ACTIVE_GOVERNED_ACCOUNTS:environment={env}")

    if requested:
        if requested not in account_to_sleeves:
            raise ValueError(
                f"GOVERNED_ACTIVE_ACCOUNT_MISMATCH:environment={env}:requested={requested}:active_accounts={sorted(account_to_sleeves)}"
            )
        selected_account = requested
    else:
        active_accounts = sorted(account_to_sleeves)
        if len(active_accounts) != 1:
            raise ValueError(f"GOVERNED_ACTIVE_ACCOUNT_AMBIGUOUS:environment={env}:active_accounts={active_accounts}")
        selected_account = active_accounts[0]

    entry = None
    for row in accounts:
        if not isinstance(row, dict):
            continue
        if str(row.get("account_id") or "").strip() == selected_account:
            entry = row
            break
    if entry is None:
        raise ValueError(f"ACCOUNT_NOT_IN_REGISTRY:environment={env}:account_id={selected_account}")
    if str(entry.get("environment") or "").strip().upper() != env:
        raise ValueError(f"ACCOUNT_ENVIRONMENT_MISMATCH:environment={env}:account_id={selected_account}")
    if entry.get("enabled_for_submission") is not True:
        raise ValueError(f"ACCOUNT_DISABLED_FOR_SUBMISSION:environment={env}:account_id={selected_account}")

    allowed_sleeves = entry.get("allowed_sleeve_ids")
    if requested_sleeve:
        if not isinstance(allowed_sleeves, list):
            raise ValueError(f"ACCOUNT_ALLOWED_SLEEVES_INVALID:account_id={selected_account}")
        allowed_sleeves_norm = {str(x).strip().upper() for x in allowed_sleeves if str(x).strip()}
        if requested_sleeve not in allowed_sleeves_norm:
            raise ValueError(
                f"SLEEVE_NOT_ALLOWED_FOR_ACCOUNT:sleeve_id={requested_sleeve}:account_id={selected_account}"
            )

    return GovernedAccountBinding(
        environment=env,
        ib_account=selected_account,
        sleeve_ids=tuple(sorted(account_to_sleeves[selected_account])),
        account_registry_path=account_registry_path,
        account_registry_sha256=_sha256_file(account_registry_path),
        sleeve_registry_path=sleeve_registry_path,
        sleeve_registry_sha256=_sha256_file(sleeve_registry_path),
    )


def resolve_governed_sleeve_truth_bindings(
    *,
    repo_root: Path,
    environment: str,
    requested_ib_account: str = "",
    sleeve_id: str = "",
) -> Tuple[GovernedSleeveTruthBinding, ...]:
    binding = resolve_governed_account_binding(
        repo_root=repo_root,
        environment=environment,
        requested_ib_account=requested_ib_account,
        sleeve_id=sleeve_id,
    )
    env = binding.environment
    requested_sleeve = str(sleeve_id or "").strip().upper()
    sleeve_registry_path = binding.sleeve_registry_path
    account_registry_path = binding.account_registry_path
    sleeve_registry = _load_registry(sleeve_registry_path, schema_id="c2_sleeve_registry", schema_version="v1")
    account_registry = _load_registry(account_registry_path, schema_id="c2_ib_account_registry", schema_version="v1")

    accounts = account_registry.get("accounts")
    if not isinstance(accounts, list):
        raise ValueError("IB_ACCOUNT_REGISTRY_ACCOUNTS_NOT_LIST")
    account_row = None
    for row in accounts:
        if not isinstance(row, dict):
            continue
        if str(row.get("account_id") or "").strip() == binding.ib_account:
            account_row = row
            break
    if account_row is None:
        raise ValueError(f"ACCOUNT_NOT_IN_REGISTRY:environment={env}:account_id={binding.ib_account}")

    allowed_sleeve_ids = account_row.get("allowed_sleeve_ids")
    if not isinstance(allowed_sleeve_ids, list):
        raise ValueError(f"ACCOUNT_ALLOWED_SLEEVES_INVALID:account_id={binding.ib_account}")
    allowed_sleeves_norm = {str(item).strip().upper() for item in allowed_sleeve_ids if str(item).strip()}

    sleeves = sleeve_registry.get("sleeves")
    if not isinstance(sleeves, list):
        raise ValueError("SLEEVE_REGISTRY_SLEEVES_NOT_LIST")

    scoped: list[GovernedSleeveTruthBinding] = []
    for row in sleeves:
        if not isinstance(row, dict):
            continue
        if row.get("enabled") is not True:
            continue
        row_mode = str(row.get("mode") or "").strip().upper()
        if row_mode != env:
            continue
        row_execution_mode = str(row.get("execution_mode") or "").strip().upper()
        row_status = str(row.get("status") or "").strip().upper()
        if row_execution_mode != "AUTO" or row_status != "PRODUCTION":
            continue
        row_account = str(row.get("ib_account") or "").strip()
        if row_account != binding.ib_account:
            continue
        row_sleeve_id = str(row.get("sleeve_id") or "").strip().upper()
        if requested_sleeve and row_sleeve_id != requested_sleeve:
            continue
        if row_sleeve_id not in allowed_sleeves_norm:
            raise ValueError(
                f"ACTIVE_SLEEVE_NOT_ALLOWED_FOR_ACCOUNT:sleeve_id={row_sleeve_id}:account_id={binding.ib_account}"
            )
        truth_partition = str(row.get("truth_partition") or "").strip()
        if not truth_partition:
            raise ValueError(f"SLEEVE_TRUTH_PARTITION_MISSING:sleeve_id={row_sleeve_id}")
        truth_root = (Path(repo_root).resolve() / "constellation_2" / "runtime" / truth_partition).resolve()
        if not truth_root.exists() or not truth_root.is_dir():
            raise ValueError(f"SLEEVE_TRUTH_ROOT_MISSING:sleeve_id={row_sleeve_id}:path={truth_root}")
        scoped.append(
            GovernedSleeveTruthBinding(
                environment=env,
                ib_account=binding.ib_account,
                sleeve_id=row_sleeve_id,
                truth_partition=truth_partition,
                truth_root=truth_root,
                sleeve_registry_path=sleeve_registry_path,
                sleeve_registry_sha256=binding.sleeve_registry_sha256,
            )
        )

    if not scoped:
        if requested_sleeve:
            raise ValueError(
                f"SLEEVE_TRUTH_BINDING_NOT_FOUND:sleeve_id={requested_sleeve}:environment={env}:account_id={binding.ib_account}"
            )
        raise ValueError(f"NO_ACTIVE_SLEEVE_TRUTH_BINDINGS:environment={env}:account_id={binding.ib_account}")
    return tuple(sorted(scoped, key=lambda item: item.sleeve_id))


def resolve_canonical_governed_sleeve_truth_root(binding: GovernedSleeveTruthBinding) -> Path:
    truth_partition = str(binding.truth_partition or "").strip().replace("\\", "/").lstrip("/")
    if not truth_partition:
        raise ValueError(f"SLEEVE_TRUTH_PARTITION_MISSING:sleeve_id={binding.sleeve_id}")
    relpath = truth_partition
    if relpath == "truth_sleeves":
        relpath = ""
    elif relpath.startswith("truth_sleeves/"):
        relpath = relpath[len("truth_sleeves/") :]
    canonical_root = resolve_truth_sleeves_root().resolve()
    resolved = (canonical_root / relpath).resolve()
    if not resolved.exists() or not resolved.is_dir():
        raise ValueError(f"SLEEVE_TRUTH_ROOT_MISSING_CANONICAL:sleeve_id={binding.sleeve_id}:path={resolved}")
    return resolved


def resolve_pointer_bound_handshake_state(
    *,
    truth_root: Path,
    day_utc: str,
    environment: str,
    ib_account: str,
) -> HandshakeAuthorityState:
    day = str(day_utc or "").strip()
    env = str(environment or "").strip().upper()
    account_id = str(ib_account or "").strip()
    pointer_path = (truth_root / "ib_api_handshake" / "latest_pointer.v1.json").resolve()
    if not pointer_path.exists() or not pointer_path.is_file():
        raise ValueError(f"IB_API_HANDSHAKE_POINTER_MISSING:path={pointer_path}")

    pointer_sha256 = _sha256_file(pointer_path)
    ptr = _read_json_obj(pointer_path)
    if str(ptr.get("schema_id") or "").strip() != "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1" or int(ptr.get("schema_version") or 0) != 1:
        raise ValueError(f"IB_API_HANDSHAKE_POINTER_SCHEMA_MISMATCH:path={pointer_path}")

    pointer_day = str(ptr.get("day_utc") or "").strip()
    expected_handshake_path = (truth_root / "ib_api_handshake" / day / "ib_api_handshake.v1.json").resolve()

    pointers = ptr.get("pointers")
    if not isinstance(pointers, dict):
        raise ValueError(f"IB_API_HANDSHAKE_POINTER_MALFORMED:path={pointer_path}")

    handshake_path_raw = str(pointers.get("handshake_path") or "").strip()
    if not handshake_path_raw:
        raise ValueError(f"IB_API_HANDSHAKE_POINTER_TARGET_MISSING:path={pointer_path}")
    pointer_handshake_path = Path(handshake_path_raw).resolve()

    if pointer_day == day:
        handshake_path = pointer_handshake_path
        if handshake_path != expected_handshake_path:
            raise ValueError(
                f"IB_API_HANDSHAKE_POINTER_TARGET_MISMATCH:expected={expected_handshake_path}:actual={handshake_path}"
            )
        if not handshake_path.exists() or not handshake_path.is_file():
            raise ValueError(f"IB_API_HANDSHAKE_ARTIFACT_MISSING:path={handshake_path}")
    else:
        if not expected_handshake_path.exists() or not expected_handshake_path.is_file():
            raise ValueError(f"IB_API_HANDSHAKE_STALE_POINTER:expected_day={day}:actual_day={pointer_day or 'MISSING'}")
        handshake_path = expected_handshake_path

    handshake_sha256 = _sha256_file(handshake_path)
    expected_sha256 = str(pointers.get("handshake_sha256") or "").strip()
    if pointer_day == day and expected_sha256 and expected_sha256 != handshake_sha256:
        raise ValueError(
            f"IB_API_HANDSHAKE_SHA256_MISMATCH:path={handshake_path}:expected={expected_sha256}:actual={handshake_sha256}"
        )

    handshake = _read_json_obj(handshake_path)
    if str(handshake.get("schema_id") or "").strip() != "C2_IB_API_HANDSHAKE_V1" or int(handshake.get("schema_version") or 0) != 1:
        raise ValueError(f"IB_API_HANDSHAKE_SCHEMA_MISMATCH:path={handshake_path}")

    handshake_day = str(handshake.get("day_utc") or "").strip()
    if handshake_day != day:
        raise ValueError(f"IB_API_HANDSHAKE_STALE_ARTIFACT:expected_day={day}:actual_day={handshake_day or 'MISSING'}")

    status = str(handshake.get("status") or "").strip().upper()
    ok = bool(handshake.get("ok") is True)
    if not ok or status not in {"OK", "PASS", "READY", "CONNECTED"}:
        raise ValueError(f"IB_API_HANDSHAKE_NOT_OK:path={handshake_path}:status={status or 'MISSING'}:ok={ok}")

    handshake_env_raw = str(handshake.get("environment") or "").strip().upper()
    handshake_env = handshake_env_raw or env
    if handshake_env != env:
        raise ValueError(
            f"IB_API_HANDSHAKE_ENVIRONMENT_MISMATCH:path={handshake_path}:expected={env}:actual={handshake_env}"
        )

    handshake_account_raw = str(handshake.get("ib_account") or handshake.get("account_id") or "").strip()
    handshake_account = handshake_account_raw or account_id
    if handshake_account != account_id:
        raise ValueError(
            f"IB_API_HANDSHAKE_ACCOUNT_MISMATCH:path={handshake_path}:expected={account_id}:actual={handshake_account}"
        )

    return HandshakeAuthorityState(
        pointer_path=pointer_path,
        handshake_path=handshake_path,
        pointer_day_utc=pointer_day,
        handshake_day_utc=handshake_day,
        environment=handshake_env,
        ib_account=handshake_account,
        handshake_sha256=handshake_sha256,
        pointer_sha256=pointer_sha256,
    )
