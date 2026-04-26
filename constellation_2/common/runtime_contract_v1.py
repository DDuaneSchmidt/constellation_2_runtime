from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.paper_session_fact_plane_v1 import read_validated_surface_v1
from constellation_2.common.release_current_shadow_validator_v1 import RELEASE_CURRENT_SCHEMA_RELPATH_V1
from constellation_2.common.release_current_shadow_validator_v1 import validate_release_current_shadow_v1
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]
ACTIVE_RUNTIME_CONTRACT_PATH = (
    Path("/home/node/constellation_runtime_data")
    / "runtime_contract_v1"
    / "active_runtime_contract.v1.json"
).resolve()
ACTIVE_RUNTIME_CONTRACT_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json"
)
RELEASE_MANIFEST_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json"
CANONICAL_POINTER_INDEX_NAME = "canonical_pointer_index.v1.jsonl"
ACTIVATION_IN_PROGRESS_LOCK = (
    Path("/home/node/constellation_runtime_data")
    / "activations_v1"
    / ".activation_in_progress.lock"
).resolve()


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_absolute_dir(path: Path, *, label: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_absolute():
        raise SystemExit(f"FAIL: {label} must be absolute: {resolved}")
    if not resolved.exists() or not resolved.is_dir():
        raise SystemExit(f"FAIL: {label} must exist and be a directory: {resolved}")
    return resolved


def _require_absolute_file(path: Path, *, label: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_absolute():
        raise SystemExit(f"FAIL: {label} must be absolute: {resolved}")
    if not resolved.exists() or not resolved.is_file():
        raise SystemExit(f"FAIL: {label} must exist and be a file: {resolved}")
    return resolved


def _under(path: Path, *, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _contract_path() -> Path:
    return _require_absolute_file(ACTIVE_RUNTIME_CONTRACT_PATH, label="ACTIVE_RUNTIME_CONTRACT_PATH")


def _release_current_path_for_contract(contract: dict[str, Any]) -> Path:
    canonical_truth_root = _require_absolute_dir(
        Path(str(contract.get("canonical_truth_root") or "")),
        label="canonical_truth_root",
    )
    return (canonical_truth_root / "release_current_v1" / "current.json").resolve()


def _require_text(value: Any, *, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SystemExit(f"FAIL: {code}")
    return text


def _require_hex64(value: Any, *, code: str) -> str:
    text = _require_text(value, code=code).lower()
    if len(text) != 64 or any(ch not in "0123456789abcdef" for ch in text):
        raise SystemExit(f"FAIL: {code}")
    return text


def _require_ref_mapping(value: Any, *, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SystemExit(f"FAIL: {code}")
    return value


def _require_authoritative_repo_root(contract: dict[str, Any]) -> Path:
    return _require_absolute_dir(
        Path(str(contract.get("authoritative_repo_root") or "")),
        label="authoritative_repo_root",
    )


def _activation_in_progress() -> bool:
    return ACTIVATION_IN_PROGRESS_LOCK.exists() and ACTIVATION_IN_PROGRESS_LOCK.is_file()


def _repo_local_truth_roots(authoritative_repo_root: Path) -> tuple[Path, Path]:
    return (
        (authoritative_repo_root / "constellation_2" / "runtime" / "truth").resolve(),
        (authoritative_repo_root / "constellation_2" / "runtime" / "truth_sleeves").resolve(),
    )


def _require_contract_truth_dir(
    path: Path,
    *,
    label: str,
    authoritative_repo_root: Path,
) -> Path:
    resolved = _require_absolute_dir(path, label=label)
    repo_truth_root, repo_truth_sleeves_root = _repo_local_truth_roots(authoritative_repo_root)
    if resolved == repo_truth_root or _under(resolved, root=repo_truth_root):
        raise SystemExit(f"FAIL: {label} must not use repo_local_truth_root: {resolved}")
    if resolved == repo_truth_sleeves_root or _under(resolved, root=repo_truth_sleeves_root):
        raise SystemExit(f"FAIL: {label} must not use repo_local_truth_sleeves_root: {resolved}")
    return resolved


def _load_active_runtime_contract_ref_or_fail(*, validate_shadow: bool) -> Any:
    contract_path = _contract_path()
    try:
        contract_ref = read_control_plane_surface_v1(domain="release", surface="active_runtime_contract")
    except Exception as exc:
        raise SystemExit(
            f"FAIL: active runtime contract unreadable path={contract_path} err={type(exc).__name__}:{exc}"
        ) from exc
    payload = dict(contract_ref.payload)
    if str(payload.get("status") or "").strip().upper() != "ACTIVE":
        raise SystemExit(f"FAIL: active runtime contract status must be ACTIVE path={contract_path}")
    if validate_shadow:
        canonical_truth_root = str(payload.get("canonical_truth_root") or "").strip()
        if not canonical_truth_root:
            raise SystemExit("FAIL: active runtime contract canonical_truth_root missing for shadow validation")
        manifest_ref = read_control_plane_surface_v1(domain="release", surface="release_manifest_active")
        configuration_state_ref = read_control_plane_surface_v1(
            domain="policy",
            surface="configuration_state_current",
            truth_root=canonical_truth_root,
        )
        validate_release_current_shadow_v1(
            active_runtime_contract=payload,
            release_manifest=dict(manifest_ref.payload),
            configuration_state=dict(configuration_state_ref.payload),
        )
    return contract_ref


def _load_release_current_ref_or_fail(*, contract_payload: Mapping[str, Any]) -> Any | None:
    current_path = _release_current_path_for_contract(dict(contract_payload))
    if not current_path.exists():
        return None
    try:
        return read_validated_surface_v1(
            path=current_path,
            schema_relpath=RELEASE_CURRENT_SCHEMA_RELPATH_V1,
        )
    except Exception as exc:
        raise SystemExit(
            f"FAIL: release_current_shadow_invalid path={current_path} err={type(exc).__name__}:{exc}"
        ) from exc


def _load_active_runtime_contract_ref_from_release_current_or_fail(*, release_current_payload: Mapping[str, Any]) -> Any:
    contract_ref_payload = _require_ref_mapping(
        release_current_payload.get("active_runtime_contract_ref"),
        code="release_current_active_runtime_contract_ref_invalid",
    )
    contract_path = _require_absolute_file(
        Path(_require_text(contract_ref_payload.get("path"), code="release_current_active_runtime_contract_ref_path_missing")),
        label="release_current.active_runtime_contract_ref.path",
    )
    schema_relpath = _require_text(
        contract_ref_payload.get("schema_relpath"),
        code="release_current_active_runtime_contract_ref_schema_relpath_missing",
    )
    expected_sha256 = _require_hex64(
        contract_ref_payload.get("sha256"),
        code="release_current_active_runtime_contract_ref_sha256_invalid",
    )
    try:
        runtime_contract_ref = read_validated_surface_v1(
            path=contract_path,
            schema_relpath=schema_relpath,
        )
    except Exception as exc:
        raise SystemExit(
            "FAIL: release_current_active_runtime_contract_ref_invalid "
            f"path={contract_path} err={type(exc).__name__}:{exc}"
        ) from exc
    actual_sha256 = _require_hex64(
        runtime_contract_ref.sha256,
        code="release_current_active_runtime_contract_ref_payload_sha256_invalid",
    )
    if actual_sha256 != expected_sha256:
        raise SystemExit(
            "FAIL: release_current_active_runtime_contract_ref_sha256_mismatch "
            f"path={contract_path} expected={expected_sha256} actual={actual_sha256}"
        )
    payload = dict(runtime_contract_ref.payload)
    if str(payload.get("status") or "").strip().upper() != "ACTIVE":
        raise SystemExit(f"FAIL: active runtime contract status must be ACTIVE path={contract_path}")
    return runtime_contract_ref


def _assert_release_current_contract_alignment(
    *,
    release_current_payload: Mapping[str, Any],
    runtime_contract_payload: Mapping[str, Any],
) -> None:
    checks = (
        "release_id",
        "git_sha",
        "authoritative_repo_root",
        "release_root",
        "runtime_data_root",
        "canonical_truth_root",
        "truth_sleeves_root",
        "pointer_index_family",
        "runtime_environment",
    )
    for field in checks:
        current_value = str(release_current_payload.get(field) or "").strip()
        contract_value = str(runtime_contract_payload.get(field) or "").strip()
        if current_value != contract_value:
            raise SystemExit(
                "FAIL: release_current_active_runtime_contract_ref_field_mismatch "
                f"field={field} release_current={current_value!r} contract={contract_value!r}"
            )

    current_allowed_truth_roots = sorted(
        str(Path(str(item)).resolve())
        for item in (release_current_payload.get("allowed_truth_roots") or [])
    )
    contract_allowed_truth_roots = sorted(
        str(Path(str(item)).resolve())
        for item in (runtime_contract_payload.get("allowed_truth_roots") or [])
    )
    if current_allowed_truth_roots != contract_allowed_truth_roots:
        raise SystemExit("FAIL: release_current_active_runtime_contract_ref_allowed_truth_roots_mismatch")

    current_identity_ref = dict(
        _require_ref_mapping(
            release_current_payload.get("primary_execution_identity_ref"),
            code="release_current_primary_execution_identity_ref_invalid",
        )
    )
    contract_identity_ref = dict(
        _require_ref_mapping(
            runtime_contract_payload.get("primary_execution_identity_ref"),
            code="runtime_contract_primary_execution_identity_ref_invalid",
        )
    )
    if current_identity_ref != contract_identity_ref:
        raise SystemExit("FAIL: release_current_active_runtime_contract_ref_primary_execution_identity_ref_mismatch")


def _log_active_runtime_contract_cutover_mismatch(
    *,
    legacy_contract_payload: Mapping[str, Any],
    release_current_contract_payload: Mapping[str, Any],
    release_current_payload: Mapping[str, Any],
) -> None:
    mismatches: list[dict[str, Any]] = []

    def _check(field: str, old_value: Any, new_value: Any) -> None:
        if old_value != new_value:
            mismatches.append(
                {
                    "field": field,
                    "old_value": old_value,
                    "new_value": new_value,
                }
            )

    for field in (
        "release_id",
        "git_sha",
        "authoritative_repo_root",
        "release_root",
        "runtime_data_root",
        "canonical_truth_root",
        "truth_sleeves_root",
        "pointer_index_family",
        "runtime_environment",
        "provenance_mode",
        "generated_at_utc",
        "status",
    ):
        _check(
            field,
            str(legacy_contract_payload.get(field) or ""),
            str(release_current_contract_payload.get(field) or ""),
        )

    _check(
        "allowed_truth_roots",
        sorted(str(item) for item in legacy_contract_payload.get("allowed_truth_roots") or []),
        sorted(str(item) for item in release_current_contract_payload.get("allowed_truth_roots") or []),
    )
    _check(
        "primary_execution_identity_ref",
        dict(legacy_contract_payload.get("primary_execution_identity_ref") or {}),
        dict(release_current_contract_payload.get("primary_execution_identity_ref") or {}),
    )

    if mismatches:
        LOGGER.warning(
            "active_runtime_contract_cutover_mismatch %s",
            json.dumps(
                {
                    "release_current_id": str(release_current_payload.get("release_current_id") or "").strip(),
                    "mismatches": mismatches,
                },
                sort_keys=True,
            ),
        )


def _load_active_runtime_contract_ref_release_current_first_or_fail(*, validate_shadow: bool) -> Any:
    deadline = time.monotonic() + 5.0
    while True:
        legacy_contract_ref = _load_active_runtime_contract_ref_or_fail(validate_shadow=False)
        legacy_contract_payload = dict(legacy_contract_ref.payload)
        release_current_ref = _load_release_current_ref_or_fail(contract_payload=legacy_contract_payload)
        if release_current_ref is None:
            release_current_path = _release_current_path_for_contract(legacy_contract_payload)
            raise SystemExit(f"FAIL: release_current_required_missing path={release_current_path}")

        release_current_payload = dict(release_current_ref.payload)
        try:
            release_current_contract_ref = _load_active_runtime_contract_ref_from_release_current_or_fail(
                release_current_payload=release_current_payload
            )
            release_current_contract_payload = dict(release_current_contract_ref.payload)
            _assert_release_current_contract_alignment(
                release_current_payload=release_current_payload,
                runtime_contract_payload=release_current_contract_payload,
            )
            _log_active_runtime_contract_cutover_mismatch(
                legacy_contract_payload=legacy_contract_payload,
                release_current_contract_payload=release_current_contract_payload,
                release_current_payload=release_current_payload,
            )
            break
        except SystemExit as exc:
            message = str(exc)
            transient_mismatch = "release_current_active_runtime_contract_ref_sha256_mismatch" in message
            if transient_mismatch and _activation_in_progress() and time.monotonic() < deadline:
                time.sleep(0.2)
                continue
            raise

    if validate_shadow:
        manifest_ref = read_control_plane_surface_v1(domain="release", surface="release_manifest_active")
        configuration_state_ref = read_control_plane_surface_v1(
            domain="policy",
            surface="configuration_state_current",
            truth_root=str(release_current_contract_payload.get("canonical_truth_root") or ""),
        )
        validate_release_current_shadow_v1(
            active_runtime_contract=release_current_contract_payload,
            release_manifest=dict(manifest_ref.payload),
            configuration_state=dict(configuration_state_ref.payload),
        )
    return release_current_contract_ref


def load_active_runtime_contract_or_fail() -> dict[str, Any]:
    return dict(_load_active_runtime_contract_ref_release_current_first_or_fail(validate_shadow=True).payload)


def resolve_runtime_data_root() -> Path:
    contract = load_active_runtime_contract_or_fail()
    return _require_absolute_dir(Path(str(contract["runtime_data_root"])), label="runtime_data_root")


def resolve_canonical_truth_root() -> Path:
    contract = load_active_runtime_contract_or_fail()
    authoritative_repo_root = _require_authoritative_repo_root(contract)
    return _require_contract_truth_dir(
        Path(str(contract["canonical_truth_root"])),
        label="canonical_truth_root",
        authoritative_repo_root=authoritative_repo_root,
    )


def resolve_truth_sleeves_root() -> Path:
    contract = load_active_runtime_contract_or_fail()
    authoritative_repo_root = _require_authoritative_repo_root(contract)
    return _require_contract_truth_dir(
        Path(str(contract["truth_sleeves_root"])),
        label="truth_sleeves_root",
        authoritative_repo_root=authoritative_repo_root,
    )


def _allowed_truth_roots(contract: dict[str, Any]) -> list[Path]:
    raw = contract.get("allowed_truth_roots")
    if not isinstance(raw, list) or not raw:
        raise SystemExit("FAIL: active runtime contract allowed_truth_roots must be non-empty list")
    authoritative_repo_root = _require_authoritative_repo_root(contract)
    roots = [
        _require_contract_truth_dir(
            Path(str(item)),
            label="allowed_truth_root",
            authoritative_repo_root=authoritative_repo_root,
        )
        for item in raw
    ]
    for required in (resolve_canonical_truth_root(), resolve_truth_sleeves_root()):
        if not any(root == required for root in roots):
            raise SystemExit(f"FAIL: active runtime contract missing required allowed_truth_root: {required}")
    return roots


def require_truth_root_under_contract(truth_root: Path) -> Path:
    contract = load_active_runtime_contract_or_fail()
    authoritative_repo_root = _require_authoritative_repo_root(contract)
    resolved = _require_contract_truth_dir(
        Path(truth_root),
        label="truth_root",
        authoritative_repo_root=authoritative_repo_root,
    )
    for allowed_root in _allowed_truth_roots(contract):
        if resolved == allowed_root or _under(resolved, root=allowed_root):
            return resolved
    raise SystemExit(f"FAIL: truth_root_outside_active_runtime_contract truth_root={resolved}")


def resolve_pointer_index_root(family: str | None = None) -> Path:
    contract = load_active_runtime_contract_or_fail()
    selected_family = str(contract.get("pointer_index_family") or "").strip()
    if family is not None and str(family).strip() != selected_family:
        raise SystemExit(
            f"FAIL: pointer_index_family_mismatch requested={family!r} active={selected_family!r}"
        )
    return (resolve_canonical_truth_root() / selected_family).resolve()


def resolve_pointer_index_root_for_truth_root(truth_root: Path, family: str | None = None) -> Path:
    contract = load_active_runtime_contract_or_fail()
    selected_family = str(contract.get("pointer_index_family") or "").strip()
    if family is not None and str(family).strip() != selected_family:
        raise SystemExit(
            f"FAIL: pointer_index_family_mismatch requested={family!r} active={selected_family!r}"
        )
    resolved_truth_root = require_truth_root_under_contract(Path(truth_root))
    return (resolved_truth_root / selected_family).resolve()


def resolve_pointer_index_path_for_truth_root(truth_root: Path, family: str | None = None) -> Path:
    return (resolve_pointer_index_root_for_truth_root(truth_root, family=family) / CANONICAL_POINTER_INDEX_NAME).resolve()


def resolve_release_provenance() -> dict[str, Any]:
    return _resolve_release_provenance_from_old_surfaces(validate_shadow=True)


def _resolve_release_provenance_from_old_surfaces(*, validate_shadow: bool) -> dict[str, Any]:
    contract_ref = _load_active_runtime_contract_ref_or_fail(validate_shadow=False)
    contract = dict(contract_ref.payload)
    provenance_mode = str(contract.get("provenance_mode") or "").strip()
    if provenance_mode != "release_manifest":
        raise SystemExit(f"FAIL: unsupported active runtime contract provenance_mode={provenance_mode!r}")
    release_root = _require_absolute_dir(Path(str(contract["release_root"])), label="release_root")
    manifest_ref = read_control_plane_surface_v1(domain="release", surface="release_manifest_active")
    manifest_path = _require_absolute_file(manifest_ref.path, label="release_manifest")
    manifest = dict(manifest_ref.payload)
    if validate_shadow:
        configuration_state_ref = read_control_plane_surface_v1(
            domain="policy",
            surface="configuration_state_current",
            truth_root=str(contract.get("canonical_truth_root") or ""),
        )
        validate_release_current_shadow_v1(
            active_runtime_contract=contract,
            release_manifest=manifest,
            configuration_state=dict(configuration_state_ref.payload),
        )

    release_id = str(manifest.get("release_id") or "").strip()
    git_sha = str(manifest.get("git_sha") or "").strip()
    if release_id != str(contract.get("release_id") or "").strip():
        raise SystemExit(
            f"FAIL: active runtime contract release_id mismatch contract={contract.get('release_id')!r} manifest={release_id!r}"
        )
    if git_sha != str(contract.get("git_sha") or "").strip():
        raise SystemExit(
            f"FAIL: active runtime contract git_sha mismatch contract={contract.get('git_sha')!r} manifest={git_sha!r}"
        )
    if str(manifest.get("release_root") or "") != str(release_root):
        raise SystemExit(
            f"FAIL: release manifest release_root mismatch manifest={manifest.get('release_root')!r} expected={release_root}"
        )
    return {
        "release_id": release_id,
        "git_sha": git_sha,
        "release_root": str(release_root),
        "release_manifest_path": str(manifest_path),
        "provenance_mode": provenance_mode,
        "resolved_at_utc": _utc_now(),
    }


def resolve_release_provenance_release_current_first_v1(*, caller: str) -> dict[str, Any]:
    contract_ref = _load_active_runtime_contract_ref_or_fail(validate_shadow=False)
    contract = dict(contract_ref.payload)
    current_path = _release_current_path_for_contract(contract)
    if current_path.exists():
        try:
            current_ref = read_validated_surface_v1(
                path=current_path,
                schema_relpath=RELEASE_CURRENT_SCHEMA_RELPATH_V1,
            )
            current = dict(current_ref.payload)
            release_id = str(current.get("release_id") or "").strip()
            git_sha = str(current.get("git_sha") or "").strip()
            release_root = _require_absolute_dir(
                Path(str(current.get("release_root") or "")),
                label="release_root",
            )
            manifest_path = _require_absolute_file(
                Path(str((current.get("release_manifest_ref") or {}).get("path") or "")),
                label="release_manifest_path",
            )
            if release_id and git_sha:
                LOGGER.info(
                    "release_current_cutover_active %s",
                    json.dumps(
                        {
                            "caller": str(caller or "").strip(),
                            "release_id": release_id,
                            "git_sha": git_sha,
                        },
                        sort_keys=True,
                    ),
                )
                return {
                    "release_id": release_id,
                    "git_sha": git_sha,
                    "release_root": str(release_root),
                    "release_manifest_path": str(manifest_path),
                    "provenance_mode": "release_current_v1",
                    "resolved_at_utc": _utc_now(),
                }
        except Exception:
            pass
    return _resolve_release_provenance_from_old_surfaces(validate_shadow=False)


def write_active_runtime_contract_bytes(payload: dict[str, Any]) -> bytes:
    validate_against_repo_schema_v1(payload, REPO_ROOT, ACTIVE_RUNTIME_CONTRACT_SCHEMA_RELPATH)
    return canonical_json_bytes_v1(payload) + b"\n"
