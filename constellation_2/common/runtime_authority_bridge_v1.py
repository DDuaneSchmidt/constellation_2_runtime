from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.paper_session_fact_plane_v1 import read_validated_surface_v1
from constellation_2.common.runtime_contract_v1 import (
    load_active_runtime_contract_or_fail,
    resolve_canonical_truth_root,
    resolve_release_provenance,
    resolve_runtime_data_root,
    resolve_truth_sleeves_root,
)
from constellation_2.common.truth_root_v1 import (
    AUTHORITY_MODE_GOVERNANCE_PRIMARY,
    ENV_VAR,
    resolve_authority_mode,
)


LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]
RELEASE_CURRENT_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/RUNTIME/release_current.v1.schema.json"


def _require_text(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SystemExit(f"FAIL: {code}")
    return text


def _under(path: Path, *, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _require_absolute_existing_dir(path: Path, *, label: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_absolute():
        raise SystemExit(f"FAIL: {label} must be absolute: {resolved}")
    if not resolved.exists() or not resolved.is_dir():
        raise SystemExit(f"FAIL: {label} must exist and be a directory: {resolved}")
    return resolved


def _require_under_repo(path: Path, *, label: str) -> Path:
    try:
        path.relative_to(REPO_ROOT.resolve())
    except ValueError as exc:
        raise SystemExit(f"FAIL: {label} must remain under active repo root: {path}") from exc
    return path


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
    resolved = _require_absolute_existing_dir(path, label=label)
    repo_truth_root, repo_truth_sleeves_root = _repo_local_truth_roots(authoritative_repo_root)
    if resolved == repo_truth_root or _under(resolved, root=repo_truth_root):
        raise SystemExit(f"FAIL: {label} must not use repo_local_truth_root: {resolved}")
    if resolved == repo_truth_sleeves_root or _under(resolved, root=repo_truth_sleeves_root):
        raise SystemExit(f"FAIL: {label} must not use repo_local_truth_sleeves_root: {resolved}")
    return resolved


def _require_allowed_truth_roots(
    *,
    raw: Any,
    canonical_truth_root: Path,
    truth_sleeves_root: Path,
    authoritative_repo_root: Path,
    code_prefix: str,
) -> list[Path]:
    if not isinstance(raw, list) or len(raw) < 2:
        raise SystemExit(f"FAIL: {code_prefix}_allowed_truth_roots_invalid")
    roots = [
        _require_contract_truth_dir(
            Path(str(item)),
            label=f"{code_prefix}_allowed_truth_root",
            authoritative_repo_root=authoritative_repo_root,
        )
        for item in raw
    ]
    for required in (canonical_truth_root, truth_sleeves_root):
        if not any(root == required for root in roots):
            raise SystemExit(
                f"FAIL: {code_prefix}_allowed_truth_roots_missing_required_root: {required}"
            )
    return roots


def _load_old_runtime_authority() -> dict[str, Any]:
    contract = load_active_runtime_contract_or_fail()
    authoritative_repo_root = _require_absolute_existing_dir(
        Path(_require_text(contract.get("authoritative_repo_root"), "OLD_RUNTIME_AUTHORITY_REPO_ROOT_MISSING")),
        label="authoritative_repo_root",
    )
    canonical_truth_root = _require_contract_truth_dir(
        resolve_canonical_truth_root(),
        label="canonical_truth_root",
        authoritative_repo_root=authoritative_repo_root,
    )
    truth_sleeves_root = _require_contract_truth_dir(
        resolve_truth_sleeves_root(),
        label="truth_sleeves_root",
        authoritative_repo_root=authoritative_repo_root,
    )
    runtime_data_root = _require_absolute_existing_dir(resolve_runtime_data_root(), label="runtime_data_root")
    release_root = _require_absolute_existing_dir(
        Path(_require_text(resolve_release_provenance().get("release_root"), "OLD_RUNTIME_AUTHORITY_RELEASE_ROOT_MISSING")),
        label="release_root",
    )
    runtime_environment = _require_text(
        contract.get("runtime_environment"),
        "OLD_RUNTIME_AUTHORITY_RUNTIME_ENVIRONMENT_MISSING",
    )
    pointer_index_family = _require_text(
        contract.get("pointer_index_family"),
        "OLD_RUNTIME_AUTHORITY_POINTER_INDEX_FAMILY_MISSING",
    )
    allowed_truth_roots = _require_allowed_truth_roots(
        raw=contract.get("allowed_truth_roots"),
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
        authoritative_repo_root=authoritative_repo_root,
        code_prefix="old_runtime_authority",
    )
    return {
        "source": "legacy_fallback",
        "release_current_path": str((canonical_truth_root / "release_current_v1" / "current.json").resolve()),
        "authoritative_repo_root": str(authoritative_repo_root),
        "release_root": str(release_root),
        "canonical_truth_root": str(canonical_truth_root),
        "truth_sleeves_root": str(truth_sleeves_root),
        "runtime_data_root": str(runtime_data_root),
        "runtime_environment": runtime_environment,
        "pointer_index_family": pointer_index_family,
        "allowed_truth_roots": [str(item) for item in allowed_truth_roots],
    }


def _load_release_current_runtime_authority(
    *,
    old_runtime_authority: Mapping[str, Any],
) -> dict[str, Any] | None:
    current_path = Path(str(old_runtime_authority.get("release_current_path") or "")).resolve()
    if not current_path.exists():
        return None
    try:
        current_ref = read_validated_surface_v1(path=current_path, schema_relpath=RELEASE_CURRENT_SCHEMA_RELPATH_V1)
    except Exception as exc:
        raise SystemExit(
            f"FAIL: runtime_authority_bridge_release_current_invalid path={current_path} err={type(exc).__name__}:{exc}"
        ) from exc

    payload = dict(current_ref.payload)
    authoritative_repo_root = _require_absolute_existing_dir(
        Path(_require_text(payload.get("authoritative_repo_root"), "RELEASE_CURRENT_REPO_ROOT_MISSING")),
        label="authoritative_repo_root",
    )
    canonical_truth_root = _require_contract_truth_dir(
        Path(_require_text(payload.get("canonical_truth_root"), "RELEASE_CURRENT_CANONICAL_TRUTH_ROOT_MISSING")),
        label="canonical_truth_root",
        authoritative_repo_root=authoritative_repo_root,
    )
    truth_sleeves_root = _require_contract_truth_dir(
        Path(_require_text(payload.get("truth_sleeves_root"), "RELEASE_CURRENT_TRUTH_SLEEVES_ROOT_MISSING")),
        label="truth_sleeves_root",
        authoritative_repo_root=authoritative_repo_root,
    )
    runtime_data_root = _require_absolute_existing_dir(
        Path(_require_text(payload.get("runtime_data_root"), "RELEASE_CURRENT_RUNTIME_DATA_ROOT_MISSING")),
        label="runtime_data_root",
    )
    release_root = _require_absolute_existing_dir(
        Path(_require_text(payload.get("release_root"), "RELEASE_CURRENT_RELEASE_ROOT_MISSING")),
        label="release_root",
    )
    runtime_environment = _require_text(
        payload.get("runtime_environment"),
        "RELEASE_CURRENT_RUNTIME_ENVIRONMENT_MISSING",
    )
    pointer_index_family = _require_text(
        payload.get("pointer_index_family"),
        "RELEASE_CURRENT_POINTER_INDEX_FAMILY_MISSING",
    )
    allowed_truth_roots = _require_allowed_truth_roots(
        raw=payload.get("allowed_truth_roots"),
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
        authoritative_repo_root=authoritative_repo_root,
        code_prefix="release_current",
    )
    return {
        "source": "release_current_v1",
        "release_current_id": str(payload.get("release_current_id") or ""),
        "release_current_path": str(current_path),
        "authoritative_repo_root": str(authoritative_repo_root),
        "release_root": str(release_root),
        "canonical_truth_root": str(canonical_truth_root),
        "truth_sleeves_root": str(truth_sleeves_root),
        "runtime_data_root": str(runtime_data_root),
        "runtime_environment": runtime_environment,
        "pointer_index_family": pointer_index_family,
        "allowed_truth_roots": [str(item) for item in allowed_truth_roots],
    }


def _log_runtime_authority_bridge_mismatch(
    *,
    caller: str,
    old: Mapping[str, Any],
    new: Mapping[str, Any],
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

    _check("canonical_truth_root", str(old.get("canonical_truth_root") or ""), str(new.get("canonical_truth_root") or ""))
    _check("truth_sleeves_root", str(old.get("truth_sleeves_root") or ""), str(new.get("truth_sleeves_root") or ""))
    _check("release_root", str(old.get("release_root") or ""), str(new.get("release_root") or ""))
    _check("runtime_environment", str(old.get("runtime_environment") or ""), str(new.get("runtime_environment") or ""))
    _check("pointer_index_family", str(old.get("pointer_index_family") or ""), str(new.get("pointer_index_family") or ""))

    old_allowed = sorted(str(item) for item in old.get("allowed_truth_roots") or [])
    new_allowed = sorted(str(item) for item in new.get("allowed_truth_roots") or [])
    _check("allowed_truth_roots", old_allowed, new_allowed)

    if mismatches:
        LOGGER.warning(
            "runtime_authority_bridge_mismatch %s",
            json.dumps(
                {
                    "caller": str(caller or "").strip(),
                    "release_current_path": str(new.get("release_current_path") or ""),
                    "release_current_id": str(new.get("release_current_id") or ""),
                    "mismatches": mismatches,
                },
                sort_keys=True,
            ),
        )


def _require_truth_root_under_bridge_contract(*, truth_root: Path, authority: Mapping[str, Any]) -> Path:
    authoritative_repo_root = _require_absolute_existing_dir(
        Path(_require_text(authority.get("authoritative_repo_root"), "BRIDGE_REPO_ROOT_MISSING")),
        label="authoritative_repo_root",
    )
    resolved = _require_contract_truth_dir(
        truth_root,
        label="truth_root",
        authoritative_repo_root=authoritative_repo_root,
    )
    for allowed_root in authority.get("allowed_truth_roots") or []:
        allowed = Path(str(allowed_root)).resolve()
        if resolved == allowed or _under(resolved, root=allowed):
            return resolved
    raise SystemExit(f"FAIL: truth_root_outside_release_current_contract truth_root={resolved}")


def load_release_current_runtime_authority_v1(*, caller: str = "") -> dict[str, Any]:
    old_runtime_authority = _load_old_runtime_authority()
    release_current_runtime_authority = _load_release_current_runtime_authority(
        old_runtime_authority=old_runtime_authority
    )
    if release_current_runtime_authority is None:
        return dict(old_runtime_authority)
    _log_runtime_authority_bridge_mismatch(
        caller=caller,
        old=old_runtime_authority,
        new=release_current_runtime_authority,
    )
    return dict(release_current_runtime_authority)


def resolve_canonical_truth_root_bridge_v1(*, caller: str = "") -> Path:
    authority = load_release_current_runtime_authority_v1(caller=caller)
    return _require_absolute_existing_dir(
        Path(_require_text(authority.get("canonical_truth_root"), "BRIDGE_CANONICAL_TRUTH_ROOT_MISSING")),
        label="canonical_truth_root",
    )


def resolve_truth_sleeves_root_bridge_v1(*, caller: str = "") -> Path:
    authority = load_release_current_runtime_authority_v1(caller=caller)
    return _require_absolute_existing_dir(
        Path(_require_text(authority.get("truth_sleeves_root"), "BRIDGE_TRUTH_SLEEVES_ROOT_MISSING")),
        label="truth_sleeves_root",
    )


def resolve_runtime_data_root_bridge_v1(*, caller: str = "") -> Path:
    authority = load_release_current_runtime_authority_v1(caller=caller)
    return _require_absolute_existing_dir(
        Path(_require_text(authority.get("runtime_data_root"), "BRIDGE_RUNTIME_DATA_ROOT_MISSING")),
        label="runtime_data_root",
    )


def resolve_release_root_bridge_v1(*, caller: str = "") -> Path:
    authority = load_release_current_runtime_authority_v1(caller=caller)
    return _require_absolute_existing_dir(
        Path(_require_text(authority.get("release_root"), "BRIDGE_RELEASE_ROOT_MISSING")),
        label="release_root",
    )


def resolve_truth_root_bridge_v1(*, repo_root: Path, caller: str = "") -> Path:
    repo_root_resolved = Path(repo_root).resolve()
    _require_under_repo(repo_root_resolved, label="repo_root")
    authority_mode = resolve_authority_mode()
    authority = load_release_current_runtime_authority_v1(caller=caller)
    default_root = _require_absolute_existing_dir(
        (resolve_truth_sleeves_root_bridge_v1(caller=caller) / "PRIMARY" / "PAPER").resolve(),
        label="RUNTIME_TRUTH_ROOT",
    )
    raw = (os.environ.get(ENV_VAR) or "").strip()
    if authority_mode == AUTHORITY_MODE_GOVERNANCE_PRIMARY:
        if not raw:
            return default_root
        return _require_truth_root_under_bridge_contract(
            truth_root=_require_absolute_existing_dir(Path(raw).expanduser().resolve(), label=ENV_VAR),
            authority=authority,
        )
    if not raw:
        return default_root
    return _require_truth_root_under_bridge_contract(
        truth_root=_require_absolute_existing_dir(Path(raw).expanduser().resolve(), label=ENV_VAR),
        authority=authority,
    )
