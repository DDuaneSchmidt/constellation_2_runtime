from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.runtime_authority_bridge_v1 import load_release_current_runtime_authority_v1
from constellation_2.common.runtime_contract_v1 import require_truth_root_under_contract
from constellation_2.common.runtime_path_authority_v1 import (
    REPO_ROOT,
    RuntimePathAuthorityV1,
    load_runtime_path_authority_v1,
    resolve_runtime_path_authority_snapshot_v1,
)


LOGGER = logging.getLogger(__name__)
_SNAPSHOT_FIELD_ORDER: tuple[str, ...] = (
    "authoritative_repo_root",
    "canonical_runtime_truth_root",
    "canonical_runtime_truth_sleeves_root",
    "authoritative_repo_truth_root",
    "authoritative_repo_truth_sleeves_root",
    "active_release_root",
)


def _require_text(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SystemExit(f"FAIL: {code}")
    return text


def _require_absolute_existing_dir(path: Path, *, label: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_absolute():
        raise SystemExit(f"FAIL: {label} must be absolute: {resolved}")
    if not resolved.exists() or not resolved.is_dir():
        raise SystemExit(f"FAIL: {label} must exist and be a directory: {resolved}")
    return resolved


def _repo_local_truth_roots(authoritative_repo_root: Path) -> tuple[Path, Path]:
    return (
        (authoritative_repo_root / "constellation_2" / "runtime" / "truth").resolve(),
        (authoritative_repo_root / "constellation_2" / "runtime" / "truth_sleeves").resolve(),
    )


def _authority_to_snapshot(authority: RuntimePathAuthorityV1) -> dict[str, str]:
    return {
        "authoritative_repo_root": str(authority.authoritative_repo_root),
        "canonical_runtime_truth_root": str(authority.canonical_runtime_truth_root),
        "canonical_runtime_truth_sleeves_root": str(authority.canonical_runtime_truth_sleeves_root),
        "authoritative_repo_truth_root": str(authority.authoritative_repo_truth_root),
        "authoritative_repo_truth_sleeves_root": str(authority.authoritative_repo_truth_sleeves_root),
        "active_release_root": str(authority.active_release_root),
    }


def _validate_snapshot_shape(snapshot: Mapping[str, str], *, code_prefix: str) -> dict[str, str]:
    keys = tuple(snapshot.keys())
    if keys != _SNAPSHOT_FIELD_ORDER:
        raise SystemExit(
            f"FAIL: {code_prefix}_snapshot_shape_mismatch expected={_SNAPSHOT_FIELD_ORDER} actual={keys}"
        )
    return {field: str(snapshot[field]) for field in _SNAPSHOT_FIELD_ORDER}


def _validate_authority(authority: RuntimePathAuthorityV1, *, code_prefix: str) -> RuntimePathAuthorityV1:
    authoritative_repo_root = _require_absolute_existing_dir(
        authority.authoritative_repo_root,
        label=f"{code_prefix}.authoritative_repo_root",
    )
    canonical_runtime_truth_root = _require_absolute_existing_dir(
        authority.canonical_runtime_truth_root,
        label=f"{code_prefix}.canonical_runtime_truth_root",
    )
    canonical_runtime_truth_sleeves_root = _require_absolute_existing_dir(
        authority.canonical_runtime_truth_sleeves_root,
        label=f"{code_prefix}.canonical_runtime_truth_sleeves_root",
    )
    active_release_root = _require_absolute_existing_dir(
        authority.active_release_root,
        label=f"{code_prefix}.active_release_root",
    )
    authoritative_repo_truth_root = _require_absolute_existing_dir(
        authority.authoritative_repo_truth_root,
        label=f"{code_prefix}.authoritative_repo_truth_root",
    )
    authoritative_repo_truth_sleeves_root = _require_absolute_existing_dir(
        authority.authoritative_repo_truth_sleeves_root,
        label=f"{code_prefix}.authoritative_repo_truth_sleeves_root",
    )

    expected_repo_truth_root, expected_repo_truth_sleeves_root = _repo_local_truth_roots(authoritative_repo_root)
    if authoritative_repo_truth_root != expected_repo_truth_root:
        raise SystemExit(
            "FAIL: "
            f"{code_prefix}.authoritative_repo_truth_root_mismatch "
            f"expected={expected_repo_truth_root} actual={authoritative_repo_truth_root}"
        )
    if authoritative_repo_truth_sleeves_root != expected_repo_truth_sleeves_root:
        raise SystemExit(
            "FAIL: "
            f"{code_prefix}.authoritative_repo_truth_sleeves_root_mismatch "
            f"expected={expected_repo_truth_sleeves_root} actual={authoritative_repo_truth_sleeves_root}"
        )

    # Preserve existing contract policy semantics by validating roots under the active runtime contract.
    require_truth_root_under_contract(canonical_runtime_truth_root)
    require_truth_root_under_contract(canonical_runtime_truth_sleeves_root)

    return RuntimePathAuthorityV1(
        authoritative_repo_root=authoritative_repo_root,
        canonical_runtime_truth_root=canonical_runtime_truth_root,
        canonical_runtime_truth_sleeves_root=canonical_runtime_truth_sleeves_root,
        authoritative_repo_truth_root=authoritative_repo_truth_root,
        authoritative_repo_truth_sleeves_root=authoritative_repo_truth_sleeves_root,
        active_release_root=active_release_root,
    )


def _build_new_authority_from_release_current(*, release_current_authority: Mapping[str, Any]) -> RuntimePathAuthorityV1:
    authoritative_repo_root = _require_absolute_existing_dir(
        Path(_require_text(release_current_authority.get("authoritative_repo_root"), "NEW_AUTHORITY_REPO_ROOT_MISSING")),
        label="new_authority.authoritative_repo_root",
    )
    canonical_runtime_truth_root = _require_absolute_existing_dir(
        Path(_require_text(release_current_authority.get("canonical_truth_root"), "NEW_AUTHORITY_TRUTH_ROOT_MISSING")),
        label="new_authority.canonical_runtime_truth_root",
    )
    canonical_runtime_truth_sleeves_root = _require_absolute_existing_dir(
        Path(
            _require_text(
                release_current_authority.get("truth_sleeves_root"),
                "NEW_AUTHORITY_TRUTH_SLEEVES_ROOT_MISSING",
            )
        ),
        label="new_authority.canonical_runtime_truth_sleeves_root",
    )
    active_release_root = _require_absolute_existing_dir(
        Path(_require_text(release_current_authority.get("release_root"), "NEW_AUTHORITY_RELEASE_ROOT_MISSING")),
        label="new_authority.active_release_root",
    )
    authoritative_repo_truth_root, authoritative_repo_truth_sleeves_root = _repo_local_truth_roots(
        authoritative_repo_root
    )
    return RuntimePathAuthorityV1(
        authoritative_repo_root=authoritative_repo_root,
        canonical_runtime_truth_root=canonical_runtime_truth_root,
        canonical_runtime_truth_sleeves_root=canonical_runtime_truth_sleeves_root,
        authoritative_repo_truth_root=authoritative_repo_truth_root,
        authoritative_repo_truth_sleeves_root=authoritative_repo_truth_sleeves_root,
        active_release_root=active_release_root,
    )


def _log_runtime_authority_snapshot_bridge_mismatch(
    *,
    caller: str,
    release_current_id: str,
    old_snapshot: Mapping[str, str],
    new_snapshot: Mapping[str, str],
) -> None:
    mismatches: list[dict[str, str]] = []
    for field in _SNAPSHOT_FIELD_ORDER:
        old_value = str(old_snapshot.get(field) or "")
        new_value = str(new_snapshot.get(field) or "")
        if old_value != new_value:
            mismatches.append(
                {
                    "field": field,
                    "old_value": old_value,
                    "new_value": new_value,
                }
            )
    if mismatches:
        LOGGER.warning(
            "runtime_authority_snapshot_bridge_mismatch %s",
            json.dumps(
                {
                    "caller": str(caller or "").strip(),
                    "release_current_id": str(release_current_id or "").strip(),
                    "mismatches": mismatches,
                },
                sort_keys=True,
            ),
        )


def _load_runtime_authority_pair(
    *,
    repo_root: Path,
    caller: str,
) -> tuple[RuntimePathAuthorityV1, dict[str, str]]:
    old_authority = _validate_authority(
        load_runtime_path_authority_v1(repo_root=repo_root),
        code_prefix="legacy_authority",
    )
    old_snapshot = _validate_snapshot_shape(
        resolve_runtime_path_authority_snapshot_v1(repo_root=repo_root),
        code_prefix="legacy_authority",
    )

    release_current_authority = load_release_current_runtime_authority_v1(caller=caller)
    if str(release_current_authority.get("source") or "").strip() != "release_current_v1":
        return old_authority, old_snapshot

    new_authority = _validate_authority(
        _build_new_authority_from_release_current(release_current_authority=release_current_authority),
        code_prefix="release_current_authority",
    )
    new_snapshot = _validate_snapshot_shape(
        _authority_to_snapshot(new_authority),
        code_prefix="release_current_authority",
    )
    _log_runtime_authority_snapshot_bridge_mismatch(
        caller=caller,
        release_current_id=str(release_current_authority.get("release_current_id") or ""),
        old_snapshot=old_snapshot,
        new_snapshot=new_snapshot,
    )
    return new_authority, new_snapshot


def load_runtime_path_authority_bridge_v1(
    *,
    repo_root: Path | None = None,
    caller: str = "",
) -> RuntimePathAuthorityV1:
    resolved_repo_root = Path(repo_root or REPO_ROOT).resolve()
    authority, _ = _load_runtime_authority_pair(repo_root=resolved_repo_root, caller=caller)
    return authority


def resolve_runtime_path_authority_snapshot_bridge_v1(
    *,
    repo_root: Path | None = None,
    caller: str = "",
) -> dict[str, str]:
    resolved_repo_root = Path(repo_root or REPO_ROOT).resolve()
    _, snapshot = _load_runtime_authority_pair(repo_root=resolved_repo_root, caller=caller)
    return dict(snapshot)
