from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common import runtime_identity_v1 as runtime_identity_legacy_v1
from constellation_2.common.execution_identity_binding_v1 import (
    EXECUTION_IDENTITY_BINDING_OWNER,
    resolve_governed_execution_identity_v1,
)
from constellation_2.common.paper_execution_authority_v1 import (
    resolve_governed_paper_execution_roots,
)
from constellation_2.common.paper_session_fact_plane_v1 import read_validated_surface_v1
from constellation_2.common.runtime_authority_bridge_v1 import (
    RELEASE_CURRENT_SCHEMA_RELPATH_V1,
    load_release_current_runtime_authority_v1,
)
from constellation_2.common.runtime_authority_snapshot_bridge_v1 import (
    resolve_runtime_path_authority_snapshot_bridge_v1,
)
from constellation_2.common.runtime_contract_v1 import (
    ACTIVE_RUNTIME_CONTRACT_SCHEMA_RELPATH,
    resolve_release_provenance,
)
from constellation_2.common.runtime_path_authority_v1 import (
    require_authoritative_repo_runtime_v1,
)


LOGGER = logging.getLogger(__name__)


def _sha256_hex(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _require_text(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SystemExit(f"FAIL: {code}")
    return text


def _require_ref_object(value: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SystemExit(f"FAIL: {code}")
    return value


def _release_current_path_for_old_identity(old_identity: Mapping[str, Any]) -> Path:
    canonical_truth_root = Path(
        _require_text(
            old_identity.get("canonical_truth_root"),
            "runtime_identity_bridge_old_identity_canonical_truth_root_missing",
        )
    ).resolve()
    return (canonical_truth_root / "release_current_v1" / "current.json").resolve()


def _load_release_current_payload(path: Path) -> dict[str, Any]:
    try:
        current_ref = read_validated_surface_v1(
            path=path,
            schema_relpath=RELEASE_CURRENT_SCHEMA_RELPATH_V1,
        )
    except Exception as exc:
        raise SystemExit(
            "FAIL: runtime_identity_bridge_release_current_invalid "
            f"path={path} err={type(exc).__name__}:{exc}"
        ) from exc
    return dict(current_ref.payload)


def _load_contract_payload_from_release_current(*, release_current_payload: Mapping[str, Any]) -> tuple[Path, str, dict[str, Any]]:
    active_runtime_contract_ref = _require_ref_object(
        release_current_payload.get("active_runtime_contract_ref"),
        "runtime_identity_bridge_active_runtime_contract_ref_invalid",
    )
    contract_path = Path(
        _require_text(
            active_runtime_contract_ref.get("path"),
            "runtime_identity_bridge_active_runtime_contract_ref_path_missing",
        )
    ).resolve()
    schema_relpath = _require_text(
        active_runtime_contract_ref.get("schema_relpath"),
        "runtime_identity_bridge_active_runtime_contract_ref_schema_relpath_missing",
    )
    expected_sha256 = _require_text(
        active_runtime_contract_ref.get("sha256"),
        "runtime_identity_bridge_active_runtime_contract_ref_sha256_missing",
    ).lower()
    if len(expected_sha256) != 64:
        raise SystemExit("FAIL: runtime_identity_bridge_active_runtime_contract_ref_sha256_invalid")

    selected_schema_relpath = schema_relpath or ACTIVE_RUNTIME_CONTRACT_SCHEMA_RELPATH
    try:
        contract_ref = read_validated_surface_v1(path=contract_path, schema_relpath=selected_schema_relpath)
    except Exception as exc:
        raise SystemExit(
            "FAIL: runtime_identity_bridge_active_runtime_contract_payload_invalid "
            f"path={contract_path} err={type(exc).__name__}:{exc}"
        ) from exc
    actual_sha256 = _sha256_hex(contract_path)
    if actual_sha256 != expected_sha256:
        raise SystemExit(
            "FAIL: runtime_identity_bridge_active_runtime_contract_sha256_mismatch "
            f"path={contract_path} expected={expected_sha256} actual={actual_sha256}"
        )
    return contract_path, actual_sha256, dict(contract_ref.payload)


def _primary_execution_identity_ref(*, release_current_payload: Mapping[str, Any]) -> tuple[str, str]:
    primary_ref = _require_ref_object(
        release_current_payload.get("primary_execution_identity_ref"),
        "runtime_identity_bridge_primary_execution_identity_ref_invalid",
    )
    authority_owner = _require_text(
        primary_ref.get("authority_owner"),
        "runtime_identity_bridge_primary_execution_identity_ref_authority_owner_missing",
    )
    if authority_owner != EXECUTION_IDENTITY_BINDING_OWNER:
        raise SystemExit(
            "FAIL: runtime_identity_bridge_primary_execution_identity_ref_owner_invalid "
            f"expected={EXECUTION_IDENTITY_BINDING_OWNER} actual={authority_owner}"
        )
    primary_sleeve_id = _require_text(
        primary_ref.get("sleeve_id"),
        "runtime_identity_bridge_primary_execution_identity_ref_sleeve_id_missing",
    ).upper()
    return authority_owner, primary_sleeve_id


def _build_new_runtime_identity_snapshot(
    *,
    repo_root: Path | None,
    caller: str,
    old_identity: Mapping[str, Any],
    release_current_payload: Mapping[str, Any],
    release_current_runtime_authority: Mapping[str, Any],
) -> dict[str, Any]:
    authoritative_repo_root = require_authoritative_repo_runtime_v1(repo_root)
    release_current_repo_root = Path(
        _require_text(
            release_current_payload.get("authoritative_repo_root"),
            "runtime_identity_bridge_release_current_authoritative_repo_root_missing",
        )
    ).resolve()
    if release_current_repo_root != authoritative_repo_root:
        raise SystemExit(
            "FAIL: runtime_identity_bridge_authoritative_repo_root_mismatch "
            f"release_current={release_current_repo_root} runtime={authoritative_repo_root}"
        )

    contract_path, contract_sha256, contract_payload = _load_contract_payload_from_release_current(
        release_current_payload=release_current_payload
    )
    authority_owner, primary_sleeve_id = _primary_execution_identity_ref(
        release_current_payload=release_current_payload
    )
    runtime_environment = _require_text(
        release_current_runtime_authority.get("runtime_environment"),
        "runtime_identity_bridge_runtime_environment_missing",
    ).upper()

    execution_identity = resolve_governed_execution_identity_v1(
        repo_root=authoritative_repo_root,
        environment=runtime_environment,
        sleeve_id=primary_sleeve_id,
    )
    execution_roots = resolve_governed_paper_execution_roots(
        repo_root=authoritative_repo_root,
        environment=execution_identity.environment,
        ib_account=execution_identity.account_id,
        sleeve_id=execution_identity.sleeve_id,
    )
    release_provenance = resolve_release_provenance()
    path_authority_snapshot = resolve_runtime_path_authority_snapshot_bridge_v1(
        repo_root=authoritative_repo_root,
        caller=caller,
    )

    return {
        "contract_path": str(contract_path),
        "contract_sha256": contract_sha256,
        "schema_id": str(contract_payload.get("schema_id") or "").strip(),
        "schema_version": str(contract_payload.get("schema_version") or "").strip(),
        "status": str(contract_payload.get("status") or "").strip(),
        "generated_at_utc": str(contract_payload.get("generated_at_utc") or "").strip(),
        "authoritative_repo_root": str(authoritative_repo_root),
        "release_id": _require_text(
            release_current_payload.get("release_id"),
            "runtime_identity_bridge_release_id_missing",
        ),
        "git_sha": _require_text(
            release_current_payload.get("git_sha"),
            "runtime_identity_bridge_git_sha_missing",
        ),
        "release_root": str(
            Path(
                _require_text(
                    release_current_runtime_authority.get("release_root"),
                    "runtime_identity_bridge_release_root_missing",
                )
            ).resolve()
        ),
        "runtime_data_root": str(
            Path(
                _require_text(
                    release_current_runtime_authority.get("runtime_data_root"),
                    "runtime_identity_bridge_runtime_data_root_missing",
                )
            ).resolve()
        ),
        "canonical_truth_root": str(
            Path(
                _require_text(
                    release_current_runtime_authority.get("canonical_truth_root"),
                    "runtime_identity_bridge_canonical_truth_root_missing",
                )
            ).resolve()
        ),
        "truth_sleeves_root": str(
            Path(
                _require_text(
                    release_current_runtime_authority.get("truth_sleeves_root"),
                    "runtime_identity_bridge_truth_sleeves_root_missing",
                )
            ).resolve()
        ),
        "pointer_index_family": str(release_current_runtime_authority.get("pointer_index_family") or "").strip(),
        "allowed_truth_roots": [
            str(Path(str(item)).resolve())
            for item in (release_current_runtime_authority.get("allowed_truth_roots") or [])
        ],
        "provenance_mode": str(contract_payload.get("provenance_mode") or "").strip(),
        "runtime_environment": runtime_environment,
        "primary_execution_identity_ref": {
            "authority_owner": authority_owner,
            "sleeve_id": primary_sleeve_id,
        },
        "resolved_execution_identity": {
            "authority_owner": execution_identity.authority_owner,
            "environment": execution_identity.environment,
            "sleeve_id": execution_identity.sleeve_id,
            "account_id": execution_identity.account_id,
            "client_id_orders": execution_identity.client_id_orders,
            "client_id_observer": execution_identity.client_id_observer,
            "host": execution_identity.host,
            "port": execution_identity.port,
            "sleeve_registry_path": str(execution_identity.sleeve_registry_path.resolve()),
            "account_registry_path": str(execution_identity.account_registry_path.resolve()),
        },
        "resolved_execution_roots": {
            "authority_owner": execution_roots.authority_owner,
            "environment": execution_roots.environment,
            "ib_account": execution_roots.ib_account,
            "sleeve_id": execution_roots.sleeve_id,
            "mode": execution_roots.mode,
            "truth_partition": execution_roots.truth_partition,
            "execution_root_path": str(execution_roots.execution_root_path.resolve()),
            "truth_sleeves_root": str(execution_roots.truth_sleeves_root.resolve()),
            "sleeve_registry_path": str(execution_roots.sleeve_registry_path.resolve()),
        },
        "path_authority_snapshot": dict(path_authority_snapshot),
        "release_provenance": dict(release_provenance),
    }


def _log_runtime_identity_bridge_mismatch(
    *,
    caller: str,
    release_current_payload: Mapping[str, Any],
    old_identity: Mapping[str, Any],
    new_identity: Mapping[str, Any],
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

    _check(
        "canonical_truth_root",
        str(old_identity.get("canonical_truth_root") or ""),
        str(new_identity.get("canonical_truth_root") or ""),
    )
    _check(
        "truth_sleeves_root",
        str(old_identity.get("truth_sleeves_root") or ""),
        str(new_identity.get("truth_sleeves_root") or ""),
    )
    _check(
        "release_root",
        str(old_identity.get("release_root") or ""),
        str(new_identity.get("release_root") or ""),
    )
    _check(
        "runtime_environment",
        str(old_identity.get("runtime_environment") or ""),
        str(new_identity.get("runtime_environment") or ""),
    )
    _check(
        "pointer_index_family",
        str(old_identity.get("pointer_index_family") or ""),
        str(new_identity.get("pointer_index_family") or ""),
    )
    _check(
        "allowed_truth_roots",
        sorted(str(item) for item in old_identity.get("allowed_truth_roots") or []),
        sorted(str(item) for item in new_identity.get("allowed_truth_roots") or []),
    )
    _check(
        "primary_execution_identity_ref",
        dict(old_identity.get("primary_execution_identity_ref") or {}),
        dict(new_identity.get("primary_execution_identity_ref") or {}),
    )
    _check(
        "contract_path",
        str(old_identity.get("contract_path") or ""),
        str(new_identity.get("contract_path") or ""),
    )
    _check(
        "contract_sha256",
        str(old_identity.get("contract_sha256") or ""),
        str(new_identity.get("contract_sha256") or ""),
    )

    if mismatches:
        LOGGER.warning(
            "runtime_identity_bridge_mismatch %s",
            json.dumps(
                {
                    "caller": str(caller or "").strip(),
                    "release_current_id": str(release_current_payload.get("release_current_id") or "").strip(),
                    "release_current_path": str(
                        Path(
                            _require_text(
                                release_current_payload.get("canonical_truth_root"),
                                "runtime_identity_bridge_release_current_canonical_truth_root_missing",
                            )
                        ).resolve()
                        / "release_current_v1"
                        / "current.json"
                    ),
                    "mismatches": mismatches,
                },
                sort_keys=True,
            ),
        )


def load_active_runtime_identity_snapshot_bridge_v1(
    *,
    repo_root: Path | None = None,
    caller: str = "",
) -> dict[str, Any]:
    old_identity = runtime_identity_legacy_v1._load_active_runtime_identity_snapshot_legacy_v1(repo_root=repo_root)
    release_current_path = _release_current_path_for_old_identity(old_identity)
    if not release_current_path.exists():
        return dict(old_identity)

    release_current_runtime_authority = load_release_current_runtime_authority_v1(caller=caller)
    if str(release_current_runtime_authority.get("source") or "").strip() != "release_current_v1":
        return dict(old_identity)

    release_current_payload = _load_release_current_payload(release_current_path)
    new_identity = _build_new_runtime_identity_snapshot(
        repo_root=repo_root,
        caller=caller,
        old_identity=old_identity,
        release_current_payload=release_current_payload,
        release_current_runtime_authority=release_current_runtime_authority,
    )
    _log_runtime_identity_bridge_mismatch(
        caller=caller,
        release_current_payload=release_current_payload,
        old_identity=old_identity,
        new_identity=new_identity,
    )
    return dict(new_identity)

