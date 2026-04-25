from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from constellation_2.common.runtime_contract_v1 import (
    resolve_canonical_truth_root,
    resolve_truth_sleeves_root,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    resolve_canonical_governed_sleeve_truth_root,
    resolve_governed_sleeve_truth_bindings,
)


EXECUTION_ROOT_AUTHORITY_OWNER = "sleeve_execution_root_v1"

RC_EXECUTION_ROOT_SLEEVE_ID_MISSING = "EXECUTION_ROOT_SLEEVE_ID_MISSING"
RC_EXECUTION_ROOT_MODE_MISSING = "EXECUTION_ROOT_MODE_MISSING"
RC_EXECUTION_ROOT_PATH_UNRESOLVED = "EXECUTION_ROOT_PATH_UNRESOLVED"
RC_EXECUTION_ROOT_PATH_MISMATCH = "EXECUTION_ROOT_PATH_MISMATCH"
RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN = "EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN"


@dataclass(frozen=True)
class SleeveExecutionRootResolutionV1:
    authority_owner: str
    environment: str
    ib_account: str
    sleeve_id: str
    mode: str
    truth_partition: str
    execution_root_path: Path
    truth_sleeves_root: Path
    sleeve_registry_path: Path


def _under(path: Path, *, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def resolve_sleeve_execution_root_v1(
    *,
    repo_root: Path,
    environment: str,
    ib_account: str,
    sleeve_id: str,
) -> SleeveExecutionRootResolutionV1:
    env = str(environment or "").strip().upper()
    if not env:
        raise ValueError(RC_EXECUTION_ROOT_MODE_MISSING)
    target_sleeve_id = str(sleeve_id or "").strip().upper()
    if not target_sleeve_id:
        raise ValueError(RC_EXECUTION_ROOT_SLEEVE_ID_MISSING)

    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=Path(repo_root).resolve(),
        environment=env,
        requested_ib_account=str(ib_account or "").strip(),
        sleeve_id=target_sleeve_id,
    )
    binding = bindings[0]
    mode = str(binding.environment or "").strip().upper()
    if not mode:
        raise ValueError(RC_EXECUTION_ROOT_MODE_MISSING)

    truth_sleeves_root = resolve_truth_sleeves_root().resolve()
    canonical_global_truth_root = resolve_canonical_truth_root().resolve()
    expected_root = (truth_sleeves_root / target_sleeve_id / mode).resolve()
    resolved_root = resolve_canonical_governed_sleeve_truth_root(binding).resolve()

    if resolved_root == canonical_global_truth_root or _under(resolved_root, root=canonical_global_truth_root):
        raise ValueError(
            f"{RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN}:sleeve_id={target_sleeve_id}:path={resolved_root}"
        )
    if not resolved_root.exists() or not resolved_root.is_dir():
        raise ValueError(f"{RC_EXECUTION_ROOT_PATH_UNRESOLVED}:sleeve_id={target_sleeve_id}:path={resolved_root}")
    if resolved_root != expected_root:
        raise ValueError(
            f"{RC_EXECUTION_ROOT_PATH_MISMATCH}:sleeve_id={target_sleeve_id}:expected={expected_root}:actual={resolved_root}"
        )

    return SleeveExecutionRootResolutionV1(
        authority_owner=EXECUTION_ROOT_AUTHORITY_OWNER,
        environment=env,
        ib_account=str(binding.ib_account or "").strip(),
        sleeve_id=target_sleeve_id,
        mode=mode,
        truth_partition=str(binding.truth_partition or "").strip(),
        execution_root_path=resolved_root,
        truth_sleeves_root=truth_sleeves_root,
        sleeve_registry_path=Path(binding.sleeve_registry_path).resolve(),
    )


def assert_execution_root_family_path_v1(
    *,
    repo_root: Path,
    environment: str,
    ib_account: str,
    sleeve_id: str,
    family: str,
    actual_path: Path,
) -> SleeveExecutionRootResolutionV1:
    resolution = resolve_sleeve_execution_root_v1(
        repo_root=repo_root,
        environment=environment,
        ib_account=ib_account,
        sleeve_id=sleeve_id,
    )
    family_name = str(family or "").strip().strip("/")
    if not family_name:
        raise ValueError(RC_EXECUTION_ROOT_PATH_UNRESOLVED)

    canonical_global_truth_root = resolve_canonical_truth_root().resolve()
    resolved_actual = Path(actual_path).resolve()
    if resolved_actual == canonical_global_truth_root or _under(resolved_actual, root=canonical_global_truth_root):
        raise ValueError(
            f"{RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN}:sleeve_id={resolution.sleeve_id}:path={resolved_actual}"
        )

    expected_family_root = (resolution.execution_root_path / family_name).resolve()
    if resolved_actual == expected_family_root or _under(resolved_actual, root=expected_family_root):
        return resolution
    raise ValueError(
        f"{RC_EXECUTION_ROOT_PATH_MISMATCH}:sleeve_id={resolution.sleeve_id}:expected={expected_family_root}:actual={resolved_actual}"
    )
