from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from constellation_2.common.constitutional_runtime_v1 import (
    ConstitutionalRuntimeError,
    assert_constitutional_consumer_allowed_v1,
    get_constitutional_artifact_contract_v1,
    get_constitutional_artifact_mirror_contract_v1,
    iter_constitutional_artifact_contracts_v1,
    load_constitutional_artifact_authority_registry_v1,
    resolve_constitutional_artifact_path_v1,
)


ArtifactAuthorityError = ConstitutionalRuntimeError


def load_artifact_authority_registry_v1(repo_root: Path | str) -> Dict[str, Any]:
    return load_constitutional_artifact_authority_registry_v1(repo_root)


def iter_artifact_contracts_v1(repo_root: Path | str) -> Iterable[Dict[str, Any]]:
    return iter_constitutional_artifact_contracts_v1(repo_root)


def get_artifact_contract_v1(repo_root: Path | str, artifact_id: str) -> Dict[str, Any]:
    return get_constitutional_artifact_contract_v1(repo_root, artifact_id)


def assert_artifact_consumer_allowed_v1(repo_root: Path | str, artifact_id: str, consumer_id: str) -> Dict[str, Any]:
    return assert_constitutional_consumer_allowed_v1(repo_root, artifact_id, consumer_id)


def get_artifact_mirror_contract_v1(repo_root: Path | str, artifact_id: str, mirror_id: str) -> Dict[str, Any]:
    return get_constitutional_artifact_mirror_contract_v1(repo_root, artifact_id, mirror_id)


def resolve_artifact_authority_path_v1(
    *,
    repo_root: Path | str,
    artifact_id: str,
    day_utc: str,
    canonical_truth_root: Path | str | None = None,
    execution_truth_root: Path | str | None = None,
    path_role: str = "authoritative",
    mirror_id: str = "",
    extra_variables: Mapping[str, Any] | None = None,
) -> Path:
    return resolve_constitutional_artifact_path_v1(
        repo_root=repo_root,
        artifact_id=artifact_id,
        day_utc=day_utc,
        canonical_truth_root=canonical_truth_root,
        execution_truth_root=execution_truth_root,
        path_role=path_role,
        mirror_id=mirror_id,
        extra_variables=extra_variables,
    )
