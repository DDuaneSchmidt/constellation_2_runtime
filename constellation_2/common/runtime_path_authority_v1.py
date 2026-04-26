from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from constellation_2.common.paper_session_fact_plane_v1 import resolve_authoritative_repo_root_v1
from constellation_2.common.runtime_authority_bridge_v1 import load_release_current_runtime_authority_v1
from constellation_2.common.runtime_contract_v1 import (
    resolve_canonical_truth_root,
    resolve_truth_sleeves_root,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class RuntimePathAuthorityV1:
    authoritative_repo_root: Path
    canonical_runtime_truth_root: Path
    canonical_runtime_truth_sleeves_root: Path
    authoritative_repo_truth_root: Path
    authoritative_repo_truth_sleeves_root: Path
    active_release_root: Path


def _under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _authoritative_repo_truth_root(authoritative_repo_root: Path) -> Path:
    return (authoritative_repo_root / "constellation_2" / "runtime" / "truth").resolve()


def _authoritative_repo_truth_sleeves_root(authoritative_repo_root: Path) -> Path:
    return (authoritative_repo_root / "constellation_2" / "runtime" / "truth_sleeves").resolve()


def load_runtime_path_authority_v1(*, repo_root: Path | None = None) -> RuntimePathAuthorityV1:
    resolved_repo_root = Path(repo_root or REPO_ROOT).resolve()
    authoritative_repo_root = resolve_authoritative_repo_root_v1(resolved_repo_root)
    canonical_runtime_truth_root = resolve_canonical_truth_root()
    canonical_runtime_truth_sleeves_root = resolve_truth_sleeves_root()
    runtime_authority = load_release_current_runtime_authority_v1(
        caller="constellation_2/common/runtime_path_authority_v1.py"
    )
    release_root = Path(str(runtime_authority.get("release_root") or "")).resolve()
    authoritative_repo_truth_root = _authoritative_repo_truth_root(authoritative_repo_root)
    authoritative_repo_truth_sleeves_root = _authoritative_repo_truth_sleeves_root(authoritative_repo_root)
    return RuntimePathAuthorityV1(
        authoritative_repo_root=authoritative_repo_root,
        canonical_runtime_truth_root=canonical_runtime_truth_root,
        canonical_runtime_truth_sleeves_root=canonical_runtime_truth_sleeves_root,
        authoritative_repo_truth_root=authoritative_repo_truth_root,
        authoritative_repo_truth_sleeves_root=authoritative_repo_truth_sleeves_root,
        active_release_root=release_root,
    )


def classify_runtime_path_v1(
    path: str | Path,
    *,
    authority: RuntimePathAuthorityV1 | None = None,
) -> Dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    selected = authority or load_runtime_path_authority_v1()

    path_class = "OUTSIDE_AUTHORITY"
    if resolved == selected.canonical_runtime_truth_root:
        path_class = "CANONICAL_RUNTIME_TRUTH_ROOT"
    elif _under(resolved, selected.canonical_runtime_truth_root):
        path_class = "CANONICAL_RUNTIME_TRUTH_SUBPATH"
    elif resolved == selected.canonical_runtime_truth_sleeves_root:
        path_class = "CANONICAL_RUNTIME_TRUTH_SLEEVES_ROOT"
    elif _under(resolved, selected.canonical_runtime_truth_sleeves_root):
        path_class = "CANONICAL_RUNTIME_TRUTH_SLEEVES_SUBPATH"
    elif resolved == selected.authoritative_repo_truth_root:
        path_class = "AUTHORITATIVE_REPO_TRUTH_ROOT"
    elif _under(resolved, selected.authoritative_repo_truth_root):
        path_class = "AUTHORITATIVE_REPO_TRUTH_SUBPATH"
    elif resolved == selected.authoritative_repo_truth_sleeves_root:
        path_class = "AUTHORITATIVE_REPO_TRUTH_SLEEVES_ROOT"
    elif _under(resolved, selected.authoritative_repo_truth_sleeves_root):
        path_class = "AUTHORITATIVE_REPO_TRUTH_SLEEVES_SUBPATH"
    elif resolved == selected.active_release_root:
        path_class = "ACTIVE_RELEASE_ROOT"
    elif _under(resolved, selected.active_release_root):
        path_class = "ACTIVE_RELEASE_SUBPATH"

    policy_read_allowed = path_class.startswith("CANONICAL_RUNTIME_TRUTH")
    policy_write_allowed = path_class.startswith("CANONICAL_RUNTIME_TRUTH")
    advisory_repo_read_allowed = policy_read_allowed or path_class.startswith("AUTHORITATIVE_REPO_TRUTH")
    return {
        "path": str(resolved),
        "path_class": path_class,
        "policy_read_allowed": policy_read_allowed,
        "policy_write_allowed": policy_write_allowed,
        "advisory_repo_read_allowed": advisory_repo_read_allowed,
    }


def resolve_decision_truth_root_v1(
    explicit_truth_root: str | Path | None,
    *,
    repo_root: Path | None = None,
) -> Path:
    authority = load_runtime_path_authority_v1(repo_root=repo_root)
    if explicit_truth_root is None or not str(explicit_truth_root).strip():
        return authority.canonical_runtime_truth_root
    resolved = Path(str(explicit_truth_root)).expanduser().resolve()
    classification = classify_runtime_path_v1(resolved, authority=authority)
    if not bool(classification["policy_read_allowed"]):
        raise ValueError(
            f"RUNTIME_PATH_AUTHORITY_POLICY_TRUTH_ROOT_FORBIDDEN:path={resolved}:class={classification['path_class']}"
        )
    return resolved


def resolve_execution_truth_root_v1(
    explicit_truth_root: str | Path | None,
    *,
    repo_root: Path | None = None,
) -> Path:
    return resolve_decision_truth_root_v1(explicit_truth_root, repo_root=repo_root)


def require_authoritative_repo_runtime_v1(repo_root: Path | None = None) -> Path:
    resolved_repo_root = Path(repo_root or REPO_ROOT).resolve()
    authoritative_repo_root = resolve_authoritative_repo_root_v1(resolved_repo_root)
    if resolved_repo_root == authoritative_repo_root:
        return authoritative_repo_root

    # Services execute from /home/node/constellation_active -> /home/node/constellation_releases/*.
    # Accept only the currently activated release root from runtime authority.
    runtime_authority = load_release_current_runtime_authority_v1(
        caller="constellation_2/common/runtime_path_authority_v1.py::require_authoritative_repo_runtime_v1"
    )
    release_root = Path(str(runtime_authority.get("release_root") or "")).resolve()
    if release_root == resolved_repo_root:
        return release_root

    raise SystemExit(
        "FAIL: AUTHORITATIVE_REPO_RUNTIME_REQUIRED:"
        f"runtime_repo={resolved_repo_root}:authoritative_repo={authoritative_repo_root}:active_release_root={release_root}"
    )

def resolve_runtime_path_authority_snapshot_v1(*, repo_root: Path | None = None) -> Dict[str, str]:
    authority = load_runtime_path_authority_v1(repo_root=repo_root)
    return {
        "authoritative_repo_root": str(authority.authoritative_repo_root),
        "canonical_runtime_truth_root": str(authority.canonical_runtime_truth_root),
        "canonical_runtime_truth_sleeves_root": str(authority.canonical_runtime_truth_sleeves_root),
        "authoritative_repo_truth_root": str(authority.authoritative_repo_truth_root),
        "authoritative_repo_truth_sleeves_root": str(authority.authoritative_repo_truth_sleeves_root),
        "active_release_root": str(authority.active_release_root),
    }
