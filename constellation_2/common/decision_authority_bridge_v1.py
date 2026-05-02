from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.runtime_authority_bridge_v1 import load_release_current_runtime_authority_v1
from constellation_2.common.runtime_contract_v1 import load_active_runtime_contract_or_fail
from constellation_2.common.runtime_path_authority_v1 import load_runtime_path_authority_v1


LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE_CONTROLLED_PRODUCTION_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/production_truth").resolve()
PHASE_CONTROLLED_CANDIDATE_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/candidate_truth").resolve()


@dataclass(frozen=True)
class DecisionPathAuthorityBridgeV1:
    authoritative_repo_root: Path
    canonical_runtime_truth_root: Path
    canonical_runtime_truth_sleeves_root: Path
    authoritative_repo_truth_root: Path
    authoritative_repo_truth_sleeves_root: Path
    active_release_root: Path
    allowed_truth_roots: tuple[Path, ...]
    source: str
    release_current_path: str
    release_current_id: str


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


def _authoritative_repo_truth_root(authoritative_repo_root: Path) -> Path:
    return (authoritative_repo_root / "constellation_2" / "runtime" / "truth").resolve()


def _authoritative_repo_truth_sleeves_root(authoritative_repo_root: Path) -> Path:
    return (authoritative_repo_root / "constellation_2" / "runtime" / "truth_sleeves").resolve()


def _load_old_allowed_truth_roots() -> tuple[Path, ...]:
    contract = load_active_runtime_contract_or_fail()
    raw = contract.get("allowed_truth_roots")
    if not isinstance(raw, list) or not raw:
        raise SystemExit("FAIL: active runtime contract allowed_truth_roots must be non-empty list")
    return tuple(
        _require_absolute_existing_dir(Path(str(item)), label="allowed_truth_root")
        for item in raw
    )


def _build_legacy_authority(*, repo_root: Path) -> DecisionPathAuthorityBridgeV1:
    legacy = load_runtime_path_authority_v1(repo_root=repo_root)
    return DecisionPathAuthorityBridgeV1(
        authoritative_repo_root=legacy.authoritative_repo_root.resolve(),
        canonical_runtime_truth_root=legacy.canonical_runtime_truth_root.resolve(),
        canonical_runtime_truth_sleeves_root=legacy.canonical_runtime_truth_sleeves_root.resolve(),
        authoritative_repo_truth_root=legacy.authoritative_repo_truth_root.resolve(),
        authoritative_repo_truth_sleeves_root=legacy.authoritative_repo_truth_sleeves_root.resolve(),
        active_release_root=legacy.active_release_root.resolve(),
        allowed_truth_roots=tuple(path.resolve() for path in _load_old_allowed_truth_roots()),
        source="legacy_fallback",
        release_current_path="",
        release_current_id="",
    )


def _build_release_current_authority(payload: Mapping[str, Any]) -> DecisionPathAuthorityBridgeV1:
    authoritative_repo_root = _require_absolute_existing_dir(
        Path(_require_text(payload.get("authoritative_repo_root"), "DECISION_BRIDGE_REPO_ROOT_MISSING")),
        label="authoritative_repo_root",
    )
    canonical_truth_root = _require_absolute_existing_dir(
        Path(_require_text(payload.get("canonical_truth_root"), "DECISION_BRIDGE_CANONICAL_TRUTH_ROOT_MISSING")),
        label="canonical_truth_root",
    )
    truth_sleeves_root = _require_absolute_existing_dir(
        Path(_require_text(payload.get("truth_sleeves_root"), "DECISION_BRIDGE_TRUTH_SLEEVES_ROOT_MISSING")),
        label="truth_sleeves_root",
    )
    release_root = _require_absolute_existing_dir(
        Path(_require_text(payload.get("release_root"), "DECISION_BRIDGE_RELEASE_ROOT_MISSING")),
        label="release_root",
    )
    raw_allowed = payload.get("allowed_truth_roots")
    if not isinstance(raw_allowed, list) or not raw_allowed:
        raise SystemExit("FAIL: DECISION_BRIDGE_ALLOWED_TRUTH_ROOTS_INVALID")
    allowed_truth_roots = tuple(
        _require_absolute_existing_dir(
            Path(_require_text(item, "DECISION_BRIDGE_ALLOWED_TRUTH_ROOTS_INVALID")),
            label="allowed_truth_root",
        )
        for item in raw_allowed
    )
    return DecisionPathAuthorityBridgeV1(
        authoritative_repo_root=authoritative_repo_root,
        canonical_runtime_truth_root=canonical_truth_root,
        canonical_runtime_truth_sleeves_root=truth_sleeves_root,
        authoritative_repo_truth_root=_authoritative_repo_truth_root(authoritative_repo_root),
        authoritative_repo_truth_sleeves_root=_authoritative_repo_truth_sleeves_root(authoritative_repo_root),
        active_release_root=release_root,
        allowed_truth_roots=allowed_truth_roots,
        source="release_current_v1",
        release_current_path=str(payload.get("release_current_path") or ""),
        release_current_id=str(payload.get("release_current_id") or ""),
    )


def _classify_runtime_path_bridge_v1(
    path: Path | str,
    *,
    authority: DecisionPathAuthorityBridgeV1,
) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()

    path_class = "OUTSIDE_AUTHORITY"
    if resolved == authority.canonical_runtime_truth_root:
        path_class = "CANONICAL_RUNTIME_TRUTH_ROOT"
    elif _under(resolved, root=authority.canonical_runtime_truth_root):
        path_class = "CANONICAL_RUNTIME_TRUTH_SUBPATH"
    elif resolved == authority.canonical_runtime_truth_sleeves_root:
        path_class = "CANONICAL_RUNTIME_TRUTH_SLEEVES_ROOT"
    elif _under(resolved, root=authority.canonical_runtime_truth_sleeves_root):
        path_class = "CANONICAL_RUNTIME_TRUTH_SLEEVES_SUBPATH"
    elif resolved == authority.authoritative_repo_truth_root:
        path_class = "AUTHORITATIVE_REPO_TRUTH_ROOT"
    elif _under(resolved, root=authority.authoritative_repo_truth_root):
        path_class = "AUTHORITATIVE_REPO_TRUTH_SUBPATH"
    elif resolved == authority.authoritative_repo_truth_sleeves_root:
        path_class = "AUTHORITATIVE_REPO_TRUTH_SLEEVES_ROOT"
    elif _under(resolved, root=authority.authoritative_repo_truth_sleeves_root):
        path_class = "AUTHORITATIVE_REPO_TRUTH_SLEEVES_SUBPATH"
    elif resolved == authority.active_release_root:
        path_class = "ACTIVE_RELEASE_ROOT"
    elif _under(resolved, root=authority.active_release_root):
        path_class = "ACTIVE_RELEASE_SUBPATH"
    elif resolved == PHASE_CONTROLLED_PRODUCTION_TRUTH_ROOT:
        path_class = "PHASE_CONTROLLED_PRODUCTION_TRUTH_ROOT"
    elif _under(resolved, root=PHASE_CONTROLLED_PRODUCTION_TRUTH_ROOT):
        path_class = "PHASE_CONTROLLED_PRODUCTION_TRUTH_SUBPATH"
    elif resolved == PHASE_CONTROLLED_CANDIDATE_TRUTH_ROOT:
        path_class = "PHASE_CONTROLLED_CANDIDATE_TRUTH_ROOT"
    elif _under(resolved, root=PHASE_CONTROLLED_CANDIDATE_TRUTH_ROOT):
        path_class = "PHASE_CONTROLLED_CANDIDATE_TRUTH_SUBPATH"

    policy_read_allowed = path_class.startswith("CANONICAL_RUNTIME_TRUTH") or path_class.startswith("PHASE_CONTROLLED_")
    policy_write_allowed = path_class.startswith("CANONICAL_RUNTIME_TRUTH") or path_class.startswith("PHASE_CONTROLLED_")
    advisory_repo_read_allowed = policy_read_allowed or path_class.startswith("AUTHORITATIVE_REPO_TRUTH")
    return {
        "path": str(resolved),
        "path_class": path_class,
        "policy_read_allowed": policy_read_allowed,
        "policy_write_allowed": policy_write_allowed,
        "advisory_repo_read_allowed": advisory_repo_read_allowed,
    }


def _log_decision_authority_bridge_mismatch(
    *,
    caller: str,
    old: DecisionPathAuthorityBridgeV1,
    new: DecisionPathAuthorityBridgeV1,
    explicit_truth_root: str = "",
    old_explicit_allowed: bool | None = None,
    new_explicit_allowed: bool | None = None,
    old_explicit_class: str = "",
    new_explicit_class: str = "",
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

    _check("canonical_truth_root", str(old.canonical_runtime_truth_root), str(new.canonical_runtime_truth_root))
    _check("truth_sleeves_root", str(old.canonical_runtime_truth_sleeves_root), str(new.canonical_runtime_truth_sleeves_root))
    _check("release_root", str(old.active_release_root), str(new.active_release_root))
    _check(
        "allowed_truth_roots",
        sorted(str(item) for item in old.allowed_truth_roots),
        sorted(str(item) for item in new.allowed_truth_roots),
    )

    if old_explicit_allowed is not None and new_explicit_allowed is not None and old_explicit_allowed != new_explicit_allowed:
        mismatches.append(
            {
                "field": "explicit_path_policy_read_allowed",
                "old_value": bool(old_explicit_allowed),
                "new_value": bool(new_explicit_allowed),
                "old_class": str(old_explicit_class),
                "new_class": str(new_explicit_class),
            }
        )

    if mismatches:
        LOGGER.warning(
            "decision_authority_bridge_mismatch %s",
            json.dumps(
                {
                    "caller": str(caller or "").strip(),
                    "canonical_truth_root": str(new.canonical_runtime_truth_root),
                    "truth_sleeves_root": str(new.canonical_runtime_truth_sleeves_root),
                    "release_root": str(new.active_release_root),
                    "allowed_truth_roots": [str(item) for item in sorted(new.allowed_truth_roots, key=str)],
                    "explicit_truth_root": str(explicit_truth_root or "").strip(),
                    "release_current_path": str(new.release_current_path),
                    "release_current_id": str(new.release_current_id),
                    "mismatches": mismatches,
                },
                sort_keys=True,
            ),
        )


def _load_decision_authority_pair_v1(
    *,
    repo_root: Path,
    caller: str,
) -> tuple[DecisionPathAuthorityBridgeV1, DecisionPathAuthorityBridgeV1]:
    old_authority = _build_legacy_authority(repo_root=repo_root)
    release_current_authority = load_release_current_runtime_authority_v1(caller=caller)
    if str(release_current_authority.get("source") or "").strip() != "release_current_v1":
        return old_authority, old_authority
    new_authority = _build_release_current_authority(release_current_authority)
    _log_decision_authority_bridge_mismatch(
        caller=caller,
        old=old_authority,
        new=new_authority,
    )
    return new_authority, old_authority


def load_decision_path_authority_bridge_v1(
    *,
    repo_root: Path | None = None,
    caller: str = "",
) -> DecisionPathAuthorityBridgeV1:
    resolved_repo_root = Path(repo_root or REPO_ROOT).resolve()
    selected, _ = _load_decision_authority_pair_v1(
        repo_root=resolved_repo_root,
        caller=caller,
    )
    return selected


def resolve_decision_truth_root_bridge_v1(
    explicit_truth_root: str | Path | None,
    *,
    repo_root: Path | None = None,
    caller: str = "",
) -> Path:
    resolved_repo_root = Path(repo_root or REPO_ROOT).resolve()
    selected_authority, old_authority = _load_decision_authority_pair_v1(
        repo_root=resolved_repo_root,
        caller=caller,
    )
    if explicit_truth_root is None or not str(explicit_truth_root).strip():
        return selected_authority.canonical_runtime_truth_root

    resolved = Path(str(explicit_truth_root)).expanduser().resolve()
    old_classification = _classify_runtime_path_bridge_v1(resolved, authority=old_authority)
    new_classification = _classify_runtime_path_bridge_v1(resolved, authority=selected_authority)
    old_allowed = bool(old_classification["policy_read_allowed"])
    new_allowed = bool(new_classification["policy_read_allowed"])
    _log_decision_authority_bridge_mismatch(
        caller=caller,
        old=old_authority,
        new=selected_authority,
        explicit_truth_root=str(resolved),
        old_explicit_allowed=old_allowed,
        new_explicit_allowed=new_allowed,
        old_explicit_class=str(old_classification["path_class"]),
        new_explicit_class=str(new_classification["path_class"]),
    )

    # Preserve legacy policy semantics and prohibit widening.
    if not old_allowed:
        raise ValueError(
            f"RUNTIME_PATH_AUTHORITY_POLICY_TRUTH_ROOT_FORBIDDEN:path={resolved}:class={old_classification['path_class']}"
        )
    if not new_allowed:
        raise ValueError(
            f"RUNTIME_PATH_AUTHORITY_POLICY_TRUTH_ROOT_FORBIDDEN:path={resolved}:class={new_classification['path_class']}"
        )
    return resolved


def resolve_runtime_path_authority_snapshot_bridge_v1(
    *,
    repo_root: Path | None = None,
    caller: str = "",
) -> dict[str, str]:
    authority = load_decision_path_authority_bridge_v1(repo_root=repo_root, caller=caller)
    return {
        "authoritative_repo_root": str(authority.authoritative_repo_root),
        "canonical_runtime_truth_root": str(authority.canonical_runtime_truth_root),
        "canonical_runtime_truth_sleeves_root": str(authority.canonical_runtime_truth_sleeves_root),
        "authoritative_repo_truth_root": str(authority.authoritative_repo_truth_root),
        "authoritative_repo_truth_sleeves_root": str(authority.authoritative_repo_truth_sleeves_root),
        "active_release_root": str(authority.active_release_root),
    }
