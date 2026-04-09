from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


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


def load_active_runtime_contract_or_fail() -> dict[str, Any]:
    contract_path = _contract_path()
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"FAIL: active runtime contract unreadable path={contract_path} err={type(exc).__name__}:{exc}") from exc
    validate_against_repo_schema_v1(payload, REPO_ROOT, ACTIVE_RUNTIME_CONTRACT_SCHEMA_RELPATH)
    if str(payload.get("status") or "").strip().upper() != "ACTIVE":
        raise SystemExit(f"FAIL: active runtime contract status must be ACTIVE path={contract_path}")
    return payload


def resolve_runtime_data_root() -> Path:
    contract = load_active_runtime_contract_or_fail()
    return _require_absolute_dir(Path(str(contract["runtime_data_root"])), label="runtime_data_root")


def resolve_canonical_truth_root() -> Path:
    contract = load_active_runtime_contract_or_fail()
    return _require_absolute_dir(Path(str(contract["canonical_truth_root"])), label="canonical_truth_root")


def resolve_truth_sleeves_root() -> Path:
    contract = load_active_runtime_contract_or_fail()
    return _require_absolute_dir(Path(str(contract["truth_sleeves_root"])), label="truth_sleeves_root")


def _allowed_truth_roots(contract: dict[str, Any]) -> list[Path]:
    raw = contract.get("allowed_truth_roots")
    if not isinstance(raw, list) or not raw:
        raise SystemExit("FAIL: active runtime contract allowed_truth_roots must be non-empty list")
    roots = [_require_absolute_dir(Path(str(item)), label="allowed_truth_root") for item in raw]
    for required in (resolve_canonical_truth_root(), resolve_truth_sleeves_root()):
        if not any(root == required for root in roots):
            raise SystemExit(f"FAIL: active runtime contract missing required allowed_truth_root: {required}")
    return roots


def require_truth_root_under_contract(truth_root: Path) -> Path:
    contract = load_active_runtime_contract_or_fail()
    resolved = _require_absolute_dir(Path(truth_root), label="truth_root")
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
    contract = load_active_runtime_contract_or_fail()
    provenance_mode = str(contract.get("provenance_mode") or "").strip()
    if provenance_mode != "release_manifest":
        raise SystemExit(f"FAIL: unsupported active runtime contract provenance_mode={provenance_mode!r}")
    release_root = _require_absolute_dir(Path(str(contract["release_root"])), label="release_root")
    manifest_path = _require_absolute_file(release_root / "release_manifest.v1.json", label="release_manifest")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_against_repo_schema_v1(manifest, release_root, RELEASE_MANIFEST_SCHEMA_RELPATH)

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


def write_active_runtime_contract_bytes(payload: dict[str, Any]) -> bytes:
    validate_against_repo_schema_v1(payload, REPO_ROOT, ACTIVE_RUNTIME_CONTRACT_SCHEMA_RELPATH)
    return canonical_json_bytes_v1(payload) + b"\n"
