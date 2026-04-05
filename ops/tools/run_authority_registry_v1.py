from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

try:
    from constellation_2.common.runtime_base_v1 import advisor_runtime_path, advisor_runtime_root, canonical_tools_root, ensure_repo_root_on_sys_path, source_root_from_file
except ModuleNotFoundError:  # pragma: no cover - direct script execution bootstrap
    import importlib.util

    _RUNTIME_BASE_PATH = Path(__file__).resolve().parents[2] / 'constellation_2' / 'common' / 'runtime_base_v1.py'
    _RUNTIME_BASE_SPEC = importlib.util.spec_from_file_location('constellation_2.common.runtime_base_v1', _RUNTIME_BASE_PATH)
    if _RUNTIME_BASE_SPEC is None or _RUNTIME_BASE_SPEC.loader is None:
        raise RuntimeError(f'RUNTIME_BASE_IMPORT_FAILED: {_RUNTIME_BASE_PATH}')
    _runtime_base_v1 = importlib.util.module_from_spec(_RUNTIME_BASE_SPEC)
    _RUNTIME_BASE_SPEC.loader.exec_module(_runtime_base_v1)
    advisor_runtime_path = _runtime_base_v1.advisor_runtime_path
    advisor_runtime_root = _runtime_base_v1.advisor_runtime_root
    canonical_tools_root = _runtime_base_v1.canonical_tools_root
    ensure_repo_root_on_sys_path = _runtime_base_v1.ensure_repo_root_on_sys_path
    source_root_from_file = _runtime_base_v1.source_root_from_file

SOURCE_ROOT = ensure_repo_root_on_sys_path(__file__)

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1
from constellation_2.common.authority_registry_v1 import build_authority_registry

ADVISOR_RUNTIME_ROOT = advisor_runtime_root()

def _write(path: Path, obj: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
    if path.exists() and path.read_bytes() != payload:
        raise ValueError(f'IMMUTABLE_CONFLICT: {path}')
    path.write_bytes(payload)
    return path

def _runtime_path(output_root: str, mode: str, day_utc: str) -> Path:
    return advisor_runtime_path(output_root, mode, 'reports', 'authority_registry_v1', day_utc, 'authority_registry.v1.json')

def main() -> int:
    ap = argparse.ArgumentParser(description='Emit authority_registry.v1.json')
    ap.add_argument('--mode', required=True)
    ap.add_argument('--day_utc', required=True)
    ap.add_argument('--produced_utc', required=True)
    ap.add_argument('--output_root', required=True)
    args = ap.parse_args()
    env = metadata_envelope_v1(produced_utc=args.produced_utc, day_utc=args.day_utc, mode=args.mode, source_artifact_refs=(), artifact_family='authority_registry_v1')
    out = _write(_runtime_path(args.output_root, args.mode, args.day_utc), build_authority_registry(envelope=env).to_dict())
    print(f'OK: {out}')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
