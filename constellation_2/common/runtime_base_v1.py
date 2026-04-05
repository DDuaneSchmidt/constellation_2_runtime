from __future__ import annotations

from pathlib import Path
import sys

_DEFAULT_ADVISOR_RUNTIME_ROOT = Path('/tmp/constellation_2_foundation/advisor_runtime').resolve()



def source_root_from_file(file_path: str | Path) -> Path:
    return Path(file_path).resolve().parents[2]


def canonical_tools_root(file_path: str | Path) -> Path:
    return source_root_from_file(file_path) / 'ops' / 'tools'


def ensure_repo_root_on_sys_path(file_path: str | Path) -> Path:
    root = source_root_from_file(file_path)
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return root


def advisor_runtime_root() -> Path:
    return _DEFAULT_ADVISOR_RUNTIME_ROOT


def resolve_advisor_output_root(output_root: str | Path) -> Path:
    root = Path(output_root).expanduser().resolve()
    if root != _DEFAULT_ADVISOR_RUNTIME_ROOT:
        raise ValueError('OUTPUT_ROOT_MUST_BE_ADVISOR_RUNTIME_ROOT')
    return root


def advisor_runtime_path(output_root: str | Path, mode: str, *parts: str | Path) -> Path:
    return resolve_advisor_output_root(output_root) / Path(mode) / Path(*[str(part) for part in parts])
