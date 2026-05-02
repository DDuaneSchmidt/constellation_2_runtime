from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_DATA_ROOT = Path("/home/node/constellation_runtime_data").resolve()
PRODUCTION_TRUTH_ROOT = (RUNTIME_DATA_ROOT / "production_truth").resolve()
CANDIDATE_TRUTH_ROOT = (RUNTIME_DATA_ROOT / "candidate_truth").resolve()
PRODUCTION_VERSION_PATH = (PRODUCTION_TRUTH_ROOT / "governance" / "production_version.v1.json").resolve()
RUNTIME_MODES = {"PRODUCTION", "CANDIDATE"}


def now_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json_v1(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_v1(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def git_commit_v1() -> str:
    proc = subprocess.run(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], capture_output=True, text=True, check=False)
    return str(proc.stdout or "").strip() if proc.returncode == 0 else ""


def git_dirty_status_v1() -> str:
    proc = subprocess.run(["git", "-C", str(REPO_ROOT), "status", "--short"], capture_output=True, text=True, check=False)
    return "DIRTY" if str(proc.stdout or "").strip() else "CLEAN"


def normalize_runtime_mode_v1(value: str | None) -> str:
    mode = str(value or "").strip().upper()
    if mode in RUNTIME_MODES:
        return mode
    return "PRODUCTION"


def runtime_mode_from_truth_root_v1(truth_root: Path, runtime_mode: str | None = None) -> str:
    explicit = str(runtime_mode or "").strip().upper()
    if explicit in RUNTIME_MODES:
        return explicit
    env_mode = str(os.environ.get("AEGIS_RUNTIME_MODE") or "").strip().upper()
    if env_mode in RUNTIME_MODES:
        return env_mode
    resolved = Path(truth_root).expanduser().resolve()
    if resolved == CANDIDATE_TRUTH_ROOT or str(resolved).startswith(str(CANDIDATE_TRUTH_ROOT) + "/"):
        return "CANDIDATE"
    if resolved == PRODUCTION_TRUTH_ROOT or str(resolved).startswith(str(PRODUCTION_TRUTH_ROOT) + "/"):
        return "PRODUCTION"
    if resolved.name == "candidate_truth":
        return "CANDIDATE"
    if resolved.name == "production_truth":
        return "PRODUCTION"
    return "PRODUCTION"


def runtime_truth_root_v1(runtime_mode: str | None = None) -> Path:
    mode = normalize_runtime_mode_v1(runtime_mode or os.environ.get("AEGIS_RUNTIME_MODE"))
    return PRODUCTION_TRUTH_ROOT if mode == "PRODUCTION" else CANDIDATE_TRUTH_ROOT


def packet_root_v1(runtime_root: Path | None = None) -> Path:
    return Path(runtime_root or RUNTIME_DATA_ROOT).expanduser().resolve()


def production_version_path_v1(truth_root: Path | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve() if truth_root is not None else PRODUCTION_TRUTH_ROOT
    return (root / "governance" / "production_version.v1.json").resolve()


def read_production_version_v1(truth_root: Path | None = None) -> dict[str, Any]:
    return read_json_v1(production_version_path_v1(truth_root))


def ensure_runtime_roots_v1() -> None:
    for root in (PRODUCTION_TRUTH_ROOT, CANDIDATE_TRUTH_ROOT):
        (root / "reports").mkdir(parents=True, exist_ok=True)
        (root / "governance").mkdir(parents=True, exist_ok=True)


def assert_candidate_cannot_write_production_v1(*, runtime_mode: str, output_path: Path) -> None:
    mode = normalize_runtime_mode_v1(runtime_mode)
    resolved = Path(output_path).expanduser().resolve()
    parts = set(resolved.parts)
    if mode == "CANDIDATE" and (
        resolved == PRODUCTION_TRUTH_ROOT
        or str(resolved).startswith(str(PRODUCTION_TRUTH_ROOT) + "/")
        or "production_truth" in parts
    ):
        raise SystemExit(f"FAIL: CANDIDATE_RUNTIME_CANNOT_WRITE_PRODUCTION_TRUTH: {resolved}")


def copy_candidate_artifacts_v1(*, candidate_root: Path, production_root: Path, relative_paths: list[str]) -> list[dict[str, Any]]:
    copied: list[dict[str, Any]] = []
    candidate = Path(candidate_root).resolve()
    production = Path(production_root).resolve()
    for rel in relative_paths:
        rel_path = Path(str(rel).lstrip("/"))
        src = (candidate / rel_path).resolve()
        dst = (production / rel_path).resolve()
        if not str(src).startswith(str(candidate) + "/") or not str(dst).startswith(str(production) + "/"):
            raise SystemExit(f"FAIL: INVALID_PROMOTION_COPY_PATH: {rel}")
        if not src.exists() or not src.is_file():
            copied.append({"relative_path": str(rel_path), "source_path": str(src), "destination_path": str(dst), "status": "MISSING_SOURCE"})
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied.append({"relative_path": str(rel_path), "source_path": str(src), "destination_path": str(dst), "status": "COPIED"})
    return copied
