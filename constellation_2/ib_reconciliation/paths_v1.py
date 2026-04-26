from __future__ import annotations

import os
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_DATA_ROOT = Path("/home/node/constellation_runtime_data").resolve()
DEFAULT_IB_RECONCILIATION_ROOT = (RUNTIME_DATA_ROOT / "ib_reconciliation").resolve()
TRUTH_SURFACE_REPORT_ROOT = (RUNTIME_DATA_ROOT / "truth" / "reports" / "ib_reconciliation_v1").resolve()
RUNTIME_ROOT_ENV_VAR = "IB_RECONCILIATION_RUNTIME_ROOT"
ALLOW_ANY_RUNTIME_ROOT_ENV_VAR = "IB_RECONCILIATION_ALLOW_ANY_RUNTIME_ROOT"

DAY_SUBDIRS = (
    "ib_raw",
    "normalized_ib",
    "aegis_expected",
    "reconciliations",
    "ai_reviews",
)

NON_DAY_SUBDIRS = (
    "registry",
    "alerts",
)

DAY_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class PathContractError(RuntimeError):
    pass


def validate_day_utc_v1(day_utc: str) -> str:
    text = str(day_utc or "").strip()
    if not DAY_PATTERN.match(text):
        raise PathContractError(f"DAY_UTC_INVALID:{day_utc!r}")
    return text


def _require_absolute_under_runtime_data(path: Path, *, label: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_absolute():
        raise PathContractError(f"{label}_NOT_ABSOLUTE:{resolved}")
    if str(os.environ.get(ALLOW_ANY_RUNTIME_ROOT_ENV_VAR) or "").strip() == "1":
        return resolved
    try:
        resolved.relative_to(RUNTIME_DATA_ROOT)
    except ValueError as exc:
        raise PathContractError(f"{label}_OUTSIDE_RUNTIME_DATA_ROOT:{resolved}") from exc
    return resolved


def ensure_not_under_repo_v1(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    try:
        resolved.relative_to(REPO_ROOT)
    except ValueError:
        return resolved
    raise PathContractError(f"PATH_INSIDE_REPO_FORBIDDEN:{resolved}")


def runtime_root_v1() -> Path:
    raw = str(os.environ.get(RUNTIME_ROOT_ENV_VAR) or "").strip()
    if not raw:
        root = DEFAULT_IB_RECONCILIATION_ROOT
    else:
        root = Path(raw).expanduser().resolve()
    root = _require_absolute_under_runtime_data(root, label="IB_RECONCILIATION_ROOT")
    ensure_not_under_repo_v1(root)
    return root


def truth_surface_report_path_v1(day_utc: str) -> Path:
    day = validate_day_utc_v1(day_utc)
    return _require_absolute_under_runtime_data(
        (TRUTH_SURFACE_REPORT_ROOT / day / "ib_reconciliation.v1.json").resolve(),
        label="TRUTH_SURFACE_REPORT_PATH",
    )


def runtime_day_dir_v1(kind: str, day_utc: str) -> Path:
    if kind not in DAY_SUBDIRS:
        raise PathContractError(f"RUNTIME_KIND_UNSUPPORTED:{kind}")
    day = validate_day_utc_v1(day_utc)
    return _require_absolute_under_runtime_data((runtime_root_v1() / kind / day).resolve(), label=f"RUNTIME_DAY_DIR_{kind.upper()}")


def runtime_artifact_path_v1(kind: str, day_utc: str, filename: str) -> Path:
    text = str(filename or "").strip()
    if not text:
        raise PathContractError("ARTIFACT_FILENAME_MISSING")
    return (runtime_day_dir_v1(kind, day_utc) / text).resolve()


def alert_candidate_path_v1() -> Path:
    return _require_absolute_under_runtime_data(
        (runtime_root_v1() / "alerts" / "ib_reconciliation_alert_candidate.v1.json").resolve(),
        label="ALERT_CANDIDATE_PATH",
    )


def ensure_runtime_layout_v1(day_utc: str) -> dict[str, Path]:
    day = validate_day_utc_v1(day_utc)
    root = runtime_root_v1()
    root.mkdir(parents=True, exist_ok=True)
    ensure_not_under_repo_v1(root)

    for sub in NON_DAY_SUBDIRS:
        p = (root / sub).resolve()
        _require_absolute_under_runtime_data(p, label=f"RUNTIME_DIR_{sub.upper()}")
        p.mkdir(parents=True, exist_ok=True)

    out: dict[str, Path] = {}
    for sub in DAY_SUBDIRS:
        p = (root / sub / day).resolve()
        _require_absolute_under_runtime_data(p, label=f"RUNTIME_DAY_DIR_{sub.upper()}")
        p.mkdir(parents=True, exist_ok=True)
        out[sub] = p

    truth_dir = truth_surface_report_path_v1(day).parent
    truth_dir.mkdir(parents=True, exist_ok=True)
    out["truth_surface"] = truth_dir
    return out
