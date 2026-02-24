#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = REPO_ROOT / "constellation_2/runtime/truth"

DAY0_RC_ALLOWED = "DAY0_BOOTSTRAP_ENGINE_ATTRIBUTION_MISSING_ALLOWED"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _json_dumps(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _immut_write(path: Path, content: bytes) -> None:
    if path.exists():
        if _sha256_bytes(path.read_bytes()) != _sha256_bytes(content):
            raise RuntimeError(f"ImmutableWriteError: ATTEMPTED_REWRITE path={path}")
        return
    _atomic_write(path, content)


def _load_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {p}")
    return obj


def _bootstrap_window_true(day_utc: str) -> bool:
    """
    Day-0 Bootstrap Window iff:
      TRUTH/execution_evidence_v1/submissions/<DAY>/ is missing OR contains zero submission dirs.
    """
    root = (TRUTH_ROOT / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    if (not root.exists()) or (not root.is_dir()):
        return True
    try:
        for p in root.iterdir():
            if p.is_dir():
                return False
    except Exception:
        # Fail-closed: if we cannot enumerate, treat as NOT bootstrap.
        return False
    return True


def _pick_source(day: str) -> Tuple[Optional[Path], str]:
    """
    Prefer v2 attribution; fall back to v1.
    Returns (path_or_none, source_label).
    """
    p_v2 = (TRUTH_ROOT / "accounting_v2" / "attribution" / day / "engine_attribution.v2.json").resolve()
    if p_v2.exists():
        return (p_v2, "accounting_v2")

    p_v1 = (TRUTH_ROOT / "accounting_v1" / "attribution" / day / "engine_attribution.json").resolve()
    if p_v1.exists():
        return (p_v1, "accounting_v1")

    return (None, "missing")


def _extract_attribution(o: Dict[str, Any], source_label: str) -> Tuple[str, List[Any], str, List[str]]:
    """
    Normalize attribution fields across v1 and v2 sources.

    Returns:
      (status, by_engine, currency, reason_codes)
    """
    status = str(o.get("status") or "UNKNOWN").strip()
    rcs = o.get("reason_codes", [])
    reason_codes: List[str] = [str(x) for x in rcs] if isinstance(rcs, list) else []

    attr = o.get("attribution", {})
    if not isinstance(attr, dict):
        attr = {}

    by_engine = attr.get("by_engine", [])
    if not isinstance(by_engine, list):
        by_engine = []

    currency = str(attr.get("currency", "USD"))

    # If v2 is degraded/missing inputs, keep status as-is; this is a proxy surface anyway.
    return (status, by_engine, currency, reason_codes)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_engine_pnl_proxy_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    day = str(args.day_utc).strip()

    produced_utc = f"{day}T00:00:00Z"
    bootstrap = _bootstrap_window_true(day)

    src, source_label = _pick_source(day)

    # Fail-closed if missing attribution when submissions exist (not bootstrap).
    if src is None:
        if not bootstrap:
            raise SystemExit(
                "FATAL: missing engine attribution (need accounting_v2 or accounting_v1): "
                f"{TRUTH_ROOT / 'accounting_v2/attribution' / day / 'engine_attribution.v2.json'} "
                f"OR {TRUTH_ROOT / 'accounting_v1/attribution' / day / 'engine_attribution.json'}"
            )

        # Day-0 bootstrap safe: write placeholder proxy (non-blocking).
        out = {
            "schema_id": "C2_MONITORING_ENGINE_PNL_PROXY_V1",
            "schema_version": "1.0.0",
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": "ops/tools/run_engine_pnl_proxy_day_v1.py",
            "source_attribution_path": "",
            "source_attribution_sha256": "0" * 64,
            "status": "NOT_AVAILABLE",
            "reason_codes": [DAY0_RC_ALLOWED, "MISSING_ATTRIBUTION_INPUTS"],
            "currency": "USD",
            "by_engine": [],
            "not_valid_for_return_correlation": True,
            "notes": [
                "Day-0 bootstrap: engine attribution missing; proxy emitted as empty (non-blocking).",
                "This is NOT marks-based return correlation.",
            ],
        }

        out_dir = TRUTH_ROOT / "monitoring_v1" / "engine_pnl_proxy_v1" / day
        out_path = out_dir / "engine_pnl_proxy.v1.json"
        _immut_write(out_path, _json_dumps(out))
        print(f"OK: wrote {out_path} (DAY0_BOOTSTRAP)")
        return 0

    # Normal path: load attribution (v2 preferred, v1 fallback)
    o = _load_json(src)
    status, by_engine, currency, reason_codes = _extract_attribution(o, source_label)

    out = {
        "schema_id": "C2_MONITORING_ENGINE_PNL_PROXY_V1",
        "schema_version": "1.0.0",
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": "ops/tools/run_engine_pnl_proxy_day_v1.py",
        "source_attribution_path": str(src.relative_to(TRUTH_ROOT)),
        "source_attribution_sha256": _sha256_file(src),
        "status": status,
        "reason_codes": reason_codes,
        "currency": str(currency),
        "by_engine": by_engine,
        "not_valid_for_return_correlation": True,
        "notes": [
            f"Derived from {source_label} engine attribution; not marks-based returns.",
            "Correlation based on returns is blocked until marks+linkage are available.",
        ],
    }

    out_dir = TRUTH_ROOT / "monitoring_v1" / "engine_pnl_proxy_v1" / day
    out_path = out_dir / "engine_pnl_proxy.v1.json"
    _immut_write(out_path, _json_dumps(out))

    print(f"OK: wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
