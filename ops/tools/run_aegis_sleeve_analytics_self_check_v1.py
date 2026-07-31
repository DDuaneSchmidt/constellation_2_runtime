#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.sleeve_analytics_v1 import build_sleeve_analytics_v1, sleeve_analytics_path_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_sleeve_analytics_self_check_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    path = sleeve_analytics_path_v1(truth_root=root, day_utc=day)
    artifact = _read_json(path)
    rebuilt = build_sleeve_analytics_v1(truth_root=root, day_utc=day)
    failures: list[str] = []
    if not artifact:
        failures.append(f"missing canonical sleeve analytics artifact: {path}")
        artifact = rebuilt
    if artifact.get("composite_sleeve_score_included") is True:
        failures.append("composite sleeve score appears in Phase 1")
    for row in artifact.get("sleeves") or []:
        if not isinstance(row, Mapping):
            continue
        realized = _num(row.get("realized_pnl")) or 0.0
        unrealized = _num(row.get("unrealized_pnl")) or 0.0
        total = _num(row.get("total_pnl"))
        if total is None or round(total, 6) != round(realized + unrealized, 6):
            failures.append(f"total_pnl formula inconsistent for sleeve {row.get('sleeve_id')}: total={total} realized={realized} unrealized={unrealized}")
    summary = artifact.get("summary") if isinstance(artifact.get("summary"), Mapping) else {}
    if _num(summary.get("total_pnl")) is not None:
        expected = round((_num(summary.get("total_realized_pnl")) or 0.0) + (_num(summary.get("total_unrealized_pnl")) or 0.0), 6)
        if round(_num(summary.get("total_pnl")) or 0.0, 6) != expected:
            failures.append("summary total_pnl formula inconsistent")
    if any(str(row.get("sleeve_id") or "") == "UNKNOWN" for row in artifact.get("sleeves") or []) and not any("UNKNOWN" in json.dumps(row) or "attribution" in json.dumps(row).lower() for row in artifact.get("diagnostics") or []):
        failures.append("UNKNOWN sleeve exists without attribution diagnostic")
    if _stable_projection(artifact) != _stable_projection(rebuilt):
        failures.append("canonical artifact fields disagree with rebuilt API projection")
    pages = (REPO_ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js").read_text(encoding="utf-8")
    start = pages.find("function renderAegisPaperPerformancePage()")
    end = pages.find("function renderAegisSleeveAnalyticsPage", start)
    performance_page = pages[start:end] if start >= 0 and end > start else pages
    if "payload.sleeves" in performance_page or "_sleeve_position_metrics" in performance_page:
        failures.append("browser-side sleeve analytics calculation or raw performance sleeve rows detected")
    if "Sleeve diagnostics" in performance_page and "<details" not in performance_page[performance_page.find("Sleeve diagnostics") - 500:performance_page.find("Sleeve diagnostics") + 1000]:
        failures.append("sleeve diagnostics are not collapsed in UI")
    result = {
        "ok": not failures,
        "day_utc": day,
        "path": str(path),
        "status": artifact.get("status"),
        "total_sleeves": summary.get("total_sleeves"),
        "active_sleeves": summary.get("active_sleeves"),
        "total_pnl": summary.get("total_pnl"),
        "failure_count": len(failures),
        "failures": failures,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not failures else 1


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _num(value: Any) -> float | None:
    if value in (None, "", "NOT_CANONICAL", "n/a"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def _stable_projection(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": payload.get("status"),
        "summary": payload.get("summary"),
        "sleeves": payload.get("sleeves"),
        "data_quality": payload.get("data_quality"),
    }


if __name__ == "__main__":
    raise SystemExit(main())
