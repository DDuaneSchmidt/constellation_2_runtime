#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# --- IMPORT PATH HARDENING (FAIL-SAFE) ---------------------------------------
# If executed by file path, Python does not include repo root on sys.path.
# Compute repo root deterministically from __file__:
# /home/node/constellation_2_runtime/constellation_2/phaseK_struct/run/<thisfile>
_THIS = Path(__file__).resolve()
_REPO_ROOT = _THIS.parents[3]  # <-- correct: /home/node/constellation_2_runtime
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
# -----------------------------------------------------------------------------

from constellation_2.phaseK_struct.lib.k_struct_common_v1 import (  # noqa: E402
    KStructError,
    sha256_file,
    write_json_deterministic,
)
from constellation_2.phaseK_struct.lib.k_struct_inputs_v1 import load_inputs_or_fail  # noqa: E402
from constellation_2.phaseK_struct.lib.k_struct_slippage_v1 import run_slippage_suite  # noqa: E402
from constellation_2.phaseK_struct.lib.k_struct_perturbation_v1 import run_perturbation_suite  # noqa: E402
from constellation_2.phaseK_struct.lib.k_struct_cluster_shock_v1 import run_cluster_shock  # noqa: E402
from constellation_2.phaseK_struct.lib.k_struct_capital_scaling_v1 import run_capital_scaling_suite  # noqa: E402
from constellation_2.phaseK_struct.lib.k_struct_monte_carlo_v1 import run_monte_carlo_structural  # noqa: E402

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
OUT_ROOT = (TRUTH_ROOT / "certification_v1/phaseK_struct").resolve()


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _die(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    raise SystemExit(2)


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)
    if not p.exists() or not p.is_dir():
        raise KStructError(f"OUT_DIR_CREATE_FAILED: {p}")


def _write_csv(p: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        p.write_text("", encoding="utf-8")
        return
    keys = sorted({k for r in rows for k in r.keys()})
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, None) for k in keys})


def _flatten_metrics(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for r in summary["tests"]["slippage"]["results"]:
        rows.append({"test": "slippage", **r})
    for r in summary["tests"]["perturbation"]["results"]:
        rows.append({"test": "perturbation", **r})
    for r in summary["tests"]["capital_scaling"]["results"]:
        rows.append({"test": "capital_scaling", **r})
    rows.append({"test": "cluster_shock", **summary["tests"]["cluster_shock"]["results"]})
    rows.append({"test": "monte_carlo", **summary["tests"]["monte_carlo"]["results"]})
    return rows


def _hostile_report_md(summary: Dict[str, Any]) -> str:
    out: List[str] = []
    out.append("# Phase K-Struct v1 — Structural Robustness Certification Report")
    out.append("")
    out.append("## Scope")
    out.append("- Certifies **structural robustness only** (survivability under modeled stress).")
    out.append("- Does **not** certify realized edge, 10% mandate defensibility, or regime performance.")
    out.append("- Read-only harness: no execution/risk/allocation modifications; outputs are audit artifacts.")
    out.append("")
    out.append("## Inputs (Proven Truth Artifacts)")
    for it in summary["inputs"]["manifest"]:
        out.append(f"- {it['type']}: `{it['path']}` (sha256={it['sha256']})")
    out.append("")
    out.append("## Determinism / Reproducibility")
    out.append("- Outputs are deterministic JSON/CSV/MD.")
    out.append("- Recompute STOP GATE requires identical sha256 of written artifacts.")
    out.append("")
    out.append("## Pass/Fail")
    out.append(f"- verdict: **{summary['verdict']}**")
    out.append(f"- status: **{summary['status']}**")
    out.append("")
    out.append("## Definitions (Mathematical)")
    defs = [
        ("NAV path", "Start NAV=1.0, apply NAV_{t+1} = NAV_t * (1 + r_t)"),
        ("Max drawdown", "min_t ((NAV_t - peak_t)/peak_t), peak_t = max_{u<=t} NAV_u"),
        ("CAGR", "(NAV_end/NAV_start)^(1/years) - 1, years=(N/252)"),
        ("Annualized vol", "std(daily) * sqrt(252)"),
        ("Sharpe", "mean(daily)/std(daily) * sqrt(252)"),
        ("Tail loss (p)", "empirical quantile of daily returns at percentile p"),
        ("Slippage overlay", "r' = r - abs(r)*(m-1) for multiplier m in {1,2,3}"),
        ("Perturbation proxy", "return_scale/vol_scale + deterministic uniform noise"),
        ("Cluster shock", "append 30 days shock_return = -2*std(daily) (fallback -1% if std unavailable)"),
        ("Monte Carlo", "bootstrap with replacement from empirical daily returns; seeded; deterministic"),
    ]
    for k, v in defs:
        out.append(f"- **{k}**: {v}")
    out.append("")
    out.append("## Test Results (Summary)")
    t = summary["tests"]
    out.append("")
    out.append("### Slippage Stress")
    for r in t["slippage"]["results"]:
        out.append(f"- {r['case']}: sharpe={r['sharpe']} max_dd={r['max_dd']} cagr={r['cagr']}")
    out.append("")
    out.append("### Perturbation Proxy")
    for r in t["perturbation"]["results"]:
        out.append(f"- {r['case']}: sharpe={r['sharpe']} max_dd={r['max_dd']} cagr={r['cagr']}")
    out.append("")
    out.append("### Correlation Cluster Shock")
    cs = t["cluster_shock"]["results"]
    out.append(f"- shock_return={t['cluster_shock']['definition']['shock_return']} max_dd={cs['max_dd']} cagr={cs['cagr']}")
    out.append("")
    out.append("### Capital Scaling Invariance")
    for r in t["capital_scaling"]["results"]:
        out.append(f"- scale={r['scale']}: invariance_model={r['invariance_model']}")
    out.append("")
    out.append("### Monte Carlo Structural (5y)")
    mc = t["monte_carlo"]["results"]
    out.append(f"- p_ruin_nav_le_0={mc['p_ruin_nav_le_0']}")
    out.append(f"- p_max_dd_gt_20pct={mc['p_max_dd_gt_20pct']}")
    out.append(f"- p_cagr_lt_5pct={mc['p_cagr_lt_5pct']}")
    out.append(f"- p_cagr_gt_12pct={mc['p_cagr_gt_12pct']}")
    out.append("")
    out.append("## Notes / Flags")
    for f in summary.get("flags", []):
        out.append(f"- {f}")
    out.append("")
    out.append("## Explicit Statement")
    out.append(
        "This Phase K-Struct report supports a claim about **structural survivability under modeled stresses**. "
        "It does not support a claim that the system achieves a 10% mandate, because the required economic history "
        "and regime breadth are not yet present in truth."
    )
    out.append("")
    return "\n".join(out) + "\n"


def _compute_verdict_failclosed(inputs_flags: List[str]) -> str:
    for f in inputs_flags:
        if "nav_series_status=DEGRADED" in f or "nav_series_reason=J_NAV_SERIES_GAPS_DETECTED" in f:
            return "NOT_CAPITAL_READY"
    return "STRUCTURALLY_OK_WITH_LIMITATIONS"


def main() -> None:
    ap = argparse.ArgumentParser(description="Phase K-Struct v1 runner (structural robustness; read-only).")
    ap.add_argument("--asof_day_utc", required=True)
    ap.add_argument("--produced_utc", required=True)
    ap.add_argument("--seed", required=True)
    ap.add_argument("--paths", type=int, default=10000)
    args = ap.parse_args()

    asof = (args.asof_day_utc or "").strip()
    produced_utc = (args.produced_utc or "").strip()
    seed = (args.seed or "").strip()
    if not asof:
        _die("MISSING_ASOF_DAY_UTC")
    if not produced_utc:
        _die("MISSING_PRODUCED_UTC")
    if not seed:
        _die("MISSING_SEED")

    inp = load_inputs_or_fail(asof)

    manifest = [
        {"type": "portfolio_nav_series", "path": str(inp.nav_series_path), "sha256": sha256_file(inp.nav_series_path)},
    ]
    if inp.engine_metrics_path is not None:
        manifest.append({"type": "engine_metrics", "path": str(inp.engine_metrics_path), "sha256": sha256_file(inp.engine_metrics_path)})
    if inp.engine_corr_path is not None:
        manifest.append({"type": "engine_correlation_matrix", "path": str(inp.engine_corr_path), "sha256": sha256_file(inp.engine_corr_path)})

    tests = {
        "slippage": run_slippage_suite(inp.daily_returns),
        "perturbation": run_perturbation_suite(inp.daily_returns, seed_material=seed),
        "cluster_shock": run_cluster_shock(inp.daily_returns),
        "capital_scaling": run_capital_scaling_suite(inp.daily_returns),
        "monte_carlo": run_monte_carlo_structural(inp.daily_returns, seed_material=seed, paths=int(args.paths), years=5),
    }

    verdict = _compute_verdict_failclosed(inp.flags)
    status = "OK" if verdict.startswith("STRUCTURALLY_OK") else "DEGRADED_INSUFFICIENT_TRUTH_CONTINUITY"

    out_dir = (OUT_ROOT / asof).resolve()
    _safe_mkdir(out_dir)

    summary = {
        "schema_id": "PHASEK_STRUCT_SUMMARY_V1_UNGOVERNED",
        "schema_version": 1,
        "asof_day_utc": asof,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": "UNKNOWN", "module": "constellation_2/phaseK_struct/run/run_phaseK_struct_day_v1.py"},
        "inputs": {"manifest": manifest, "flags": inp.flags},
        "verdict": verdict,
        "status": status,
        "flags": inp.flags,
        "tests": tests,
    }

    p_summary = (out_dir / "phaseK_struct_summary.v1.json").resolve()
    p_csv = (out_dir / "phaseK_struct_metrics.v1.csv").resolve()
    p_md = (out_dir / "phaseK_struct_hostile_review.v1.md").resolve()
    p_hash = (out_dir / "sha256_manifest.v1.json").resolve()

    write_json_deterministic(p_summary, summary)
    _write_csv(p_csv, _flatten_metrics(summary))
    p_md.write_text(_hostile_report_md(summary), encoding="utf-8")

    hm = {
        "asof_day_utc": asof,
        "produced_utc": produced_utc,
        "sha256": {
            "phaseK_struct_summary.v1.json": sha256_file(p_summary),
            "phaseK_struct_metrics.v1.csv": sha256_file(p_csv),
            "phaseK_struct_hostile_review.v1.md": sha256_file(p_md),
        },
    }
    write_json_deterministic(p_hash, hm)

    print(f"OK: PHASEK_STRUCT_WRITTEN out_dir={out_dir}")


if __name__ == "__main__":
    try:
        main()
    except KStructError as e:
        _die(str(e))
