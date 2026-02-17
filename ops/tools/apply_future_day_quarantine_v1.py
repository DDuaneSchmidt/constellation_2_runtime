#!/usr/bin/env python3
"""
apply_future_day_quarantine_v1.py

One-shot, fail-closed patcher:
- Adds enforce_operational_day_key_invariant_v1(day) to all operational writers that accept --day_utc.
- Refuses to patch if patterns are ambiguous.
- Does NOT touch runtime truth artifacts.
- Prints sha256 before/after for every modified file.

Run:
  python3 ops/tools/apply_future_day_quarantine_v1.py
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import List, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()

TARGETS = [
    # from your rg scan (ops + constellation_2)
    "constellation_2/phaseC/tools/run_phaseC_preflight_day_v1.py",
    "constellation_2/phaseF/accounting/run/run_accounting_day_v1.py",
    "constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py",
    "constellation_2/phaseF/defined_risk/run/run_defined_risk_day_v1.py",
    "constellation_2/phaseF/execution_evidence/run/run_execution_evidence_truth_day_v1.py",
    "constellation_2/phaseF/position_lifecycle/run/run_position_lifecycle_day_v1.py",
    "constellation_2/phaseF/positions/run/run_positions_effective_pointer_day_v1.py",
    "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v1.py",
    "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
    "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v3.py",
    "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py",
    "constellation_2/phaseG/allocation/run/run_allocation_day_v1.py",
    "constellation_2/phaseG/bundles/run/run_bundle_f_to_g_day_v1.py",
    "constellation_2/phaseH/tools/c2_risk_transformer_offline_v1.py",
    "constellation_2/phaseH/tools/run_oms_decisions_day_v1.py",
    "constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py",
    "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py",
    "constellation_2/phaseI/vol_income_defined_risk/run/run_vol_income_defined_risk_intents_day_v1.py",
    "constellation_2/phaseJ/acceptance/verify_engine_attribution_reconciliation_v1.py",
    "constellation_2/phaseJ/monitoring/run/run_capital_efficiency_day_v1.py",
    "constellation_2/phaseJ/monitoring/run/run_degradation_sentinel_day_v1.py",
    "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py",
    "constellation_2/phaseJ/monitoring/run/run_engine_daily_returns_day_v1.py",
    "constellation_2/phaseJ/monitoring/run/run_engine_metrics_day_v1.py",
    "constellation_2/phaseJ/monitoring/run/run_portfolio_nav_series_day_v1.py",
    "constellation_2/phaseJ/reporting/daily_snapshot_v1.py",
    "ops/ib/run_broker_event_day_manifest_v1.py",
    "ops/run/c2_supervisor_paper_v2.py",
    "ops/tools/gen_drawdown_window_pack_v1.py",
    "ops/tools/gen_economic_truth_availability_certificate_v1.py",
    "ops/tools/gen_nav_history_ledger_v1.py",
    "ops/tools/gen_nav_snapshot_v1.py",
    "ops/tools/run_c2_daily_operator_gate_v1.py",
    "ops/tools/run_c2_paper_day_orchestrator_v1.py",
    "ops/tools/run_daily_portfolio_snapshot_v1.py",
    "ops/tools/run_engine_model_registry_gate_v1.py",
    "ops/tools/run_engine_risk_budget_ledger_v1.py",
    "ops/tools/run_exposure_reconciliation_v1.py",
    "ops/tools/run_global_kill_switch_v1.py",
    "ops/tools/run_intents_day_rollup_v1.py",
    "ops/tools/run_lifecycle_ledger_v1.py",
    "ops/tools/run_operator_daily_gate_v1.py",
    "ops/tools/run_operator_gate_verdict_v1.py",
    "ops/tools/run_pipeline_manifest_v1.py",
    "ops/tools/run_reconciliation_report_v1.py",
    "ops/tools/run_reconciliation_report_v2.py",
    "ops/tools/run_regime_snapshot_v1.py",
    "ops/tools/run_regime_snapshot_v2.py",
    "ops/tools/run_submission_index_v1.py",
    "ops/tools/validate_economic_nav_drawdown_bundle_v1.py",
]

IMPORT_LINE = "from constellation_2.phaseD.lib.enforce_operational_day_invariant_v1 import enforce_operational_day_key_invariant_v1\n"

# patterns for day assignment (we patch the line after)
PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    # day = _parse_day_utc(args.day_utc)
    (re.compile(r"^\s*day\s*=\s*_parse_day_utc\(args\.day_utc\)\s*$", re.M), "    enforce_operational_day_key_invariant_v1(day)\n"),
    # day = _parse_day_utc(args.day_utc) with different var name
    (re.compile(r"^\s*day\s*=\s*_parse_day_utc\(args\.day_utc\)\s*$", re.M), "    enforce_operational_day_key_invariant_v1(day)\n"),
    # day = str(args.day_utc).strip()
    (re.compile(r"^\s*day\s*=\s*str\(args\.day_utc\)\.strip\(\)\s*$", re.M), "    enforce_operational_day_key_invariant_v1(day)\n"),
]

def sha256_text(t: str) -> str:
    return hashlib.sha256(t.encode("utf-8")).hexdigest()

def ensure_import(text: str) -> str:
    if "enforce_operational_day_key_invariant_v1" in text:
        return text
    # Insert after existing imports block: place after last "from __future__" or after first import group.
    lines = text.splitlines(keepends=True)
    insert_at = None
    for i, line in enumerate(lines):
        if line.startswith("from __future__ import"):
            insert_at = i + 1
            continue
    if insert_at is None:
        # fallback: after shebang/docstring; find first "import"
        for i, line in enumerate(lines):
            if line.startswith("import ") or line.startswith("from "):
                insert_at = i
                break
    if insert_at is None:
        raise SystemExit("FAIL: cannot find import insertion point")
    lines.insert(insert_at, "\n" if (insert_at > 0 and lines[insert_at-1].strip() != "") else "")
    lines.insert(insert_at + 1, IMPORT_LINE)
    return "".join(lines)

def insert_call(text: str) -> str:
    if "FUTURE_DAY_UTC_DISALLOWED" in text or "enforce_operational_day_key_invariant_v1(" in text:
        return text
    matches: List[Tuple[int, int, str]] = []
    for pat, call_line in PATTERNS:
        m = pat.search(text)
        if m:
            matches.append((m.start(), m.end(), call_line))
    if len(matches) != 1:
        raise SystemExit(f"FAIL: ambiguous or missing day assignment pattern matches={len(matches)}")
    _, end, call_line = matches[0]
    # insert call line after the matched line (preserve newline)
    # find end-of-line
    nl = text.find("\n", end)
    if nl == -1:
        nl = len(text)
        suffix = ""
    else:
        suffix = text[nl+1:]
    prefix = text[:nl+1]
    return prefix + call_line + suffix

def patch_file(relpath: str) -> Tuple[str, str, bool]:
    p = (REPO_ROOT / relpath).resolve()
    if not p.exists():
        raise SystemExit(f"FAIL: missing file: {relpath}")
    orig = p.read_text(encoding="utf-8")
    before = sha256_text(orig)
    t = ensure_import(orig)
    t = insert_call(t)
    after = sha256_text(t)
    changed = (after != before)
    if changed:
        p.write_text(t, encoding="utf-8")
    return before, after, changed

def main() -> int:
    changed_any = False
    for rel in TARGETS:
        before, after, changed = patch_file(rel)
        if changed:
            changed_any = True
            print(f"PATCHED {rel} sha256_before={before} sha256_after={after}")
        else:
            print(f"SKIP {rel} sha256={before}")
    print("OK: APPLY_FUTURE_DAY_QUARANTINE_COMPLETE changed_any=" + ("true" if changed_any else "false"))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
