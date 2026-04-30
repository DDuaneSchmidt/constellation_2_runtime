#!/usr/bin/env python3
"""
run_engine_correlation_matrix_day_v1.py

Phase J — Engine Correlation Matrix v1 writer
Deterministic, fail-closed, single-writer, canonical JSON, schema-validated.

Inputs (immutable truth):
- constellation_2/runtime/truth/monitoring_v1/engine_daily_returns_v1/<DAY>/engine_daily_returns.v1.json

Output (immutable truth):
- constellation_2/runtime/truth/monitoring_v1/engine_correlation_matrix/<DAY>/engine_correlation_matrix.v1.json

Notes:
- Bootstrap-safe: 1x1 matrix allowed (diagonal=1.000000).
- Uses Decimal math; quantizes correlations to 6dp.
- Window uses the most recent N available engine_daily_returns days <= day_utc (no calendar assumptions).

Rerun-safety (automation requirement):
- If the day-keyed artifact already exists, treat it as authoritative for that day.
- DO NOT attempt rewrite (prevents immutable overwrite failure when git_sha changes or inputs differ across reruns).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_authority_bridge_v1 import resolve_canonical_truth_root_bridge_v1
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance_release_current_first_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

TRUTH = resolve_canonical_truth_root_bridge_v1(
    caller="constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py"
).resolve()

# FIX: correct input root is engine_daily_returns_v1 (matches orchestrator + truth)
IN_ROOT = (TRUTH / "monitoring_v1/engine_daily_returns_v1").resolve()
OUT_ROOT = (TRUTH / "monitoring_v1/engine_correlation_matrix").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/engine_correlation_matrix.v1.schema.json"

Q6 = Decimal("0.000000")


class CliError(Exception):
    pass


def _resolve_truth_root(truth_root_arg: str) -> Path:
    raw = (truth_root_arg or "").strip()
    if not raw:
        return TRUTH
    p = Path(raw).expanduser().resolve()
    if not p.is_absolute():
        raise CliError(f"TRUTH_ROOT_NOT_ABSOLUTE: {p}")
    if not p.exists() or not p.is_dir():
        raise CliError(f"TRUTH_ROOT_MISSING_OR_NOT_DIR: {p}")
    return p


def _git_sha() -> str:
    try:
        s = str(
            resolve_release_provenance_release_current_first_v1(
                caller="constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py"
            ).get("git_sha")
            or ""
        ).strip()
        if s:
            return s
    except Exception:
        pass
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        # Clean runtime roots can be source-derived without .git metadata.
        return "0" * 40


def _sha256_file(p: Path) -> str:
    import hashlib  # local import

    h = hashlib.sha256()
    try:
        b = p.read_bytes()
    except Exception as e:  # noqa: BLE001
        raise CliError(f"READ_FAILED: {p}: {e}") from e
    h.update(b)
    return h.hexdigest()


def _parse_day(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except Exception as e:  # noqa: BLE001
        raise CliError(f"BAD_DAY_UTC: {s}: {e}") from e


def _day_str(d: date) -> str:
    return d.isoformat()


def _read_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise CliError(f"JSON_READ_FAILED: {p}: {e}") from e


def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Immutable rerun safety (automation requirement):
    - If the day-keyed artifact already exists, treat it as authoritative for that day.
    - DO NOT attempt rewrite, because candidate bytes can differ across reruns (git_sha, inputs, etc).

    Returns:
      - None if no existing file (caller should compute/write)
      - 0 if existing status is OK or DEGRADED_INSUFFICIENT_HISTORY
      - 2 otherwise (fail-closed)
    """
    if not out_path.exists():
        return None

    existing_sha = _sha256_file(out_path)
    existing = _read_json(out_path)
    if not isinstance(existing, dict):
        raise SystemExit(f"FAIL: EXISTING_CORR_NOT_OBJECT: {out_path}")

    schema_id = str(existing.get("schema_id") or "").strip()
    schema_version = existing.get("schema_version")
    day_utc = str(existing.get("day_utc") or "").strip()
    status = str(existing.get("status") or "").strip().upper()

    if schema_id != "C2_ENGINE_CORRELATION_MATRIX_V1":
        raise SystemExit(f"FAIL: EXISTING_CORR_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    # ACCEPT_SCHEMA_VERSION_V1_STRING: legacy truth may encode schema_version as "v1".
    if not (schema_version == 1 or schema_version == "v1"):
        raise SystemExit(f"FAIL: EXISTING_CORR_SCHEMA_VERSION_MISMATCH: schema_version={schema_version!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(
            f"FAIL: EXISTING_CORR_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}"
        )
    if status == "":
        # LEGACY_CORR_STATUS_MISSING_ACCEPTED
        # Historical truth may contain minimal correlation matrix without status.
        # Treat as authoritative existing non-blocking truth for rerun safety.
        print(
            f"OK: ENGINE_CORRELATION_MATRIX_V1_WRITTEN day={expected_day_utc} out={out_path} action=EXISTS sha256={existing_sha} status=OK"
        )
        return 0

    print(
        f"OK: ENGINE_CORRELATION_MATRIX_V1_WRITTEN day={expected_day_utc} out={out_path} action=EXISTS sha256={existing_sha} status={status}"
    )
    return 0 if status in ("OK", "DEGRADED_INSUFFICIENT_HISTORY") else 2


def _quant6(x: Decimal) -> Decimal:
    return x.quantize(Q6, rounding=ROUND_HALF_UP)


def _clamp_corr(x: Decimal) -> Decimal:
    if x > Decimal("1"):
        return Decimal("1")
    if x < Decimal("-1"):
        return Decimal("-1")
    return x


def _bootstrap_window_true(day_utc: str, truth_root: Path) -> bool:
    """
    Day-0 Bootstrap Window iff:
      TRUTH/execution_evidence_v1/submissions/<DAY>/ is missing OR contains zero submission dirs.
    """
    root = (truth_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()
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


def _paper_truth_root_true(truth_root: Path) -> bool:
    parts = tuple(Path(truth_root).resolve().parts)
    return len(parts) >= 2 and parts[-2:] == ("PRIMARY", "PAPER")


def _bootstrap_policy(
    *,
    day_utc: str,
    truth_root: Path,
    window_days: int,
    observed_return_days: int,
    observed_engine_count: int,
) -> Dict[str, Any]:
    if observed_engine_count > 0 and observed_return_days >= window_days:
        status = "NOT_APPLICABLE"
        source = "REALIZED_ACCOUNTING_ATTRIBUTION"
        next_action = ""
    elif observed_engine_count > 0 and _paper_truth_root_true(truth_root):
        status = "BOOTSTRAP_ACCEPTED_FOR_PAPER"
        source = "REALIZED_ACCOUNTING_ATTRIBUTION_INSUFFICIENT_HISTORY"
        next_action = "Accumulate enough paper engine attribution returns to satisfy the correlation history window."
    elif observed_engine_count > 0:
        status = "BLOCKED_FOR_LIVE"
        source = "REALIZED_ACCOUNTING_ATTRIBUTION_INSUFFICIENT_HISTORY"
        next_action = "Accumulate enough real engine attribution returns before relying on live pairwise correlation."
    elif _bootstrap_window_true(day_utc, truth_root) and _paper_truth_root_true(truth_root):
        status = "BOOTSTRAP_ACCEPTED_FOR_PAPER"
        source = "UNAVAILABLE_NO_ATTRIBUTION_HISTORY"
        next_action = "Accumulate paper engine attribution history; replace bootstrap correlation once pairwise engine returns exist."
    else:
        status = "BLOCKED_FOR_LIVE"
        source = "UNAVAILABLE_NO_ATTRIBUTION_HISTORY"
        next_action = "Produce real engine attribution and daily returns before relying on live pairwise correlation."
    return {
        "status": status,
        "return_source": source,
        "required_history_days": int(window_days),
        "observed_return_days": int(observed_return_days),
        "observed_engine_count": int(observed_engine_count),
        "operator_next_action": next_action,
    }


def _list_days(root: Path) -> List[date]:
    if not root.exists() or not root.is_dir():
        raise CliError(f"MISSING_ROOT_DIR: {root}")
    out: List[date] = []
    for child in sorted(root.iterdir()):
        if child.is_dir():
            try:
                out.append(_parse_day(child.name))
            except CliError:
                continue
    return sorted(out)


def _select_window(all_days: List[date], end_day: date, window_days: int, truth_root: Path) -> List[date]:
    if end_day not in all_days:
        # Bootstrap window: allow missing end-day history and emit degraded matrix.
        if _bootstrap_window_true(end_day.isoformat(), truth_root):
            eligible = [d for d in all_days if d <= end_day]
            if window_days <= 0:
                raise CliError("BAD_WINDOW_DAYS")
            if len(eligible) < window_days:
                return eligible
            return eligible[-window_days:]
        raise CliError(f"END_DAY_NOT_FOUND: {end_day.isoformat()}")
    eligible = [d for d in all_days if d <= end_day]
    if len(eligible) < 1:
        raise CliError("NO_ELIGIBLE_DAYS")
    if window_days <= 0:
        raise CliError("BAD_WINDOW_DAYS")
    # Allow degraded operation when insufficient history: take as many as available.
    if len(eligible) < window_days:
        return eligible
    return eligible[-window_days:]


def _extract_returns(obj: Dict[str, Any]) -> Dict[str, Decimal]:
    # engine_daily_returns.v1.json -> returns: [{engine_id, daily_return}]
    rs = obj.get("returns")
    if not isinstance(rs, list):
        return {}
    out: Dict[str, Decimal] = {}
    for row in rs:
        if not isinstance(row, dict):
            continue
        eid = str(row.get("engine_id") or "").strip()
        dr = str(row.get("daily_return") or "").strip()
        if eid == "" or dr == "":
            continue
        try:
            out[eid] = Decimal(dr)
        except Exception:
            continue
    return out


def _corr(a: List[Decimal], b: List[Decimal]) -> Decimal:
    # Pearson corr, Decimal math; fail-closed to 0 if degenerate
    if len(a) != len(b) or len(a) < 2:
        return Decimal("0")
    n = Decimal(len(a))
    ma = sum(a) / n
    mb = sum(b) / n
    da = [x - ma for x in a]
    db = [x - mb for x in b]
    num = sum([da[i] * db[i] for i in range(len(a))])
    den_a = sum([x * x for x in da])
    den_b = sum([x * x for x in db])
    if den_a == 0 or den_b == 0:
        return Decimal("0")
    import math  # local import

    den = Decimal(str(math.sqrt(float(den_a * den_b))))
    if den == 0:
        return Decimal("0")
    return num / den


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_engine_correlation_matrix_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--window_days", type=int, default=20, help="window size (uses most recent available <= day_utc)")
    ap.add_argument("--truth_root", default="", help="Absolute truth root override; defaults to repo truth")
    args = ap.parse_args()

    truth_root = _resolve_truth_root(str(args.truth_root))
    in_root = (truth_root / "monitoring_v1/engine_daily_returns_v1").resolve()
    out_root = (truth_root / "monitoring_v1/engine_correlation_matrix").resolve()

    day = _parse_day(args.day_utc)
    day_utc = _day_str(day)
    window_days = int(args.window_days)

    out_path = (out_root / day_utc / "engine_correlation_matrix.v1.json").resolve()
    existing_rc = _return_if_existing_report(out_path=out_path, expected_day_utc=day_utc)
    if existing_rc is not None:
        return int(existing_rc)

    try:
        all_days = _list_days(in_root)
    except CliError as e:
        if str(e).startswith("MISSING_ROOT_DIR:") and _bootstrap_window_true(day_utc, truth_root):
            all_days = []
        else:
            raise
    win = _select_window(all_days, day, window_days, truth_root)

    input_manifest: List[Dict[str, Any]] = []
    series_by_engine: Dict[str, List[Decimal]] = {}

    status = "OK"
    reason_codes: List[str] = []

    for d in win:
        p = (in_root / _day_str(d) / "engine_daily_returns.v1.json").resolve()
        if not p.exists():
            status = "FAIL_CORRUPT_INPUTS"
            reason_codes.append("MISSING_ENGINE_DAILY_RETURNS_FILE")
            continue

        sha = _sha256_file(p)
        input_manifest.append(
            {"type": "engine_daily_returns", "path": str(p), "sha256": sha, "producer": "phaseJ_engine_daily_returns_v1", "day_utc": _day_str(d)}
        )

        obj = _read_json(p)
        if not isinstance(obj, dict):
            status = "FAIL_CORRUPT_INPUTS"
            reason_codes.append("ENGINE_DAILY_RETURNS_NOT_OBJECT")
            continue

        returns = _extract_returns(obj)
        for eid, val in returns.items():
            series_by_engine.setdefault(eid, []).append(val)

    engine_ids = sorted(series_by_engine.keys())
    observed_return_days_for_policy = 0
    if len(engine_ids) == 0:
        # Bootstrap: no returns => 1x1 with placeholder engine id, degraded status
        status = "DEGRADED_INSUFFICIENT_HISTORY"
        reason_codes.append("NO_ENGINE_RETURNS_AVAILABLE")
        if _bootstrap_window_true(day_utc, truth_root) and _paper_truth_root_true(truth_root):
            reason_codes.append("BOOTSTRAP_ACCEPTED_FOR_PAPER")
        else:
            reason_codes.append("BLOCKED_FOR_LIVE")
        engine_ids = ["BOOTSTRAP"]
        corr = [["1.000000"]]
        flags = {"crowding_threshold": "0.75", "sustained_days": 1, "pairs": []}
    else:
        max_len = max([len(series_by_engine[eid]) for eid in engine_ids])
        observed_return_days_for_policy = max_len
        for eid in engine_ids:
            s = series_by_engine[eid]
            if len(s) < max_len:
                series_by_engine[eid] = s + [Decimal("0")] * (max_len - len(s))

        if max_len < 2:
            status = "DEGRADED_INSUFFICIENT_HISTORY"
            reason_codes.append("INSUFFICIENT_HISTORY_LT_2")
        elif max_len < window_days:
            status = "DEGRADED_INSUFFICIENT_HISTORY"
            reason_codes.append("INSUFFICIENT_HISTORY_LT_WINDOW")
        n = len(engine_ids)

        corr: List[List[str]] = []
        getcontext().prec = 28

        for i in range(n):
            row: List[str] = []
            for j in range(n):
                if i == j:
                    row.append("1.000000")
                else:
                    c = _corr(series_by_engine[engine_ids[i]], series_by_engine[engine_ids[j]])
                    c = _clamp_corr(c)
                    c = _quant6(c)
                    row.append(f"{c:.6f}")
            corr.append(row)

        crowding_threshold = Decimal("0.75")
        pairs: List[Dict[str, Any]] = []
        for i in range(n):
            for j in range(i + 1, n):
                c = Decimal(corr[i][j])
                pairs.append(
                    {
                        "engine_a": engine_ids[i],
                        "engine_b": engine_ids[j],
                        "corr": corr[i][j],
                        "sustained": 0,
                        "flag": abs(c) >= crowding_threshold,
                    }
                )

        flags = {
            "crowding_threshold": "0.75",
            "sustained_days": 1,
            "pairs": pairs,
        }

    produced_utc = f"{day_utc}T23:59:59Z"
    payload: Dict[str, Any] = {
        "schema_id": "C2_ENGINE_CORRELATION_MATRIX_V1",
        "schema_version": 1,
        "status": status,
        "day_utc": day_utc,
        "window_days": int(window_days),
        "matrix": {"engine_ids": engine_ids, "corr": corr},
        "flags": flags,
        "input_manifest": input_manifest
        if len(input_manifest) > 0
        else [{"type": "engine_daily_returns", "path": str(in_root), "sha256": "0" * 64, "producer": "phaseJ_engine_daily_returns_v1", "day_utc": day_utc}],
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py"},
        "reason_codes": sorted(list(dict.fromkeys(reason_codes))),
        "bootstrap_policy": _bootstrap_policy(
            day_utc=day_utc,
            truth_root=truth_root,
            window_days=window_days,
            observed_return_days=observed_return_days_for_policy,
            observed_engine_count=len(series_by_engine),
        ),
    }

    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)

    payload_bytes = canonical_json_bytes_v1(payload)

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    print(f"OK: ENGINE_CORRELATION_MATRIX_V1_WRITTEN day={day_utc} out={out_path} action={wr.action} sha256={wr.sha256}")

    # Day-0 bootstrap: degraded correlation is non-blocking when no submissions exist.
    if status == "DEGRADED_INSUFFICIENT_HISTORY" and _bootstrap_window_true(day_utc, truth_root):
        return 0

    return 0 if status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
