#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1
from ops.aegis.universe.canonical_universe_discovery_v1 import resolve_canonical_universe_discovery_v1


GLOBAL_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
REPORT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/canonical_market_data_refresh_v1.schema.json"
IB_RUNTIME_CANDIDATES = (
    REPO_ROOT / ".venv_c2" / "bin" / "python",
    REPO_ROOT / ".venv_c2" / "bin" / "python3",
    REPO_ROOT / ".venv_ib" / "bin" / "python3",
)
# Keep retries deterministic but broad enough to avoid transient multi-runner clientId collisions.
CLIENT_ID_RETRY_OFFSETS = tuple(range(0, 16))


def _run_step(cmd: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)


def _python_has_ib_insync(python_bin: Path) -> bool:
    if not python_bin.exists() or not python_bin.is_file():
        return False
    probe = subprocess.run(
        [str(python_bin), "-c", "import ib_insync"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    return probe.returncode == 0


def _resolve_ib_runtime_python() -> str:
    if importlib.util.find_spec("ib_insync") is not None:
        return sys.executable
    for candidate in IB_RUNTIME_CANDIDATES:
        if _python_has_ib_insync(candidate):
            return str(candidate)
    raise SystemExit(
        "FAIL: IB_RUNTIME_PYTHON_MISSING expected_ib_insync_on_one_of="
        + ",".join(str(p) for p in IB_RUNTIME_CANDIDATES)
    )


def _intent_day_dir(*, truth_root: Path, day: str) -> Path:
    return (truth_root / "intents_v1" / "snapshots" / day).resolve()


def _list_intent_json_files(day_dir: Path) -> list[Path]:
    if not day_dir.exists() or not day_dir.is_dir():
        return []
    return sorted(
        [p for p in day_dir.iterdir() if p.is_file() and p.name.endswith(".json") and p.name != "no_intents_day.v1.json"],
        key=lambda p: p.name,
    )


def _discover_equity_symbols_from_truth(*, truth_root: Path, day: str) -> list[str]:
    symbols: list[str] = []
    for path in _list_intent_json_files(_intent_day_dir(truth_root=truth_root, day=day)):
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        underlying = obj.get("underlying")
        if not isinstance(underlying, dict):
            continue
        symbol = str(underlying.get("symbol") or "").strip().upper()
        if symbol:
            symbols.append(symbol)
    return sorted(set(symbols))


def _market_data_close_for_same_day(*, truth_root: Path, day: str, symbol: str) -> bool:
    manifest = (truth_root / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()
    if not manifest.exists() or not manifest.is_file():
        return False
    year_path = (truth_root / "market_data_snapshot_v1" / symbol.upper() / f"{day[:4]}.jsonl").resolve()
    if not year_path.exists() or not year_path.is_file():
        return False
    try:
        lines = year_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return False
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if str(row.get("symbol") or "").strip().upper() != symbol.upper():
            continue
        if str(row.get("timestamp_utc") or "").strip().startswith(day):
            return True
    return False


def _producer_block() -> dict[str, str]:
    try:
        git_sha = subprocess.check_output(
            ["/usr/bin/git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            text=True,
        ).strip()
    except Exception:
        git_sha = "UNKNOWN"
    return {
        "repo": "constellation_2_runtime",
        "git_sha": git_sha,
        "module": "ops/tools/run_c2_canonical_market_data_refresh_v1.py",
    }


def _summarize_stream(text: str, *, limit: int = 400) -> str:
    raw = " ".join((text or "").split())
    if len(raw) <= limit:
        return raw
    return raw[: limit - 3] + "..."


def _clean_symbol_list(raw: Any) -> tuple[bool, list[str], bool]:
    if raw is None:
        return False, [], True
    if not isinstance(raw, list):
        return False, [], False
    symbols = [str(value).strip().upper() for value in raw if str(value).strip()]
    return True, symbols, False


def build_market_data_refresh_diagnostics(
    *,
    coverage_payload: dict[str, Any] | None,
    target_symbols: list[str],
    terminal_decision: str,
) -> dict[str, Any]:
    coverage_status = "INCOMPLETE"
    missing_dimensions: list[str] = []
    derivation_status = "FAILED"
    derivation_source = "coverage_payload.missing_symbols+stale_symbols"
    before_filters = 0
    after_filters = len(sorted({str(symbol).strip().upper() for symbol in target_symbols if str(symbol).strip()}))
    failure_class: str | None = "UNKNOWN_DERIVATION_FAILURE"

    if isinstance(coverage_payload, dict):
        verdict = str(coverage_payload.get("verdict") or "").strip().upper()
        if verdict == "PASS":
            coverage_status = "COMPLETE"
            derivation_status = "NOT_NEEDED"
            failure_class = None
            terminal_decision = terminal_decision or "NO_REPAIR_NEEDED"
        else:
            coverage_status = "INCOMPLETE"

        _, missing_symbols, missing_symbols_absent = _clean_symbol_list(coverage_payload.get("missing_symbols"))
        _, stale_symbols, stale_symbols_absent = _clean_symbol_list(coverage_payload.get("stale_symbols"))
        missing_symbols_ok = isinstance(coverage_payload.get("missing_symbols"), list)
        stale_symbols_ok = isinstance(coverage_payload.get("stale_symbols"), list)
        raw_candidates = []
        if missing_symbols_ok:
            raw_candidates.extend(missing_symbols)
        if stale_symbols_ok:
            raw_candidates.extend(stale_symbols)
        before_filters = len(raw_candidates)

        reason_codes = coverage_payload.get("reason_codes")
        codes = [str(code).strip() for code in reason_codes if str(code).strip()] if isinstance(reason_codes, list) else []
        if "MDC_UPSTREAM_REQUIREMENT_BASIS_UNAVAILABLE" in codes or "MDC_REQUIRED_SYMBOL_BASIS_INCOMPLETE" in codes:
            missing_dimensions.append("REQUIRED_SYMBOL_BASIS")
        if "MDC_CANONICAL_DATASET_MANIFEST_MISSING" in codes:
            missing_dimensions.append("CANONICAL_DATASET_MANIFEST")
        if "MDC_SLEEVE_DATASET_MANIFEST_MISSING" in codes:
            missing_dimensions.append("SLEEVE_DATASET_MANIFEST")
        if missing_symbols:
            missing_dimensions.append("MISSING_SYMBOLS")
        if stale_symbols:
            missing_dimensions.append("STALE_SYMBOLS")
        if missing_symbols_absent or stale_symbols_absent:
            missing_dimensions.append("DERIVATION_SOURCE_FIELDS")

        if coverage_status == "COMPLETE":
            derivation_status = "NOT_NEEDED"
            failure_class = None
        elif after_filters > 0:
            derivation_status = "DERIVED"
            failure_class = None
        elif missing_symbols_absent or stale_symbols_absent:
            derivation_status = "FAILED"
            failure_class = "SOURCE_MISSING"
        elif not missing_symbols_ok or not stale_symbols_ok:
            derivation_status = "FAILED"
            failure_class = "SOURCE_MALFORMED"
        elif before_filters == 0:
            derivation_status = "FAILED"
            failure_class = "SOURCE_EMPTY"
        else:
            derivation_status = "FAILED"
            failure_class = "FILTERED_TO_ZERO"
    else:
        missing_dimensions = ["COVERAGE_ARTIFACT_UNAVAILABLE"]

    return {
        "coverage_status": coverage_status,
        "coverage_missing_dimensions": sorted(set(missing_dimensions)),
        "repair_target_derivation_status": derivation_status,
        "repair_target_derivation_source": derivation_source,
        "repair_target_candidate_count_before_filters": int(before_filters),
        "repair_target_candidate_count_after_filters": int(after_filters),
        "repair_target_derivation_failure_class": failure_class,
        "terminal_decision": terminal_decision,
    }


def _write_report(*, day: str, produced_utc: str, truth_root: Path, payload: dict[str, Any]) -> Path:
    report = {
        "schema_id": "C2_CANONICAL_MARKET_DATA_REFRESH_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": _producer_block(),
        "status": payload["status"],
        "truth_root": str(truth_root),
        "symbol_source_truth_root": payload["symbol_source_truth_root"],
        "symbols_requested": payload["symbols"],
        "symbols_succeeded": sorted(
            [
                row["symbol"]
                for row in payload["results"]
                if row["status"] in {"WROTE", "SKIP_SAME_DAY_PRESENT", "PRESENT_AFTER_REFRESH"}
            ]
        ),
        "symbols_failed": payload["failures"],
        "results": [
            {
                "symbol": row["symbol"],
                "status": row["status"],
                "returncode": int(row.get("returncode") or 0),
                "year_file": row["year_file"],
                "stdout_summary": _summarize_stream(str(row.get("stdout_summary", row.get("stdout", "")) or "")),
                "stderr_summary": _summarize_stream(str(row.get("stderr_summary", row.get("stderr", "")) or "")),
            }
            for row in payload["results"]
        ],
        "reason_codes": list(payload.get("reason_codes") or []),
        "coverage_status": payload.get("coverage_status"),
        "coverage_missing_dimensions": list(payload.get("coverage_missing_dimensions") or []),
        "repair_target_derivation_status": payload.get("repair_target_derivation_status"),
        "repair_target_derivation_source": payload.get("repair_target_derivation_source"),
        "repair_target_candidate_count_before_filters": payload.get("repair_target_candidate_count_before_filters"),
        "repair_target_candidate_count_after_filters": payload.get("repair_target_candidate_count_after_filters"),
        "repair_target_derivation_failure_class": payload.get("repair_target_derivation_failure_class"),
        "terminal_decision": payload.get("terminal_decision"),
        "canonical_universe_discovery_report_path": payload.get("canonical_universe_discovery_report_path"),
        "canonical_universe_discovery_source": payload.get("canonical_universe_discovery_source"),
        "canonical_universe_discovery_accepted_count": payload.get("canonical_universe_discovery_accepted_count"),
        "canonical_universe_minimum_successful_symbols": payload.get("canonical_universe_minimum_successful_symbols"),
        "canonical_json_hash": "",
    }
    report["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(report)
    validate_against_repo_schema_v1(report, REPO_ROOT, REPORT_SCHEMA_RELPATH)
    out_path = (truth_root / "reports" / "canonical_market_data_refresh_v1" / day / "canonical_market_data_refresh.v1.json").resolve()
    write_day_artifact_refreshable_v1(
        path=out_path,
        data=canonical_json_bytes_v1(report) + b"\n",
        expected_day_utc=day,
        expected_schema_id="C2_CANONICAL_MARKET_DATA_REFRESH_V1",
        expected_schema_version=1,
        preserve_statuses=(),
    )
    return out_path


def _is_client_id_conflict(proc: subprocess.CompletedProcess[str]) -> bool:
    text = f"{proc.stdout}\n{proc.stderr}".lower()
    return "client id is already in use" in text or "clientid " in text and "already in use" in text


def _run_symbol_refresh_with_retry(
    *,
    ib_python: str,
    env: dict[str, str],
    produced_utc: str,
    day: str,
    symbols: list[str],
    base_client_id: int,
    discover_dynamic_universe: bool,
    allow_symbol_rejections: bool,
    minimum_successful_symbols: int,
) -> subprocess.CompletedProcess[str]:
    last_proc: subprocess.CompletedProcess[str] | None = None
    for offset in CLIENT_ID_RETRY_OFFSETS:
        client_id = base_client_id + offset
        cmd = [
            ib_python,
            "constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py",
            "--run_utc",
            produced_utc,
            "--dataset_version",
            "v1",
            "--start_year",
            day[:4],
            "--end_year",
            day[:4],
            "--host",
            str(env.get("C2_IB_HOST") or "127.0.0.1").strip(),
            "--port",
            str(env.get("C2_IB_PORT") or "4002").strip(),
            "--client_id",
            str(client_id),
            "--sleep_sec",
            str(env.get("C2_IB_SLEEP_SEC") or "0.1").strip(),
            "--use_rth",
            "1",
        ]
        if discover_dynamic_universe:
            cmd.extend(
                [
                    "--discover_dynamic_universe",
                    "--discover_day_utc",
                    day,
                ]
            )
        if allow_symbol_rejections:
            cmd.extend(
                [
                    "--allow_symbol_rejections",
                    "--minimum_successful_symbols",
                    str(int(minimum_successful_symbols)),
                ]
            )
        for symbol in sorted(set(symbols)):
            cmd.extend(["--symbol", symbol])
        proc = _run_step(
            cmd,
            env=env,
        )
        last_proc = proc
        if proc.returncode == 0:
            return proc
        if not _is_client_id_conflict(proc):
            return proc
    assert last_proc is not None
    return last_proc


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_c2_canonical_market_data_refresh_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--produced_utc", default="")
    ap.add_argument("--truth_root", default=str(GLOBAL_TRUTH_ROOT))
    ap.add_argument("--symbol_source_truth_root", default="")
    ap.add_argument("--symbol", action="append", default=[])
    ap.add_argument("--coverage_status", default="")
    ap.add_argument("--coverage_missing_dimension", action="append", default=[])
    ap.add_argument("--repair_target_derivation_status", default="")
    ap.add_argument("--repair_target_derivation_source", default="")
    ap.add_argument("--repair_target_candidate_count_before_filters", default="")
    ap.add_argument("--repair_target_candidate_count_after_filters", default="")
    ap.add_argument("--repair_target_derivation_failure_class", default="")
    ap.add_argument("--terminal_decision", default="")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = str(args.produced_utc).strip() or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    truth_root = Path(str(args.truth_root).strip() or str(GLOBAL_TRUTH_ROOT)).resolve()
    source_truth_root = Path(str(args.symbol_source_truth_root).strip()).resolve() if str(args.symbol_source_truth_root).strip() else truth_root
    diagnostics = {
        "coverage_status": str(args.coverage_status).strip() or None,
        "coverage_missing_dimensions": [str(value).strip() for value in (args.coverage_missing_dimension or []) if str(value).strip()],
        "repair_target_derivation_status": str(args.repair_target_derivation_status).strip() or None,
        "repair_target_derivation_source": str(args.repair_target_derivation_source).strip() or None,
        "repair_target_candidate_count_before_filters": (
            int(str(args.repair_target_candidate_count_before_filters).strip())
            if str(args.repair_target_candidate_count_before_filters).strip()
            else None
        ),
        "repair_target_candidate_count_after_filters": (
            int(str(args.repair_target_candidate_count_after_filters).strip())
            if str(args.repair_target_candidate_count_after_filters).strip()
            else None
        ),
        "repair_target_derivation_failure_class": str(args.repair_target_derivation_failure_class).strip() or None,
        "terminal_decision": str(args.terminal_decision).strip() or None,
    }

    explicit_symbols = [str(s).strip().upper() for s in (args.symbol or []) if str(s).strip()]
    discovery_report = None
    if explicit_symbols:
        symbols = sorted(set(explicit_symbols))
        discover_dynamic_universe = False
        allow_symbol_rejections = False
        minimum_successful_symbols = 0
    else:
        discovery_report = resolve_canonical_universe_discovery_v1(
            truth_root=truth_root,
            day_utc=day,
            repo_root=REPO_ROOT,
            requested_target_count=200,
        )
        symbols = list(discovery_report.get("accepted_symbols") or [])
        discover_dynamic_universe = False
        allow_symbol_rejections = True
        minimum_successful_symbols = 160
    if not symbols:
        payload = {
            "name": "canonical_market_data_refresh_v1",
            "truth_root": str(truth_root),
            "symbol_source_truth_root": str(source_truth_root),
            "status": "SKIP",
            "symbols": [],
            "failures": [],
            "results": [],
            "reason_codes": ["SKIP_NO_CANONICAL_DISCOVERY_SYMBOLS"],
            "canonical_universe_discovery_report_path": str((discovery_report or {}).get("artifact_path") or ""),
        }
        payload.update(diagnostics)
        report_path = _write_report(day=day, produced_utc=produced_utc, truth_root=truth_root, payload=payload)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2))
        return 0

    env = dict(os.environ)
    env["C2_TRUTH_ROOT"] = str(truth_root)
    ib_python = _resolve_ib_runtime_python()
    base_client_id = int(str(env.get("C2_IB_CLIENT_ID") or "7").strip())
    proc = _run_symbol_refresh_with_retry(
        ib_python=ib_python,
        env=env,
        produced_utc=produced_utc,
        day=day,
        symbols=symbols,
        base_client_id=base_client_id,
        discover_dynamic_universe=discover_dynamic_universe,
        allow_symbol_rejections=allow_symbol_rejections,
        minimum_successful_symbols=minimum_successful_symbols,
    )
    ranked_universe_proc = None
    if proc.returncode == 0:
        ranked_universe_proc = _run_step(
            [
                ib_python,
                "ops/tools/run_ranked_symbol_universe_v1.py",
                "--day_utc",
                day,
                "--produced_utc",
                produced_utc,
                "--truth_root",
                str(truth_root),
                "--skip_refresh",
            ],
            env=env,
        )
        if ranked_universe_proc.returncode != 0:
            proc = ranked_universe_proc
    manifest_symbols = symbols
    manifest_path = (truth_root / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()
    if proc.returncode == 0 and manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}
        raw_manifest_symbols = manifest.get("symbols")
        if isinstance(raw_manifest_symbols, list):
            manifest_symbols = sorted({str(symbol).strip().upper() for symbol in raw_manifest_symbols if str(symbol).strip()})
    status = "PASS" if proc.returncode == 0 else "FAIL"
    failures = [] if proc.returncode == 0 else ["DYNAMIC_UNIVERSE_REFRESH"]
    proc_stdout_summary = _summarize_stream(str(proc.stdout or ""), limit=4096)
    proc_stderr_summary = _summarize_stream(str(proc.stderr or ""), limit=4096)
    results: list[dict[str, Any]] = [
        {
            "symbol": symbol,
            "year_file": str((truth_root / "market_data_snapshot_v1" / symbol / f"{day[:4]}.jsonl").resolve()),
            "returncode": 0 if proc.returncode == 0 else proc.returncode,
            "status": "PRESENT_AFTER_REFRESH" if proc.returncode == 0 else "FAIL",
            "stdout_summary": proc_stdout_summary,
            "stderr_summary": proc_stderr_summary,
        }
        for symbol in manifest_symbols
    ]
    if not results:
        results = [
            {
                "symbol": "DYNAMIC_UNIVERSE",
                "year_file": "",
                "returncode": proc.returncode,
                "status": "FAIL" if proc.returncode != 0 else "PRESENT_AFTER_REFRESH",
                "stdout_summary": proc_stdout_summary,
                "stderr_summary": proc_stderr_summary,
            }
        ]
    if ranked_universe_proc is not None:
        for row in results:
            row["ranked_symbol_universe_status"] = "WRITTEN" if proc.returncode == 0 else "FAILED"
    payload = {
        "name": "canonical_market_data_refresh_v1",
        "truth_root": str(truth_root),
        "symbol_source_truth_root": str(source_truth_root),
        "status": status,
        "symbols": manifest_symbols,
        "failures": failures,
        "results": results,
        "reason_codes": ([] if not failures else ["DYNAMIC_UNIVERSE_REFRESH_FAILED"]),
        "canonical_universe_discovery_report_path": str((discovery_report or {}).get("artifact_path") or ""),
        "canonical_universe_discovery_source": str((discovery_report or {}).get("discovery_source") or ("OPERATOR_EXPLICIT_SYMBOLS" if explicit_symbols else "")),
        "canonical_universe_discovery_accepted_count": int((discovery_report or {}).get("accepted_count") or len(symbols)),
        "canonical_universe_minimum_successful_symbols": int(minimum_successful_symbols),
    }
    payload.update(diagnostics)
    report_path = _write_report(day=day, produced_utc=produced_utc, truth_root=truth_root, payload=payload)
    payload["report_path"] = str(report_path)
    print(json.dumps(payload, indent=2))
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
