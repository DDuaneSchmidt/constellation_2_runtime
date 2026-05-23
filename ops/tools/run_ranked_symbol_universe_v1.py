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
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.ranked_symbol_universe_v1 import (  # noqa: E402
    emit_ranked_symbol_universe,
    load_ranked_symbol_universe,
)


GLOBAL_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
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
    raise SystemExit("FAIL: IB_RUNTIME_PYTHON_MISSING")


def _is_client_id_conflict_message(text: str) -> bool:
    lowered = str(text or "").lower()
    return "client id is already in use" in lowered or ("clientid " in lowered and "already in use" in lowered)


def _metadata_client_id_candidates() -> List[int]:
    base_client_id = int(str(os.environ.get("C2_IB_METADATA_CLIENT_ID") or "37").strip())
    return [base_client_id + offset for offset in CLIENT_ID_RETRY_OFFSETS]


def _discover_and_refresh_market_data(
    *,
    truth_root: Path,
    day: str,
    produced_utc: str,
    target_symbol_count: int,
    rows_per_query: int,
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["C2_TRUTH_ROOT"] = str(truth_root)
    ib_python = _resolve_ib_runtime_python()
    cmd = [
        ib_python,
        "constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py",
        "--run_utc",
        produced_utc,
        "--dataset_version",
        "v1",
        "--discover_dynamic_universe",
        "--discover_day_utc",
        day,
        "--discover_target_symbol_count",
        str(target_symbol_count),
        "--discover_rows_per_query",
        str(rows_per_query),
        "--start_year",
        day[:4],
        "--end_year",
        day[:4],
        "--host",
        str(env.get("C2_IB_HOST") or "127.0.0.1").strip(),
        "--port",
        str(env.get("C2_IB_PORT") or "4002").strip(),
        "--client_id",
        str(env.get("C2_IB_CLIENT_ID") or "7").strip(),
        "--sleep_sec",
        str(env.get("C2_IB_SLEEP_SEC") or "0.05").strip(),
        "--use_rth",
        "1",
    ]
    return _run_step(cmd, env=env)


def _symbol_metadata_from_ib(*, truth_root: Path) -> Dict[str, Dict[str, Any]]:
    manifest = json.loads((truth_root / "market_data_snapshot_v1" / "dataset_manifest.json").read_text(encoding="utf-8"))
    symbols = sorted({str(symbol).strip().upper() for symbol in manifest.get("symbols") or [] if str(symbol).strip()})
    if not symbols:
        return {}

    try:
        from ib_insync import IB, Stock  # type: ignore
    except Exception as exc:
        raise SystemExit(f"FAIL: ib_insync import failed: {exc!r}") from exc

    host = str(os.environ.get("C2_IB_HOST") or "127.0.0.1").strip()
    port = int(str(os.environ.get("C2_IB_PORT") or "4002").strip())
    last_error: Exception | None = None
    for client_id in _metadata_client_id_candidates():
        ib = IB()
        try:
            ib.connect(
                host,
                port,
                clientId=client_id,
                timeout=15,
            )
            if not ib.isConnected():
                raise RuntimeError("ib_connect_failed: not connected")
            out: Dict[str, Dict[str, Any]] = {}
            for symbol in symbols:
                details_list = ib.reqContractDetails(Stock(symbol, "SMART", "USD"))
                if not details_list:
                    out[symbol] = {
                        "asset_type": "UNKNOWN",
                        "eligible": False,
                        "reason_codes": ["MISSING_CONTRACT_DETAILS"],
                    }
                    continue
                details = details_list[0]
                contract = details.contract
                stock_type = str(getattr(details, "stockType", "") or "").strip().upper()
                long_name = str(getattr(details, "longName", "") or "").strip().upper()
                category = str(getattr(details, "category", "") or "").strip().upper()
                subcategory = str(getattr(details, "subcategory", "") or "").strip().upper()
                primary_exchange = str(getattr(contract, "primaryExchange", "") or "").strip().upper()
                sec_type = str(getattr(contract, "secType", "") or "").strip().upper()
                currency = str(getattr(contract, "currency", "") or "").strip().upper()
                blob = " ".join([stock_type, long_name, category, subcategory, primary_exchange])
                reason_codes: List[str] = []
                asset_type = "US_EQUITY"
                eligible = True

                if sec_type != "STK":
                    eligible = False
                    reason_codes.append("SEC_TYPE_NOT_STK")
                if currency and currency != "USD":
                    eligible = False
                    reason_codes.append("NON_USD")
                if any(token in primary_exchange for token in ("PINK", "OTC")):
                    eligible = False
                    reason_codes.append("OTC_PRIMARY_EXCHANGE")
                if any(token in blob for token in ("PREFERRED", "ADR", "ETN", "CEF", "CLOSED END", "FUND")):
                    eligible = False
                    reason_codes.append("NON_STANDARD_LISTING")
                if "ETF" in blob:
                    asset_type = "STANDARD_ETF"
                if asset_type == "STANDARD_ETF" and any(
                    token in blob
                    for token in (
                        " ULTRA ",
                        " ULTRAPRO ",
                        " SHORT ",
                        " BEAR ",
                        " INVERSE ",
                        " LEVERAGED ",
                        " 2X ",
                        " 3X ",
                        " DAILY ",
                    )
                ):
                    eligible = False
                    reason_codes.append("LEVERAGED_OR_INVERSE_ETF")

                out[symbol] = {
                    "asset_type": asset_type,
                    "eligible": eligible,
                    "reason_codes": sorted(set(reason_codes)),
                }
            return out
        except Exception as exc:
            last_error = exc
            if not _is_client_id_conflict_message(repr(exc)):
                raise SystemExit(f"FAIL: ib_connect_failed: {exc!r}") from exc
        finally:
            try:
                ib.disconnect()
            except Exception:
                pass
    raise SystemExit(f"FAIL: ib_connect_failed_after_client_id_retries: {last_error!r}")


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_ranked_symbol_universe_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--produced_utc", default="")
    ap.add_argument("--truth_root", default=str(GLOBAL_TRUTH_ROOT))
    ap.add_argument("--skip_refresh", action="store_true")
    ap.add_argument("--discover_target_symbol_count", type=int, default=200)
    ap.add_argument("--discover_rows_per_query", type=int, default=50)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = str(args.produced_utc).strip() or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    truth_root = Path(str(args.truth_root).strip() or str(GLOBAL_TRUTH_ROOT)).resolve()

    refresh_proc = None
    dynamic_discovery_used = False
    if not bool(args.skip_refresh):
        refresh_proc = _discover_and_refresh_market_data(
            truth_root=truth_root,
            day=day,
            produced_utc=produced_utc,
            target_symbol_count=int(args.discover_target_symbol_count),
            rows_per_query=int(args.discover_rows_per_query),
        )
        if refresh_proc.returncode != 0:
            sys.stdout.write(refresh_proc.stdout)
            sys.stderr.write(refresh_proc.stderr)
            return int(refresh_proc.returncode)
        dynamic_discovery_used = True

    metadata_by_symbol = _symbol_metadata_from_ib(truth_root=truth_root)
    out_path = emit_ranked_symbol_universe(
        day_utc=day,
        produced_utc=produced_utc,
        truth_root=truth_root,
        metadata_by_symbol=metadata_by_symbol,
        dynamic_discovery_used=dynamic_discovery_used,
        refreshable=True,
    )
    payload = load_ranked_symbol_universe(truth_root=truth_root, day=day, validate=True)
    result = {
        "name": "ranked_symbol_universe_v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "truth_root": str(truth_root),
        "dynamic_discovery_used": dynamic_discovery_used,
        "selected_symbol_count": payload["selected_symbol_count"],
        "symbols": payload["symbols"],
        "path": str(out_path),
        "status": payload["status"],
    }
    if refresh_proc is not None:
        result["refresh_stdout"] = refresh_proc.stdout.strip()
        result["refresh_stderr"] = refresh_proc.stderr.strip()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
