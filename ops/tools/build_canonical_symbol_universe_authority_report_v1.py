#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.universe.canonical_symbol_universe_resolver_v1 import (  # noqa: E402
    CanonicalSymbolUniverseError,
    resolve_canonical_symbol_universe_v1,
)

DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
DEFAULT_SLEEVES = [
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
    "C2_DEFENSIVE_TAIL_V1",
    "C2_INTENT_SIMULATOR_V1",
]


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(path: str) -> str:
    try:
        p = Path(path).expanduser().resolve()
        return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() and p.is_file() else ""
    except Exception:
        return ""


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _effective_symbol_count(row: dict[str, Any]) -> int:
    count = int(row.get("raw_resolved_symbol_count") or 0)
    target = int(row.get("policy_target_symbol_count") or 0)
    if row.get("source") in {
        "engine_universe_candidate_basis_v1",
        "engine_universe_candidate_basis_v1.operational_latest_valid",
        "ranked_symbol_universe_v1",
        "market_data_snapshot_v1.dataset_manifest",
    } and target > 0 and count > target:
        return target
    return count


def _status_from_error(text: str) -> tuple[str, str]:
    if "UNIVERSE_BREADTH_FAILURE" in text:
        return ("UNAVAILABLE", "UNIVERSE_BREADTH_FAILURE")
    if "CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE" in text:
        return ("UNAVAILABLE", "CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE")
    return ("INVALID", text[:240])


def _row(*, truth_root: Path, day_utc: str, sleeve_id: str, market_data_mode: str) -> dict[str, Any]:
    try:
        result = resolve_canonical_symbol_universe_v1(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=day_utc,
            engine_id=sleeve_id,
            allow_deprecated_fallback=False,
            market_data_mode=market_data_mode,
        )
        data = asdict(result)
        source_path = str(data.get("source_path") or "")
        status = "VALID"
        freshness = "CURRENT"
        if data.get("source") == "engine_universe_candidate_basis_v1.operational_latest_valid":
            freshness = "CURRENT_OPERATIONAL_FROM_LATEST_GOVERNED_UNIVERSE"
        row = {
            "sleeve_id": sleeve_id,
            "expected_universe_source": "ENGINE_UNIVERSE_POLICY_V1 plus governed canonical symbol universe resolver",
            "source": data.get("source"),
            "resolved_path": source_path,
            "resolved_hash": _sha256_file(source_path),
            "raw_resolved_symbol_count": int(data.get("symbol_count") or 0),
            "policy_target_symbol_count": int(data.get("policy_target_symbol_count") or 0),
            "symbol_count": 0,
            "freshness_status": freshness,
            "authority_status": status,
            "reason_if_unavailable": "",
            "deprecated_fallback_active": bool(data.get("deprecated_fallback_used")),
            "source_rank": data.get("source_rank"),
            "source_artifacts": data.get("source_artifacts") or [],
            "blockers": data.get("blockers") or [],
        }
        row["symbol_count"] = _effective_symbol_count(row)
        return row
    except CanonicalSymbolUniverseError as exc:
        status, reason = _status_from_error(str(exc))
        return {
            "sleeve_id": sleeve_id,
            "expected_universe_source": "ENGINE_UNIVERSE_POLICY_V1 plus governed canonical symbol universe resolver",
            "source": "CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE",
            "resolved_path": "",
            "resolved_hash": "",
            "raw_resolved_symbol_count": 0,
            "policy_target_symbol_count": 0,
            "symbol_count": 0,
            "freshness_status": "UNAVAILABLE",
            "authority_status": status,
            "reason_if_unavailable": reason,
            "deprecated_fallback_active": False,
            "source_rank": "",
            "source_artifacts": [],
            "blockers": [{"blocker_type": reason, "detail": str(exc)}],
        }


def render_text(payload: dict[str, Any]) -> str:
    lines = [
        "Canonical Symbol Universe Authority v1",
        f"day_utc: {payload.get('day_utc')}",
        f"market_data_mode: {payload.get('market_data_mode')}",
        f"status: {payload.get('status')}",
        f"deprecated_fallback_active_any: {payload.get('deprecated_fallback_active_any')}",
        "",
    ]
    for row in payload.get("sleeves") or []:
        lines.append(f"{row.get('sleeve_id')}: {row.get('authority_status')} count={row.get('symbol_count')} source={row.get('source')}")
        lines.append(f"  freshness: {row.get('freshness_status')}")
        lines.append(f"  path: {row.get('resolved_path')}")
        lines.append(f"  hash: {row.get('resolved_hash')}")
        if row.get("reason_if_unavailable"):
            lines.append(f"  reason: {row.get('reason_if_unavailable')}")
        lines.append("  deprecated_fallback_active: false")
    return "\n".join(lines) + "\n"


def build_report(*, truth_root: Path, day_utc: str, market_data_mode: str, sleeves: list[str]) -> dict[str, Any]:
    rows = [_row(truth_root=truth_root, day_utc=day_utc, sleeve_id=sleeve, market_data_mode=market_data_mode) for sleeve in sleeves]
    status = "PASS" if all(row.get("authority_status") == "VALID" for row in rows) else "FAIL"
    return {
        "schema_id": "canonical_symbol_universe_authority_report",
        "schema_version": "v1",
        "day_utc": day_utc,
        "generated_at_utc": _now_utc(),
        "truth_root": str(truth_root),
        "market_data_mode": market_data_mode,
        "status": status,
        "deprecated_fallback_active_any": any(bool(row.get("deprecated_fallback_active")) for row in rows),
        "sleeves": rows,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_canonical_symbol_universe_authority_report_v1")
    parser.add_argument("--truth-root", "--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day-utc", "--day_utc", "--day", dest="day_utc", required=True)
    parser.add_argument("--market-data-mode", "--market_data_mode", default="INTRADAY_OPERATIONAL")
    parser.add_argument("--sleeve-id", "--sleeve_id", action="append", default=[])
    args = parser.parse_args(argv)
    truth_root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc).strip()
    sleeves = sorted({str(item).strip().upper() for item in args.sleeve_id if str(item).strip()}) or DEFAULT_SLEEVES
    payload = build_report(truth_root=truth_root, day_utc=day, market_data_mode=str(args.market_data_mode or "INTRADAY_OPERATIONAL").strip().upper(), sleeves=sleeves)
    out_dir = truth_root / "reports" / "canonical_symbol_universe_authority_v1" / day
    json_path = out_dir / "canonical_symbol_universe_authority.v1.json"
    txt_path = out_dir / "canonical_symbol_universe_authority.v1.txt"
    _write_json(json_path, payload)
    txt_path.write_text(render_text(payload), encoding="utf-8")
    print(json.dumps({"status": payload["status"], "json": str(json_path), "txt": str(txt_path)}, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
