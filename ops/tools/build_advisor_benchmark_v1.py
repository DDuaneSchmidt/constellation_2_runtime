#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _num(value: str) -> float:
    return float(str(value).strip().replace("%", ""))


def _fmt(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_advisor_benchmark_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--benchmark_name", required=True)
    parser.add_argument("--period_start", required=True)
    parser.add_argument("--period_end", required=True)
    parser.add_argument("--gross_return", required=True)
    parser.add_argument("--fee_rate", required=True)
    parser.add_argument("--aegis_return", required=True)
    parser.add_argument("--notes", default="")
    args = parser.parse_args(argv)

    gross = _num(args.gross_return)
    fee = _num(args.fee_rate)
    aegis = _num(args.aegis_return)
    net = gross - fee
    payload = {
        "schema_id": "advisor_benchmark",
        "schema_version": "v1",
        "artifact_id": "advisor_benchmark_v1",
        "generated_at_utc": _now(),
        "benchmark_name": args.benchmark_name,
        "period_start": args.period_start,
        "period_end": args.period_end,
        "gross_return": _fmt(gross),
        "fee_rate": _fmt(fee),
        "net_return": _fmt(net),
        "aegis_return": _fmt(aegis),
        "difference": _fmt(aegis - net),
        "fee_drag": _fmt(fee),
        "notes": args.notes,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    root = Path(args.truth_root).expanduser().resolve()
    path = root / "reports" / "advisor_benchmark_v1" / args.period_end / _safe(args.benchmark_name) / "advisor_benchmark.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    print(json.dumps({"path": str(path), "advisor_net_return": payload["net_return"], "aegis_return": payload["aegis_return"], "difference": payload["difference"]}, sort_keys=True))
    return 0


def _safe(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value) or "advisor"


if __name__ == "__main__":
    raise SystemExit(main())
