#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.data_registry_v1 import build_data_registry_v1, write_data_registry_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.market_data.symbol_map_v1 import build_runtime_symbol_universe_v1
from ops.aegis.market_data_inputs_v1 import build_market_data_inputs_v1, write_market_data_inputs_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT, build_runtime_truth_kernel_v1, write_runtime_truth_kernel_reports_v1
from ops.aegis.sleeve_input_contracts_v1 import build_sleeve_input_contracts_v1, write_sleeve_input_contracts_v1
from ops.aegis.sleeve_readiness_v1 import build_sleeve_readiness_v1, write_sleeve_readiness_v1
from ops.tools.build_aegis_audit_bundle_v1 import build_aegis_audit_bundle_v1
from ops.tools.replay_aegis_runtime_v1 import replay_aegis_runtime_v1
from ops.tools.run_sleeve_evaluation_kernel_v1 import build_sleeve_evaluation_kernel

REPORT_FAMILY = "readiness_burnin_v1"
DEFAULT_DAYS = ("2026-05-20", "2026-05-21")


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_run_id(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)


def run_day_v1(*, truth_root: Path, day_utc: str, generated_at_utc: str, environment: str) -> dict[str, Any]:
    row: dict[str, Any] = {
        "day_utc": day_utc,
        "status": "UNKNOWN",
        "steps": {},
        "remaining_blockers": [],
        "optional_warnings": [],
        "authority_hash_consistency": "UNKNOWN",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }
    try:
        contracts = build_sleeve_input_contracts_v1(repo_root=REPO_ROOT, day_utc=day_utc)
        contract_paths = write_sleeve_input_contracts_v1(truth_root=truth_root, day_utc=day_utc, payload=contracts)
        row["steps"]["sleeve_input_contracts"] = {"status": "PASS", **contract_paths}

        market_inputs = build_market_data_inputs_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
        market_paths = write_market_data_inputs_v1(truth_root=truth_root, day_utc=day_utc, payload=market_inputs)
        row["steps"]["market_data_inputs"] = {"status": market_inputs.get("status"), **market_paths}

        universe = build_runtime_symbol_universe_v1(repo_root=REPO_ROOT)
        registry = build_data_registry_v1(truth_root=truth_root, day_utc=day_utc, symbols=universe.get("requested_symbols") or [], universe_metadata=universe)
        registry_paths = write_data_registry_v1(truth_root=truth_root, day_utc=day_utc, payload=registry)
        row["steps"]["data_registry"] = {"status": "PASS", **registry_paths}

        readiness = build_sleeve_readiness_v1(truth_root=truth_root, day_utc=day_utc)
        readiness_paths = write_sleeve_readiness_v1(truth_root=truth_root, day_utc=day_utc, payload=readiness)
        row["steps"]["sleeve_readiness"] = {"status": "PASS", **readiness_paths}

        sleeve_eval = build_sleeve_evaluation_kernel(
            day_utc=day_utc,
            truth_root=truth_root,
            environment=environment,
            readiness_path=readiness_paths["json"],
            allow_deprecated_symbol_fallback=False,
        )
        row["steps"]["sleeve_evaluation_kernel"] = {"status": sleeve_eval.get("status"), "path": sleeve_eval.get("artifact_path"), "canonical_blocker": sleeve_eval.get("canonical_blocker")}

        runtime = build_runtime_truth_kernel_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
        runtime_paths = write_runtime_truth_kernel_reports_v1(truth_root=truth_root, payload=runtime)
        runtime_hash = str(runtime.get("runtime_evaluation_hash") or "")
        row["steps"]["runtime_truth_kernel"] = {"status": runtime.get("runtime_truth_classification"), **runtime_paths}

        audit = build_aegis_audit_bundle_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            run_id=f"readiness-burnin:{day_utc}:{generated_at_utc}",
            parent_run_id="",
            generated_at_utc=generated_at_utc,
            command_args=["run_aegis_readiness_burnin_v1", "--day", day_utc],
        )
        replay = replay_aegis_runtime_v1(audit_bundle=Path(str(audit["bundle_path"])))
        row["steps"]["audit_bundle"] = audit
        row["steps"]["replay"] = replay

        row.update(
            {
                "status": "PASS" if replay.get("ok") else "FAIL",
                "runtime_evaluation_hash": runtime_hash,
                "replay_status": replay.get("status"),
                "market_readiness_status": market_inputs.get("status"),
                "sleeve_readiness_status": "BLOCKED" if readiness.get("blocked_count") else ("READY_WITH_WARNINGS" if readiness.get("ready_with_warnings_count") else "READY"),
                "remaining_blockers": list(registry.get("blocking_items") or []) + list(runtime.get("blocked_capabilities") or []),
                "optional_warnings": list(registry.get("warning_items") or []),
                "authority_hash_consistency": "MATCH" if runtime_hash and runtime_hash == audit.get("runtime_evaluation_hash") == replay.get("runtime_evaluation_hash") else "MISMATCH",
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            }
        )
    except Exception as exc:  # noqa: BLE001
        row["status"] = "FAIL"
        row["error"] = f"{type(exc).__name__}: {exc}"
    return row


def render_report_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS READINESS BURN-IN v1",
        f"generated_at_utc: {payload.get('generated_at_utc')}",
        f"truth_root: {payload.get('truth_root')}",
        f"overall_status: {payload.get('status')}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
    ]
    for row in payload.get("days") or []:
        remaining = ",".join(row.get("remaining_blockers") or [])
        warnings = ",".join(row.get("optional_warnings") or [])
        lines.extend(
            [
                f"day_utc: {row.get('day_utc')}",
                f"  status: {row.get('status')}",
                f"  runtime_evaluation_hash: {row.get('runtime_evaluation_hash') or ''}",
                f"  replay_status: {row.get('replay_status') or ''}",
                f"  market_readiness_status: {row.get('market_readiness_status') or ''}",
                f"  sleeve_readiness_status: {row.get('sleeve_readiness_status') or ''}",
                f"  remaining_blockers: {remaining}",
                f"  optional_warnings: {warnings}",
                f"  authority_hash_consistency: {row.get('authority_hash_consistency') or ''}",
            ]
        )
        if row.get("error"):
            lines.append(f"  error: {row.get('error')}")
    return "\n".join(lines) + "\n"

def write_burnin_report_v1(*, truth_root: Path, payload: dict[str, Any], run_id: str) -> dict[str, str]:
    out_dir = truth_root / "reports" / REPORT_FAMILY / safe_run_id(run_id)
    json_path = write_json_v1(out_dir / "readiness_burnin.v1.json", payload)
    txt_path = out_dir / "readiness_burnin.v1.txt"
    txt_path.write_text(render_report_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "txt": str(txt_path)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_readiness_burnin_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--days", default=",".join(DEFAULT_DAYS))
    parser.add_argument("--generated-at-utc", dest="generated_at_utc", default="")
    parser.add_argument("--environment", default="PAPER")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    generated_at = args.generated_at_utc or utc_now_v1()
    days = [day.strip() for day in str(args.days or "").split(",") if day.strip()]
    results = [run_day_v1(truth_root=truth_root, day_utc=day, generated_at_utc=generated_at, environment=str(args.environment or "PAPER").upper()) for day in days]
    status = "PASS" if results and all(row.get("status") == "PASS" for row in results) else "FAIL"
    payload = {
        "schema_id": "readiness_burnin",
        "schema_version": "v1",
        "artifact_id": "readiness_burnin_v1",
        "generated_at_utc": generated_at,
        "truth_root": str(truth_root),
        "days_requested": days,
        "status": status,
        "days": results,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }
    paths = write_burnin_report_v1(truth_root=truth_root, payload=payload, run_id=f"{generated_at}:{'-'.join(days)}")
    print(json.dumps({**paths, "status": status, "day_count": len(results)}, sort_keys=True))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
