#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    NON_AUTHORITY_SCOPE,
    atomic_write_idempotent_validated_json_v1,
    build_source_dependency_row_v1,
    canonical_paper_session_id_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    repo_git_sha_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_market_calendar_record_v1,
    resolve_paper_trading_posture_path,
)


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_paper_trading_posture_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    session_id = canonical_paper_session_id_v1(day_utc)
    produced_at_utc = now_utc_iso_v1()
    calendar_state = resolve_market_calendar_record_v1(truth_root=truth_root, day_utc=day_utc)
    reason_code = str(calendar_state["reason_code"])
    source_dependencies = [
        build_source_dependency_row_v1(
            logical_name="market_calendar_manifest",
            absolute_path=calendar_state["manifest_path"],
            status=str(calendar_state["status"]),
            reason_codes=[reason_code],
            producer="market_calendar_v1",
            day_utc=day_utc,
        ),
        build_source_dependency_row_v1(
            logical_name="market_calendar_year_file",
            absolute_path=calendar_state.get("year_path"),
            status=str(calendar_state["status"]),
            reason_codes=[reason_code],
            producer="market_calendar_v1",
            day_utc=day_utc,
        ),
    ]

    blocking_codes: List[str] = []
    posture_status = "UNKNOWN"
    posture_class = "PAPER_POSTURE_UNKNOWN"
    blocking_family = "INPUT_INVALID"
    expected_no_op_today = False
    system_ready = False
    policy_reasons: List[str] = []
    freshness_verdict = "UNKNOWN"
    linkage_verdict = "UNLINKED"

    if str(calendar_state["status"]) == "OK":
        record = calendar_state["record"]
        assert isinstance(record, dict)
        if bool(record.get("is_trading_session")):
            posture_status = "ENABLED"
            posture_class = "PAPER_READY_ACTIVE"
            blocking_family = "NONE"
            system_ready = True
            expected_no_op_today = False
            policy_reasons = ["MARKET_CALENDAR_TRADING_SESSION"]
            freshness_verdict = "CURRENT"
            linkage_verdict = "LINKED"
        else:
            posture_status = "DISABLED"
            posture_class = "PAPER_READY_NO_OP"
            blocking_family = "EXPECTED_NO_OP"
            system_ready = False
            expected_no_op_today = True
            policy_reasons = ["MARKET_CALENDAR_NON_TRADING_SESSION"]
            blocking_codes = ["MARKET_CALENDAR_NON_TRADING_SESSION"]
            freshness_verdict = "CURRENT"
            linkage_verdict = "LINKED"
    else:
        reason = reason_code
        policy_reasons = [reason]
        blocking_codes = [reason]
        if str(calendar_state["status"]) == "MISSING":
            posture_status = "UNKNOWN"
            freshness_verdict = "UNKNOWN"
        else:
            posture_status = "MALFORMED"
            freshness_verdict = "UNKNOWN"
        linkage_verdict = "UNLINKED"

    payload: Dict[str, Any] = {
        "schema_id": "paper_trading_posture",
        "schema_version": "v1",
        "authority_scope": NON_AUTHORITY_SCOPE,
        "day_utc": day_utc,
        "session_id": session_id,
        "system_ready": bool(system_ready),
        "posture_status": posture_status,
        "posture_class": posture_class,
        "blocking_family": blocking_family,
        "expected_no_op_today": bool(expected_no_op_today),
        "policy_reasons": sorted(set(policy_reasons)),
        "blocking_codes": sorted(set(blocking_codes)),
        "blocking_reason_codes": sorted(set(blocking_codes)),
        "source_dependencies": source_dependencies,
        "producer": producer_block_v1(module="ops/tools/run_paper_trading_posture_v1.py", git_sha=repo_git_sha_v1()),
        "produced_at_utc": produced_at_utc,
        "freshness_verdict": freshness_verdict,
        "linkage_verdict": linkage_verdict,
    }
    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_paper_trading_posture_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trading_posture.v1.schema.json",
        volatile_field_names=("produced_at_utc",),
    )
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "posture_status": posture_status}, sort_keys=True))
    return 0 if posture_status in {"ENABLED", "DISABLED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
