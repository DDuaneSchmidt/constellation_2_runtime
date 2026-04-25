#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.next_day_readiness_consistency_gate_v1 import (  # noqa: E402
    CONSISTENCY_GATE_STATUS_PASS,
    evaluate_next_day_readiness_consistency_gate_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (  # noqa: E402
    parse_day_utc_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1  # noqa: E402


LIVE_ONLY_BLOCKER_CODES = {
    "TARGET_DAY_DATE_MISMATCH",
    "IB_API_HANDSHAKE_NOT_OK",
    "BROKER_EVENTS_MISSING",
    "C2_KILL_SWITCH_DEFAULT_ACTIVE_MISSING_INPUTS",
}


def _read_json(path: Path) -> Dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def classify_rollover_readiness_v1(*, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    root = Path(truth_root).resolve()
    day = parse_day_utc_v1(day_utc)

    consistency = evaluate_next_day_readiness_consistency_gate_v1(
        truth_root=root,
        day_utc=day,
    )
    consistency_codes = [str(code).strip() for code in consistency.blocking_reason_codes if str(code).strip()]

    pre_open_path = (
        root / "reports" / "pre_open_bundle_v1" / day / "pre_open_bundle.v1.json"
    ).resolve()
    pre_open_payload = _read_json(pre_open_path)
    pre_open_state = ""
    pre_open_codes: list[str] = []
    if isinstance(pre_open_payload, dict):
        pre_open_state = str(pre_open_payload.get("materialization_state") or "").strip().upper()
        pre_open_codes = sorted(
            {
                str(code).strip()
                for code in (pre_open_payload.get("reason_codes") or [])
                if str(code).strip()
            }
        )

    if consistency.status != CONSISTENCY_GATE_STATUS_PASS:
        classification = "NOT_READY_STRUCTURAL"
        reason_codes = sorted(set(consistency_codes or ["CONSISTENCY_GATE_FAILURE"]))
    elif pre_open_state == "BLOCKED" and pre_open_codes:
        if set(pre_open_codes).issubset(LIVE_ONLY_BLOCKER_CODES):
            classification = "AWAITING_TOMORROW_LIVE_ONLY"
        else:
            classification = "NOT_READY_STRUCTURAL"
        reason_codes = pre_open_codes
    else:
        classification = "STRUCTURALLY_READY_TONIGHT"
        reason_codes = []

    return {
        "day_utc": day,
        "truth_root": str(root),
        "classification": classification,
        "consistency_gate_status": consistency.status,
        "consistency_gate_reason_codes": consistency_codes,
        "pre_open_bundle_path": str(pre_open_path),
        "pre_open_materialization_state": pre_open_state,
        "pre_open_reason_codes": pre_open_codes,
        "reason_codes": reason_codes,
    }


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    ap = argparse.ArgumentParser(prog="run_rollover_readiness_check_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = classify_rollover_readiness_v1(truth_root=truth_root, day_utc=day_utc)
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["classification"] != "NOT_READY_STRUCTURAL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
