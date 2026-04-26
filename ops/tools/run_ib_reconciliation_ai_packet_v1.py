#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.ib_reconciliation.ai_exception_packet_v1 import (  # noqa: E402
    build_ai_exception_packet_v1,
    build_alert_candidate_v1,
    render_ai_exception_packet_markdown_v1,
)
from constellation_2.ib_reconciliation.paths_v1 import (  # noqa: E402
    alert_candidate_path_v1,
    ensure_runtime_layout_v1,
    runtime_artifact_path_v1,
    validate_day_utc_v1,
)
from constellation_2.ib_reconciliation.schema_v1 import (  # noqa: E402
    read_json_object_v1,
    write_deterministic_json_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="run_ib_reconciliation_ai_packet_v1",
        description="Generate AI exception review packet from deterministic IB reconciliation results.",
    )
    parser.add_argument("--day_utc", required=True, help="UTC reconciliation day (YYYY-MM-DD).")
    args = parser.parse_args()

    day_utc = validate_day_utc_v1(args.day_utc)
    ensure_runtime_layout_v1(day_utc)

    reconciliation_path = runtime_artifact_path_v1("reconciliations", day_utc, "ib_reconciliation.v1.json")
    if not reconciliation_path.exists():
        raise SystemExit(f"FAIL: missing reconciliation artifact: {reconciliation_path}")
    reconciliation_payload = read_json_object_v1(reconciliation_path)

    packet_payload = build_ai_exception_packet_v1(
        day_utc=day_utc,
        reconciliation_payload=reconciliation_payload,
        reconciliation_path=str(reconciliation_path),
    )
    packet_md = render_ai_exception_packet_markdown_v1(packet_payload)

    packet_json_path = runtime_artifact_path_v1("ai_reviews", day_utc, "ib_reconciliation_ai_packet.v1.json")
    packet_md_path = runtime_artifact_path_v1("ai_reviews", day_utc, "ib_reconciliation_ai_packet.md")
    write_deterministic_json_v1(packet_json_path, packet_payload)
    packet_md_path.write_text(packet_md, encoding="utf-8")

    ai_review_path = runtime_artifact_path_v1("ai_reviews", day_utc, "ib_reconciliation_ai_review.v1.json")
    ai_review_payload = read_json_object_v1(ai_review_path) if ai_review_path.exists() else None

    alert_candidate = build_alert_candidate_v1(
        day_utc=day_utc,
        reconciliation_payload=reconciliation_payload,
        ai_review_payload=ai_review_payload,
    )
    alert_path = None
    if alert_candidate is not None:
        alert_path = alert_candidate_path_v1()
        write_deterministic_json_v1(alert_path, alert_candidate)

    print(
        json.dumps(
            {
                "status": "OK",
                "day_utc": day_utc,
                "packet_json_path": str(packet_json_path),
                "packet_md_path": str(packet_md_path),
                "alert_candidate_path": str(alert_path) if alert_path else None,
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
