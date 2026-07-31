#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.vix_drop_validation_v1 import expected_vix_drop_path_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="create_aegis_vix_template_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    day_utc = str(args.day_utc)
    vix_path = expected_vix_drop_path_v1(day_utc=day_utc)
    template_path = vix_path.with_name("vix.csv.template")
    template_path.parent.mkdir(parents=True, exist_ok=True)
    template_path.write_text(
        "day_utc,vix_level,source,timestamp_utc\n"
        f"{day_utc},18.25,MANUAL_OPERATOR_DROP,{day_utc}T20:00:00Z\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "template_path": str(template_path),
        "target_vix_csv_path": str(vix_path),
        "created": True,
        "certified_vix_created": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
