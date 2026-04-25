#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_lifecycle_v1 import (  # noqa: E402
    record_runtime_lifecycle_stop_v1,
    record_runtime_lifecycle_start_v1,
    release_runtime_lifecycle_active_state_v1,
    request_runtime_lifecycle_admission_v1,
)


def _common_entrypoint_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--entrypoint_name", required=True)
    parser.add_argument("--entrypoint_path", required=True)
    parser.add_argument("--service_name", default="")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_runtime_lifecycle_v1")
    subparsers = parser.add_subparsers(dest="command", required=True)

    admit = subparsers.add_parser("admit")
    _common_entrypoint_args(admit)
    admit.add_argument("--launcher_pid", required=True)

    record = subparsers.add_parser("record-start")
    _common_entrypoint_args(record)
    record.add_argument("--launcher_pid", required=True)
    record.add_argument("--run_id", required=True)
    record.add_argument("--startup_id", required=True)
    record.add_argument("--startup_receipt_path", required=True)

    stop = subparsers.add_parser("record-stop")
    _common_entrypoint_args(stop)
    stop.add_argument("--launcher_pid", required=True)
    stop.add_argument("--run_id", required=True)
    stop.add_argument("--launch_phase", required=True)
    stop.add_argument("--wrapper_exit_code", required=True)
    stop.add_argument("--termination_signal", default="")

    release = subparsers.add_parser("release")
    release.add_argument("--entrypoint_name", required=True)
    release.add_argument("--service_name", default="")
    release.add_argument("--run_id", required=True)

    args = parser.parse_args(argv)

    if args.command == "admit":
        result = request_runtime_lifecycle_admission_v1(
            repo_root=REPO_ROOT,
            entrypoint_name=str(args.entrypoint_name).strip(),
            entrypoint_path=Path(str(args.entrypoint_path).strip()),
            service_name=str(args.service_name).strip(),
            launcher_pid=int(str(args.launcher_pid).strip()),
        )
        print(json.dumps(result, sort_keys=True))
        return 0 if result.get("admission_decision") == "ADMITTED" else 2

    if args.command == "record-start":
        receipt_path = record_runtime_lifecycle_start_v1(
            repo_root=REPO_ROOT,
            entrypoint_name=str(args.entrypoint_name).strip(),
            entrypoint_path=Path(str(args.entrypoint_path).strip()),
            service_name=str(args.service_name).strip(),
            launcher_pid=int(str(args.launcher_pid).strip()),
            run_id=str(args.run_id).strip(),
            startup_id=str(args.startup_id).strip(),
            startup_receipt_path=Path(str(args.startup_receipt_path).strip()),
        )
        print(json.dumps({"path": str(receipt_path), "run_id": str(args.run_id).strip()}, sort_keys=True))
        return 0

    if args.command == "record-stop":
        receipt_path = record_runtime_lifecycle_stop_v1(
            repo_root=REPO_ROOT,
            entrypoint_name=str(args.entrypoint_name).strip(),
            entrypoint_path=Path(str(args.entrypoint_path).strip()),
            service_name=str(args.service_name).strip(),
            launcher_pid=int(str(args.launcher_pid).strip()),
            run_id=str(args.run_id).strip(),
            launch_phase=str(args.launch_phase).strip(),
            wrapper_exit_code=int(str(args.wrapper_exit_code).strip()),
            termination_signal=str(args.termination_signal).strip(),
        )
        print(json.dumps({"path": str(receipt_path), "run_id": str(args.run_id).strip()}, sort_keys=True))
        return 0

    result = release_runtime_lifecycle_active_state_v1(
        repo_root=REPO_ROOT,
        entrypoint_name=str(args.entrypoint_name).strip(),
        service_name=str(args.service_name).strip(),
        run_id=str(args.run_id).strip(),
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
