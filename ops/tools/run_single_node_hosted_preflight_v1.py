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

from constellation_2.common.single_node_hosted_deployment_v1 import (  # noqa: E402
    derive_single_node_hosted_preflight_payload_v1,
    write_single_node_hosted_preflight_receipt_v1,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_single_node_hosted_preflight_v1")
    ap.add_argument("--entrypoint_name", required=True)
    ap.add_argument("--entrypoint_path", required=True)
    ap.add_argument("--service_name", required=True)
    ap.add_argument("--requested_python", default="")
    ap.add_argument("--required_module", action="append", default=[])
    args = ap.parse_args(argv)

    payload = derive_single_node_hosted_preflight_payload_v1(
        repo_root=REPO_ROOT,
        entrypoint_name=str(args.entrypoint_name).strip(),
        entrypoint_path=Path(str(args.entrypoint_path).strip()),
        service_name=str(args.service_name).strip(),
        requested_python=str(args.requested_python or "").strip(),
        required_python_modules=[str(item).strip() for item in list(args.required_module or [])],
    )
    receipt_path = write_single_node_hosted_preflight_receipt_v1(
        repo_root=REPO_ROOT,
        payload=payload,
    )
    print(
        json.dumps(
            {
                "path": str(receipt_path),
                "preflight_id": str(payload.get("preflight_id") or "").strip(),
                "status": str(payload.get("status") or "").strip(),
                "resolved_python_executable": str(payload.get("resolved_python_executable") or "").strip(),
                "blocking_codes": list(payload.get("blocking_codes") or []),
            },
            sort_keys=True,
        )
    )
    return 0 if str(payload.get("status") or "").strip().upper() == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
