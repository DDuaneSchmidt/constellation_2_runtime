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

from constellation_2.common.runtime_identity_v1 import (  # noqa: E402
    derive_runtime_startup_identity_payload,
    write_runtime_startup_identity_receipt_v1,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_runtime_startup_identity_v1")
    ap.add_argument("--entrypoint_name", required=True)
    ap.add_argument("--entrypoint_path", required=True)
    ap.add_argument("--service_name", default="")
    ap.add_argument("--requested_python", required=True)
    ap.add_argument("--resolved_python_executable", default="")
    args = ap.parse_args(argv)

    payload = derive_runtime_startup_identity_payload(
        repo_root=REPO_ROOT,
        entrypoint_name=str(args.entrypoint_name).strip(),
        entrypoint_path=Path(str(args.entrypoint_path).strip()),
        service_name=str(args.service_name).strip(),
        requested_python=str(args.requested_python).strip(),
        resolved_python_executable=str(args.resolved_python_executable).strip(),
    )
    path = write_runtime_startup_identity_receipt_v1(repo_root=REPO_ROOT, payload=payload)
    print(
        json.dumps(
            {
                "path": str(path),
                "startup_id": str(payload.get("startup_id") or "").strip(),
                "release_id": str(
                    ((payload.get("runtime_identity") or {}).get("release_id") or "")
                ).strip(),
                "runtime_environment": str(
                    ((payload.get("runtime_identity") or {}).get("runtime_environment") or "")
                ).strip(),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
