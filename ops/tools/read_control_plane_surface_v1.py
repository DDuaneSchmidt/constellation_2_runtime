#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.control_plane_read_gateway_v1 import (  # noqa: E402
    gateway_metadata_v1,
    read_control_plane_collection_v1,
    read_control_plane_surface_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="read_control_plane_surface_v1")
    parser.add_argument("--domain", required=True)
    parser.add_argument("--surface", required=True)
    parser.add_argument("--truth_root")
    parser.add_argument("--truth_sleeves_root")
    parser.add_argument("--day_utc")
    parser.add_argument("--sleeve_id", default="PRIMARY")
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--ib_account", default="")
    parser.add_argument("--mode", choices=("surface", "collection"), default="surface")
    args = parser.parse_args(argv)

    try:
        if args.mode == "collection":
            collection = read_control_plane_collection_v1(
                domain=args.domain,
                surface=args.surface,
                truth_root=args.truth_root,
                day_utc=args.day_utc,
            )
            payload = {
                "gateway": gateway_metadata_v1(collection),
                "rows": [
                    {
                        "domain": ref.domain,
                        "surface": ref.surface,
                        "read_kind": ref.read_kind,
                        "path": str(ref.path),
                        "sha256": ref.sha256,
                        "schema_relpath": ref.schema_relpath,
                        "metadata": dict(ref.metadata),
                        "payload": ref.payload,
                    }
                    for ref in collection.refs
                ],
            }
        else:
            ref = read_control_plane_surface_v1(
                domain=args.domain,
                surface=args.surface,
                truth_root=args.truth_root,
                truth_sleeves_root=args.truth_sleeves_root,
                day_utc=args.day_utc,
                sleeve_id=args.sleeve_id,
                environment=args.environment,
                ib_account=args.ib_account,
            )
            payload = {
                "gateway": gateway_metadata_v1(ref),
                "path": str(ref.path),
                "sha256": ref.sha256,
                "schema_relpath": ref.schema_relpath,
                "metadata": dict(ref.metadata),
                "payload": ref.payload,
            }
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": f"{type(exc).__name__}:{exc}",
                    "domain": args.domain,
                    "surface": args.surface,
                    "mode": args.mode,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 1

    print(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
